//! Multi-raft executor read source.
//!
//! Wraps the local `StorageEngine` with partition-aware leader
//! forwarding. Two modes:
//!
//! * **Local (relaxed)** — point reads hit the local store directly,
//!   matching the historical multi-raft behaviour. Cheap and fast,
//!   but a follower replica may be slightly behind the partition
//!   leader's commit index, so a very recent write can be missed.
//! * **Linearizable** — point reads route to the partition leader,
//!   which gates on `Raft::ensure_linearizable` before answering.
//!   Every committed write that landed before the read started is
//!   guaranteed visible. Costs one quorum round trip on the leader
//!   and one extra gRPC hop when the local peer isn't the leader.
//!
//! Scatter-gather reads (`all_node_ids`, `nodes_by_label`,
//! `nodes_by_property`, `edges_by_property`, `get_edge`) fall back to
//! the local store in both modes. Linearizable scatter-gather
//! requires fanning out to every partition leader and unioning the
//! results — that's a v3 follow-up and is gated behind a comment in
//! the trait impl below. The point-read path is what actually
//! matters for the typical workloads (Bolt sessions, MERGE,
//! property-by-id traversals) so end-to-end linearizability ships
//! with the v2 surface and the gap is documented.

use crate::cluster_auth::ClusterAuth;
use crate::convert::{node_from_proto, uuid_from_proto, uuid_to_proto};
use crate::proto::mesh_query_client::MeshQueryClient;
use crate::proto::{GetNodeRequest, NeighborRequest};
use crate::MultiRaftCluster;
use meshdb_cluster::{PartitionId, Partitioner, PeerId};
use meshdb_core::{Edge, EdgeId, Node, NodeId, Property};
use meshdb_executor::{Error as ExecError, GraphReader, Result as ExecResult};
use meshdb_storage::StorageEngine;
use std::sync::Arc;
use tokio::runtime::Handle;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

/// Executor read source for `mode = "multi-raft"`. See module docs
/// for the linearizable vs. relaxed split.
///
/// Like other clustered readers, this bridges the sync `GraphReader`
/// trait onto async gRPC via `Handle::block_on` — callers MUST
/// invoke the executor inside `tokio::task::spawn_blocking`.
pub struct MultiRaftGraphReader {
    local: Arc<dyn StorageEngine>,
    multi_raft: Arc<MultiRaftCluster>,
    handle: Handle,
    linearizable: bool,
    client_tls: Option<ClientTlsConfig>,
    cluster_auth: ClusterAuth,
}

impl MultiRaftGraphReader {
    pub fn new(
        local: Arc<dyn StorageEngine>,
        multi_raft: Arc<MultiRaftCluster>,
        linearizable: bool,
        client_tls: Option<ClientTlsConfig>,
        cluster_auth: ClusterAuth,
    ) -> Self {
        Self {
            local,
            multi_raft,
            handle: Handle::current(),
            linearizable,
            client_tls,
            cluster_auth,
        }
    }

    fn block<F, T>(&self, fut: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        self.handle.block_on(fut)
    }

    fn partition_for(&self, id: NodeId) -> PartitionId {
        let p = Partitioner::new(self.multi_raft.replica_map.num_partitions());
        p.partition_for(id)
    }

    fn is_local_leader(&self, partition: PartitionId) -> bool {
        matches!(self.multi_raft.leader_of(partition), Some(l) if l == self.multi_raft.self_id)
    }

    async fn fence_local(&self, partition: PartitionId) -> ExecResult<()> {
        self.multi_raft
            .ensure_partition_linearizable(partition)
            .await
            .map_err(|e| {
                ExecError::Remote(format!(
                    "ensure_linearizable on partition {}: {e}",
                    partition.0
                ))
            })
    }

    /// Build a fresh leader-bound `MeshQueryClient`. Channels are
    /// not pooled here because partition leadership can shift between
    /// reads; pooling would require leader-cache invalidation hooks
    /// and rebuilding on miss anyway. The cost is one `connect_lazy`
    /// per linearizable forward — connect_lazy returns immediately,
    /// the actual TCP / TLS handshake amortises across subsequent
    /// requests on the same channel.
    async fn leader_client(&self, partition: PartitionId) -> ExecResult<MeshQueryClient<Channel>> {
        let leader_id = self.multi_raft.leader_of(partition).ok_or_else(|| {
            ExecError::Remote(format!("no known leader for partition {}", partition.0))
        })?;
        let state = self.multi_raft.meta.current_state().await;
        let addr = state
            .membership
            .address(PeerId(leader_id))
            .ok_or_else(|| ExecError::Remote(format!("no address for leader peer {}", leader_id)))?
            .to_string();
        drop(state);
        let scheme = if self.client_tls.is_some() {
            "https"
        } else {
            "http"
        };
        let uri = format!("{scheme}://{addr}");
        let mut endpoint = Endpoint::from_shared(uri)
            .map_err(|e| ExecError::Remote(format!("invalid peer address {addr}: {e}")))?;
        if let Some(tls) = self.client_tls.clone() {
            endpoint = endpoint
                .tls_config(tls)
                .map_err(|e| ExecError::Remote(format!("tls config: {e}")))?;
        }
        Ok(MeshQueryClient::new(endpoint.connect_lazy()))
    }

    /// Build a `tonic::Request` with cluster-auth bearer token
    /// injected when configured. Reused by the three forwarding
    /// helpers below.
    fn auth_request<T>(&self, body: T) -> ExecResult<Request<T>> {
        let mut probe: Request<()> = Request::new(());
        self.cluster_auth
            .inject(&mut probe)
            .map_err(|e: Status| ExecError::Remote(format!("auth inject: {e}")))?;
        let mut req = Request::new(body);
        for kv in probe.metadata().clone().into_headers().iter() {
            // Copy over any header the auth injector added.
            if let Ok(name) = tonic::metadata::MetadataKey::from_bytes(kv.0.as_ref()) {
                if let Ok(value) =
                    tonic::metadata::MetadataValue::try_from(kv.1.to_str().unwrap_or(""))
                {
                    req.metadata_mut().insert(name, value);
                }
            }
        }
        Ok(req)
    }
}

impl GraphReader for MultiRaftGraphReader {
    fn get_node(&self, id: NodeId) -> ExecResult<Option<Node>> {
        if !self.linearizable {
            return Ok(self.local.get_node(id)?);
        }
        let partition = self.partition_for(id);
        if self.is_local_leader(partition) {
            self.block(self.fence_local(partition))?;
            return Ok(self.local.get_node(id)?);
        }
        let id_proto = uuid_to_proto(id.as_uuid());
        let resp = self.block(async {
            let mut client = self.leader_client(partition).await?;
            let req = self.auth_request(GetNodeRequest {
                id: Some(id_proto),
                local_only: true,
                linearizable: true,
            })?;
            client
                .get_node(req)
                .await
                .map_err(|e| ExecError::Remote(format!("forward get_node: {e}")))
        })?;
        let inner = resp.into_inner();
        if !inner.found {
            return Ok(None);
        }
        let node = node_from_proto(
            inner
                .node
                .ok_or_else(|| ExecError::Remote("missing node in response".into()))?,
        )
        .map_err(|e| ExecError::Remote(e.to_string()))?;
        Ok(Some(node))
    }

    fn get_edge(&self, id: EdgeId) -> ExecResult<Option<Edge>> {
        // Edges may live on two partitions (source-owner + target-
        // owner). Linearizable scatter-gather across both is v3 work
        // (would require fencing on each partition's leader and
        // fanning out the request); for now we read locally.
        Ok(self.local.get_edge(id)?)
    }

    fn all_node_ids(&self) -> ExecResult<Vec<NodeId>> {
        Ok(self.local.all_node_ids()?)
    }

    fn nodes_by_label(&self, label: &str) -> ExecResult<Vec<NodeId>> {
        Ok(self.local.nodes_by_label(label)?)
    }

    fn outgoing(&self, id: NodeId) -> ExecResult<Vec<(EdgeId, NodeId)>> {
        if !self.linearizable {
            return Ok(self.local.outgoing(id)?);
        }
        let partition = self.partition_for(id);
        if self.is_local_leader(partition) {
            self.block(self.fence_local(partition))?;
            return Ok(self.local.outgoing(id)?);
        }
        let id_proto = uuid_to_proto(id.as_uuid());
        let resp = self.block(async {
            let mut client = self.leader_client(partition).await?;
            let req = self.auth_request(NeighborRequest {
                node_id: Some(id_proto),
                local_only: true,
                linearizable: true,
            })?;
            client
                .outgoing(req)
                .await
                .map_err(|e| ExecError::Remote(format!("forward outgoing: {e}")))
        })?;
        neighbors_from_proto(resp.into_inner().neighbors)
    }

    fn incoming(&self, id: NodeId) -> ExecResult<Vec<(EdgeId, NodeId)>> {
        if !self.linearizable {
            return Ok(self.local.incoming(id)?);
        }
        let partition = self.partition_for(id);
        if self.is_local_leader(partition) {
            self.block(self.fence_local(partition))?;
            return Ok(self.local.incoming(id)?);
        }
        let id_proto = uuid_to_proto(id.as_uuid());
        let resp = self.block(async {
            let mut client = self.leader_client(partition).await?;
            let req = self.auth_request(NeighborRequest {
                node_id: Some(id_proto),
                local_only: true,
                linearizable: true,
            })?;
            client
                .incoming(req)
                .await
                .map_err(|e| ExecError::Remote(format!("forward incoming: {e}")))
        })?;
        neighbors_from_proto(resp.into_inner().neighbors)
    }

    fn list_property_indexes(&self) -> ExecResult<Vec<(String, Vec<String>)>> {
        Ok(self
            .local
            .list_property_indexes()
            .into_iter()
            .map(|s| (s.label, s.properties))
            .collect())
    }

    fn list_edge_property_indexes(&self) -> ExecResult<Vec<(String, Vec<String>)>> {
        Ok(self
            .local
            .list_edge_property_indexes()
            .into_iter()
            .map(|s| (s.edge_type, s.properties))
            .collect())
    }

    fn list_point_indexes(&self) -> ExecResult<Vec<(String, String)>> {
        Ok(self
            .local
            .list_point_indexes()
            .into_iter()
            .map(|s| (s.label, s.property))
            .collect())
    }

    fn list_edge_point_indexes(&self) -> ExecResult<Vec<(String, String)>> {
        Ok(self
            .local
            .list_edge_point_indexes()
            .into_iter()
            .map(|s| (s.edge_type, s.property))
            .collect())
    }

    fn list_property_constraints(&self) -> ExecResult<Vec<meshdb_storage::PropertyConstraintSpec>> {
        Ok(self.local.list_property_constraints())
    }

    fn nodes_by_property(
        &self,
        label: &str,
        property: &str,
        value: &Property,
    ) -> ExecResult<Vec<NodeId>> {
        // See `get_edge` — scatter-gather linearizable is v3.
        Ok(self.local.nodes_by_property(label, property, value)?)
    }

    fn edges_by_property(
        &self,
        edge_type: &str,
        property: &str,
        value: &Property,
    ) -> ExecResult<Vec<EdgeId>> {
        Ok(self.local.edges_by_property(edge_type, property, value)?)
    }
}

fn neighbors_from_proto(
    neighbors: Vec<crate::proto::NeighborInfo>,
) -> ExecResult<Vec<(EdgeId, NodeId)>> {
    let mut out = Vec::with_capacity(neighbors.len());
    for n in neighbors {
        let edge_proto = n
            .edge_id
            .ok_or_else(|| ExecError::Remote("missing edge_id".into()))?;
        let node_proto = n
            .neighbor_id
            .ok_or_else(|| ExecError::Remote("missing neighbor_id".into()))?;
        let edge_id = EdgeId::from_uuid(
            uuid_from_proto(&edge_proto).map_err(|e| ExecError::Remote(e.to_string()))?,
        );
        let node_id = NodeId::from_uuid(
            uuid_from_proto(&node_proto).map_err(|e| ExecError::Remote(e.to_string()))?,
        );
        out.push((edge_id, node_id));
    }
    Ok(out)
}
