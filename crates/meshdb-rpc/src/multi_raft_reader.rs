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
//! `nodes_by_property`, `edges_by_property`, `get_edge`) under
//! linearizable mode fence on every partition this peer leads,
//! filter the local-store result to ids whose partition is led
//! locally, and fan out to every other peer to union their
//! filtered contributions. Each partition leader's local store
//! is the authoritative source for its partition (after the
//! `ensure_linearizable` quorum check), so the union covers
//! every partition exactly once with no duplicates and no stale
//! follower data. Default (relaxed) reads still hit the local
//! store directly without the scatter, matching pre-v2.5
//! behaviour.

use crate::cluster_auth::ClusterAuth;
use crate::convert::{
    edge_from_proto, edge_id_from_proto, node_from_proto, node_id_from_proto, uuid_from_proto,
    uuid_to_proto,
};
use crate::proto::mesh_query_client::MeshQueryClient;
use crate::proto::{
    AllNodeIdsRequest, EdgesByPropertyRequest, GetEdgeRequest, GetNodeRequest, NeighborRequest,
    NodesByLabelRequest, NodesByPropertyRequest,
};
use crate::MultiRaftCluster;
use meshdb_cluster::{PartitionId, Partitioner, PeerId};
use meshdb_core::{Edge, EdgeId, Node, NodeId, Property};
use meshdb_executor::{Error as ExecError, GraphReader, Result as ExecResult};
use meshdb_storage::StorageEngine;
use std::collections::{BTreeSet, HashSet};
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

    /// Build a `MeshQueryClient` for a peer at `addr`. Used by both
    /// the leader-forwarding path (point reads) and the scatter
    /// path (label scans, etc.) — neither pools channels because
    /// partition leadership shifts between reads, but `connect_lazy`
    /// returns immediately so the cost is just per-call.
    async fn peer_client(&self, addr: &str) -> ExecResult<MeshQueryClient<Channel>> {
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

    /// Resolve the partition leader's address and build a query
    /// client for it.
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
        self.peer_client(&addr).await
    }

    /// Identify every partition this peer leads, fence each via
    /// `Raft::ensure_linearizable`, and return the set. Used by
    /// the scatter-gather methods to filter their local-store
    /// result to "data this peer is the leader of, with the read
    /// index quorum-checked". Together with the fan-out below,
    /// the union covers every partition exactly once.
    async fn led_partitions_with_fence(&self) -> ExecResult<BTreeSet<PartitionId>> {
        let led: Vec<PartitionId> = self
            .multi_raft
            .partitions_snapshot()
            .into_iter()
            .filter(|(_, raft)| {
                matches!(raft.current_leader(), Some(l) if l.0 == self.multi_raft.self_id)
            })
            .map(|(p, _)| p)
            .collect();
        for p in &led {
            self.fence_local(*p).await?;
        }
        Ok(led.into_iter().collect())
    }

    /// Build the list of (peer_id, addr) pairs for every peer
    /// other than self, looked up from the meta cluster's
    /// membership.
    async fn other_peer_addrs(&self) -> Vec<(u64, String)> {
        let state = self.multi_raft.meta.current_state().await;
        state
            .membership
            .iter()
            .filter(|(id, _)| id.0 != self.multi_raft.self_id)
            .map(|(id, addr)| (id.0, addr.to_string()))
            .collect()
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
        if !self.linearizable {
            return Ok(self.local.get_edge(id)?);
        }
        self.block(async {
            // Local fence + check.
            let led = self.led_partitions_with_fence().await?;
            let partitioner = Partitioner::new(self.multi_raft.replica_map.num_partitions());
            if let Some(edge) = self.local.get_edge(id)? {
                if led.contains(&partitioner.partition_for(edge.source)) {
                    return Ok(Some(edge));
                }
            }
            // Scatter: every other peer; whichever leads the edge's
            // source partition returns `found=true`.
            let id_proto = uuid_to_proto(id.as_uuid());
            for (_peer_id, addr) in self.other_peer_addrs().await {
                let mut client = self.peer_client(&addr).await?;
                let req = self.auth_request(GetEdgeRequest {
                    id: Some(id_proto.clone()),
                    local_only: true,
                    linearizable: true,
                })?;
                let resp = client
                    .get_edge(req)
                    .await
                    .map_err(|e| ExecError::Remote(format!("forward get_edge: {e}")))?;
                let inner = resp.into_inner();
                if inner.found {
                    let edge = edge_from_proto(
                        inner
                            .edge
                            .ok_or_else(|| ExecError::Remote("missing edge in response".into()))?,
                    )
                    .map_err(|e| ExecError::Remote(e.to_string()))?;
                    return Ok(Some(edge));
                }
            }
            Ok(None)
        })
    }

    fn all_node_ids(&self) -> ExecResult<Vec<NodeId>> {
        if !self.linearizable {
            return Ok(self.local.all_node_ids()?);
        }
        self.block(async {
            let partitioner = Partitioner::new(self.multi_raft.replica_map.num_partitions());
            let led = self.led_partitions_with_fence().await?;
            let mut seen: HashSet<NodeId> = self
                .local
                .all_node_ids()?
                .into_iter()
                .filter(|id| led.contains(&partitioner.partition_for(*id)))
                .collect();
            for (_peer_id, addr) in self.other_peer_addrs().await {
                let mut client = self.peer_client(&addr).await?;
                let req = self.auth_request(AllNodeIdsRequest {
                    local_only: true,
                    linearizable: true,
                })?;
                let resp = client
                    .all_node_ids(req)
                    .await
                    .map_err(|e| ExecError::Remote(format!("forward all_node_ids: {e}")))?;
                for id_proto in resp.into_inner().ids {
                    let id = node_id_from_proto(&id_proto)
                        .map_err(|e| ExecError::Remote(e.to_string()))?;
                    seen.insert(id);
                }
            }
            Ok(seen.into_iter().collect())
        })
    }

    fn nodes_by_label(&self, label: &str) -> ExecResult<Vec<NodeId>> {
        if !self.linearizable {
            return Ok(self.local.nodes_by_label(label)?);
        }
        let label = label.to_string();
        self.block(async {
            let partitioner = Partitioner::new(self.multi_raft.replica_map.num_partitions());
            let led = self.led_partitions_with_fence().await?;
            let mut seen: HashSet<NodeId> = self
                .local
                .nodes_by_label(&label)?
                .into_iter()
                .filter(|id| led.contains(&partitioner.partition_for(*id)))
                .collect();
            for (_peer_id, addr) in self.other_peer_addrs().await {
                let mut client = self.peer_client(&addr).await?;
                let req = self.auth_request(NodesByLabelRequest {
                    label: label.clone(),
                    local_only: true,
                    linearizable: true,
                })?;
                let resp = client
                    .nodes_by_label(req)
                    .await
                    .map_err(|e| ExecError::Remote(format!("forward nodes_by_label: {e}")))?;
                for id_proto in resp.into_inner().ids {
                    let id = node_id_from_proto(&id_proto)
                        .map_err(|e| ExecError::Remote(e.to_string()))?;
                    seen.insert(id);
                }
            }
            Ok(seen.into_iter().collect())
        })
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
        if !self.linearizable {
            return Ok(self.local.nodes_by_property(label, property, value)?);
        }
        let label = label.to_string();
        let property = property.to_string();
        let value_json = serde_json::to_vec(value)
            .map_err(|e| ExecError::Remote(format!("encoding property value: {e}")))?;
        let local_value = value.clone();
        self.block(async {
            let partitioner = Partitioner::new(self.multi_raft.replica_map.num_partitions());
            let led = self.led_partitions_with_fence().await?;
            let mut seen: HashSet<NodeId> = self
                .local
                .nodes_by_property(&label, &property, &local_value)?
                .into_iter()
                .filter(|id| led.contains(&partitioner.partition_for(*id)))
                .collect();
            for (_peer_id, addr) in self.other_peer_addrs().await {
                let mut client = self.peer_client(&addr).await?;
                let req = self.auth_request(NodesByPropertyRequest {
                    label: label.clone(),
                    property: property.clone(),
                    value_json: value_json.clone(),
                    local_only: true,
                    linearizable: true,
                })?;
                let resp = client
                    .nodes_by_property(req)
                    .await
                    .map_err(|e| ExecError::Remote(format!("forward nodes_by_property: {e}")))?;
                for id_proto in resp.into_inner().ids {
                    let id = node_id_from_proto(&id_proto)
                        .map_err(|e| ExecError::Remote(e.to_string()))?;
                    seen.insert(id);
                }
            }
            Ok(seen.into_iter().collect())
        })
    }

    fn edges_by_property(
        &self,
        edge_type: &str,
        property: &str,
        value: &Property,
    ) -> ExecResult<Vec<EdgeId>> {
        if !self.linearizable {
            return Ok(self.local.edges_by_property(edge_type, property, value)?);
        }
        let edge_type = edge_type.to_string();
        let property = property.to_string();
        let value_json = serde_json::to_vec(value)
            .map_err(|e| ExecError::Remote(format!("encoding property value: {e}")))?;
        let local_value = value.clone();
        self.block(async {
            let partitioner = Partitioner::new(self.multi_raft.replica_map.num_partitions());
            let led = self.led_partitions_with_fence().await?;
            // Local: filter edge ids by source partition.
            let mut seen: HashSet<EdgeId> = HashSet::new();
            for eid in self
                .local
                .edges_by_property(&edge_type, &property, &local_value)?
            {
                if let Some(edge) = self.local.get_edge(eid)? {
                    if led.contains(&partitioner.partition_for(edge.source)) {
                        seen.insert(eid);
                    }
                }
            }
            for (_peer_id, addr) in self.other_peer_addrs().await {
                let mut client = self.peer_client(&addr).await?;
                let req = self.auth_request(EdgesByPropertyRequest {
                    edge_type: edge_type.clone(),
                    property: property.clone(),
                    value_json: value_json.clone(),
                    local_only: true,
                    linearizable: true,
                })?;
                let resp = client
                    .edges_by_property(req)
                    .await
                    .map_err(|e| ExecError::Remote(format!("forward edges_by_property: {e}")))?;
                for id_proto in resp.into_inner().ids {
                    let id = edge_id_from_proto(&id_proto)
                        .map_err(|e| ExecError::Remote(e.to_string()))?;
                    seen.insert(id);
                }
            }
            Ok(seen.into_iter().collect())
        })
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
