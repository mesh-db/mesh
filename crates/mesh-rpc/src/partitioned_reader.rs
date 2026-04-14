use crate::convert::{
    edge_from_proto, node_from_proto, node_id_from_proto, uuid_from_proto, uuid_to_proto,
};
use crate::proto::{
    AllNodeIdsRequest, GetEdgeRequest, GetNodeRequest, NeighborRequest, NodesByLabelRequest,
};
use crate::routing::Routing;
use mesh_core::{Edge, EdgeId, Node, NodeId};
use mesh_executor::{Error as ExecError, GraphReader, Result as ExecResult};
use mesh_storage::Store;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::runtime::Handle;

/// Executor read source that presents a unified view of a sharded graph.
/// Point reads (`get_node`, `outgoing`, `incoming`) route to the partition
/// owner; bulk scans (`all_node_ids`, `nodes_by_label`) scatter-gather across
/// every peer and union the results; `get_edge` tries the local store and
/// falls back to a scatter-gather because edges can live on two partitions
/// (source-owner + target-owner).
///
/// All remote calls pin `local_only = true` so the remote peer answers from
/// its local store only — the server's own scatter-gather logic is disabled
/// on the forwarded hop, preventing infinite recursion.
///
/// Like [`crate::RaftGraphWriter`], this bridges a sync [`GraphReader`] API
/// onto async gRPC via `Handle::block_on`, so callers MUST invoke the
/// executor inside `tokio::task::spawn_blocking`.
pub struct PartitionedGraphReader {
    local: Arc<Store>,
    routing: Arc<Routing>,
    handle: Handle,
}

impl PartitionedGraphReader {
    pub fn new(local: Arc<Store>, routing: Arc<Routing>) -> Self {
        Self {
            local,
            routing,
            handle: Handle::current(),
        }
    }

    fn block<F, T>(&self, fut: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        self.handle.block_on(fut)
    }

    fn remote<E: std::fmt::Display>(e: E) -> ExecError {
        ExecError::Remote(e.to_string())
    }
}

impl GraphReader for PartitionedGraphReader {
    fn get_node(&self, id: NodeId) -> ExecResult<Option<Node>> {
        if self.routing.cluster().is_local(id) {
            return Ok(self.local.get_node(id)?);
        }
        let owner = self.routing.cluster().owner_of(id);
        let routing = self.routing.clone();
        self.block(async move {
            let mut client = routing.query_client(owner).ok_or_else(|| {
                ExecError::Remote(format!("no client registered for peer {}", owner))
            })?;
            let resp = client
                .get_node(GetNodeRequest {
                    id: Some(uuid_to_proto(id.as_uuid())),
                    local_only: true,
                })
                .await
                .map_err(Self::remote)?;
            let inner = resp.into_inner();
            if !inner.found {
                return Ok(None);
            }
            let node = node_from_proto(
                inner
                    .node
                    .ok_or_else(|| ExecError::Remote("missing node in response".into()))?,
            )
            .map_err(Self::remote)?;
            Ok(Some(node))
        })
    }

    fn get_edge(&self, id: EdgeId) -> ExecResult<Option<Edge>> {
        // Edges may live on up to two peers (source owner + target owner),
        // so there is no single "right" peer to ask. Try local first — the
        // traversal that produced this edge id almost certainly came from
        // either `outgoing()` or `incoming()` on a locally-owned node, so
        // the edge is usually here. Fall back to a scatter-gather.
        if let Some(edge) = self.local.get_edge(id)? {
            return Ok(Some(edge));
        }
        let routing = self.routing.clone();
        self.block(async move {
            let self_id = routing.cluster().self_id();
            for peer_id in routing.cluster().membership().peer_ids() {
                if peer_id == self_id {
                    continue;
                }
                let mut client = routing.query_client(peer_id).ok_or_else(|| {
                    ExecError::Remote(format!("no client registered for peer {}", peer_id))
                })?;
                let resp = client
                    .get_edge(GetEdgeRequest {
                        id: Some(uuid_to_proto(id.as_uuid())),
                        local_only: true,
                    })
                    .await
                    .map_err(Self::remote)?;
                let inner = resp.into_inner();
                if inner.found {
                    let edge = edge_from_proto(
                        inner
                            .edge
                            .ok_or_else(|| ExecError::Remote("missing edge".into()))?,
                    )
                    .map_err(Self::remote)?;
                    return Ok(Some(edge));
                }
            }
            Ok(None)
        })
    }

    fn all_node_ids(&self) -> ExecResult<Vec<NodeId>> {
        let mut seen: HashSet<NodeId> = self.local.all_node_ids()?.into_iter().collect();
        let routing = self.routing.clone();
        let remote: ExecResult<Vec<NodeId>> = self.block(async move {
            let self_id = routing.cluster().self_id();
            let mut out: Vec<NodeId> = Vec::new();
            for peer_id in routing.cluster().membership().peer_ids() {
                if peer_id == self_id {
                    continue;
                }
                let mut client = routing.query_client(peer_id).ok_or_else(|| {
                    ExecError::Remote(format!("no client registered for peer {}", peer_id))
                })?;
                let resp = client
                    .all_node_ids(AllNodeIdsRequest { local_only: true })
                    .await
                    .map_err(Self::remote)?;
                for id_proto in resp.into_inner().ids {
                    let id = node_id_from_proto(&id_proto).map_err(Self::remote)?;
                    out.push(id);
                }
            }
            Ok(out)
        });
        for id in remote? {
            seen.insert(id);
        }
        Ok(seen.into_iter().collect())
    }

    fn nodes_by_label(&self, label: &str) -> ExecResult<Vec<NodeId>> {
        let mut seen: HashSet<NodeId> =
            self.local.nodes_by_label(label)?.into_iter().collect();
        let label = label.to_string();
        let routing = self.routing.clone();
        let remote: ExecResult<Vec<NodeId>> = self.block(async move {
            let self_id = routing.cluster().self_id();
            let mut out: Vec<NodeId> = Vec::new();
            for peer_id in routing.cluster().membership().peer_ids() {
                if peer_id == self_id {
                    continue;
                }
                let mut client = routing.query_client(peer_id).ok_or_else(|| {
                    ExecError::Remote(format!("no client registered for peer {}", peer_id))
                })?;
                let resp = client
                    .nodes_by_label(NodesByLabelRequest {
                        label: label.clone(),
                        local_only: true,
                    })
                    .await
                    .map_err(Self::remote)?;
                for id_proto in resp.into_inner().ids {
                    let id = node_id_from_proto(&id_proto).map_err(Self::remote)?;
                    out.push(id);
                }
            }
            Ok(out)
        });
        for id in remote? {
            seen.insert(id);
        }
        Ok(seen.into_iter().collect())
    }

    fn outgoing(&self, id: NodeId) -> ExecResult<Vec<(EdgeId, NodeId)>> {
        if self.routing.cluster().is_local(id) {
            return Ok(self.local.outgoing(id)?);
        }
        let owner = self.routing.cluster().owner_of(id);
        let routing = self.routing.clone();
        self.block(async move {
            let mut client = routing.query_client(owner).ok_or_else(|| {
                ExecError::Remote(format!("no client registered for peer {}", owner))
            })?;
            let resp = client
                .outgoing(NeighborRequest {
                    node_id: Some(uuid_to_proto(id.as_uuid())),
                    local_only: true,
                })
                .await
                .map_err(Self::remote)?;
            neighbors_from_proto(resp.into_inner().neighbors)
        })
    }

    fn incoming(&self, id: NodeId) -> ExecResult<Vec<(EdgeId, NodeId)>> {
        if self.routing.cluster().is_local(id) {
            return Ok(self.local.incoming(id)?);
        }
        let owner = self.routing.cluster().owner_of(id);
        let routing = self.routing.clone();
        self.block(async move {
            let mut client = routing.query_client(owner).ok_or_else(|| {
                ExecError::Remote(format!("no client registered for peer {}", owner))
            })?;
            let resp = client
                .incoming(NeighborRequest {
                    node_id: Some(uuid_to_proto(id.as_uuid())),
                    local_only: true,
                })
                .await
                .map_err(Self::remote)?;
            neighbors_from_proto(resp.into_inner().neighbors)
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
            uuid_from_proto(&edge_proto).map_err(PartitionedGraphReader::remote)?,
        );
        let node_id = NodeId::from_uuid(
            uuid_from_proto(&node_proto).map_err(PartitionedGraphReader::remote)?,
        );
        out.push((edge_id, node_id));
    }
    Ok(out)
}
