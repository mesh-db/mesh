use crate::convert::{edge_to_proto, node_to_proto, uuid_to_proto};
use crate::proto::{
    DeleteEdgeRequest, DetachDeleteNodeRequest, PutEdgeRequest, PutNodeRequest,
};
use crate::routing::Routing;
use mesh_core::{Edge, EdgeId, Node, NodeId};
use mesh_executor::{Error as ExecError, GraphWriter, Result as ExecResult};
use mesh_storage::Store;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::runtime::Handle;

/// Executor write sink that routes each mutation to the correct partition
/// owner. Symmetric to [`crate::PartitionedGraphReader`] on the write side:
/// `put_node` goes to the single owner, `put_edge` goes to both the source
/// and target owners (so each peer's local adjacency is complete for nodes
/// it owns), and `delete_edge` / `detach_delete_node` scatter-gather to
/// every peer since the executor can't cheaply tell where ghost copies
/// live.
///
/// All remote calls pin `local_only = true` so the remote peer writes
/// directly to its local store without re-entering its own routing logic.
///
/// Mirrors the shape of [`crate::RaftGraphWriter`] / the existing MeshWrite
/// handlers, and bridges the sync [`GraphWriter`] API onto async gRPC via
/// `Handle::block_on`. Callers must run the executor inside
/// `tokio::task::spawn_blocking`.
pub struct RoutingGraphWriter {
    local: Arc<Store>,
    routing: Arc<Routing>,
    handle: Handle,
}

impl RoutingGraphWriter {
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
        ExecError::Write(e.to_string())
    }
}

impl GraphWriter for RoutingGraphWriter {
    fn put_node(&self, node: &Node) -> ExecResult<()> {
        let cluster = self.routing.cluster();
        if cluster.is_local(node.id) {
            self.local.put_node(node)?;
            return Ok(());
        }
        let owner = cluster.owner_of(node.id);
        let proto_node =
            node_to_proto(node).map_err(|e| ExecError::Write(e.to_string()))?;
        let routing = self.routing.clone();
        self.block(async move {
            let mut client = routing.write_client(owner).ok_or_else(|| {
                ExecError::Write(format!("no client registered for peer {}", owner))
            })?;
            client
                .put_node(PutNodeRequest {
                    node: Some(proto_node),
                    local_only: true,
                })
                .await
                .map_err(Self::remote)?;
            Ok(())
        })
    }

    fn put_edge(&self, edge: &Edge) -> ExecResult<()> {
        let cluster = self.routing.cluster();
        let self_id = cluster.self_id();
        let mut targets: HashSet<_> = HashSet::new();
        targets.insert(cluster.owner_of(edge.source));
        targets.insert(cluster.owner_of(edge.target));
        let self_is_target = targets.remove(&self_id);

        if self_is_target {
            self.local.put_edge(edge)?;
        }

        if targets.is_empty() {
            return Ok(());
        }

        let proto_edge =
            edge_to_proto(edge).map_err(|e| ExecError::Write(e.to_string()))?;
        let routing = self.routing.clone();
        self.block(async move {
            for owner in targets {
                let mut client = routing.write_client(owner).ok_or_else(|| {
                    ExecError::Write(format!("no client registered for peer {}", owner))
                })?;
                client
                    .put_edge(PutEdgeRequest {
                        edge: Some(proto_edge.clone()),
                        local_only: true,
                    })
                    .await
                    .map_err(Self::remote)?;
            }
            Ok(())
        })
    }

    fn delete_edge(&self, id: EdgeId) -> ExecResult<()> {
        // Local delete is idempotent (check-then-delete) — skip the hop if
        // the edge doesn't live here.
        if self.local.get_edge(id)?.is_some() {
            self.local.delete_edge(id)?;
        }

        let routing = self.routing.clone();
        self.block(async move {
            let self_id = routing.cluster().self_id();
            let id_proto = uuid_to_proto(id.as_uuid());
            for peer_id in routing.cluster().membership().peer_ids() {
                if peer_id == self_id {
                    continue;
                }
                let mut client = routing.write_client(peer_id).ok_or_else(|| {
                    ExecError::Write(format!("no client registered for peer {}", peer_id))
                })?;
                client
                    .delete_edge(DeleteEdgeRequest {
                        edge_id: Some(id_proto.clone()),
                        local_only: true,
                    })
                    .await
                    .map_err(Self::remote)?;
            }
            Ok(())
        })
    }

    fn detach_delete_node(&self, id: NodeId) -> ExecResult<()> {
        // Local detach-delete is idempotent — safe to call on any peer,
        // including ones that don't own the node but may hold ghost edges.
        self.local.detach_delete_node(id)?;

        let routing = self.routing.clone();
        self.block(async move {
            let self_id = routing.cluster().self_id();
            let id_proto = uuid_to_proto(id.as_uuid());
            for peer_id in routing.cluster().membership().peer_ids() {
                if peer_id == self_id {
                    continue;
                }
                let mut client = routing.write_client(peer_id).ok_or_else(|| {
                    ExecError::Write(format!("no client registered for peer {}", peer_id))
                })?;
                client
                    .detach_delete_node(DetachDeleteNodeRequest {
                        node_id: Some(id_proto.clone()),
                        local_only: true,
                    })
                    .await
                    .map_err(Self::remote)?;
            }
            Ok(())
        })
    }
}
