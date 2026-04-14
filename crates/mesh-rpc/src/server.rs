use crate::convert::{
    edge_from_proto, edge_id_from_proto, edge_to_proto, node_from_proto, node_id_from_proto,
    node_to_proto, uuid_to_proto,
};
use crate::proto::mesh_query_server::{MeshQuery, MeshQueryServer};
use crate::proto::mesh_write_server::{MeshWrite, MeshWriteServer};
use crate::proto::{
    DeleteEdgeRequest, DeleteEdgeResponse, DetachDeleteNodeRequest, DetachDeleteNodeResponse,
    GetEdgeRequest, GetEdgeResponse, GetNodeRequest, GetNodeResponse, HealthRequest,
    HealthResponse, NeighborInfo, NeighborRequest, NeighborResponse, NodesByLabelRequest,
    NodesByLabelResponse, PutEdgeRequest, PutEdgeResponse, PutNodeRequest, PutNodeResponse,
};
use crate::routing::Routing;
use mesh_cluster::raft::RaftCluster;
use mesh_cluster::{GraphCommand, PeerId};
use mesh_storage::Store;
use std::collections::HashSet;
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct MeshService {
    store: Arc<Store>,
    routing: Option<Arc<Routing>>,
    raft: Option<Arc<RaftCluster>>,
}

impl MeshService {
    /// Local-only service: every request is answered from the local store.
    pub fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            routing: None,
            raft: None,
        }
    }

    /// Routed service: point queries go to the partition owner; scan
    /// queries scatter-gather across all known peers. No Raft.
    pub fn with_routing(store: Arc<Store>, routing: Arc<Routing>) -> Self {
        Self {
            store,
            routing: Some(routing),
            raft: None,
        }
    }

    /// Raft-backed service: writes go through `RaftCluster::propose_graph`
    /// so every replica's local store ends up with the same data via the
    /// state machine's apply path. Reads go straight to the local store
    /// (every peer holds the full graph in this single-Raft-group model).
    pub fn with_raft(store: Arc<Store>, raft: Arc<RaftCluster>) -> Self {
        Self {
            store,
            routing: None,
            raft: Some(raft),
        }
    }

    pub fn into_query_server(self) -> MeshQueryServer<Self> {
        MeshQueryServer::new(self)
    }

    pub fn into_write_server(self) -> MeshWriteServer<Self> {
        MeshWriteServer::new(self)
    }
}

fn raft_propose_failed<E: std::fmt::Display>(e: E) -> Status {
    Status::internal(format!("raft propose failed: {e}"))
}

fn internal<E: std::fmt::Display>(e: E) -> Status {
    Status::internal(e.to_string())
}

fn bad_request<E: std::fmt::Display>(e: E) -> Status {
    Status::invalid_argument(e.to_string())
}

fn no_client(peer: PeerId) -> Status {
    Status::internal(format!("no client registered for peer {}", peer))
}

#[tonic::async_trait]
impl MeshQuery for MeshService {
    async fn get_node(
        &self,
        request: Request<GetNodeRequest>,
    ) -> Result<Response<GetNodeResponse>, Status> {
        let id_proto = request
            .into_inner()
            .id
            .ok_or_else(|| Status::invalid_argument("missing id"))?;
        let id = node_id_from_proto(&id_proto).map_err(bad_request)?;

        // Forward to the partition owner if this node doesn't live locally.
        if let Some(routing) = &self.routing {
            if !routing.cluster().is_local(id) {
                let owner = routing.cluster().owner_of(id);
                let mut client = routing.query_client(owner).ok_or_else(|| no_client(owner))?;
                return client
                    .get_node(GetNodeRequest { id: Some(id_proto) })
                    .await;
            }
        }

        let node = self.store.get_node(id).map_err(internal)?;
        let (found, node) = match node {
            Some(n) => (true, Some(node_to_proto(&n).map_err(internal)?)),
            None => (false, None),
        };
        Ok(Response::new(GetNodeResponse { found, node }))
    }

    async fn get_edge(
        &self,
        request: Request<GetEdgeRequest>,
    ) -> Result<Response<GetEdgeResponse>, Status> {
        let req = request.into_inner();
        let id_proto = req
            .id
            .ok_or_else(|| Status::invalid_argument("missing id"))?;
        let local_only = req.local_only;
        let id = edge_id_from_proto(&id_proto).map_err(bad_request)?;

        // Always check local first — if the edge lives here, we're done.
        if let Some(edge) = self.store.get_edge(id).map_err(internal)? {
            let proto_edge = edge_to_proto(&edge).map_err(internal)?;
            return Ok(Response::new(GetEdgeResponse {
                found: true,
                edge: Some(proto_edge),
            }));
        }

        // Otherwise scatter-gather to each remote peer until one returns a hit.
        // `local_only` on the forwarded request prevents infinite recursion.
        if !local_only {
            if let Some(routing) = &self.routing {
                let self_id = routing.cluster().self_id();
                for peer_id in routing.cluster().membership().peer_ids() {
                    if peer_id == self_id {
                        continue;
                    }
                    let mut client =
                        routing.query_client(peer_id).ok_or_else(|| no_client(peer_id))?;
                    let resp = client
                        .get_edge(GetEdgeRequest {
                            id: Some(id_proto.clone()),
                            local_only: true,
                        })
                        .await?;
                    let inner = resp.into_inner();
                    if inner.found {
                        return Ok(Response::new(inner));
                    }
                }
            }
        }

        Ok(Response::new(GetEdgeResponse {
            found: false,
            edge: None,
        }))
    }

    async fn nodes_by_label(
        &self,
        request: Request<NodesByLabelRequest>,
    ) -> Result<Response<NodesByLabelResponse>, Status> {
        let req = request.into_inner();
        let label = req.label;
        let local_only = req.local_only;

        let mut ids: Vec<_> = self
            .store
            .nodes_by_label(&label)
            .map_err(internal)?
            .into_iter()
            .map(|id| uuid_to_proto(id.as_uuid()))
            .collect();

        if !local_only {
            if let Some(routing) = &self.routing {
                let self_id = routing.cluster().self_id();
                for peer_id in routing.cluster().membership().peer_ids() {
                    if peer_id == self_id {
                        continue;
                    }
                    let mut client =
                        routing.query_client(peer_id).ok_or_else(|| no_client(peer_id))?;
                    let resp = client
                        .nodes_by_label(NodesByLabelRequest {
                            label: label.clone(),
                            local_only: true,
                        })
                        .await?;
                    ids.extend(resp.into_inner().ids);
                }
            }
        }

        Ok(Response::new(NodesByLabelResponse { ids }))
    }

    async fn outgoing(
        &self,
        request: Request<NeighborRequest>,
    ) -> Result<Response<NeighborResponse>, Status> {
        let id_proto = request
            .into_inner()
            .node_id
            .ok_or_else(|| Status::invalid_argument("missing node_id"))?;
        let id = node_id_from_proto(&id_proto).map_err(bad_request)?;

        if let Some(routing) = &self.routing {
            if !routing.cluster().is_local(id) {
                let owner = routing.cluster().owner_of(id);
                let mut client = routing.query_client(owner).ok_or_else(|| no_client(owner))?;
                return client
                    .outgoing(NeighborRequest {
                        node_id: Some(id_proto),
                    })
                    .await;
            }
        }

        let out = self.store.outgoing(id).map_err(internal)?;
        let neighbors = out
            .into_iter()
            .map(|(eid, nid)| NeighborInfo {
                edge_id: Some(uuid_to_proto(eid.as_uuid())),
                neighbor_id: Some(uuid_to_proto(nid.as_uuid())),
            })
            .collect();
        Ok(Response::new(NeighborResponse { neighbors }))
    }

    async fn incoming(
        &self,
        request: Request<NeighborRequest>,
    ) -> Result<Response<NeighborResponse>, Status> {
        let id_proto = request
            .into_inner()
            .node_id
            .ok_or_else(|| Status::invalid_argument("missing node_id"))?;
        let id = node_id_from_proto(&id_proto).map_err(bad_request)?;

        if let Some(routing) = &self.routing {
            if !routing.cluster().is_local(id) {
                let owner = routing.cluster().owner_of(id);
                let mut client = routing.query_client(owner).ok_or_else(|| no_client(owner))?;
                return client
                    .incoming(NeighborRequest {
                        node_id: Some(id_proto),
                    })
                    .await;
            }
        }

        let inc = self.store.incoming(id).map_err(internal)?;
        let neighbors = inc
            .into_iter()
            .map(|(eid, nid)| NeighborInfo {
                edge_id: Some(uuid_to_proto(eid.as_uuid())),
                neighbor_id: Some(uuid_to_proto(nid.as_uuid())),
            })
            .collect();
        Ok(Response::new(NeighborResponse { neighbors }))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse { serving: true }))
    }
}

#[tonic::async_trait]
impl MeshWrite for MeshService {
    async fn put_node(
        &self,
        request: Request<PutNodeRequest>,
    ) -> Result<Response<PutNodeResponse>, Status> {
        let req = request.into_inner();
        let local_only = req.local_only;
        let proto_node = req
            .node
            .ok_or_else(|| Status::invalid_argument("missing node"))?;
        let node = node_from_proto(proto_node.clone()).map_err(bad_request)?;
        let id = node.id;

        // Raft mode: propose through consensus. Every replica's state
        // machine applies the same write via StoreGraphApplier.
        if let Some(raft) = &self.raft {
            raft.propose_graph(GraphCommand::PutNode(node))
                .await
                .map_err(raft_propose_failed)?;
            return Ok(Response::new(PutNodeResponse {}));
        }

        // Route to the owner when we aren't it.
        if let Some(routing) = &self.routing {
            if !routing.cluster().is_local(id) && !local_only {
                let owner = routing.cluster().owner_of(id);
                let mut client = routing
                    .write_client(owner)
                    .ok_or_else(|| no_client(owner))?;
                client
                    .put_node(PutNodeRequest {
                        node: Some(proto_node),
                        local_only: true,
                    })
                    .await?;
                return Ok(Response::new(PutNodeResponse {}));
            }
        }

        self.store.put_node(&node).map_err(internal)?;
        Ok(Response::new(PutNodeResponse {}))
    }

    async fn put_edge(
        &self,
        request: Request<PutEdgeRequest>,
    ) -> Result<Response<PutEdgeResponse>, Status> {
        let req = request.into_inner();
        let local_only = req.local_only;
        let proto_edge = req
            .edge
            .ok_or_else(|| Status::invalid_argument("missing edge"))?;
        let edge = edge_from_proto(proto_edge.clone()).map_err(bad_request)?;

        if let Some(raft) = &self.raft {
            raft.propose_graph(GraphCommand::PutEdge(edge))
                .await
                .map_err(raft_propose_failed)?;
            return Ok(Response::new(PutEdgeResponse {}));
        }

        if let Some(routing) = &self.routing {
            let cluster = routing.cluster();
            let self_id = cluster.self_id();
            let mut targets: HashSet<PeerId> = HashSet::new();
            targets.insert(cluster.owner_of(edge.source));
            targets.insert(cluster.owner_of(edge.target));
            let self_is_target = targets.remove(&self_id);

            if self_is_target {
                self.store.put_edge(&edge).map_err(internal)?;
            }

            if !local_only {
                for owner in targets {
                    let mut client =
                        routing.write_client(owner).ok_or_else(|| no_client(owner))?;
                    client
                        .put_edge(PutEdgeRequest {
                            edge: Some(proto_edge.clone()),
                            local_only: true,
                        })
                        .await?;
                }
            }

            return Ok(Response::new(PutEdgeResponse {}));
        }

        // No routing — local-only behavior.
        self.store.put_edge(&edge).map_err(internal)?;
        Ok(Response::new(PutEdgeResponse {}))
    }

    async fn delete_edge(
        &self,
        request: Request<DeleteEdgeRequest>,
    ) -> Result<Response<DeleteEdgeResponse>, Status> {
        let req = request.into_inner();
        let local_only = req.local_only;
        let id_proto = req
            .edge_id
            .ok_or_else(|| Status::invalid_argument("missing edge_id"))?;
        let id = edge_id_from_proto(&id_proto).map_err(bad_request)?;

        if let Some(raft) = &self.raft {
            raft.propose_graph(GraphCommand::DeleteEdge(id))
                .await
                .map_err(raft_propose_failed)?;
            return Ok(Response::new(DeleteEdgeResponse {}));
        }

        // Local delete is idempotent (check-then-delete).
        if self.store.get_edge(id).map_err(internal)?.is_some() {
            self.store.delete_edge(id).map_err(internal)?;
        }

        // Scatter-gather to all other peers.
        if !local_only {
            if let Some(routing) = &self.routing {
                let self_id = routing.cluster().self_id();
                for peer_id in routing.cluster().membership().peer_ids() {
                    if peer_id == self_id {
                        continue;
                    }
                    let mut client = routing
                        .write_client(peer_id)
                        .ok_or_else(|| no_client(peer_id))?;
                    client
                        .delete_edge(DeleteEdgeRequest {
                            edge_id: Some(id_proto.clone()),
                            local_only: true,
                        })
                        .await?;
                }
            }
        }

        Ok(Response::new(DeleteEdgeResponse {}))
    }

    async fn detach_delete_node(
        &self,
        request: Request<DetachDeleteNodeRequest>,
    ) -> Result<Response<DetachDeleteNodeResponse>, Status> {
        let req = request.into_inner();
        let local_only = req.local_only;
        let id_proto = req
            .node_id
            .ok_or_else(|| Status::invalid_argument("missing node_id"))?;
        let id = node_id_from_proto(&id_proto).map_err(bad_request)?;

        if let Some(raft) = &self.raft {
            raft.propose_graph(GraphCommand::DetachDeleteNode(id))
                .await
                .map_err(raft_propose_failed)?;
            return Ok(Response::new(DetachDeleteNodeResponse {}));
        }

        // Local detach-delete is idempotent — no-op if the node isn't here.
        self.store.detach_delete_node(id).map_err(internal)?;

        // Scatter-gather so every peer cleans up any incident edges it owns.
        if !local_only {
            if let Some(routing) = &self.routing {
                let self_id = routing.cluster().self_id();
                for peer_id in routing.cluster().membership().peer_ids() {
                    if peer_id == self_id {
                        continue;
                    }
                    let mut client = routing
                        .write_client(peer_id)
                        .ok_or_else(|| no_client(peer_id))?;
                    client
                        .detach_delete_node(DetachDeleteNodeRequest {
                            node_id: Some(id_proto.clone()),
                            local_only: true,
                        })
                        .await?;
                }
            }
        }

        Ok(Response::new(DetachDeleteNodeResponse {}))
    }
}
