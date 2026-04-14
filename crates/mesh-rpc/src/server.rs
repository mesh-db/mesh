use crate::convert::{
    edge_from_proto, edge_id_from_proto, edge_to_proto, node_from_proto, node_id_from_proto,
    node_to_proto, uuid_to_proto,
};
use crate::executor_writer::BufferingGraphWriter;
use crate::partitioned_reader::PartitionedGraphReader;
use crate::tx_coordinator::TxCoordinator;
use crate::proto::mesh_query_server::{MeshQuery, MeshQueryServer};
use crate::proto::mesh_write_client::MeshWriteClient;
use crate::proto::mesh_write_server::{MeshWrite, MeshWriteServer};
use crate::proto::{
    AllNodeIdsRequest, AllNodeIdsResponse, BatchPhase, BatchWriteRequest, BatchWriteResponse,
    DeleteEdgeRequest, DeleteEdgeResponse, DetachDeleteNodeRequest, DetachDeleteNodeResponse,
    ExecuteCypherRequest, ExecuteCypherResponse, GetEdgeRequest, GetEdgeResponse,
    GetNodeRequest, GetNodeResponse, HealthRequest, HealthResponse, NeighborInfo,
    NeighborRequest, NeighborResponse, NodesByLabelRequest, NodesByLabelResponse,
    PutEdgeRequest, PutEdgeResponse, PutNodeRequest, PutNodeResponse,
};
use crate::routing::Routing;
use mesh_cluster::raft::RaftCluster;
use mesh_cluster::{Error as ClusterError, GraphCommand, PeerId};
use mesh_executor::{execute_with_reader, execute_with_writer, GraphReader, GraphWriter};
use mesh_storage::Store;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

/// Per-service in-memory staging for multi-peer [`BatchWrite`] transactions
/// in the PREPARE state. Keyed by the coordinator-supplied `txid`; a
/// successful PREPARE inserts an entry, COMMIT removes + applies, ABORT
/// just removes. If the process crashes between PREPARE and COMMIT the
/// staged batch is lost — standard 2PC participant semantics, equivalent
/// to an implicit ABORT from the coordinator's perspective.
type PendingBatches = Arc<Mutex<HashMap<String, Vec<GraphCommand>>>>;

#[derive(Clone)]
pub struct MeshService {
    store: Arc<Store>,
    routing: Option<Arc<Routing>>,
    raft: Option<Arc<RaftCluster>>,
    pending_batches: PendingBatches,
}

impl MeshService {
    /// Local-only service: every request is answered from the local store.
    pub fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            routing: None,
            raft: None,
            pending_batches: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Routed service: point queries go to the partition owner; scan
    /// queries scatter-gather across all known peers. No Raft.
    pub fn with_routing(store: Arc<Store>, routing: Arc<Routing>) -> Self {
        Self {
            store,
            routing: Some(routing),
            raft: None,
            pending_batches: Arc::new(Mutex::new(HashMap::new())),
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
            pending_batches: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn into_query_server(self) -> MeshQueryServer<Self> {
        MeshQueryServer::new(self)
    }

    pub fn into_write_server(self) -> MeshWriteServer<Self> {
        MeshWriteServer::new(self)
    }

    /// Run a Cypher query end-to-end against this service and return the
    /// raw [`mesh_executor::Row`]s. Shared between the gRPC
    /// `execute_cypher` handler and the Bolt protocol listener so both
    /// entry points drive the exact same parsing, planning, execution,
    /// routing, and 2PC commit logic.
    ///
    /// On cluster-mode Raft writes where this peer isn't the leader,
    /// the helper transparently forwards the original query to the
    /// leader via gRPC and deserializes the rows back — identical
    /// semantics to the direct gRPC call path.
    pub async fn execute_cypher_local(
        &self,
        query: String,
    ) -> std::result::Result<Vec<mesh_executor::Row>, Status> {
        // Parse + plan on the async task — cheap and synchronous.
        let statement = mesh_cypher::parse(&query).map_err(bad_request)?;
        let plan = mesh_cypher::plan(&statement).map_err(bad_request)?;

        let store = self.store.clone();
        let raft = self.raft.clone();
        let routing = self.routing.clone();
        let (rows, raft_batch, routing_batch) = tokio::task::spawn_blocking(
            move || -> std::result::Result<
                (
                    Vec<mesh_executor::Row>,
                    Option<Vec<GraphCommand>>,
                    Option<Vec<GraphCommand>>,
                ),
                mesh_executor::Error,
            > {
                let store_ref: &Store = &store;
                if raft.is_some() {
                    // Raft mode: reads go local (every replica holds the
                    // full graph), writes buffer into a single Raft entry
                    // further down so a crash mid-query can't leave a
                    // partial result on any replica.
                    let writer = BufferingGraphWriter::new();
                    let rows = execute_with_writer(
                        &plan,
                        store_ref,
                        &writer as &dyn GraphWriter,
                    )?;
                    Ok((rows, Some(writer.into_commands()), None))
                } else if let Some(r) = routing.as_ref() {
                    // Routing mode: reads use a partitioned reader that
                    // fans out to owners / scatter-gathers scans; writes
                    // buffer so the async side can drive a 2PC batch
                    // against every participating peer — per-peer and
                    // cross-peer atomicity via prepare/commit.
                    let reader = PartitionedGraphReader::new(store.clone(), r.clone());
                    let writer = BufferingGraphWriter::new();
                    let rows = execute_with_reader(
                        &plan,
                        &reader as &dyn GraphReader,
                        &writer as &dyn GraphWriter,
                    )?;
                    Ok((rows, None, Some(writer.into_commands())))
                } else {
                    let rows = execute_with_writer(
                        &plan,
                        store_ref,
                        store_ref as &dyn GraphWriter,
                    )?;
                    Ok((rows, None, None))
                }
            },
        )
        .await
        .map_err(|e| Status::internal(format!("executor task panicked: {e}")))?
        .map_err(|e| Status::internal(format!("cypher execution failed: {e}")))?;

        // Routing mode: run the buffered writes through the 2PC
        // coordinator so the whole Cypher transaction lands atomically
        // across every participating peer (or not at all).
        if let (Some(routing), Some(commands)) = (&self.routing, routing_batch) {
            if !commands.is_empty() {
                let coordinator =
                    TxCoordinator::new(&self.store, &self.pending_batches, routing);
                coordinator.run(commands).await?;
            }
        }

        if let (Some(raft), Some(commands)) = (&self.raft, raft_batch) {
            if !commands.is_empty() {
                let entry = if commands.len() == 1 {
                    commands.into_iter().next().unwrap()
                } else {
                    GraphCommand::Batch(commands)
                };
                match raft.propose_graph(entry).await {
                    Ok(_) => {}
                    Err(ClusterError::ForwardToLeader {
                        leader_address: Some(addr),
                        ..
                    }) => {
                        // Re-issue the *original* query to the leader.
                        // The leader will re-parse, re-plan, re-execute,
                        // and commit through its own Raft group — the
                        // only safe option, because our buffered
                        // GraphCommands carry fresh ids that would clash
                        // with anything the leader minted for itself.
                        let endpoint = Endpoint::from_shared(format!("http://{}", addr))
                            .map_err(|e| {
                                Status::internal(format!(
                                    "invalid leader address {addr}: {e}"
                                ))
                            })?;
                        let mut client =
                            crate::proto::mesh_query_client::MeshQueryClient::new(
                                endpoint.connect_lazy(),
                            );
                        let resp = client
                            .execute_cypher(ExecuteCypherRequest { query })
                            .await?;
                        let forwarded: Vec<mesh_executor::Row> =
                            serde_json::from_slice(&resp.into_inner().rows_json)
                                .map_err(|e| {
                                    Status::internal(format!(
                                        "decoding forwarded rows: {e}"
                                    ))
                                })?;
                        return Ok(forwarded);
                    }
                    Err(e) => return Err(raft_propose_failed(e)),
                }
            }
        }

        Ok(rows)
    }
}

/// Flatten a tree of [`GraphCommand`] (which may nest `Batch` variants)
/// into a flat `Vec<StoreMutation>` so `Store::apply_batch` can commit
/// them as one rocksdb `WriteBatch`.
pub(crate) fn flatten_commands(
    cmds: &[GraphCommand],
    out: &mut Vec<mesh_storage::StoreMutation>,
) {
    use mesh_storage::StoreMutation;
    for cmd in cmds {
        match cmd {
            GraphCommand::PutNode(n) => out.push(StoreMutation::PutNode(n.clone())),
            GraphCommand::PutEdge(e) => out.push(StoreMutation::PutEdge(e.clone())),
            GraphCommand::DeleteEdge(id) => out.push(StoreMutation::DeleteEdge(*id)),
            GraphCommand::DetachDeleteNode(id) => {
                out.push(StoreMutation::DetachDeleteNode(*id))
            }
            GraphCommand::Batch(inner) => flatten_commands(inner, out),
        }
    }
}

/// Apply a prepared batch to `store` atomically. Used by both the
/// `BatchWrite` commit phase and the in-process coordinator shortcut.
pub(crate) fn apply_prepared_batch(
    store: &Store,
    cmds: &[GraphCommand],
) -> std::result::Result<(), mesh_storage::Error> {
    let mut flat = Vec::with_capacity(cmds.len());
    flatten_commands(cmds, &mut flat);
    store.apply_batch(&flat)
}

fn raft_propose_failed<E: std::fmt::Display>(e: E) -> Status {
    Status::internal(format!("raft propose failed: {e}"))
}

fn leader_write_client(addr: &str) -> Result<MeshWriteClient<Channel>, Status> {
    let endpoint = Endpoint::from_shared(format!("http://{}", addr))
        .map_err(|e| Status::internal(format!("invalid leader address {addr}: {e}")))?;
    Ok(MeshWriteClient::new(endpoint.connect_lazy()))
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
    #[tracing::instrument(skip_all, fields(rpc = "get_node"))]
    async fn get_node(
        &self,
        request: Request<GetNodeRequest>,
    ) -> Result<Response<GetNodeResponse>, Status> {
        let req = request.into_inner();
        let local_only = req.local_only;
        let id_proto = req
            .id
            .ok_or_else(|| Status::invalid_argument("missing id"))?;
        let id = node_id_from_proto(&id_proto).map_err(bad_request)?;

        // Forward to the partition owner if this node doesn't live locally.
        // `local_only` short-circuits forwarding so the partitioned reader
        // can issue direct point reads against a specific peer.
        if !local_only {
            if let Some(routing) = &self.routing {
                if !routing.cluster().is_local(id) {
                    let owner = routing.cluster().owner_of(id);
                    let mut client =
                        routing.query_client(owner).ok_or_else(|| no_client(owner))?;
                    return client
                        .get_node(GetNodeRequest {
                            id: Some(id_proto),
                            local_only: false,
                        })
                        .await;
                }
            }
        }

        let node = self.store.get_node(id).map_err(internal)?;
        let (found, node) = match node {
            Some(n) => (true, Some(node_to_proto(&n).map_err(internal)?)),
            None => (false, None),
        };
        Ok(Response::new(GetNodeResponse { found, node }))
    }

    #[tracing::instrument(skip_all, fields(rpc = "get_edge"))]
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

    #[tracing::instrument(skip_all, fields(rpc = "nodes_by_label"))]
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

    #[tracing::instrument(skip_all, fields(rpc = "outgoing"))]
    async fn outgoing(
        &self,
        request: Request<NeighborRequest>,
    ) -> Result<Response<NeighborResponse>, Status> {
        let req = request.into_inner();
        let local_only = req.local_only;
        let id_proto = req
            .node_id
            .ok_or_else(|| Status::invalid_argument("missing node_id"))?;
        let id = node_id_from_proto(&id_proto).map_err(bad_request)?;

        if !local_only {
            if let Some(routing) = &self.routing {
                if !routing.cluster().is_local(id) {
                    let owner = routing.cluster().owner_of(id);
                    let mut client =
                        routing.query_client(owner).ok_or_else(|| no_client(owner))?;
                    return client
                        .outgoing(NeighborRequest {
                            node_id: Some(id_proto),
                            local_only: false,
                        })
                        .await;
                }
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

    #[tracing::instrument(skip_all, fields(rpc = "incoming"))]
    async fn incoming(
        &self,
        request: Request<NeighborRequest>,
    ) -> Result<Response<NeighborResponse>, Status> {
        let req = request.into_inner();
        let local_only = req.local_only;
        let id_proto = req
            .node_id
            .ok_or_else(|| Status::invalid_argument("missing node_id"))?;
        let id = node_id_from_proto(&id_proto).map_err(bad_request)?;

        if !local_only {
            if let Some(routing) = &self.routing {
                if !routing.cluster().is_local(id) {
                    let owner = routing.cluster().owner_of(id);
                    let mut client =
                        routing.query_client(owner).ok_or_else(|| no_client(owner))?;
                    return client
                        .incoming(NeighborRequest {
                            node_id: Some(id_proto),
                            local_only: false,
                        })
                        .await;
                }
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

    #[tracing::instrument(skip_all, fields(rpc = "all_node_ids"))]
    async fn all_node_ids(
        &self,
        request: Request<AllNodeIdsRequest>,
    ) -> Result<Response<AllNodeIdsResponse>, Status> {
        let local_only = request.into_inner().local_only;

        let mut ids: Vec<_> = self
            .store
            .all_node_ids()
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
                        .all_node_ids(AllNodeIdsRequest { local_only: true })
                        .await?;
                    ids.extend(resp.into_inner().ids);
                }
            }
        }

        Ok(Response::new(AllNodeIdsResponse { ids }))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse { serving: true }))
    }

    #[tracing::instrument(skip_all, fields(rpc = "execute_cypher", query_len))]
    async fn execute_cypher(
        &self,
        request: Request<ExecuteCypherRequest>,
    ) -> Result<Response<ExecuteCypherResponse>, Status> {
        let query = request.into_inner().query;
        tracing::Span::current().record("query_len", query.len());
        let rows = self.execute_cypher_local(query).await?;
        let rows_json = serde_json::to_vec(&rows)
            .map_err(|e| Status::internal(format!("encoding rows: {e}")))?;
        Ok(Response::new(ExecuteCypherResponse { rows_json }))
    }
}

#[tonic::async_trait]
impl MeshWrite for MeshService {
    #[tracing::instrument(skip_all, fields(rpc = "put_node"))]
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
        // machine applies the same write via StoreGraphApplier. If we are
        // not the leader, openraft surfaces a ForwardToLeader error with
        // the leader's address; we transparently forward the original RPC
        // there so clients can write to any peer.
        if let Some(raft) = &self.raft {
            match raft.propose_graph(GraphCommand::PutNode(node)).await {
                Ok(_) => return Ok(Response::new(PutNodeResponse {})),
                Err(ClusterError::ForwardToLeader {
                    leader_address: Some(addr),
                    ..
                }) => {
                    let mut client = leader_write_client(&addr)?;
                    return client
                        .put_node(PutNodeRequest {
                            node: Some(proto_node),
                            local_only,
                        })
                        .await;
                }
                Err(e) => return Err(raft_propose_failed(e)),
            }
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

    #[tracing::instrument(skip_all, fields(rpc = "put_edge"))]
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
            match raft.propose_graph(GraphCommand::PutEdge(edge)).await {
                Ok(_) => return Ok(Response::new(PutEdgeResponse {})),
                Err(ClusterError::ForwardToLeader {
                    leader_address: Some(addr),
                    ..
                }) => {
                    let mut client = leader_write_client(&addr)?;
                    return client
                        .put_edge(PutEdgeRequest {
                            edge: Some(proto_edge),
                            local_only,
                        })
                        .await;
                }
                Err(e) => return Err(raft_propose_failed(e)),
            }
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

    #[tracing::instrument(skip_all, fields(rpc = "delete_edge"))]
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
            match raft.propose_graph(GraphCommand::DeleteEdge(id)).await {
                Ok(_) => return Ok(Response::new(DeleteEdgeResponse {})),
                Err(ClusterError::ForwardToLeader {
                    leader_address: Some(addr),
                    ..
                }) => {
                    let mut client = leader_write_client(&addr)?;
                    return client
                        .delete_edge(DeleteEdgeRequest {
                            edge_id: Some(id_proto),
                            local_only,
                        })
                        .await;
                }
                Err(e) => return Err(raft_propose_failed(e)),
            }
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

    #[tracing::instrument(skip_all, fields(rpc = "detach_delete_node"))]
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
            match raft.propose_graph(GraphCommand::DetachDeleteNode(id)).await {
                Ok(_) => return Ok(Response::new(DetachDeleteNodeResponse {})),
                Err(ClusterError::ForwardToLeader {
                    leader_address: Some(addr),
                    ..
                }) => {
                    let mut client = leader_write_client(&addr)?;
                    return client
                        .detach_delete_node(DetachDeleteNodeRequest {
                            node_id: Some(id_proto),
                            local_only,
                        })
                        .await;
                }
                Err(e) => return Err(raft_propose_failed(e)),
            }
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

    #[tracing::instrument(skip_all, fields(rpc = "batch_write", txid, phase))]
    async fn batch_write(
        &self,
        request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>, Status> {
        let req = request.into_inner();
        let txid = req.txid;
        let phase = BatchPhase::try_from(req.phase)
            .map_err(|_| Status::invalid_argument(format!("unknown phase {}", req.phase)))?;
        tracing::Span::current().record("txid", txid.as_str());
        tracing::Span::current().record("phase", format!("{:?}", phase).as_str());

        if txid.is_empty() {
            return Err(Status::invalid_argument("empty txid"));
        }

        match phase {
            BatchPhase::Unspecified => {
                Err(Status::invalid_argument("phase UNSPECIFIED is not valid"))
            }
            BatchPhase::Prepare => {
                let commands: Vec<GraphCommand> =
                    serde_json::from_slice(&req.commands_json).map_err(bad_request)?;
                let mut pending = self.pending_batches.lock().unwrap();
                if pending.contains_key(&txid) {
                    return Err(Status::already_exists(format!(
                        "txid {} already prepared",
                        txid
                    )));
                }
                pending.insert(txid, commands);
                Ok(Response::new(BatchWriteResponse {}))
            }
            BatchPhase::Commit => {
                let cmds = {
                    let mut pending = self.pending_batches.lock().unwrap();
                    pending.remove(&txid).ok_or_else(|| {
                        Status::failed_precondition(format!(
                            "txid {} not prepared on this peer",
                            txid
                        ))
                    })?
                };
                apply_prepared_batch(&self.store, &cmds).map_err(internal)?;
                Ok(Response::new(BatchWriteResponse {}))
            }
            BatchPhase::Abort => {
                // Abort is always idempotent: dropping the staged entry is
                // safe even if PREPARE never arrived (e.g., coordinator
                // sends ABORT defensively on a partial failure).
                let _ = self.pending_batches.lock().unwrap().remove(&txid);
                Ok(Response::new(BatchWriteResponse {}))
            }
        }
    }
}
