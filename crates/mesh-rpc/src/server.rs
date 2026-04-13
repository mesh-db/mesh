use crate::convert::{
    edge_to_proto, node_id_from_proto, node_to_proto, uuid_to_proto,
};
use crate::proto::mesh_query_server::{MeshQuery, MeshQueryServer};
use crate::proto::{
    GetEdgeRequest, GetEdgeResponse, GetNodeRequest, GetNodeResponse, HealthRequest,
    HealthResponse, NeighborInfo, NeighborRequest, NeighborResponse, NodesByLabelRequest,
    NodesByLabelResponse,
};
use mesh_storage::Store;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct MeshService {
    store: Arc<Store>,
}

impl MeshService {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }

    pub fn into_server(self) -> MeshQueryServer<Self> {
        MeshQueryServer::new(self)
    }
}

fn internal<E: std::fmt::Display>(e: E) -> Status {
    Status::internal(e.to_string())
}

fn bad_request<E: std::fmt::Display>(e: E) -> Status {
    Status::invalid_argument(e.to_string())
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
        let id_proto = request
            .into_inner()
            .id
            .ok_or_else(|| Status::invalid_argument("missing id"))?;
        let id = crate::convert::edge_id_from_proto(&id_proto).map_err(bad_request)?;
        let edge = self.store.get_edge(id).map_err(internal)?;
        let (found, edge) = match edge {
            Some(e) => (true, Some(edge_to_proto(&e).map_err(internal)?)),
            None => (false, None),
        };
        Ok(Response::new(GetEdgeResponse { found, edge }))
    }

    async fn nodes_by_label(
        &self,
        request: Request<NodesByLabelRequest>,
    ) -> Result<Response<NodesByLabelResponse>, Status> {
        let label = request.into_inner().label;
        let ids = self.store.nodes_by_label(&label).map_err(internal)?;
        let ids = ids
            .into_iter()
            .map(|id| uuid_to_proto(id.as_uuid()))
            .collect();
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
