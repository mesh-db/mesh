//! Server-side handler for the [`MeshRaft`] gRPC service.
//!
//! Wraps a local `Raft<MeshRaftConfig>` instance and dispatches incoming
//! Raft RPCs (AppendEntries / Vote / InstallSnapshot) to it. Each RPC's
//! payload is a serde_json-encoded openraft request struct; the response
//! payload is a serde_json-encoded `Result<openraft response, RaftError>`.

use crate::proto::mesh_raft_server::{MeshRaft, MeshRaftServer};
use crate::proto::{RaftRpcRequest, RaftRpcResponse};
use mesh_cluster::raft::{MeshRaftConfig, NodeId};
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::Raft;
use tonic::{Request, Response, Status};

pub struct MeshRaftService {
    raft: Raft<MeshRaftConfig>,
}

impl MeshRaftService {
    pub fn new(raft: Raft<MeshRaftConfig>) -> Self {
        Self { raft }
    }

    pub fn into_server(self) -> MeshRaftServer<Self> {
        MeshRaftServer::new(self)
    }
}

fn decode_payload<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, Status> {
    serde_json::from_slice(payload)
        .map_err(|e| Status::invalid_argument(format!("invalid raft payload: {e}")))
}

fn encode_payload<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, Status> {
    serde_json::to_vec(value)
        .map_err(|e| Status::internal(format!("encoding raft response: {e}")))
}

#[tonic::async_trait]
impl MeshRaft for MeshRaftService {
    async fn append_entries(
        &self,
        request: Request<RaftRpcRequest>,
    ) -> Result<Response<RaftRpcResponse>, Status> {
        let rpc: AppendEntriesRequest<MeshRaftConfig> =
            decode_payload(&request.into_inner().payload)?;
        let result: Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>> =
            self.raft.append_entries(rpc).await;
        let payload = encode_payload(&result)?;
        Ok(Response::new(RaftRpcResponse { payload }))
    }

    async fn vote(
        &self,
        request: Request<RaftRpcRequest>,
    ) -> Result<Response<RaftRpcResponse>, Status> {
        let rpc: VoteRequest<NodeId> = decode_payload(&request.into_inner().payload)?;
        let result: Result<VoteResponse<NodeId>, RaftError<NodeId>> =
            self.raft.vote(rpc).await;
        let payload = encode_payload(&result)?;
        Ok(Response::new(RaftRpcResponse { payload }))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftRpcRequest>,
    ) -> Result<Response<RaftRpcResponse>, Status> {
        let rpc: InstallSnapshotRequest<MeshRaftConfig> =
            decode_payload(&request.into_inner().payload)?;
        let result: Result<
            InstallSnapshotResponse<NodeId>,
            RaftError<NodeId, InstallSnapshotError>,
        > = self.raft.install_snapshot(rpc).await;
        let payload = encode_payload(&result)?;
        Ok(Response::new(RaftRpcResponse { payload }))
    }
}
