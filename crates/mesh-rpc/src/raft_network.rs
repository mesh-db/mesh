//! gRPC-backed [`RaftNetwork`] for openraft.
//!
//! Replaces mesh-cluster's [`NoOpNetwork`] for multi-peer clusters. Each
//! openraft request/response is serialized via serde_json into a generic
//! `bytes payload` proto field, so the wire schema stays stable even when
//! openraft's internal types evolve.

use crate::proto::mesh_raft_client::MeshRaftClient;
use crate::proto::{RaftRpcRequest, RaftRpcResponse};
use mesh_cluster::raft::{MeshRaftConfig, NodeId};
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::{Channel, Endpoint};

/// Maps peer ids to their gRPC addresses. The factory holds lazy channels so
/// `new_client` is cheap to call from openraft.
#[derive(Debug, Clone)]
pub struct GrpcNetwork {
    channels: Arc<HashMap<NodeId, Channel>>,
}

impl GrpcNetwork {
    /// Build a network with one lazy gRPC channel per peer.
    pub fn new(
        peers: impl IntoIterator<Item = (NodeId, String)>,
    ) -> Result<Self, GrpcNetworkError> {
        let mut channels = HashMap::new();
        for (id, addr) in peers {
            let uri = format!("http://{}", addr);
            let endpoint =
                Endpoint::from_shared(uri).map_err(|e| GrpcNetworkError::InvalidEndpoint {
                    id,
                    message: e.to_string(),
                })?;
            channels.insert(id, endpoint.connect_lazy());
        }
        Ok(Self {
            channels: Arc::new(channels),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GrpcNetworkError {
    #[error("invalid endpoint for peer {id}: {message}")]
    InvalidEndpoint { id: NodeId, message: String },
}

impl RaftNetworkFactory<MeshRaftConfig> for GrpcNetwork {
    type Network = GrpcNetworkClient;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        // Lookup the pre-built channel for this peer. If it's missing
        // (shouldn't happen in a correctly-configured cluster) the client
        // will surface the error on its first RPC as an Unreachable error.
        let channel = self.channels.get(&target).cloned();
        GrpcNetworkClient { target, channel }
    }
}

pub struct GrpcNetworkClient {
    target: NodeId,
    channel: Option<Channel>,
}

impl GrpcNetworkClient {
    fn client(&self) -> Result<MeshRaftClient<Channel>, NetworkError> {
        match &self.channel {
            Some(ch) => Ok(MeshRaftClient::new(ch.clone())),
            None => Err(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no channel registered for target {}", self.target),
            ))),
        }
    }
}

impl RaftNetwork<MeshRaftConfig> for GrpcNetworkClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MeshRaftConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let payload =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mut client = self.client().map_err(RPCError::Network)?;
        let resp = client
            .append_entries(RaftRpcRequest { payload })
            .await
            .map_err(|s| {
                RPCError::Network(NetworkError::new(&std::io::Error::new(
                    std::io::ErrorKind::Other,
                    s.message().to_string(),
                )))
            })?;
        let result: Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>> =
            serde_json::from_slice(&resp.into_inner().payload)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        result.map_err(|raft_err| RPCError::RemoteError(RemoteError::new(self.target, raft_err)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<MeshRaftConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let payload =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mut client = self.client().map_err(RPCError::Network)?;
        let resp = client
            .install_snapshot(RaftRpcRequest { payload })
            .await
            .map_err(|s| {
                RPCError::Network(NetworkError::new(&std::io::Error::new(
                    std::io::ErrorKind::Other,
                    s.message().to_string(),
                )))
            })?;
        let result: Result<
            InstallSnapshotResponse<NodeId>,
            RaftError<NodeId, InstallSnapshotError>,
        > = serde_json::from_slice(&resp.into_inner().payload)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        result.map_err(|raft_err| RPCError::RemoteError(RemoteError::new(self.target, raft_err)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let payload =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mut client = self.client().map_err(RPCError::Network)?;
        let resp = client.vote(RaftRpcRequest { payload }).await.map_err(|s| {
            RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::Other,
                s.message().to_string(),
            )))
        })?;
        let result: Result<VoteResponse<NodeId>, RaftError<NodeId>> =
            serde_json::from_slice(&resp.into_inner().payload)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        result.map_err(|raft_err| RPCError::RemoteError(RemoteError::new(self.target, raft_err)))
    }
}

// `RaftRpcResponse` is only used via the generated client/server code above,
// but we keep this re-use marker so clippy doesn't complain.
#[allow(dead_code)]
fn _mark_response_used(_r: RaftRpcResponse) {}
