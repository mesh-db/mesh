//! Server-side handler for the [`MeshRaft`] gRPC service.
//!
//! Wraps a [`RaftGroupRegistry`] and dispatches incoming Raft RPCs by the
//! request's `target` oneof:
//!
//!   * unset → the legacy single-Raft group ([`mode = "raft"`])
//!   * `meta = true` → the multi-raft metadata group
//!   * `partition = N` → multi-raft partition group N
//!
//! Each RPC's payload is a serde_json-encoded openraft request struct; the
//! response payload is a serde_json-encoded `Result<openraft response,
//! RaftError>`.

use crate::proto::mesh_raft_server::{MeshRaft, MeshRaftServer};
use crate::proto::raft_rpc_request::Target;
use crate::proto::{RaftRpcRequest, RaftRpcResponse};
use meshdb_cluster::raft::{MeshRaftConfig, NodeId};
use meshdb_cluster::PartitionId;
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::Raft;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tonic::{Request, Response, Status};

/// Lookup table for which Raft group a given RPC routes to. A peer
/// runs either single-Raft (just `single`) or multi-raft (`meta` plus
/// some subset of `partitions`); the two are mutually exclusive but
/// the registry itself doesn't enforce that — it just dispatches what
/// the bootstrap code wired in.
///
/// `partitions` is RwLock-wrapped so a runtime rebalance can grow the
/// dispatch table mid-flight without restart — see
/// [`Self::register_partition`]. Reads are short read-locks; the
/// dispatch path clones the matched `Arc<Raft>` out of the lock so
/// the lock guard doesn't outlive the RPC handler.
#[derive(Default, Clone)]
pub struct RaftGroupRegistry {
    /// Single-Raft mode's one-and-only group. Targeted by RPCs with no
    /// `target` set, which is what every pre-multi-raft peer sends.
    single: Option<Arc<Raft<MeshRaftConfig>>>,
    /// Multi-raft metadata group, spans every peer.
    meta: Option<Arc<Raft<MeshRaftConfig>>>,
    /// Multi-raft per-partition groups. Only populated for partitions
    /// this peer is a replica of, so a routed RPC for an unknown
    /// partition surfaces as "not found" rather than dispatching to
    /// an uninitialized group.
    partitions: Arc<RwLock<HashMap<PartitionId, Arc<Raft<MeshRaftConfig>>>>>,
}

impl RaftGroupRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a registry with one single-Raft group. The legacy code
    /// path used by `mode = "raft"` and by every pre-multi-raft peer.
    pub fn single(raft: Arc<Raft<MeshRaftConfig>>) -> Self {
        Self {
            single: Some(raft),
            ..Self::default()
        }
    }

    pub fn with_meta(mut self, raft: Arc<Raft<MeshRaftConfig>>) -> Self {
        self.meta = Some(raft);
        self
    }

    pub fn with_partition(self, partition: PartitionId, raft: Arc<Raft<MeshRaftConfig>>) -> Self {
        self.partitions
            .write()
            .expect("registry partitions lock poisoned")
            .insert(partition, raft);
        self
    }

    /// Register a partition Raft handle at runtime. Used by the
    /// rebalancing path when a peer is added as a replica of a
    /// partition it didn't bootstrap with: after spinning up the
    /// new partition Raft, the dispatch table needs to know about
    /// it so other peers' AppendEntries land. Idempotent — re-
    /// registering replaces the existing entry, which is how a
    /// re-issued rebalance reconciles after a transient failure.
    pub fn register_partition(&self, partition: PartitionId, raft: Arc<Raft<MeshRaftConfig>>) {
        self.partitions
            .write()
            .expect("registry partitions lock poisoned")
            .insert(partition, raft);
    }

    /// Drop a partition Raft handle from the dispatch table. Used
    /// when a peer is evacuated from a partition's replica set —
    /// after the partition Raft's `change_membership` removes this
    /// peer, the dispatch table should reject inbound RPCs targeting
    /// the now-orphaned local replica. Idempotent.
    pub fn unregister_partition(&self, partition: PartitionId) {
        self.partitions
            .write()
            .expect("registry partitions lock poisoned")
            .remove(&partition);
    }

    fn route(&self, target: Option<&Target>) -> Result<Arc<Raft<MeshRaftConfig>>, Status> {
        match target {
            None => self.single.clone().ok_or_else(|| {
                // Legacy unset target = single-Raft mode caller.
                // If this peer is running multi-raft (meta or
                // partitions populated), the caller is mixed-mode,
                // which we don't support; surface the diagnosis
                // explicitly so the operator can fix the config.
                let has_partitions = !self
                    .partitions
                    .read()
                    .expect("registry partitions lock poisoned")
                    .is_empty();
                if self.meta.is_some() || has_partitions {
                    Status::failed_precondition(
                        "received single-Raft RPC on a multi-raft peer — \
                         all peers in a cluster must run the same `mode` \
                         (mixed `raft` / `multi-raft` clusters are not supported)",
                    )
                } else {
                    Status::not_found("no single-Raft group configured on this peer")
                }
            }),
            Some(Target::Meta(true)) => self.meta.clone().ok_or_else(|| {
                if self.single.is_some() {
                    Status::failed_precondition(
                        "received multi-raft meta RPC on a single-Raft peer — \
                         all peers in a cluster must run the same `mode`",
                    )
                } else {
                    Status::not_found("no metadata Raft group configured on this peer")
                }
            }),
            Some(Target::Meta(false)) => Err(Status::invalid_argument(
                "RaftRpcRequest.target.meta = false; pass `true` or omit the field",
            )),
            Some(Target::Partition(p)) => {
                let guard = self
                    .partitions
                    .read()
                    .expect("registry partitions lock poisoned");
                guard.get(&PartitionId(*p)).cloned().ok_or_else(|| {
                    if self.single.is_some() {
                        Status::failed_precondition(
                            "received multi-raft partition RPC on a single-Raft peer — \
                             all peers in a cluster must run the same `mode`",
                        )
                    } else {
                        Status::not_found(format!(
                            "partition Raft group {p} is not hosted on this peer"
                        ))
                    }
                })
            }
        }
    }
}

pub struct MeshRaftService {
    registry: RaftGroupRegistry,
}

impl MeshRaftService {
    /// Single-Raft constructor — used by `mode = "raft"`.
    pub fn new(raft: Raft<MeshRaftConfig>) -> Self {
        Self {
            registry: RaftGroupRegistry::single(Arc::new(raft)),
        }
    }

    /// Multi-raft constructor.
    pub fn with_registry(registry: RaftGroupRegistry) -> Self {
        Self { registry }
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
    serde_json::to_vec(value).map_err(|e| Status::internal(format!("encoding raft response: {e}")))
}

#[tonic::async_trait]
impl MeshRaft for MeshRaftService {
    async fn append_entries(
        &self,
        request: Request<RaftRpcRequest>,
    ) -> Result<Response<RaftRpcResponse>, Status> {
        let req = request.into_inner();
        let raft = self.registry.route(req.target.as_ref())?;
        let rpc: AppendEntriesRequest<MeshRaftConfig> = decode_payload(&req.payload)?;
        let result: Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>> =
            raft.append_entries(rpc).await;
        let payload = encode_payload(&result)?;
        Ok(Response::new(RaftRpcResponse { payload }))
    }

    async fn vote(
        &self,
        request: Request<RaftRpcRequest>,
    ) -> Result<Response<RaftRpcResponse>, Status> {
        let req = request.into_inner();
        let raft = self.registry.route(req.target.as_ref())?;
        let rpc: VoteRequest<NodeId> = decode_payload(&req.payload)?;
        let result: Result<VoteResponse<NodeId>, RaftError<NodeId>> = raft.vote(rpc).await;
        let payload = encode_payload(&result)?;
        Ok(Response::new(RaftRpcResponse { payload }))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftRpcRequest>,
    ) -> Result<Response<RaftRpcResponse>, Status> {
        let req = request.into_inner();
        let raft = self.registry.route(req.target.as_ref())?;
        let rpc: InstallSnapshotRequest<MeshRaftConfig> = decode_payload(&req.payload)?;
        let result: Result<
            InstallSnapshotResponse<NodeId>,
            RaftError<NodeId, InstallSnapshotError>,
        > = raft.install_snapshot(rpc).await;
        let payload = encode_payload(&result)?;
        Ok(Response::new(RaftRpcResponse { payload }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Result::unwrap_err` requires `Debug` on the Ok branch, but
    /// `Raft<MeshRaftConfig>` doesn't implement it. This helper
    /// extracts the `Status` without that bound.
    fn err(result: Result<Arc<Raft<MeshRaftConfig>>, Status>) -> Status {
        match result {
            Ok(_) => panic!("expected error, got Ok"),
            Err(e) => e,
        }
    }

    #[test]
    fn raft_rpc_request_default_target_routes_to_legacy() {
        // Pre-multi-raft peers serialize `RaftRpcRequest` without
        // setting any `target` variant — prost decodes that as
        // `target == None`, which the registry dispatches to the
        // legacy single-Raft group. Pin the contract so a future
        // proto change that flips the default doesn't silently
        // re-route legacy traffic to the meta or a partition group.
        let req = RaftRpcRequest {
            payload: vec![1, 2, 3],
            target: None,
        };
        // Sanity-round-trip through prost bytes — this is what an
        // older binary on the wire produces.
        use prost::Message;
        let bytes = req.encode_to_vec();
        let decoded = RaftRpcRequest::decode(&bytes[..]).unwrap();
        assert_eq!(decoded.payload, vec![1, 2, 3]);
        assert!(
            decoded.target.is_none(),
            "decoded target should be None for unset wire field"
        );

        // Empty registry routes the unset target to the
        // single-Raft slot, which is also unset → NotFound (vs.
        // dispatching to the meta or partition slot, which would
        // be a wire-compat regression).
        let registry = RaftGroupRegistry::new();
        let e = err(registry.route(decoded.target.as_ref()));
        assert_eq!(e.code(), tonic::Code::NotFound);
        assert!(
            e.message().contains("single-Raft"),
            "unset target must route to single-Raft slot; got {}",
            e.message()
        );
    }

    #[test]
    fn empty_registry_rejects_legacy_target_with_not_found() {
        let registry = RaftGroupRegistry::new();
        let e = err(registry.route(None));
        assert_eq!(e.code(), tonic::Code::NotFound);
        assert!(e.message().contains("single-Raft"), "msg: {}", e.message());
    }

    #[test]
    fn empty_registry_rejects_meta_target_with_not_found() {
        let registry = RaftGroupRegistry::new();
        let e = err(registry.route(Some(&Target::Meta(true))));
        assert_eq!(e.code(), tonic::Code::NotFound);
        assert!(e.message().contains("metadata"), "msg: {}", e.message());
    }

    #[test]
    fn empty_registry_rejects_partition_target_with_not_found() {
        let registry = RaftGroupRegistry::new();
        let e = err(registry.route(Some(&Target::Partition(2))));
        assert_eq!(e.code(), tonic::Code::NotFound);
        assert!(e.message().contains("partition"), "msg: {}", e.message());
    }

    #[test]
    fn meta_false_is_explicitly_rejected_as_invalid_argument() {
        // The wire format reserves `meta = true` for the metadata
        // group; an explicit `meta = false` would be ambiguous (does
        // it mean "not meta, not partition either" or a poorly-formed
        // single-Raft request?). Reject loudly so it's a configuration
        // bug rather than a silent dispatch.
        let registry = RaftGroupRegistry::new();
        let e = err(registry.route(Some(&Target::Meta(false))));
        assert_eq!(e.code(), tonic::Code::InvalidArgument);
    }
}
