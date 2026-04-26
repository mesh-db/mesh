// The combination of tonic's generated types, deeply-nested
// async fns with `#[tracing::instrument]`, and the
// async-recursive trigger fire path (commit_buffered_commands
// → fire_triggers_after_commit → commit_buffered_commands_inner)
// pushes rustc's per-query layout computation past the default
// 128 depth on at least the CI toolchain — manifests as
// `error: queries overflow the depth limit!` on the
// `execute_cypher` async block. The compiler suggests exactly
// this attribute; bumping it once for the crate keeps adding
// new `#[tracing::instrument]` sites from surprising the next
// CI run.
#![recursion_limit = "256"]

pub mod proto {
    tonic::include_proto!("mesh");
}

pub mod convert;
mod coordinator_log;
mod error;
mod executor_writer;
#[cfg(any(test, feature = "fault-inject"))]
pub mod fault_points;
pub mod metrics;
mod multi_raft_applier;
mod multi_raft_cluster;
mod participant_log;
mod partitioned_reader;
mod raft_applier;
mod raft_network;
mod raft_service;
mod routing;
mod routing_writer;
mod server;
mod staging;
pub mod tls;
mod tx_coordinator;
mod tx_overlay;

pub use coordinator_log::{
    CoordinatorLog, TxDecision, TxLogEntry, TxState, DEFAULT_MIN_COMPLETED,
    DEFAULT_ROTATION_INTERVAL,
};
pub use error::ConvertError;
pub use executor_writer::{BufferingGraphWriter, RaftGraphWriter};
#[cfg(any(test, feature = "fault-inject"))]
pub use fault_points::FaultPoints;
pub use multi_raft_applier::{MetaGraphApplier, PartitionGraphApplier};
pub use multi_raft_cluster::{
    MultiRaftCluster, PartitionLeaderCache, DEFAULT_DDL_STRICT_TIMEOUT, DEFAULT_RECOVERY_INTERVAL,
};
pub use participant_log::{
    replay_in_doubt_commands, replay_outcomes, ParticipantLog, ParticipantLogEntry,
    ParticipantOutcome, PARTICIPANT_LOG_MIN_TERMINAL, PARTICIPANT_LOG_ROTATION_INTERVAL,
};
pub use partitioned_reader::PartitionedGraphReader;
pub use raft_applier::StoreGraphApplier;
pub use raft_network::{GrpcNetwork, GrpcNetworkError, RaftGroupTarget};
pub use raft_service::{MeshRaftService, RaftGroupRegistry};
pub use routing::{Routing, RoutingError};
pub use routing_writer::RoutingGraphWriter;
pub use server::MeshService;
pub use staging::{
    InsertOutcome, ParticipantStaging, TerminalOutcome, DEFAULT_STAGING_TTL, DEFAULT_SWEEP_INTERVAL,
};
pub use tx_coordinator::TxCoordinatorTimeouts;
pub use tx_overlay::{OverlayGraphReader, TxOverlayState};
