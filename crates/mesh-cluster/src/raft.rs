//! openraft integration — type-layer scaffold.
//!
//! This module binds our domain types ([`ClusterCommand`] as the log payload,
//! [`ApplyResponse`] as the state-machine reply) to openraft's replication
//! machinery via [`MeshRaftConfig`]. The actual `RaftLogStorage`,
//! `RaftStateMachine`, and `RaftNetwork` trait impls — plus a single-node
//! bootstrap harness — land in follow-up steps. Getting the type layer right
//! in isolation means the storage/state-machine/network layers can each be
//! debugged against a stable `RaftTypeConfig` rather than a moving target.

use crate::ClusterCommand;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

/// Cluster-wide peer identifier as seen by openraft. Distinct from
/// [`crate::PeerId`] only so that the openraft machinery can have its own
/// type alias it treats as a primitive; the `From` impls below convert
/// between them.
pub type NodeId = u64;

/// Response returned from the state machine when an entry is applied.
///
/// Today this is a unit marker — applying a [`ClusterCommand`] is
/// side-effecting (it mutates the cluster state) and doesn't produce a
/// useful reply, but the type needs a slot for Raft's internal plumbing.
/// A future step will enrich this with, for example, the applied log index
/// so clients can wait for their write to be committed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyResponse;

openraft::declare_raft_types!(
    /// Type binding for Mesh's Raft consensus layer.
    pub MeshRaftConfig:
        D = ClusterCommand,
        R = ApplyResponse,
        NodeId = NodeId,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<MeshRaftConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

impl From<crate::PeerId> for NodeId {
    fn from(p: crate::PeerId) -> Self {
        p.0
    }
}

impl From<NodeId> for crate::PeerId {
    fn from(id: NodeId) -> Self {
        crate::PeerId(id)
    }
}

/// Default Raft config tuned for tests — short heartbeats and elections so
/// the single-node bootstrap path resolves quickly.
pub fn default_config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 250,
        election_timeout_min: 500,
        election_timeout_max: 1000,
        ..openraft::Config::default()
    }
}
