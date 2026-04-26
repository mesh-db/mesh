use crate::PeerId;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("cluster must have at least one peer")]
    NoPeers,

    #[error("partition count must be non-zero")]
    ZeroPartitions,

    #[error("replication_factor {replication_factor} is invalid: must be in [1, {peer_count}]")]
    InvalidReplicationFactor {
        replication_factor: usize,
        peer_count: usize,
    },

    #[error("unknown peer: {0:?}")]
    UnknownPeer(PeerId),

    #[error("partition map length {map_len} does not match partition count {expected}")]
    PartitionMapLengthMismatch { map_len: usize, expected: u32 },

    #[error("peer already exists: {0}")]
    PeerAlreadyExists(PeerId),

    #[error("cannot remove the local peer from the cluster")]
    CannotRemoveSelf,

    #[error("raft error: {0}")]
    Raft(String),

    #[error("apply rejected by state machine: {0}")]
    Apply(String),

    #[error("not the raft leader; forward to peer {leader_id:?} at {leader_address:?}")]
    ForwardToLeader {
        leader_id: Option<PeerId>,
        leader_address: Option<String>,
    },
}
