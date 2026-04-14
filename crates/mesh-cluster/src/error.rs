use crate::PeerId;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("cluster must have at least one peer")]
    NoPeers,

    #[error("partition count must be non-zero")]
    ZeroPartitions,

    #[error("unknown peer: {0:?}")]
    UnknownPeer(PeerId),

    #[error(
        "partition map length {map_len} does not match partition count {expected}"
    )]
    PartitionMapLengthMismatch { map_len: usize, expected: u32 },

    #[error("peer already exists: {0}")]
    PeerAlreadyExists(PeerId),

    #[error("cannot remove the local peer from the cluster")]
    CannotRemoveSelf,

    #[error("raft error: {0}")]
    Raft(String),

    #[error(
        "not the raft leader; forward to peer {leader_id:?} at {leader_address:?}"
    )]
    ForwardToLeader {
        leader_id: Option<PeerId>,
        leader_address: Option<String>,
    },
}
