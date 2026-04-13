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
}
