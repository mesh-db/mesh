mod cluster;
mod error;
mod membership;
mod partition_map;
mod partitioner;
mod state;

pub use cluster::Cluster;
pub use error::Error;
pub use membership::{Membership, Peer, PeerId};
pub use partition_map::PartitionMap;
pub use partitioner::{PartitionId, Partitioner};
pub use state::{ClusterCommand, ClusterState};

pub type Result<T> = std::result::Result<T, Error>;
