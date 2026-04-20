mod cluster;
mod error;
mod membership;
mod partition_map;
mod partitioner;
pub mod raft;
mod state;

pub use cluster::Cluster;
pub use error::Error;
pub use membership::{Membership, Peer, PeerId};
pub use partition_map::PartitionMap;
pub use partitioner::{PartitionId, Partitioner};
pub use state::{
    resolved_constraint_name, ClusterCommand, ClusterState, ConstraintKind, ConstraintScope,
    GraphCommand, MeshLogEntry, PropertyType,
};

pub type Result<T> = std::result::Result<T, Error>;
