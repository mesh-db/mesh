pub mod proto {
    tonic::include_proto!("mesh");
}

pub mod convert;
mod error;
mod raft_network;
mod raft_service;
mod routing;
mod server;

pub use error::ConvertError;
pub use raft_network::{GrpcNetwork, GrpcNetworkError};
pub use raft_service::MeshRaftService;
pub use routing::{Routing, RoutingError};
pub use server::MeshService;
