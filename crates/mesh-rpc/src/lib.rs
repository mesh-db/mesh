pub mod proto {
    tonic::include_proto!("mesh");
}

pub mod convert;
mod error;
mod routing;
mod server;

pub use error::ConvertError;
pub use routing::{Routing, RoutingError};
pub use server::MeshService;
