pub mod proto {
    tonic::include_proto!("mesh");
}

pub mod convert;
mod error;
mod server;

pub use error::ConvertError;
pub use server::MeshService;
