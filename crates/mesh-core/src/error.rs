use crate::{EdgeId, NodeId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid property type: expected {expected}, got {actual}")]
    InvalidPropertyType {
        expected: &'static str,
        actual: &'static str,
    },

    #[error("node not found: {0}")]
    NodeNotFound(NodeId),

    #[error("edge not found: {0}")]
    EdgeNotFound(EdgeId),
}

pub type Result<T> = std::result::Result<T, Error>;
