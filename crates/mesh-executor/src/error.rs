use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("storage: {0}")]
    Storage(#[from] mesh_storage::Error),

    #[error("unbound variable: {0}")]
    UnboundVariable(String),

    #[error("expected boolean value")]
    NotBoolean,

    #[error("type mismatch in comparison")]
    TypeMismatch,

    #[error("cannot access property on non-node/edge value")]
    NotNodeOrEdge,

    #[error("unsupported comparison for type")]
    UnsupportedComparison,

    #[error("cannot DELETE node with attached edges (use DETACH DELETE)")]
    CannotDeleteAttachedNode,

    #[error("SET value must be a primitive property, not a node or edge")]
    InvalidSetValue,
}

pub type Result<T> = std::result::Result<T, Error>;
