use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("storage: {0}")]
    Storage(#[from] mesh_storage::Error),

    #[error("unbound variable: {0}")]
    UnboundVariable(String),

    #[error("unbound parameter: ${0}")]
    UnboundParameter(String),

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

    #[error("function `{0}` is not a scalar function; only aggregate calls are supported")]
    UnknownScalarFunction(String),

    #[error("aggregate argument has unsupported type")]
    AggregateTypeError,

    #[error("write failed: {0}")]
    Write(String),

    #[error("remote read failed: {0}")]
    Remote(String),

    #[error("unsupported: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, Error>;
