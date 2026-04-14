use mesh_core::{EdgeId, NodeId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("rocksdb: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("core: {0}")]
    Core(#[from] mesh_core::Error),

    #[error("missing column family: {0}")]
    MissingColumnFamily(&'static str),

    #[error("corrupt bytes in {cf}: expected {expected}, got {actual}")]
    CorruptBytes {
        cf: &'static str,
        expected: usize,
        actual: usize,
    },

    #[error("node not found: {0}")]
    NodeNotFound(NodeId),

    #[error("edge not found: {0}")]
    EdgeNotFound(EdgeId),

    #[error("property {property} of type {kind} is not indexable")]
    UnindexableValue {
        property: String,
        kind: &'static str,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
