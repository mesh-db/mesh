use meshdb_core::{EdgeId, NodeId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("rocksdb: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("core: {0}")]
    Core(#[from] meshdb_core::Error),

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

    /// Two constraint declarations collided on the same name with
    /// incompatible specs. Raised by `create_property_constraint` when
    /// `IF NOT EXISTS` is absent and the name is already taken by a
    /// different `(label, property, kind)`. The resolver is "name wins"
    /// so the caller can't transparently re-declare under a different
    /// shape — they have to DROP first.
    #[error(
        "a constraint named `{name}` already exists with a different definition; \
         drop it before re-declaring"
    )]
    ConstraintNameConflict { name: String },

    /// `DROP CONSTRAINT` targeted a name that isn't registered and the
    /// `IF EXISTS` escape wasn't supplied. Callers wrap this for
    /// user-facing surfaces.
    #[error("no constraint named `{name}`")]
    ConstraintNotFound { name: String },

    /// Property-arity mismatch: the caller passed a property list
    /// whose length is wrong for the constraint kind — e.g. two
    /// properties to a `UNIQUE` (which accepts exactly one) or an
    /// empty list to any kind. Surfaced so the Cypher surface can
    /// give a clear error instead of silently clipping the list.
    #[error("invalid property arity for {kind}: {details}")]
    ConstraintArity { kind: String, details: String },

    /// A write would put the store into a state that violates a
    /// registered constraint. The `kind` field carries the constraint
    /// type (e.g. `UNIQUE`, `NOT NULL`, `IS :: STRING`) so callers can
    /// format a clear message. `kind` is `String` rather than
    /// `&'static str` because `PropertyConstraintKind::PropertyType`
    /// carries a runtime-selected type name.
    #[error("constraint `{name}` violated: {kind} on {label}.{property} {details}")]
    ConstraintViolation {
        name: String,
        kind: String,
        label: String,
        property: String,
        details: String,
    },

    /// `DROP INDEX` targeted a property index that backs an active
    /// `UNIQUE` or `NODE KEY` constraint. Without this guard, dropping
    /// the backing index silently defeats the constraint — the
    /// enforcement path seeks through the same index, so a missing
    /// index collapses the uniqueness check to "always passes". The
    /// caller must `DROP CONSTRAINT` first.
    #[error(
        "cannot drop property index: it backs active constraint `{constraint}`; \
         drop the constraint first"
    )]
    IndexInUse { constraint: String },
}

pub type Result<T> = std::result::Result<T, Error>;
