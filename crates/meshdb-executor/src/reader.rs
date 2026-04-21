use crate::error::Result;
use meshdb_core::{Edge, EdgeId, Node, NodeId, Property};
use meshdb_storage::{PropertyConstraintSpec, StorageEngine};

/// Read-side counterpart to [`crate::GraphWriter`]. Gives the executor a
/// uniform view of the graph regardless of whether the data behind it lives
/// entirely in a local storage engine (single-node or full-replica Raft
/// mode) or is sharded across cluster peers (routing mode, where a
/// partitioned reader fans out point reads to owners and scatter-gathers
/// bulk scans).
///
/// Methods are sync because the executor's iterator model is sync. Async-
/// backed implementations (e.g. a remote reader that talks gRPC) bridge via
/// `Handle::block_on`; callers must run the executor inside `spawn_blocking`
/// so they don't stall the tokio runtime.
pub trait GraphReader: Send + Sync {
    fn get_node(&self, id: NodeId) -> Result<Option<Node>>;
    fn get_edge(&self, id: EdgeId) -> Result<Option<Edge>>;
    fn all_node_ids(&self) -> Result<Vec<NodeId>>;
    fn nodes_by_label(&self, label: &str) -> Result<Vec<NodeId>>;
    /// Equality lookup via a property index. Callers (planner) must
    /// have verified the `(label, property)` index exists before
    /// emitting this call — fallback implementations are free to do a
    /// label-scan-and-filter for impls that don't maintain their own
    /// property index, but the storage-backed reader treats a call on
    /// a non-existent index as an empty result since no entries are
    /// maintained.
    fn nodes_by_property(
        &self,
        label: &str,
        property: &str,
        value: &Property,
    ) -> Result<Vec<NodeId>>;
    /// Snapshot the `(label, property)` pairs of every property
    /// index visible through this reader. Used by `SHOW INDEXES`.
    /// Default impl returns empty — the storage-backed reader
    /// overrides via the blanket impl, and partitioned/overlay
    /// readers delegate to their bases.
    fn list_property_indexes(&self) -> Result<Vec<(String, String)>> {
        Ok(Vec::new())
    }
    /// Relationship-scope analogue of [`Self::list_property_indexes`].
    /// Returns `(edge_type, property)` pairs for every registered
    /// edge property index. Default impl returns empty; overlay
    /// and partitioned readers delegate to their bases.
    fn list_edge_property_indexes(&self) -> Result<Vec<(String, String)>> {
        Ok(Vec::new())
    }
    /// Snapshot every registered constraint visible through this
    /// reader, for `SHOW CONSTRAINTS` and `db.constraints()`. Default
    /// impl returns empty; storage-backed readers override.
    fn list_property_constraints(&self) -> Result<Vec<PropertyConstraintSpec>> {
        Ok(Vec::new())
    }
    fn outgoing(&self, id: NodeId) -> Result<Vec<(EdgeId, NodeId)>>;
    fn incoming(&self, id: NodeId) -> Result<Vec<(EdgeId, NodeId)>>;
}

/// Blanket impl: any **sized** type that implements [`StorageEngine`]
/// is automatically a [`GraphReader`]. Covers the concrete
/// [`meshdb_storage::RocksDbStorageEngine`] — a `&RocksDbStorageEngine`
/// coerces to `&dyn GraphReader` via this blanket.
///
/// Not covered: `dyn StorageEngine` itself. Rust does not transitively
/// coerce `&dyn StorageEngine` to `&dyn GraphReader` because trait
/// objects of unrelated traits carry different vtables and there's no
/// supertrait relationship connecting them. Call sites that hold a
/// `&dyn StorageEngine` should use [`StorageReaderAdapter`] to wrap it
/// as a `GraphReader`.
impl<T: StorageEngine> GraphReader for T {
    fn get_node(&self, id: NodeId) -> Result<Option<Node>> {
        Ok(StorageEngine::get_node(self, id)?)
    }

    fn get_edge(&self, id: EdgeId) -> Result<Option<Edge>> {
        Ok(StorageEngine::get_edge(self, id)?)
    }

    fn all_node_ids(&self) -> Result<Vec<NodeId>> {
        Ok(StorageEngine::all_node_ids(self)?)
    }

    fn nodes_by_label(&self, label: &str) -> Result<Vec<NodeId>> {
        Ok(StorageEngine::nodes_by_label(self, label)?)
    }

    fn nodes_by_property(
        &self,
        label: &str,
        property: &str,
        value: &Property,
    ) -> Result<Vec<NodeId>> {
        Ok(StorageEngine::nodes_by_property(
            self, label, property, value,
        )?)
    }

    fn list_property_indexes(&self) -> Result<Vec<(String, String)>> {
        Ok(StorageEngine::list_property_indexes(self)
            .into_iter()
            .map(|s| (s.label, s.property))
            .collect())
    }

    fn list_edge_property_indexes(&self) -> Result<Vec<(String, String)>> {
        Ok(StorageEngine::list_edge_property_indexes(self)
            .into_iter()
            .map(|s| (s.edge_type, s.property))
            .collect())
    }

    fn list_property_constraints(&self) -> Result<Vec<PropertyConstraintSpec>> {
        Ok(StorageEngine::list_property_constraints(self))
    }

    fn outgoing(&self, id: NodeId) -> Result<Vec<(EdgeId, NodeId)>> {
        Ok(StorageEngine::outgoing(self, id)?)
    }

    fn incoming(&self, id: NodeId) -> Result<Vec<(EdgeId, NodeId)>> {
        Ok(StorageEngine::incoming(self, id)?)
    }
}

/// Adapter that lets a `&dyn StorageEngine` act as a `GraphReader`.
/// Needed because trait objects of unrelated traits don't coerce —
/// see the note on the blanket `impl<T: StorageEngine> GraphReader for T`.
/// Wraps a trait-object reference; no heap allocation. Works for both
/// reads and writes via the sibling [`crate::writer::StorageWriterAdapter`].
pub struct StorageReaderAdapter<'a>(pub &'a dyn StorageEngine);

impl GraphReader for StorageReaderAdapter<'_> {
    fn get_node(&self, id: NodeId) -> Result<Option<Node>> {
        Ok(self.0.get_node(id)?)
    }

    fn get_edge(&self, id: EdgeId) -> Result<Option<Edge>> {
        Ok(self.0.get_edge(id)?)
    }

    fn all_node_ids(&self) -> Result<Vec<NodeId>> {
        Ok(self.0.all_node_ids()?)
    }

    fn nodes_by_label(&self, label: &str) -> Result<Vec<NodeId>> {
        Ok(self.0.nodes_by_label(label)?)
    }

    fn nodes_by_property(
        &self,
        label: &str,
        property: &str,
        value: &Property,
    ) -> Result<Vec<NodeId>> {
        Ok(self.0.nodes_by_property(label, property, value)?)
    }

    fn list_property_indexes(&self) -> Result<Vec<(String, String)>> {
        Ok(self
            .0
            .list_property_indexes()
            .into_iter()
            .map(|s| (s.label, s.property))
            .collect())
    }

    fn list_edge_property_indexes(&self) -> Result<Vec<(String, String)>> {
        Ok(self
            .0
            .list_edge_property_indexes()
            .into_iter()
            .map(|s| (s.edge_type, s.property))
            .collect())
    }

    fn list_property_constraints(&self) -> Result<Vec<PropertyConstraintSpec>> {
        Ok(self.0.list_property_constraints())
    }

    fn outgoing(&self, id: NodeId) -> Result<Vec<(EdgeId, NodeId)>> {
        Ok(self.0.outgoing(id)?)
    }

    fn incoming(&self, id: NodeId) -> Result<Vec<(EdgeId, NodeId)>> {
        Ok(self.0.incoming(id)?)
    }
}
