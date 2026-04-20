use crate::error::Result;
use meshdb_core::{Edge, EdgeId, Node, NodeId};
use meshdb_storage::{
    ConstraintScope, PropertyConstraintKind, PropertyConstraintSpec, StorageEngine,
};

/// Sink for mutating graph operations produced by the executor. Isolates
/// write-side concerns from read-side traversal so we can plug in either a
/// direct-to-storage writer (single-node mode) or a Raft-backed writer that
/// proposes each mutation through consensus (cluster mode).
///
/// Methods are sync because the executor's iterator model is sync.
/// Async-backed implementations (e.g. the Raft writer) bridge via
/// `Handle::block_on`; callers must run the executor inside
/// `spawn_blocking` so they don't stall the tokio runtime.
pub trait GraphWriter {
    fn put_node(&self, node: &Node) -> Result<()>;
    fn put_edge(&self, edge: &Edge) -> Result<()>;
    fn delete_edge(&self, id: EdgeId) -> Result<()>;
    fn detach_delete_node(&self, id: NodeId) -> Result<()>;

    /// Declare a new property index. Default impl errors so remote
    /// writers (Raft, routing) that don't yet support cluster-aware
    /// DDL surface the limitation immediately. Storage-backed writers
    /// override this via the blanket impl.
    fn create_property_index(&self, _label: &str, _property: &str) -> Result<()> {
        Err(crate::error::Error::Unsupported(
            "property-index DDL is not supported by this writer".into(),
        ))
    }

    /// Tear down a property index. Mirrors [`Self::create_property_index`].
    fn drop_property_index(&self, _label: &str, _property: &str) -> Result<()> {
        Err(crate::error::Error::Unsupported(
            "property-index DDL is not supported by this writer".into(),
        ))
    }

    /// Snapshot the currently-registered property indexes as
    /// `(label, property)` pairs for `SHOW INDEXES`. Default impl
    /// returns an empty list — remote writers will wire real
    /// fan-out in Phase C.
    fn list_property_indexes(&self) -> Result<Vec<(String, String)>> {
        Ok(Vec::new())
    }

    /// Declare a new property constraint. Default impl errors so
    /// remote writers that haven't plumbed constraint DDL yet surface
    /// the limitation immediately — storage-backed writers override
    /// via the blanket impl. `properties` is a list to accommodate
    /// composite kinds (`NodeKey`); single-property kinds pass a
    /// one-element slice.
    fn create_property_constraint(
        &self,
        _name: Option<&str>,
        _scope: &ConstraintScope,
        _properties: &[String],
        _kind: PropertyConstraintKind,
        _if_not_exists: bool,
    ) -> Result<PropertyConstraintSpec> {
        Err(crate::error::Error::Unsupported(
            "constraint DDL is not supported by this writer".into(),
        ))
    }

    /// Tear down a constraint by name. Mirrors
    /// [`Self::create_property_constraint`].
    fn drop_property_constraint(&self, _name: &str, _if_exists: bool) -> Result<()> {
        Err(crate::error::Error::Unsupported(
            "constraint DDL is not supported by this writer".into(),
        ))
    }

    /// Snapshot the currently-registered constraints for
    /// `SHOW CONSTRAINTS`. Default impl returns an empty list.
    fn list_property_constraints(&self) -> Result<Vec<PropertyConstraintSpec>> {
        Ok(Vec::new())
    }
}

/// Blanket impl: any **sized** type that implements [`StorageEngine`]
/// is automatically a [`GraphWriter`]. See the matching
/// [`crate::reader::GraphReader`] blanket for rationale, and
/// [`StorageWriterAdapter`] for the trait-object adapter.
impl<T: StorageEngine> GraphWriter for T {
    fn put_node(&self, node: &Node) -> Result<()> {
        StorageEngine::put_node(self, node)?;
        Ok(())
    }

    fn put_edge(&self, edge: &Edge) -> Result<()> {
        StorageEngine::put_edge(self, edge)?;
        Ok(())
    }

    fn delete_edge(&self, id: EdgeId) -> Result<()> {
        if StorageEngine::get_edge(self, id)?.is_some() {
            StorageEngine::delete_edge(self, id)?;
        }
        Ok(())
    }

    fn detach_delete_node(&self, id: NodeId) -> Result<()> {
        StorageEngine::detach_delete_node(self, id)?;
        Ok(())
    }

    fn create_property_index(&self, label: &str, property: &str) -> Result<()> {
        StorageEngine::create_property_index(self, label, property)?;
        Ok(())
    }

    fn drop_property_index(&self, label: &str, property: &str) -> Result<()> {
        StorageEngine::drop_property_index(self, label, property)?;
        Ok(())
    }

    fn list_property_indexes(&self) -> Result<Vec<(String, String)>> {
        Ok(StorageEngine::list_property_indexes(self)
            .into_iter()
            .map(|s| (s.label, s.property))
            .collect())
    }

    fn create_property_constraint(
        &self,
        name: Option<&str>,
        scope: &ConstraintScope,
        properties: &[String],
        kind: PropertyConstraintKind,
        if_not_exists: bool,
    ) -> Result<PropertyConstraintSpec> {
        Ok(StorageEngine::create_property_constraint(
            self,
            name,
            scope,
            properties,
            kind,
            if_not_exists,
        )?)
    }

    fn drop_property_constraint(&self, name: &str, if_exists: bool) -> Result<()> {
        StorageEngine::drop_property_constraint(self, name, if_exists)?;
        Ok(())
    }

    fn list_property_constraints(&self) -> Result<Vec<PropertyConstraintSpec>> {
        Ok(StorageEngine::list_property_constraints(self))
    }
}

/// Adapter that lets a `&dyn StorageEngine` act as a `GraphWriter`.
/// See [`crate::reader::StorageReaderAdapter`] for the rationale.
pub struct StorageWriterAdapter<'a>(pub &'a dyn StorageEngine);

impl GraphWriter for StorageWriterAdapter<'_> {
    fn put_node(&self, node: &Node) -> Result<()> {
        self.0.put_node(node)?;
        Ok(())
    }

    fn put_edge(&self, edge: &Edge) -> Result<()> {
        self.0.put_edge(edge)?;
        Ok(())
    }

    fn delete_edge(&self, id: EdgeId) -> Result<()> {
        if self.0.get_edge(id)?.is_some() {
            self.0.delete_edge(id)?;
        }
        Ok(())
    }

    fn detach_delete_node(&self, id: NodeId) -> Result<()> {
        self.0.detach_delete_node(id)?;
        Ok(())
    }

    fn create_property_index(&self, label: &str, property: &str) -> Result<()> {
        self.0.create_property_index(label, property)?;
        Ok(())
    }

    fn drop_property_index(&self, label: &str, property: &str) -> Result<()> {
        self.0.drop_property_index(label, property)?;
        Ok(())
    }

    fn list_property_indexes(&self) -> Result<Vec<(String, String)>> {
        Ok(self
            .0
            .list_property_indexes()
            .into_iter()
            .map(|s| (s.label, s.property))
            .collect())
    }

    fn create_property_constraint(
        &self,
        name: Option<&str>,
        scope: &ConstraintScope,
        properties: &[String],
        kind: PropertyConstraintKind,
        if_not_exists: bool,
    ) -> Result<PropertyConstraintSpec> {
        Ok(self
            .0
            .create_property_constraint(name, scope, properties, kind, if_not_exists)?)
    }

    fn drop_property_constraint(&self, name: &str, if_exists: bool) -> Result<()> {
        self.0.drop_property_constraint(name, if_exists)?;
        Ok(())
    }

    fn list_property_constraints(&self) -> Result<Vec<PropertyConstraintSpec>> {
        Ok(self.0.list_property_constraints())
    }
}
