use crate::error::Result;
use mesh_core::{Edge, EdgeId, Node, NodeId};
use mesh_storage::Store;

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
    /// DDL surface the limitation immediately. The `Store` writer
    /// overrides this to call the backing store directly.
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
}

impl GraphWriter for Store {
    fn put_node(&self, node: &Node) -> Result<()> {
        Store::put_node(self, node)?;
        Ok(())
    }

    fn put_edge(&self, edge: &Edge) -> Result<()> {
        Store::put_edge(self, edge)?;
        Ok(())
    }

    fn delete_edge(&self, id: EdgeId) -> Result<()> {
        if Store::get_edge(self, id)?.is_some() {
            Store::delete_edge(self, id)?;
        }
        Ok(())
    }

    fn detach_delete_node(&self, id: NodeId) -> Result<()> {
        Store::detach_delete_node(self, id)?;
        Ok(())
    }

    fn create_property_index(&self, label: &str, property: &str) -> Result<()> {
        Store::create_property_index(self, label, property)?;
        Ok(())
    }

    fn drop_property_index(&self, label: &str, property: &str) -> Result<()> {
        Store::drop_property_index(self, label, property)?;
        Ok(())
    }

    fn list_property_indexes(&self) -> Result<Vec<(String, String)>> {
        Ok(Store::list_property_indexes(self)
            .into_iter()
            .map(|s| (s.label, s.property))
            .collect())
    }
}
