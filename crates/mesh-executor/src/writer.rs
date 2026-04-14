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
}
