use crate::error::Result;
use mesh_core::{Edge, EdgeId, Node, NodeId};
use mesh_storage::Store;

/// Read-side counterpart to [`crate::GraphWriter`]. Gives the executor a
/// uniform view of the graph regardless of whether the data behind it lives
/// entirely in the local `Store` (single-node or full-replica Raft mode) or is
/// sharded across cluster peers (routing mode, where a partitioned reader
/// fans out point reads to owners and scatter-gathers bulk scans).
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
    fn outgoing(&self, id: NodeId) -> Result<Vec<(EdgeId, NodeId)>>;
    fn incoming(&self, id: NodeId) -> Result<Vec<(EdgeId, NodeId)>>;
}

impl GraphReader for Store {
    fn get_node(&self, id: NodeId) -> Result<Option<Node>> {
        Ok(Store::get_node(self, id)?)
    }

    fn get_edge(&self, id: EdgeId) -> Result<Option<Edge>> {
        Ok(Store::get_edge(self, id)?)
    }

    fn all_node_ids(&self) -> Result<Vec<NodeId>> {
        Ok(Store::all_node_ids(self)?)
    }

    fn nodes_by_label(&self, label: &str) -> Result<Vec<NodeId>> {
        Ok(Store::nodes_by_label(self, label)?)
    }

    fn outgoing(&self, id: NodeId) -> Result<Vec<(EdgeId, NodeId)>> {
        Ok(Store::outgoing(self, id)?)
    }

    fn incoming(&self, id: NodeId) -> Result<Vec<(EdgeId, NodeId)>> {
        Ok(Store::incoming(self, id)?)
    }
}
