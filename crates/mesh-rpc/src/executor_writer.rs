use mesh_cluster::raft::RaftCluster;
use mesh_cluster::GraphCommand;
use mesh_core::{Edge, EdgeId, Node, NodeId};
use mesh_executor::{Error as ExecError, GraphWriter, Result as ExecResult};
use std::sync::Arc;
use tokio::runtime::Handle;

/// Executor write sink that proposes every mutation through Raft. Must be
/// invoked from inside a `spawn_blocking` context, because the methods
/// bridge the sync executor to async Raft RPCs via `Handle::block_on`.
pub struct RaftGraphWriter {
    raft: Arc<RaftCluster>,
    handle: Handle,
}

impl RaftGraphWriter {
    pub fn new(raft: Arc<RaftCluster>) -> Self {
        Self {
            raft,
            handle: Handle::current(),
        }
    }

    fn propose(&self, cmd: GraphCommand) -> ExecResult<()> {
        let raft = self.raft.clone();
        self.handle
            .block_on(async move { raft.propose_graph(cmd).await })
            .map(|_| ())
            .map_err(|e| ExecError::Write(e.to_string()))
    }
}

impl GraphWriter for RaftGraphWriter {
    fn put_node(&self, node: &Node) -> ExecResult<()> {
        self.propose(GraphCommand::PutNode(node.clone()))
    }

    fn put_edge(&self, edge: &Edge) -> ExecResult<()> {
        self.propose(GraphCommand::PutEdge(edge.clone()))
    }

    fn delete_edge(&self, id: EdgeId) -> ExecResult<()> {
        self.propose(GraphCommand::DeleteEdge(id))
    }

    fn detach_delete_node(&self, id: NodeId) -> ExecResult<()> {
        self.propose(GraphCommand::DetachDeleteNode(id))
    }
}
