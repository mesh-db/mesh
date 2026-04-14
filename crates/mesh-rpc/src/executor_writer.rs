use mesh_cluster::raft::RaftCluster;
use mesh_cluster::GraphCommand;
use mesh_core::{Edge, EdgeId, Node, NodeId};
use mesh_executor::{Error as ExecError, GraphWriter, Result as ExecResult};
use std::sync::{Arc, Mutex};
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

/// Executor write sink that accumulates mutations in memory instead of
/// proposing them one-by-one. Lets the caller commit a multi-write Cypher
/// query as a single Raft entry (`GraphCommand::Batch`) so a crash mid-query
/// can't leave behind a partial result on any replica.
pub struct BufferingGraphWriter {
    buffer: Mutex<Vec<GraphCommand>>,
}

impl Default for BufferingGraphWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferingGraphWriter {
    pub fn new() -> Self {
        Self {
            buffer: Mutex::new(Vec::new()),
        }
    }

    /// Drain the buffer, returning everything the executor wrote.
    pub fn into_commands(self) -> Vec<GraphCommand> {
        self.buffer.into_inner().unwrap_or_default()
    }
}

impl GraphWriter for BufferingGraphWriter {
    fn put_node(&self, node: &Node) -> ExecResult<()> {
        self.buffer
            .lock()
            .unwrap()
            .push(GraphCommand::PutNode(node.clone()));
        Ok(())
    }

    fn put_edge(&self, edge: &Edge) -> ExecResult<()> {
        self.buffer
            .lock()
            .unwrap()
            .push(GraphCommand::PutEdge(edge.clone()));
        Ok(())
    }

    fn delete_edge(&self, id: EdgeId) -> ExecResult<()> {
        self.buffer
            .lock()
            .unwrap()
            .push(GraphCommand::DeleteEdge(id));
        Ok(())
    }

    fn detach_delete_node(&self, id: NodeId) -> ExecResult<()> {
        self.buffer
            .lock()
            .unwrap()
            .push(GraphCommand::DetachDeleteNode(id));
        Ok(())
    }
}
