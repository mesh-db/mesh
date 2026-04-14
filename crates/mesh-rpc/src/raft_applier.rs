//! [`GraphStateMachine`] implementation that applies graph mutations to a
//! local [`mesh_storage::Store`].
//!
//! Plugged into [`mesh_cluster::raft::RaftCluster::new_with_applier`] so
//! every Raft replica's local store ends up with the same graph data after
//! a `MeshLogEntry::Graph` entry commits.

use mesh_cluster::raft::GraphStateMachine;
use mesh_cluster::GraphCommand;
use mesh_storage::Store;
use std::sync::Arc;

pub struct StoreGraphApplier {
    store: Arc<Store>,
}

impl std::fmt::Debug for StoreGraphApplier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreGraphApplier").finish_non_exhaustive()
    }
}

impl StoreGraphApplier {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }
}

impl GraphStateMachine for StoreGraphApplier {
    fn apply(&self, command: &GraphCommand) -> Result<(), String> {
        match command {
            GraphCommand::PutNode(node) => {
                self.store.put_node(node).map_err(|e| e.to_string())
            }
            GraphCommand::PutEdge(edge) => {
                self.store.put_edge(edge).map_err(|e| e.to_string())
            }
            GraphCommand::DeleteEdge(id) => {
                // Idempotent: a redundant Raft replay shouldn't error.
                if self
                    .store
                    .get_edge(*id)
                    .map_err(|e| e.to_string())?
                    .is_some()
                {
                    self.store.delete_edge(*id).map_err(|e| e.to_string())?;
                }
                Ok(())
            }
            GraphCommand::DetachDeleteNode(id) => {
                // detach_delete_node is already idempotent for missing nodes.
                self.store
                    .detach_delete_node(*id)
                    .map_err(|e| e.to_string())
            }
            GraphCommand::Batch(cmds) => {
                // Sequential apply on top of the per-op idempotency above.
                // Local atomicity isn't enforced (no Store-level WriteBatch
                // yet), but it doesn't have to be: log replay re-applies
                // the entire MeshLogEntry::Graph entry from the start, so a
                // mid-batch crash recovers correctly because every sub-op
                // is idempotent.
                for cmd in cmds {
                    self.apply(cmd)?;
                }
                Ok(())
            }
        }
    }
}
