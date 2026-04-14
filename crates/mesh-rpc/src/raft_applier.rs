//! [`GraphStateMachine`] implementation that applies graph mutations to a
//! local [`mesh_storage::Store`].
//!
//! Plugged into [`mesh_cluster::raft::RaftCluster::new_with_applier`] so
//! every Raft replica's local store ends up with the same graph data after
//! a `MeshLogEntry::Graph` entry commits.

use mesh_cluster::raft::GraphStateMachine;
use mesh_cluster::GraphCommand;
use mesh_storage::{Store, StoreMutation};
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
                // Translate to StoreMutation and apply atomically through
                // a single rocksdb WriteBatch — either every mutation in
                // the Cypher query lands or none does, even across a
                // process crash. Nested Batch variants are flattened
                // because `Store::apply_batch` only knows the leaf ops.
                let mut flat = Vec::with_capacity(cmds.len());
                flatten_into(cmds, &mut flat)?;
                self.store.apply_batch(&flat).map_err(|e| e.to_string())
            }
        }
    }
}

fn flatten_into(
    cmds: &[GraphCommand],
    out: &mut Vec<StoreMutation>,
) -> Result<(), String> {
    for cmd in cmds {
        match cmd {
            GraphCommand::PutNode(n) => out.push(StoreMutation::PutNode(n.clone())),
            GraphCommand::PutEdge(e) => out.push(StoreMutation::PutEdge(e.clone())),
            GraphCommand::DeleteEdge(id) => out.push(StoreMutation::DeleteEdge(*id)),
            GraphCommand::DetachDeleteNode(id) => {
                out.push(StoreMutation::DetachDeleteNode(*id))
            }
            GraphCommand::Batch(inner) => flatten_into(inner, out)?,
        }
    }
    Ok(())
}
