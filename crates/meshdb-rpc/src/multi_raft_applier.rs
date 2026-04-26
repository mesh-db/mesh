//! Per-group state-machine appliers for multi-raft mode.
//!
//! Single-Raft mode has one applier — [`StoreGraphApplier`] — that
//! handles every variant of [`GraphCommand`]. Multi-raft splits the
//! responsibility:
//!
//! * [`MetaGraphApplier`] handles DDL (`CreateIndex` / `CreateConstraint`
//!   / `InstallTrigger` etc.) and the cluster's metadata Raft entries.
//!   Replicates to every peer through the metadata Raft group.
//! * [`PartitionGraphApplier`] handles data writes (`PutNode`, `PutEdge`,
//!   `Batch`) plus the multi-raft tx-coordination markers
//!   (`PreparedTx` / `CommitTx` / `AbortTx`). One instance per
//!   partition-group replica this peer hosts; filters out commands
//!   targeting other partitions in case of misroutes.
//!
//! The two appliers share helpers — `apply_ddl_command` for the
//! schema-DDL surface, `apply_data_command` for the leaf-graph surface
//! — so the underlying storage interactions stay unified with
//! `StoreGraphApplier`.

use crate::raft_applier::{storage_kind, storage_scope};
use meshdb_cluster::raft::GraphStateMachine;
use meshdb_cluster::{GraphCommand, PartitionId, Partitioner, TxId};
use meshdb_core::NodeId;
use meshdb_storage::{GraphMutation, StorageEngine};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// State-machine applier for a multi-raft partition group. Lives on
/// every replica of partition `partition_id`; applies the partition-
/// scoped subset of `GraphCommand`s and stages cross-partition
/// transactions in `pending_txs` until they commit or abort.
pub struct PartitionGraphApplier {
    pub partition_id: PartitionId,
    store: Arc<dyn StorageEngine>,
    partitioner: Partitioner,
    /// Cross-partition transactions staged via `PreparedTx`,
    /// keyed by the coordinator-issued txid. `CommitTx` drains
    /// the entry into a single atomic `apply_batch`; `AbortTx`
    /// drops it. The map lives in memory because the partition
    /// Raft log is the durable record — every replica replays
    /// the same `PreparedTx` entries on startup, so the map
    /// reconstructs deterministically without any additional
    /// fsync.
    pending_txs: Mutex<HashMap<TxId, Vec<GraphMutation>>>,
}

impl std::fmt::Debug for PartitionGraphApplier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionGraphApplier")
            .field("partition_id", &self.partition_id)
            .finish_non_exhaustive()
    }
}

impl PartitionGraphApplier {
    pub fn new(
        partition_id: PartitionId,
        store: Arc<dyn StorageEngine>,
        partitioner: Partitioner,
    ) -> Self {
        Self {
            partition_id,
            store,
            partitioner,
            pending_txs: Mutex::new(HashMap::new()),
        }
    }

    /// True when `node_id` is in this applier's partition. Used to
    /// drop misrouted commands silently — under correct routing the
    /// caller never sends a non-target command to us, but a
    /// rebalance race or a bug in the coordinator's grouping logic
    /// shouldn't cause data corruption either way.
    fn owns(&self, node_id: NodeId) -> bool {
        self.partitioner.partition_for(node_id) == self.partition_id
    }

    fn collect_partition_mutations(&self, cmds: &[GraphCommand], out: &mut Vec<GraphMutation>) {
        for cmd in cmds {
            match cmd {
                GraphCommand::PutNode(n) if self.owns(n.id) => {
                    out.push(GraphMutation::PutNode(n.clone()))
                }
                GraphCommand::PutEdge(e) => {
                    // Edges live on the source's partition, with a
                    // ghost copy on the target's partition for reverse
                    // traversal. Both endpoints' partitions accept
                    // the edge; non-endpoints drop it.
                    if self.owns(e.source) || self.owns(e.target) {
                        out.push(GraphMutation::PutEdge(e.clone()));
                    }
                }
                GraphCommand::DeleteEdge(id) => out.push(GraphMutation::DeleteEdge(*id)),
                GraphCommand::DetachDeleteNode(id) if self.owns(*id) => {
                    out.push(GraphMutation::DetachDeleteNode(*id));
                }
                GraphCommand::Batch(inner) => self.collect_partition_mutations(inner, out),
                _ => {
                    // Non-data variants (DDL, triggers, tx markers)
                    // don't show up here — they're filtered by the
                    // top-level apply path.
                }
            }
        }
    }

    fn apply_immediate(&self, cmd: &GraphCommand) -> Result<(), String> {
        let mut mutations = Vec::new();
        self.collect_partition_mutations(std::slice::from_ref(cmd), &mut mutations);
        if mutations.is_empty() {
            return Ok(());
        }
        self.store
            .apply_batch(&mutations)
            .map_err(|e| e.to_string())
    }
}

impl GraphStateMachine for PartitionGraphApplier {
    fn apply(&self, command: &GraphCommand) -> Result<(), String> {
        match command {
            GraphCommand::PutNode(_)
            | GraphCommand::PutEdge(_)
            | GraphCommand::DeleteEdge(_)
            | GraphCommand::DetachDeleteNode(_)
            | GraphCommand::Batch(_) => self.apply_immediate(command),

            GraphCommand::PreparedTx { txid, commands } => {
                let mut mutations = Vec::new();
                self.collect_partition_mutations(commands, &mut mutations);
                self.pending_txs
                    .lock()
                    .expect("pending_txs mutex poisoned")
                    .insert(txid.clone(), mutations);
                Ok(())
            }
            GraphCommand::CommitTx { txid } => {
                let mutations = self
                    .pending_txs
                    .lock()
                    .expect("pending_txs mutex poisoned")
                    .remove(txid);
                match mutations {
                    Some(m) if !m.is_empty() => {
                        self.store.apply_batch(&m).map_err(|e| e.to_string())
                    }
                    // Unknown txid or empty staging → idempotent no-op.
                    // A replay of an already-committed tx, or a
                    // tx that PREPARed before this peer had any data
                    // commands targeting our partition, both land here.
                    _ => Ok(()),
                }
            }
            GraphCommand::AbortTx { txid } => {
                self.pending_txs
                    .lock()
                    .expect("pending_txs mutex poisoned")
                    .remove(txid);
                Ok(())
            }

            // DDL and triggers belong on the metadata group, not on
            // partition groups. Reject loudly so a misroute is a
            // config bug rather than silent divergence.
            GraphCommand::CreateIndex { .. }
            | GraphCommand::DropIndex { .. }
            | GraphCommand::CreateEdgeIndex { .. }
            | GraphCommand::DropEdgeIndex { .. }
            | GraphCommand::CreatePointIndex { .. }
            | GraphCommand::DropPointIndex { .. }
            | GraphCommand::CreateEdgePointIndex { .. }
            | GraphCommand::DropEdgePointIndex { .. }
            | GraphCommand::CreateConstraint { .. }
            | GraphCommand::DropConstraint { .. }
            | GraphCommand::InstallTrigger { .. }
            | GraphCommand::DropTrigger { .. } => {
                Err("DDL / trigger command applied to a partition Raft group; \
                 should land in the metadata group's applier"
                    .into())
            }
        }
    }

    fn snapshot(&self) -> Result<Vec<u8>, String> {
        // Per-partition snapshots ride on top of the same checkpoint
        // mechanism as single-Raft. The snapshot captures *the entire
        // local store* — non-target partitions' data was never written
        // here, so each replica's snapshot only contains its own
        // partition's keys by construction.
        crate::raft_applier::StoreGraphApplier::new(self.store.clone()).snapshot()
    }

    fn restore(&self, snapshot: &[u8]) -> Result<(), String> {
        crate::raft_applier::StoreGraphApplier::new(self.store.clone()).restore(snapshot)?;
        // A fresh snapshot install replaces the on-disk state, so any
        // in-memory pending tx staging is now stale — drop it. The
        // new leader will replay the partition Raft log past the
        // snapshot index and reconstruct staging from `PreparedTx`
        // entries naturally.
        self.pending_txs
            .lock()
            .expect("pending_txs mutex poisoned")
            .clear();
        Ok(())
    }
}

/// State-machine applier for the multi-raft metadata group. Handles
/// the DDL and trigger surface (replicated to every peer) and rejects
/// data writes — those flow through the per-partition appliers.
pub struct MetaGraphApplier {
    store: Arc<dyn StorageEngine>,
    /// Optional trigger registry handle; refreshed when an
    /// `InstallTrigger` / `DropTrigger` entry applies. Mirrors
    /// [`StoreGraphApplier`] so single-Raft and multi-raft modes
    /// share the same trigger lifecycle.
    #[cfg(feature = "apoc-trigger")]
    trigger_registry: Option<meshdb_executor::apoc_trigger::TriggerRegistry>,
}

impl std::fmt::Debug for MetaGraphApplier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaGraphApplier").finish_non_exhaustive()
    }
}

impl MetaGraphApplier {
    pub fn new(store: Arc<dyn StorageEngine>) -> Self {
        Self {
            store,
            #[cfg(feature = "apoc-trigger")]
            trigger_registry: None,
        }
    }

    #[cfg(feature = "apoc-trigger")]
    pub fn with_trigger_registry(
        mut self,
        registry: meshdb_executor::apoc_trigger::TriggerRegistry,
    ) -> Self {
        self.trigger_registry = Some(registry);
        self
    }

    fn notify_trigger_change(&self) {
        #[cfg(feature = "apoc-trigger")]
        if let Some(reg) = &self.trigger_registry {
            if let Err(e) = reg.refresh() {
                tracing::warn!(error = %e, "refreshing trigger registry after meta-Raft apply failed");
            }
        }
    }
}

impl GraphStateMachine for MetaGraphApplier {
    fn apply(&self, command: &GraphCommand) -> Result<(), String> {
        match command {
            // DDL — replicate to every peer. Same storage calls as
            // single-Raft `StoreGraphApplier`.
            GraphCommand::CreateIndex { label, properties } => self
                .store
                .create_property_index_composite(label, properties)
                .map_err(|e| e.to_string()),
            GraphCommand::DropIndex { label, properties } => self
                .store
                .drop_property_index_composite(label, properties)
                .map_err(|e| e.to_string()),
            GraphCommand::CreateEdgeIndex {
                edge_type,
                properties,
            } => self
                .store
                .create_edge_property_index_composite(edge_type, properties)
                .map_err(|e| e.to_string()),
            GraphCommand::DropEdgeIndex {
                edge_type,
                properties,
            } => self
                .store
                .drop_edge_property_index_composite(edge_type, properties)
                .map_err(|e| e.to_string()),
            GraphCommand::CreatePointIndex { label, property } => self
                .store
                .create_point_index(label, property)
                .map_err(|e| e.to_string()),
            GraphCommand::DropPointIndex { label, property } => self
                .store
                .drop_point_index(label, property)
                .map_err(|e| e.to_string()),
            GraphCommand::CreateEdgePointIndex {
                edge_type,
                property,
            } => self
                .store
                .create_edge_point_index(edge_type, property)
                .map_err(|e| e.to_string()),
            GraphCommand::DropEdgePointIndex {
                edge_type,
                property,
            } => self
                .store
                .drop_edge_point_index(edge_type, property)
                .map_err(|e| e.to_string()),
            GraphCommand::CreateConstraint {
                name,
                scope,
                properties,
                kind,
                if_not_exists,
            } => self
                .store
                .create_property_constraint(
                    name.as_deref(),
                    &storage_scope(scope),
                    properties,
                    storage_kind(*kind),
                    *if_not_exists,
                )
                .map(|_| ())
                .map_err(|e| e.to_string()),
            GraphCommand::DropConstraint { name, if_exists } => self
                .store
                .drop_property_constraint(name, *if_exists)
                .map_err(|e| e.to_string()),
            GraphCommand::InstallTrigger { name, spec_blob } => {
                self.store
                    .put_trigger(name, spec_blob)
                    .map_err(|e| e.to_string())?;
                self.notify_trigger_change();
                Ok(())
            }
            GraphCommand::DropTrigger { name } => {
                self.store.delete_trigger(name).map_err(|e| e.to_string())?;
                self.notify_trigger_change();
                Ok(())
            }

            // Data writes belong on partition groups, not on the
            // metadata group. Reject loudly.
            GraphCommand::PutNode(_)
            | GraphCommand::PutEdge(_)
            | GraphCommand::DeleteEdge(_)
            | GraphCommand::DetachDeleteNode(_)
            | GraphCommand::Batch(_) => Err("data write applied to the metadata Raft group; \
                 should land in a partition group's applier"
                .into()),
            GraphCommand::PreparedTx { .. }
            | GraphCommand::CommitTx { .. }
            | GraphCommand::AbortTx { .. } => Err(
                "tx-coordination command applied to the metadata Raft group; \
                 should land in a partition group's applier"
                    .into(),
            ),
        }
    }

    fn snapshot(&self) -> Result<Vec<u8>, String> {
        // Same checkpoint mechanism as single-Raft. The metadata
        // group's snapshot captures the full local store; partition
        // groups' snapshots overwrite it on restore (they target the
        // same `data_dir/data` rocksdb instance), so a peer joining
        // mid-cluster reconstructs schema from the meta snapshot
        // first, then per-partition data from each partition group's
        // snapshot.
        crate::raft_applier::StoreGraphApplier::new(self.store.clone()).snapshot()
    }

    fn restore(&self, snapshot: &[u8]) -> Result<(), String> {
        crate::raft_applier::StoreGraphApplier::new(self.store.clone()).restore(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meshdb_core::{Edge, Node};
    use meshdb_storage::RocksDbStorageEngine;

    fn store() -> (tempfile::TempDir, Arc<dyn StorageEngine>) {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn StorageEngine> =
            Arc::new(RocksDbStorageEngine::open(dir.path()).unwrap());
        (dir, store)
    }

    /// Construct a node whose FNV-1a partition lands on `target`.
    /// Brute force: generate random nodes until one hashes correctly.
    /// Cheap because the partition space is small.
    fn node_in_partition(p: &Partitioner, target: PartitionId) -> Node {
        loop {
            let n = Node::new();
            if p.partition_for(n.id) == target {
                return n;
            }
        }
    }

    #[test]
    fn partition_applier_immediately_applies_owned_put_node() {
        let (_d, store) = store();
        let partitioner = Partitioner::new(4);
        let applier =
            PartitionGraphApplier::new(PartitionId(2), store.clone(), partitioner.clone());
        let n = node_in_partition(&partitioner, PartitionId(2));
        let id = n.id;
        applier.apply(&GraphCommand::PutNode(n)).unwrap();
        assert!(store.get_node(id).unwrap().is_some());
    }

    #[test]
    fn partition_applier_filters_non_target_put_node() {
        let (_d, store) = store();
        let partitioner = Partitioner::new(4);
        let applier =
            PartitionGraphApplier::new(PartitionId(0), store.clone(), partitioner.clone());
        let n = node_in_partition(&partitioner, PartitionId(2));
        let id = n.id;
        // Apply succeeds (it's a no-op), but no data lands.
        applier.apply(&GraphCommand::PutNode(n)).unwrap();
        assert!(store.get_node(id).unwrap().is_none());
    }

    #[test]
    fn partition_applier_stages_then_commits_prepared_tx() {
        let (_d, store) = store();
        let partitioner = Partitioner::new(4);
        let applier =
            PartitionGraphApplier::new(PartitionId(1), store.clone(), partitioner.clone());
        let n = node_in_partition(&partitioner, PartitionId(1));
        let id = n.id;
        applier
            .apply(&GraphCommand::PreparedTx {
                txid: "tx-123".into(),
                commands: vec![GraphCommand::PutNode(n)],
            })
            .unwrap();
        // PREPARE staged but didn't apply.
        assert!(store.get_node(id).unwrap().is_none());

        applier
            .apply(&GraphCommand::CommitTx {
                txid: "tx-123".into(),
            })
            .unwrap();
        assert!(store.get_node(id).unwrap().is_some());
    }

    #[test]
    fn partition_applier_aborts_prepared_tx_without_applying() {
        let (_d, store) = store();
        let partitioner = Partitioner::new(4);
        let applier =
            PartitionGraphApplier::new(PartitionId(1), store.clone(), partitioner.clone());
        let n = node_in_partition(&partitioner, PartitionId(1));
        let id = n.id;
        applier
            .apply(&GraphCommand::PreparedTx {
                txid: "tx-abort".into(),
                commands: vec![GraphCommand::PutNode(n)],
            })
            .unwrap();
        applier
            .apply(&GraphCommand::AbortTx {
                txid: "tx-abort".into(),
            })
            .unwrap();
        assert!(store.get_node(id).unwrap().is_none());

        // Re-applying the commit after abort is a no-op (idempotent).
        applier
            .apply(&GraphCommand::CommitTx {
                txid: "tx-abort".into(),
            })
            .unwrap();
        assert!(store.get_node(id).unwrap().is_none());
    }

    #[test]
    fn partition_applier_rejects_ddl() {
        let (_d, store) = store();
        let partitioner = Partitioner::new(4);
        let applier = PartitionGraphApplier::new(PartitionId(0), store, partitioner);
        let err = applier
            .apply(&GraphCommand::CreateIndex {
                label: "L".into(),
                properties: vec!["p".into()],
            })
            .unwrap_err();
        assert!(err.contains("DDL"), "got: {err}");
    }

    #[test]
    fn meta_applier_applies_create_index() {
        let (_d, store) = store();
        let applier = MetaGraphApplier::new(store.clone());
        applier
            .apply(&GraphCommand::CreateIndex {
                label: "L".into(),
                properties: vec!["p".into()],
            })
            .unwrap();
        // Verify the index landed on storage by listing.
        let indexes = store.list_property_indexes();
        assert!(
            indexes
                .iter()
                .any(|spec| spec.label == "L" && spec.properties == vec!["p".to_string()]),
            "index missing in {indexes:?}"
        );
    }

    #[test]
    fn meta_applier_rejects_data_write() {
        let (_d, store) = store();
        let applier = MetaGraphApplier::new(store);
        let err = applier
            .apply(&GraphCommand::PutNode(Node::new()))
            .unwrap_err();
        assert!(err.contains("data write"), "got: {err}");
    }

    #[test]
    fn meta_applier_rejects_tx_marker() {
        let (_d, store) = store();
        let applier = MetaGraphApplier::new(store);
        let err = applier
            .apply(&GraphCommand::CommitTx {
                txid: "tx-1".into(),
            })
            .unwrap_err();
        assert!(err.contains("tx-coordination"), "got: {err}");
    }

    #[test]
    fn partition_applier_handles_cross_partition_edge_on_either_endpoint() {
        // An edge whose source is on partition 1 and target on
        // partition 2 must apply on the partition-1 replica AND on
        // the partition-2 replica (forward + reverse adjacency).
        let (_d1, s1) = store();
        let (_d2, s2) = store();
        let p = Partitioner::new(4);
        let src = node_in_partition(&p, PartitionId(1));
        let dst = node_in_partition(&p, PartitionId(2));
        s1.put_node(&src).unwrap();
        s2.put_node(&dst).unwrap();
        let edge = Edge::new("LINKS", src.id, dst.id);
        let edge_id = edge.id;

        let p1 = PartitionGraphApplier::new(PartitionId(1), s1.clone(), p.clone());
        let p2 = PartitionGraphApplier::new(PartitionId(2), s2.clone(), p.clone());

        p1.apply(&GraphCommand::PutEdge(edge.clone())).unwrap();
        p2.apply(&GraphCommand::PutEdge(edge)).unwrap();

        assert!(s1.get_edge(edge_id).unwrap().is_some());
        assert!(s2.get_edge(edge_id).unwrap().is_some());
    }
}
