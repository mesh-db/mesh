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

/// Encode `(partition, txid)` as a `pending_tx_meta` rocksdb key.
/// Layout: 4 LE bytes of partition id, then the txid string bytes.
/// Partition prefix lets a single CF host every partition's
/// staging on a peer that hosts multiple partitions, while
/// `list_pending_txs` lookups on each applier filter by partition.
fn encode_pending_tx_key(partition: PartitionId, txid: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + txid.len());
    out.extend_from_slice(&partition.0.to_le_bytes());
    out.extend_from_slice(txid.as_bytes());
    out
}

/// Inverse of [`encode_pending_tx_key`]. Returns `None` for keys
/// shorter than the 4-byte partition prefix or whose txid bytes
/// aren't valid UTF-8 — corrupted entries are skipped silently
/// rather than aborting recovery.
fn decode_pending_tx_key(key: &[u8]) -> Option<(PartitionId, TxId)> {
    if key.len() < 4 {
        return None;
    }
    let partition = u32::from_le_bytes([key[0], key[1], key[2], key[3]]);
    let txid = std::str::from_utf8(&key[4..]).ok()?.to_string();
    Some((PartitionId(partition), txid))
}

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
    /// Cross-partition transactions staged via `PreparedTx`,
    /// keyed by the coordinator-issued txid. Stored as the
    /// original `Vec<GraphCommand>` (which is serializable;
    /// `GraphMutation` isn't) — the partition-filtering pass
    /// re-runs on `CommitTx` to derive the materialized
    /// mutations. The on-disk row in `pending_tx_meta` mirrors
    /// this map exactly so a restart reconstructs in-memory state
    /// from storage.
    pending_txs: Mutex<HashMap<TxId, Vec<GraphCommand>>>,
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
        // Rebuild the pending_txs map from durable storage. Without
        // this, an applied `PreparedTx` would be invisible to
        // post-restart recovery — openraft replays only entries past
        // `last_applied`, so the in-memory staging set wouldn't
        // reconstruct from the partition Raft log alone.
        let mut pending: HashMap<TxId, Vec<GraphCommand>> = HashMap::new();
        if let Ok(entries) = store.list_pending_txs() {
            for (key, value) in entries {
                if let Some((p, txid)) = decode_pending_tx_key(&key) {
                    if p != partition_id {
                        continue;
                    }
                    if let Ok(cmds) = serde_json::from_slice::<Vec<GraphCommand>>(&value) {
                        pending.insert(txid, cmds);
                    }
                }
            }
        }
        // Account restored entries on the process-wide gauge so a
        // restart with in-doubt PreparedTx state surfaces immediately
        // in `/metrics`.
        if !pending.is_empty() {
            crate::metrics::MULTI_RAFT_PENDING_TX_STAGED.add(pending.len() as i64);
        }
        Self {
            partition_id,
            store,
            partitioner,
            pending_txs: Mutex::new(pending),
        }
    }

    /// All in-doubt transaction ids — those whose `PreparedTx` entry
    /// has been applied but whose matching `CommitTx` / `AbortTx` has
    /// not yet been replicated. The recovery loop on a partition's
    /// new leader uses this to drive `ResolveTransaction` against
    /// the coordinator's log and propose the resolution into this
    /// partition's Raft.
    pub fn pending_tx_ids(&self) -> Vec<TxId> {
        self.pending_txs
            .lock()
            .expect("pending_txs mutex poisoned")
            .keys()
            .cloned()
            .collect()
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
                // Durable copy first — the in-memory entry lasts only
                // until the next restart; the on-disk row survives.
                let key = encode_pending_tx_key(self.partition_id, txid);
                let value = serde_json::to_vec(commands)
                    .map_err(|e| format!("encoding pending tx: {e}"))?;
                self.store
                    .put_pending_tx(&key, &value)
                    .map_err(|e| e.to_string())?;
                let prior = self
                    .pending_txs
                    .lock()
                    .expect("pending_txs mutex poisoned")
                    .insert(txid.clone(), commands.clone());
                if prior.is_none() {
                    crate::metrics::MULTI_RAFT_PENDING_TX_STAGED.inc();
                }
                Ok(())
            }
            GraphCommand::CommitTx { txid } => {
                let staged = self
                    .pending_txs
                    .lock()
                    .expect("pending_txs mutex poisoned")
                    .remove(txid);
                if staged.is_some() {
                    crate::metrics::MULTI_RAFT_PENDING_TX_STAGED.dec();
                }
                let key = encode_pending_tx_key(self.partition_id, txid);
                // Always clear the durable row, even when the in-
                // memory entry is missing — a CommitTx after restart
                // looks at storage as the source of truth.
                let _ = self.store.delete_pending_tx(&key);
                match staged {
                    Some(cmds) if !cmds.is_empty() => {
                        // Re-derive partition-filtered mutations from
                        // the staged commands list. Equivalent to
                        // applying the immediate-commit single-
                        // partition path with the same source data.
                        let mut mutations = Vec::new();
                        self.collect_partition_mutations(&cmds, &mut mutations);
                        if mutations.is_empty() {
                            return Ok(());
                        }
                        self.store
                            .apply_batch(&mutations)
                            .map_err(|e| e.to_string())
                    }
                    // Unknown txid or empty staging → idempotent no-op.
                    _ => Ok(()),
                }
            }
            GraphCommand::AbortTx { txid } => {
                let removed = self
                    .pending_txs
                    .lock()
                    .expect("pending_txs mutex poisoned")
                    .remove(txid);
                if removed.is_some() {
                    crate::metrics::MULTI_RAFT_PENDING_TX_STAGED.dec();
                }
                let key = encode_pending_tx_key(self.partition_id, txid);
                let _ = self.store.delete_pending_tx(&key);
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
        // Pack only this partition's nodes + edges + pending-tx
        // staging into a JSON document. Smaller than the
        // StoreGraphApplier checkpoint (which packs the whole
        // store), so a new replica catching up via InstallSnapshot
        // downloads ~1/N of the cluster's data instead of the full
        // graph.
        let mut nodes = Vec::new();
        for node in self.store.all_nodes().map_err(|e| e.to_string())? {
            if self.owns(node.id) {
                nodes.push(node);
            }
        }
        let mut edges = Vec::new();
        for edge in self.store.all_edges().map_err(|e| e.to_string())? {
            // Edges live on the source's partition, with a ghost
            // copy on the target's. Both endpoints' replicas pack
            // the edge so reverse adjacency stays intact.
            if self.owns(edge.source) || self.owns(edge.target) {
                edges.push(edge);
            }
        }
        let mut pending_rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for (key, value) in self.store.list_pending_txs().map_err(|e| e.to_string())? {
            if let Some((p, _)) = decode_pending_tx_key(&key) {
                if p == self.partition_id {
                    pending_rows.push((key, value));
                }
            }
        }
        let snap = PartitionSnapshot {
            partition_id: self.partition_id.0,
            nodes,
            edges,
            pending_rows,
        };
        serde_json::to_vec(&snap).map_err(|e| format!("encoding partition snapshot: {e}"))
    }

    fn restore(&self, snapshot: &[u8]) -> Result<(), String> {
        if snapshot.is_empty() {
            return Ok(());
        }
        let snap: PartitionSnapshot = serde_json::from_slice(snapshot)
            .map_err(|e| format!("decoding partition snapshot: {e}"))?;
        if snap.partition_id != self.partition_id.0 {
            return Err(format!(
                "snapshot partition_id {} does not match this applier's partition {}",
                snap.partition_id, self.partition_id.0
            ));
        }
        // Apply nodes + edges via the storage layer. PutNode /
        // PutEdge are idempotent overwrites — a previously-stale
        // replica that's catching up sees its old data replaced.
        let mut muts: Vec<GraphMutation> = Vec::new();
        for n in snap.nodes {
            muts.push(GraphMutation::PutNode(n));
        }
        for e in snap.edges {
            muts.push(GraphMutation::PutEdge(e));
        }
        if !muts.is_empty() {
            self.store.apply_batch(&muts).map_err(|e| e.to_string())?;
        }
        // Replace pending_tx_meta rows for this partition. The
        // snapshot captures the leader's view at snapshot time;
        // anything stale on disk gets overwritten via put.
        for (key, value) in snap.pending_rows {
            self.store
                .put_pending_tx(&key, &value)
                .map_err(|e| e.to_string())?;
        }
        // Rebuild the in-memory pending_txs from the freshly-
        // restored on-disk rows. Same logic as `new` after a
        // restart — list_pending_txs filtered by partition.
        let mut pending = self.pending_txs.lock().expect("pending_txs mutex poisoned");
        let prior_len = pending.len();
        pending.clear();
        for (key, value) in self.store.list_pending_txs().map_err(|e| e.to_string())? {
            if let Some((p, txid)) = decode_pending_tx_key(&key) {
                if p == self.partition_id {
                    if let Ok(cmds) = serde_json::from_slice::<Vec<GraphCommand>>(&value) {
                        pending.insert(txid, cmds);
                    }
                }
            }
        }
        // Adjust the gauge to reflect the snapshot-restored state.
        let new_len = pending.len();
        let delta = new_len as i64 - prior_len as i64;
        if delta != 0 {
            crate::metrics::MULTI_RAFT_PENDING_TX_STAGED.add(delta);
        }
        Ok(())
    }
}

/// On-the-wire shape for [`PartitionGraphApplier`] snapshots.
/// JSON-encoded — fields are ordered to keep the serialized form
/// stable for cross-version replay during a rolling upgrade.
#[derive(serde::Serialize, serde::Deserialize)]
struct PartitionSnapshot {
    /// Partition this snapshot belongs to. Restore rejects a
    /// mismatch — defensive against a misrouted InstallSnapshot.
    partition_id: u32,
    nodes: Vec<meshdb_core::Node>,
    edges: Vec<meshdb_core::Edge>,
    /// Raw `(key, value)` pairs from `pending_tx_meta`, restored
    /// via `put_pending_tx` so the in-memory `pending_txs` map
    /// reconstructs from disk on the next applier read.
    pending_rows: Vec<(Vec<u8>, Vec<u8>)>,
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
    fn partition_snapshot_round_trips_only_owned_data() {
        // Source store has 50 nodes split across 4 partitions plus
        // some edges. The partition-1 applier's snapshot should
        // contain ~25% of the nodes (those owned by partition 1)
        // plus edges with at least one endpoint on partition 1.
        // Restoring into a fresh store on a different "peer" via
        // a partition-1 applier reproduces only that subset.
        let (_d, src) = store();
        let p = Partitioner::new(4);
        let mut node_ids = Vec::new();
        for _ in 0..50 {
            let n = Node::new();
            node_ids.push(n.id);
            src.put_node(&n).unwrap();
        }
        // Edges across consecutive ids — covers same-partition and
        // cross-partition cases.
        for w in node_ids.windows(2).take(25) {
            src.put_edge(&Edge::new("LINK", w[0], w[1])).unwrap();
        }

        let applier = PartitionGraphApplier::new(PartitionId(1), src.clone(), p.clone());
        let snap = applier.snapshot().expect("snapshot");
        // Decode and verify it only carries partition-1's data.
        let parsed: PartitionSnapshot = serde_json::from_slice(&snap).unwrap();
        assert_eq!(parsed.partition_id, 1);
        assert!(
            parsed
                .nodes
                .iter()
                .all(|n| p.partition_for(n.id) == PartitionId(1)),
            "snapshot leaked non-partition-1 nodes"
        );
        assert!(
            parsed.edges.iter().all(|e| {
                p.partition_for(e.source) == PartitionId(1)
                    || p.partition_for(e.target) == PartitionId(1)
            }),
            "snapshot leaked non-partition-1 edges"
        );

        // Restore into a fresh "destination peer". The dst applier
        // applies whatever the snapshot contains.
        let (_d2, dst) = store();
        let dst_applier = PartitionGraphApplier::new(PartitionId(1), dst.clone(), p.clone());
        dst_applier.restore(&snap).expect("restore");

        // Every partition-1 node from src should now be on dst.
        for id in node_ids {
            if p.partition_for(id) == PartitionId(1) {
                assert!(
                    dst.get_node(id).unwrap().is_some(),
                    "partition-1 node {id:?} missing from restored store"
                );
            }
        }
    }

    #[test]
    fn partition_snapshot_rejects_mismatched_partition_id() {
        let (_d, src) = store();
        let p = Partitioner::new(4);
        let applier = PartitionGraphApplier::new(PartitionId(1), src, p);
        let snap = applier.snapshot().unwrap();

        let (_d2, dst) = store();
        let dst_applier = PartitionGraphApplier::new(PartitionId(2), dst, Partitioner::new(4));
        let err = dst_applier.restore(&snap).unwrap_err();
        assert!(
            err.contains("does not match"),
            "expected partition-id mismatch error, got {err}"
        );
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
