//! RocksDB-backed implementation of [`StorageEngine`].
//!
//! Holds the on-disk state for a single local peer: nodes, edges,
//! adjacency lists, label/type/property indexes, and the index metadata
//! needed to rehydrate at `open` time. Column families carve the backend
//! into purpose-keyed namespaces so each access pattern walks a tight
//! prefix.
//!
//! External callers should hold this as `Arc<dyn StorageEngine>` and
//! route through the trait — the concrete type is only visible at the
//! construction site (`RocksDbStorageEngine::open`) and at the Raft
//! snapshot restore path, which is backend-bound by design.

use crate::{
    engine::{
        GraphMutation, PropertyConstraintKind, PropertyConstraintSpec, PropertyIndexSpec,
        PropertyType, StorageEngine,
    },
    error::{Error, Result},
    keys::{
        adj_key, constraint_meta_decode, constraint_meta_encode, edge_from_adj_key,
        encode_index_value, id_from_str_index_key, label_index_key, label_index_prefix,
        node_from_adj_value, node_id_from_property_index_key, property_index_key,
        property_index_name_prefix, property_index_value_prefix, type_index_key, type_index_prefix,
        ID_LEN,
    },
};
use meshdb_core::{Edge, EdgeId, Node, NodeId, Property};
use rocksdb::{
    checkpoint::Checkpoint, ColumnFamilyDescriptor, Direction, IteratorMode, Options, WriteBatch,
    DB,
};
use std::path::Path;
use std::sync::RwLock;

const CF_NODES: &str = "nodes";
const CF_EDGES: &str = "edges";
const CF_ADJ_OUT: &str = "adj_out";
const CF_ADJ_IN: &str = "adj_in";
const CF_LABEL_INDEX: &str = "label_index";
const CF_TYPE_INDEX: &str = "type_index";
const CF_PROPERTY_INDEX: &str = "property_index";
const CF_INDEX_META: &str = "index_meta";
const CF_CONSTRAINT_META: &str = "constraint_meta";

const ALL_CFS: &[&str] = &[
    CF_NODES,
    CF_EDGES,
    CF_ADJ_OUT,
    CF_ADJ_IN,
    CF_LABEL_INDEX,
    CF_TYPE_INDEX,
    CF_PROPERTY_INDEX,
    CF_INDEX_META,
    CF_CONSTRAINT_META,
];

const EMPTY: &[u8] = &[];

/// Encode a [`PropertyIndexSpec`] as a stable `CF_INDEX_META` key:
/// `<label>\0<property>`. Neither label nor property key can contain a
/// NUL byte (the grammar restricts them to identifier characters), so
/// the NUL separator is unambiguous on decode.
fn index_meta_key(spec: &PropertyIndexSpec) -> Vec<u8> {
    let mut k = Vec::with_capacity(spec.label.len() + 1 + spec.property.len());
    k.extend_from_slice(spec.label.as_bytes());
    k.push(0);
    k.extend_from_slice(spec.property.as_bytes());
    k
}

fn index_meta_key_decode(key: &[u8]) -> Result<PropertyIndexSpec> {
    let sep = key
        .iter()
        .position(|b| *b == 0)
        .ok_or(Error::CorruptBytes {
            cf: CF_INDEX_META,
            expected: 1,
            actual: 0,
        })?;
    let label = std::str::from_utf8(&key[..sep])
        .map_err(|_| Error::CorruptBytes {
            cf: CF_INDEX_META,
            expected: sep,
            actual: sep,
        })?
        .to_string();
    let property = std::str::from_utf8(&key[sep + 1..])
        .map_err(|_| Error::CorruptBytes {
            cf: CF_INDEX_META,
            expected: key.len() - sep - 1,
            actual: key.len() - sep - 1,
        })?
        .to_string();
    Ok(PropertyIndexSpec { label, property })
}

pub struct RocksDbStorageEngine {
    db: DB,
    /// In-memory view of the registered property indexes, populated
    /// from [`CF_INDEX_META`] at open time and kept in sync by
    /// [`RocksDbStorageEngine::create_property_index`] and
    /// [`RocksDbStorageEngine::drop_property_index`].
    ///
    /// The write-path consults this on every `append_put_node` /
    /// `append_detach_delete_node` call to decide which index entries
    /// to emit, so it absolutely must not hit the DB on the hot path.
    /// `RwLock` rather than `Mutex` because the common access pattern
    /// is many concurrent reads with only DDL-time writes.
    indexes: RwLock<Vec<PropertyIndexSpec>>,
    /// In-memory view of the registered property constraints,
    /// populated from [`CF_CONSTRAINT_META`] at open time and kept in
    /// sync by [`RocksDbStorageEngine::create_property_constraint`] and
    /// [`RocksDbStorageEngine::drop_property_constraint`].
    ///
    /// Every `put_node` consults this to validate UNIQUE / NOT NULL
    /// invariants, so — as with `indexes` — it must not hit the DB on
    /// the hot path. Enforcement reads the catalog under a read lock
    /// and runs `nodes_by_property` against the backing index only
    /// when a UNIQUE check actually needs it.
    constraints: RwLock<Vec<PropertyConstraintSpec>>,
}

impl RocksDbStorageEngine {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cfs: Vec<ColumnFamilyDescriptor> = ALL_CFS
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect();

        let db = DB::open_cf_descriptors(&db_opts, path, cfs)?;
        let indexes = load_index_meta(&db)?;
        let constraints = load_constraint_meta(&db)?;
        Ok(Self {
            db,
            indexes: RwLock::new(indexes),
            constraints: RwLock::new(constraints),
        })
    }

    fn cf(&self, name: &'static str) -> Result<&rocksdb::ColumnFamily> {
        self.db
            .cf_handle(name)
            .ok_or(Error::MissingColumnFamily(name))
    }

    pub fn put_node(&self, node: &Node) -> Result<()> {
        let mut batch = WriteBatch::default();
        self.append_put_node(&mut batch, node)?;
        self.db.write(batch)?;
        Ok(())
    }

    fn append_put_node(&self, batch: &mut WriteBatch, node: &Node) -> Result<()> {
        let nodes_cf = self.cf(CF_NODES)?;
        let label_cf = self.cf(CF_LABEL_INDEX)?;
        let prop_cf = self.cf(CF_PROPERTY_INDEX)?;

        let existing: Option<Node> = match self.db.get_cf(nodes_cf, node.id.as_bytes())? {
            Some(bytes) => Some(serde_json::from_slice(&bytes)?),
            None => None,
        };
        let existing_labels: &[String] = existing.as_ref().map(|n| &n.labels[..]).unwrap_or(&[]);

        // Constraint enforcement: walk every registered constraint and
        // confirm the post-write state satisfies it. UNIQUE queries
        // the backing property index (guaranteed to exist — see
        // `create_property_constraint`) for rival holders of the same
        // value. NOT NULL checks the incoming property map directly.
        // Runs before we touch the batch, so a violation short-circuits
        // without leaving partial writes behind.
        self.enforce_constraints(node, existing.as_ref())?;

        let bytes = serde_json::to_vec(node)?;
        batch.put_cf(nodes_cf, node.id.as_bytes(), bytes);

        for old in existing_labels {
            if !node.labels.contains(old) {
                batch.delete_cf(label_cf, label_index_key(old, node.id));
            }
        }
        for new in &node.labels {
            if !existing_labels.contains(new) {
                batch.put_cf(label_cf, label_index_key(new, node.id), EMPTY);
            }
        }

        // Property-index maintenance: for each registered (label, prop)
        // index, compute the delta between the previous entry (if any)
        // and the new one, and emit the minimal set of put/delete
        // operations. Skipping an update when the entry is unchanged
        // keeps steady-state writes free of index churn.
        let indexes = self.indexes.read().expect("indexes lock poisoned");
        for spec in indexes.iter() {
            let was_indexed = existing_labels.iter().any(|l| l == &spec.label);
            let now_indexed = node.labels.iter().any(|l| l == &spec.label);
            let old_encoded = if was_indexed {
                existing
                    .as_ref()
                    .and_then(|n| n.properties.get(&spec.property))
                    .and_then(encode_index_value)
            } else {
                None
            };
            let new_encoded = if now_indexed {
                node.properties
                    .get(&spec.property)
                    .and_then(encode_index_value)
            } else {
                None
            };
            if old_encoded == new_encoded {
                continue;
            }
            if let Some(bytes) = &old_encoded {
                batch.delete_cf(
                    prop_cf,
                    property_index_key(&spec.label, &spec.property, bytes, node.id),
                );
            }
            if let Some(bytes) = &new_encoded {
                batch.put_cf(
                    prop_cf,
                    property_index_key(&spec.label, &spec.property, bytes, node.id),
                    EMPTY,
                );
            }
        }

        Ok(())
    }

    pub fn get_node(&self, id: NodeId) -> Result<Option<Node>> {
        let cf = self.cf(CF_NODES)?;
        match self.db.get_cf(cf, id.as_bytes())? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    pub fn put_edge(&self, edge: &Edge) -> Result<()> {
        let mut batch = WriteBatch::default();
        self.append_put_edge(&mut batch, edge)?;
        self.db.write(batch)?;
        Ok(())
    }

    fn append_put_edge(&self, batch: &mut WriteBatch, edge: &Edge) -> Result<()> {
        let edges_cf = self.cf(CF_EDGES)?;
        let out_cf = self.cf(CF_ADJ_OUT)?;
        let in_cf = self.cf(CF_ADJ_IN)?;
        let type_cf = self.cf(CF_TYPE_INDEX)?;

        let bytes = serde_json::to_vec(edge)?;
        batch.put_cf(edges_cf, edge.id.as_bytes(), bytes);
        batch.put_cf(
            out_cf,
            adj_key(edge.source, edge.id),
            edge.target.as_bytes(),
        );
        batch.put_cf(in_cf, adj_key(edge.target, edge.id), edge.source.as_bytes());
        batch.put_cf(type_cf, type_index_key(&edge.edge_type, edge.id), EMPTY);
        Ok(())
    }

    pub fn get_edge(&self, id: EdgeId) -> Result<Option<Edge>> {
        let cf = self.cf(CF_EDGES)?;
        match self.db.get_cf(cf, id.as_bytes())? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    pub fn delete_edge(&self, id: EdgeId) -> Result<()> {
        let edge = self.get_edge(id)?.ok_or(Error::EdgeNotFound(id))?;
        let mut batch = WriteBatch::default();
        self.append_delete_edge(&mut batch, id, &edge)?;
        self.db.write(batch)?;
        Ok(())
    }

    fn append_delete_edge(&self, batch: &mut WriteBatch, id: EdgeId, edge: &Edge) -> Result<()> {
        let edges_cf = self.cf(CF_EDGES)?;
        let out_cf = self.cf(CF_ADJ_OUT)?;
        let in_cf = self.cf(CF_ADJ_IN)?;
        let type_cf = self.cf(CF_TYPE_INDEX)?;

        batch.delete_cf(edges_cf, id.as_bytes());
        batch.delete_cf(out_cf, adj_key(edge.source, id));
        batch.delete_cf(in_cf, adj_key(edge.target, id));
        batch.delete_cf(type_cf, type_index_key(&edge.edge_type, id));
        Ok(())
    }

    pub fn detach_delete_node(&self, id: NodeId) -> Result<()> {
        let mut batch = WriteBatch::default();
        self.append_detach_delete_node(&mut batch, id)?;
        self.db.write(batch)?;
        Ok(())
    }

    fn append_detach_delete_node(&self, batch: &mut WriteBatch, id: NodeId) -> Result<()> {
        let node = self.get_node(id)?;
        let outgoing = self.outgoing(id)?;
        let incoming = self.incoming(id)?;

        let nodes_cf = self.cf(CF_NODES)?;
        let edges_cf = self.cf(CF_EDGES)?;
        let out_cf = self.cf(CF_ADJ_OUT)?;
        let in_cf = self.cf(CF_ADJ_IN)?;
        let label_cf = self.cf(CF_LABEL_INDEX)?;
        let type_cf = self.cf(CF_TYPE_INDEX)?;
        let prop_cf = self.cf(CF_PROPERTY_INDEX)?;

        if let Some(n) = &node {
            for label in &n.labels {
                batch.delete_cf(label_cf, label_index_key(label, id));
            }
            // Remove every property-index entry this node was holding
            // open. Mirrors the put path: for each registered index
            // whose label matches, if the node carries an indexable
            // value for the property, delete the corresponding entry.
            let indexes = self.indexes.read().expect("indexes lock poisoned");
            for spec in indexes.iter() {
                if !n.labels.iter().any(|l| l == &spec.label) {
                    continue;
                }
                if let Some(value) = n.properties.get(&spec.property) {
                    if let Some(encoded) = encode_index_value(value) {
                        batch.delete_cf(
                            prop_cf,
                            property_index_key(&spec.label, &spec.property, &encoded, id),
                        );
                    }
                }
            }
        }

        for (edge_id, target) in &outgoing {
            if let Some(e) = self.get_edge(*edge_id)? {
                batch.delete_cf(type_cf, type_index_key(&e.edge_type, *edge_id));
            }
            batch.delete_cf(edges_cf, edge_id.as_bytes());
            batch.delete_cf(out_cf, adj_key(id, *edge_id));
            batch.delete_cf(in_cf, adj_key(*target, *edge_id));
        }
        for (edge_id, source) in &incoming {
            if let Some(e) = self.get_edge(*edge_id)? {
                batch.delete_cf(type_cf, type_index_key(&e.edge_type, *edge_id));
            }
            batch.delete_cf(edges_cf, edge_id.as_bytes());
            batch.delete_cf(out_cf, adj_key(*source, *edge_id));
            batch.delete_cf(in_cf, adj_key(id, *edge_id));
        }
        batch.delete_cf(nodes_cf, id.as_bytes());
        Ok(())
    }

    /// Apply a sequence of mutations as one atomic rocksdb write. Either
    /// every mutation lands or none does — no replica can observe a
    /// partial result, even across a process crash.
    ///
    /// Reads performed during batch construction (existing labels for
    /// PutNode, edge metadata for DeleteEdge / DetachDeleteNode) hit the
    /// live store, not the in-flight batch. Read-your-writes across
    /// mutations in the same batch is *not* supported. The executor never
    /// generates such patterns: it produces fresh ids per row and
    /// dedupes mutated entities before flushing.
    pub fn apply_batch(&self, mutations: &[GraphMutation]) -> Result<()> {
        let mut batch = WriteBatch::default();
        for m in mutations {
            match m {
                GraphMutation::PutNode(n) => self.append_put_node(&mut batch, n)?,
                GraphMutation::PutEdge(e) => self.append_put_edge(&mut batch, e)?,
                GraphMutation::DeleteEdge(id) => {
                    if let Some(edge) = self.get_edge(*id)? {
                        self.append_delete_edge(&mut batch, *id, &edge)?;
                    }
                }
                GraphMutation::DetachDeleteNode(id) => {
                    self.append_detach_delete_node(&mut batch, *id)?;
                }
            }
        }
        self.db.write(batch)?;
        Ok(())
    }

    /// Walk every node in the store. Used for snapshot construction; not
    /// suitable as a query primitive since it materializes the full set.
    pub fn all_nodes(&self) -> Result<Vec<Node>> {
        let cf = self.cf(CF_NODES)?;
        let mut nodes = Vec::new();
        for item in self.db.iterator_cf(cf, IteratorMode::Start) {
            let (_, value) = item?;
            nodes.push(serde_json::from_slice(&value)?);
        }
        Ok(nodes)
    }

    /// Walk every edge in the store. Used for snapshot construction.
    pub fn all_edges(&self) -> Result<Vec<Edge>> {
        let cf = self.cf(CF_EDGES)?;
        let mut edges = Vec::new();
        for item in self.db.iterator_cf(cf, IteratorMode::Start) {
            let (_, value) = item?;
            edges.push(serde_json::from_slice(&value)?);
        }
        Ok(edges)
    }

    /// Produce a consistent point-in-time checkpoint of the store at
    /// `path`. Wraps rocksdb's [`Checkpoint`] API — on the same
    /// filesystem the checkpoint is effectively free (hard links over
    /// the underlying SST files); cross-filesystem it falls back to a
    /// full copy. `path` must NOT already exist; rocksdb creates it.
    ///
    /// Used by the Raft state machine's streaming snapshot path: a
    /// snapshot is built by checkpointing into a temp dir, packing
    /// the checkpoint's files into a length-prefixed archive, and
    /// shipping the bytes. Skips the "Vec<Node> in memory" materialization
    /// path that the previous JSON-over-everything snapshot used.
    pub fn create_checkpoint(&self, path: impl AsRef<Path>) -> Result<()> {
        let checkpoint = Checkpoint::new(&self.db)?;
        checkpoint.create_checkpoint(path)?;
        Ok(())
    }

    /// Drop every key from every column family. Used by snapshot install
    /// to wipe local state before applying the leader's snapshot. Cheap
    /// for empty / small stores; for large stores this is O(n).
    pub fn clear_all(&self) -> Result<()> {
        let mut batch = WriteBatch::default();
        for cf_name in ALL_CFS {
            let cf = self.cf(cf_name)?;
            for item in self.db.iterator_cf(cf, IteratorMode::Start) {
                let (key, _) = item?;
                batch.delete_cf(cf, key);
            }
        }
        self.db.write(batch)?;
        Ok(())
    }

    pub fn all_node_ids(&self) -> Result<Vec<NodeId>> {
        let cf = self.cf(CF_NODES)?;
        let mut results = Vec::new();
        for item in self.db.iterator_cf(cf, IteratorMode::Start) {
            let (key, _) = item?;
            if key.len() != ID_LEN {
                return Err(Error::CorruptBytes {
                    cf: CF_NODES,
                    expected: ID_LEN,
                    actual: key.len(),
                });
            }
            let mut bytes = [0u8; ID_LEN];
            bytes.copy_from_slice(&key);
            results.push(NodeId::from_bytes(bytes));
        }
        Ok(results)
    }

    pub fn outgoing(&self, source: NodeId) -> Result<Vec<(EdgeId, NodeId)>> {
        self.scan_adj(CF_ADJ_OUT, source)
    }

    pub fn incoming(&self, target: NodeId) -> Result<Vec<(EdgeId, NodeId)>> {
        self.scan_adj(CF_ADJ_IN, target)
    }

    /// Snapshot the currently-registered property indexes. Cheap clone
    /// — specs are tiny (a label + a property key).
    pub fn list_property_indexes(&self) -> Vec<PropertyIndexSpec> {
        self.indexes.read().expect("indexes lock poisoned").clone()
    }

    /// Declare a new `(label, property)` single-property equality
    /// index and backfill it by scanning every node that currently
    /// carries `label`. Idempotent: re-creating an already-registered
    /// index is a no-op, matching how Neo4j's `CREATE INDEX IF NOT
    /// EXISTS` behaves.
    ///
    /// Backfill is done in the same [`WriteBatch`] as the meta insert,
    /// so a crash mid-backfill leaves the store with either zero
    /// entries (batch not yet written) or a fully-populated index
    /// (batch committed). No partial-build state is ever visible.
    pub fn create_property_index(&self, label: &str, property: &str) -> Result<()> {
        let spec = PropertyIndexSpec {
            label: label.to_string(),
            property: property.to_string(),
        };
        let mut guard = self.indexes.write().expect("indexes lock poisoned");
        if guard.contains(&spec) {
            return Ok(());
        }

        let meta_cf = self.cf(CF_INDEX_META)?;
        let prop_cf = self.cf(CF_PROPERTY_INDEX)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(meta_cf, index_meta_key(&spec), EMPTY);

        // Backfill: walk the label index to find every current member
        // and probe its node record for the property. Nodes missing
        // the property (or carrying an unindexable type like Float)
        // contribute nothing.
        for node_id in self.nodes_by_label(label)? {
            let node = match self.get_node(node_id)? {
                Some(n) => n,
                None => continue,
            };
            if let Some(value) = node.properties.get(property) {
                if let Some(encoded) = encode_index_value(value) {
                    batch.put_cf(
                        prop_cf,
                        property_index_key(label, property, &encoded, node_id),
                        EMPTY,
                    );
                }
            }
        }

        self.db.write(batch)?;
        guard.push(spec);
        Ok(())
    }

    /// Declare a new property constraint. Behavior summary:
    ///
    /// * The name is the user-facing identifier. When `name` is
    ///   `None`, a deterministic default (`constraint_<label>_<prop>_<kind>`)
    ///   is generated so `DROP CONSTRAINT` can still target it.
    /// * If the chosen name is already registered with the same
    ///   `(label, property, kind)`, this is a no-op (the existing
    ///   spec is returned). If the name collides on a *different*
    ///   spec, returns `ConstraintNameConflict` unless
    ///   `if_not_exists` is set, in which case the existing spec is
    ///   preserved and returned unchanged.
    /// * UNIQUE constraints implicitly require a backing property
    ///   index. `create_property_constraint` creates one on
    ///   `(label, property)` if it isn't already registered —
    ///   matching Neo4j's "a unique constraint also provides an
    ///   index" contract.
    /// * Before committing, the method scans existing nodes to
    ///   verify the constraint is satisfied. Any violation aborts
    ///   the creation with `ConstraintViolation`; the registry is
    ///   left unchanged.
    ///
    /// The meta write and the implicit-index write are batched into a
    /// single [`WriteBatch`] so the on-disk state stays consistent
    /// under crash.
    pub fn create_property_constraint(
        &self,
        name: Option<&str>,
        label: &str,
        property: &str,
        kind: PropertyConstraintKind,
        if_not_exists: bool,
    ) -> Result<PropertyConstraintSpec> {
        let resolved_name = match name {
            Some(n) => n.to_string(),
            None => default_constraint_name(label, property, kind),
        };
        let spec = PropertyConstraintSpec {
            name: resolved_name.clone(),
            label: label.to_string(),
            property: property.to_string(),
            kind,
        };

        // Idempotency / conflict check against the existing registry
        // under a read lock first — avoids taking the write lock when
        // the answer is "nothing to do".
        {
            let guard = self.constraints.read().expect("constraints lock poisoned");
            if let Some(existing) = guard.iter().find(|s| s.name == resolved_name) {
                if existing == &spec {
                    return Ok(existing.clone());
                }
                if if_not_exists {
                    return Ok(existing.clone());
                }
                return Err(Error::ConstraintNameConflict {
                    name: resolved_name,
                });
            }
        }

        // UNIQUE needs a backing property index for enforcement. The
        // call is idempotent — a user-declared index on the same
        // `(label, property)` is reused. We do this before validating
        // the existing data so the scan below can cover it too.
        if matches!(kind, PropertyConstraintKind::Unique) {
            self.create_property_index(label, property)?;
        }

        // Validate existing data against the new constraint before we
        // commit the meta entry. A single pre-existing violation makes
        // the whole declaration fail — matching how Neo4j rejects
        // constraint creation on data that doesn't satisfy it.
        validate_existing_data(self, &spec)?;

        let meta_cf = self.cf(CF_CONSTRAINT_META)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(
            meta_cf,
            resolved_name.as_bytes(),
            constraint_meta_encode(&spec),
        );
        self.db.write(batch)?;

        let mut guard = self.constraints.write().expect("constraints lock poisoned");
        // Re-check under the write lock to avoid a TOCTOU where two
        // concurrent CREATEs race through the read-lock idempotency
        // check. The second writer sees the first's insertion and
        // returns the existing spec.
        if let Some(existing) = guard.iter().find(|s| s.name == resolved_name) {
            return Ok(existing.clone());
        }
        guard.push(spec.clone());
        Ok(spec)
    }

    /// Tear down a constraint by name. When `if_exists` is true,
    /// dropping a non-existent constraint is a no-op. Never drops the
    /// backing index for UNIQUE — users who want it gone issue a
    /// separate `DROP INDEX`.
    pub fn drop_property_constraint(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut guard = self.constraints.write().expect("constraints lock poisoned");
        let idx = guard.iter().position(|s| s.name == name);
        match idx {
            None if if_exists => Ok(()),
            None => Err(Error::ConstraintNotFound {
                name: name.to_string(),
            }),
            Some(i) => {
                let meta_cf = self.cf(CF_CONSTRAINT_META)?;
                let mut batch = WriteBatch::default();
                batch.delete_cf(meta_cf, name.as_bytes());
                self.db.write(batch)?;
                guard.remove(i);
                Ok(())
            }
        }
    }

    /// Snapshot the currently-registered constraints. Cheap clone —
    /// every spec is a handful of small strings and an enum.
    pub fn list_property_constraints(&self) -> Vec<PropertyConstraintSpec> {
        self.constraints
            .read()
            .expect("constraints lock poisoned")
            .clone()
    }

    /// Enforce registered constraints against the post-write state of
    /// `node`. Invoked by the `put_node` path before the batch is
    /// constructed. `existing` is the previously-stored snapshot of
    /// the same node (if any) so the check can distinguish "this node
    /// is updating its own value" from "some other node owns this
    /// value".
    fn enforce_constraints(&self, node: &Node, existing: Option<&Node>) -> Result<()> {
        let constraints = self.constraints.read().expect("constraints lock poisoned");
        if constraints.is_empty() {
            return Ok(());
        }
        let now_has_label = |label: &str| node.labels.iter().any(|l| l == label);
        for spec in constraints.iter() {
            if !now_has_label(&spec.label) {
                continue;
            }
            match spec.kind {
                PropertyConstraintKind::NotNull => {
                    let present = node
                        .properties
                        .get(&spec.property)
                        .is_some_and(|v| !matches!(v, Property::Null));
                    if !present {
                        return Err(Error::ConstraintViolation {
                            name: spec.name.clone(),
                            kind: spec.kind.as_string(),
                            label: spec.label.clone(),
                            property: spec.property.clone(),
                            details: format!(
                                "node {} is missing required property `{}`",
                                node.id, spec.property
                            ),
                        });
                    }
                }
                PropertyConstraintKind::Unique => {
                    let Some(value) = node.properties.get(&spec.property) else {
                        // No value → no uniqueness collision possible.
                        // Matches Neo4j: UNIQUE alone allows NULL;
                        // combine with NOT NULL for strict presence.
                        continue;
                    };
                    if matches!(value, Property::Null) {
                        continue;
                    }
                    // Skip probes on unindexable types — the backing
                    // index can't represent them so uniqueness is
                    // effectively undefined. Keeping the write allowed
                    // here matches how the index layer itself skips
                    // these values.
                    if encode_index_value(value).is_none() {
                        continue;
                    }
                    let holders = self.nodes_by_property(&spec.label, &spec.property, value)?;
                    // The backing property index already reflects the
                    // previously-committed state; `existing` (if any)
                    // is in that index. We allow a match on our own id
                    // so updating the same node to the same value is
                    // legal; we also allow the "same id, value stays
                    // the same" carryover. Any other matching id means
                    // a distinct node owns the value.
                    for other in holders {
                        if other == node.id {
                            continue;
                        }
                        let was_self = existing
                            .and_then(|n| n.properties.get(&spec.property))
                            .is_some_and(|v| v == value);
                        let _ = was_self;
                        return Err(Error::ConstraintViolation {
                            name: spec.name.clone(),
                            kind: spec.kind.as_string(),
                            label: spec.label.clone(),
                            property: spec.property.clone(),
                            details: format!("value already held by node {}", other),
                        });
                    }
                }
                PropertyConstraintKind::PropertyType(target) => {
                    // `IS :: <TYPE>` — the property, if present, must
                    // be of the declared type. Null / missing is
                    // allowed; pair with `IS NOT NULL` for strict
                    // presence. Matching Neo4j: strict kind match
                    // (no numeric coercion between Int and Float).
                    let Some(value) = node.properties.get(&spec.property) else {
                        continue;
                    };
                    if matches!(value, Property::Null) {
                        continue;
                    }
                    if !property_matches_type(value, target) {
                        return Err(Error::ConstraintViolation {
                            name: spec.name.clone(),
                            kind: spec.kind.as_string(),
                            label: spec.label.clone(),
                            property: spec.property.clone(),
                            details: format!(
                                "node {} has value of type {} (expected {})",
                                node.id,
                                value.type_name(),
                                target.as_str()
                            ),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Tear down a property index: removes the meta entry and every
    /// entry under the `(label, prop)` prefix. Idempotent: dropping a
    /// non-existent index is a no-op. Atomic via one [`WriteBatch`].
    pub fn drop_property_index(&self, label: &str, property: &str) -> Result<()> {
        let spec = PropertyIndexSpec {
            label: label.to_string(),
            property: property.to_string(),
        };
        let mut guard = self.indexes.write().expect("indexes lock poisoned");
        if !guard.contains(&spec) {
            return Ok(());
        }

        let meta_cf = self.cf(CF_INDEX_META)?;
        let prop_cf = self.cf(CF_PROPERTY_INDEX)?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(meta_cf, index_meta_key(&spec));

        let prefix = property_index_name_prefix(label, property);
        let iter = self
            .db
            .iterator_cf(prop_cf, IteratorMode::From(&prefix, Direction::Forward));
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            batch.delete_cf(prop_cf, key);
        }

        self.db.write(batch)?;
        guard.retain(|s| s != &spec);
        Ok(())
    }

    /// Look up node ids for a `(label, property, value)` equality via
    /// the property index CF. The caller is responsible for checking
    /// that the index exists first (e.g. the planner only emits
    /// `IndexSeek` when it has verified via
    /// [`RocksDbStorageEngine::list_property_indexes`]).
    ///
    /// Unindexable value types (Float64, List, Map, Null) return
    /// [`Error::UnindexableValue`] — callers should surface this to
    /// the user rather than silently returning an empty result.
    pub fn nodes_by_property(
        &self,
        label: &str,
        property: &str,
        value: &Property,
    ) -> Result<Vec<NodeId>> {
        let encoded = encode_index_value(value).ok_or_else(|| Error::UnindexableValue {
            property: property.to_string(),
            kind: value.type_name(),
        })?;
        let cf = self.cf(CF_PROPERTY_INDEX)?;
        let prefix = property_index_value_prefix(label, property, &encoded);
        let mut results = Vec::new();
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let bytes = node_id_from_property_index_key(&key, prefix.len())?;
            results.push(NodeId::from_bytes(bytes));
        }
        Ok(results)
    }

    pub fn nodes_by_label(&self, label: &str) -> Result<Vec<NodeId>> {
        let cf = self.cf(CF_LABEL_INDEX)?;
        let prefix = label_index_prefix(label);
        let mut results = Vec::new();
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let bytes = id_from_str_index_key(CF_LABEL_INDEX, &key, label.len())?;
            results.push(NodeId::from_bytes(bytes));
        }
        Ok(results)
    }

    pub fn edges_by_type(&self, edge_type: &str) -> Result<Vec<EdgeId>> {
        let cf = self.cf(CF_TYPE_INDEX)?;
        let prefix = type_index_prefix(edge_type);
        let mut results = Vec::new();
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let bytes = id_from_str_index_key(CF_TYPE_INDEX, &key, edge_type.len())?;
            results.push(EdgeId::from_bytes(bytes));
        }
        Ok(results)
    }

    fn scan_adj(&self, cf_name: &'static str, node: NodeId) -> Result<Vec<(EdgeId, NodeId)>> {
        let cf = self.cf(cf_name)?;
        let prefix: &[u8] = node.as_bytes();
        let mut results = Vec::new();
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(prefix, Direction::Forward));
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(prefix) {
                break;
            }
            let edge_id = edge_from_adj_key(cf_name, &key)?;
            let other = node_from_adj_value(cf_name, &value)?;
            results.push((edge_id, other));
        }
        Ok(results)
    }
}

/// Deterministic auto-name for a constraint the user didn't name.
/// Shape: `constraint_<label>_<property>_<kind>` where `<kind>` is
/// `unique`, `not_null`, or `type_<t>` (e.g. `type_string`). Stable
/// across restarts — users can target the auto-name with `DROP
/// CONSTRAINT` without having to inspect `SHOW CONSTRAINTS` first.
fn default_constraint_name(label: &str, property: &str, kind: PropertyConstraintKind) -> String {
    format!("constraint_{label}_{property}_{}", kind.name_tag())
}

/// Scan existing nodes and verify that `spec` holds. Called on
/// constraint creation so a user declaring a constraint against
/// already-invalid data gets a clear error instead of a partial
/// rollout. Walks only the nodes carrying the constrained label — for
/// label-sparse graphs this avoids the full-node scan.
fn validate_existing_data(
    engine: &RocksDbStorageEngine,
    spec: &PropertyConstraintSpec,
) -> Result<()> {
    let label_members = engine.nodes_by_label(&spec.label)?;
    match spec.kind {
        PropertyConstraintKind::NotNull => {
            for id in label_members {
                let node = match engine.get_node(id)? {
                    Some(n) => n,
                    None => continue,
                };
                let present = node
                    .properties
                    .get(&spec.property)
                    .is_some_and(|v| !matches!(v, Property::Null));
                if !present {
                    return Err(Error::ConstraintViolation {
                        name: spec.name.clone(),
                        kind: spec.kind.as_string(),
                        label: spec.label.clone(),
                        property: spec.property.clone(),
                        details: format!("node {id} is missing required property"),
                    });
                }
            }
        }
        PropertyConstraintKind::Unique => {
            // For UNIQUE we walk members once, collecting the first
            // node id per distinct value; a duplicate surfaces the
            // pair in the error so operators can trace the collision.
            use std::collections::HashMap;
            let mut seen: HashMap<Vec<u8>, meshdb_core::NodeId> = HashMap::new();
            for id in label_members {
                let node = match engine.get_node(id)? {
                    Some(n) => n,
                    None => continue,
                };
                let Some(value) = node.properties.get(&spec.property) else {
                    continue;
                };
                let Some(encoded) = encode_index_value(value) else {
                    continue;
                };
                if let Some(&first) = seen.get(&encoded) {
                    return Err(Error::ConstraintViolation {
                        name: spec.name.clone(),
                        kind: spec.kind.as_string(),
                        label: spec.label.clone(),
                        property: spec.property.clone(),
                        details: format!("duplicate value held by nodes {first} and {id}"),
                    });
                }
                seen.insert(encoded, id);
            }
        }
        PropertyConstraintKind::PropertyType(target) => {
            for id in label_members {
                let node = match engine.get_node(id)? {
                    Some(n) => n,
                    None => continue,
                };
                let Some(value) = node.properties.get(&spec.property) else {
                    continue;
                };
                if matches!(value, Property::Null) {
                    continue;
                }
                if !property_matches_type(value, target) {
                    return Err(Error::ConstraintViolation {
                        name: spec.name.clone(),
                        kind: spec.kind.as_string(),
                        label: spec.label.clone(),
                        property: spec.property.clone(),
                        details: format!(
                            "node {id} has value of type {} (expected {})",
                            value.type_name(),
                            target.as_str()
                        ),
                    });
                }
            }
        }
    }
    Ok(())
}

/// Strict `Property` / `PropertyType` match. No numeric coercion — an
/// `Int64` does NOT satisfy `FLOAT`, matching Neo4j's `IS :: FLOAT`
/// semantics. Returns `false` for `Null`; callers should pre-filter
/// null values (`IS :: T` allows nulls by convention — pair with
/// `IS NOT NULL` for strict presence).
fn property_matches_type(value: &Property, target: PropertyType) -> bool {
    matches!(
        (target, value),
        (PropertyType::String, Property::String(_))
            | (PropertyType::Integer, Property::Int64(_))
            | (PropertyType::Float, Property::Float64(_))
            | (PropertyType::Boolean, Property::Bool(_))
    )
}

/// Rehydrate the constraint registry from [`CF_CONSTRAINT_META`] at
/// [`RocksDbStorageEngine::open`] time. Walks the CF sequentially —
/// constraint counts are tiny (single digits in practice) so this
/// stays cheap regardless of graph size.
fn load_constraint_meta(db: &DB) -> Result<Vec<PropertyConstraintSpec>> {
    let cf = db
        .cf_handle(CF_CONSTRAINT_META)
        .ok_or(Error::MissingColumnFamily(CF_CONSTRAINT_META))?;
    let mut specs = Vec::new();
    for item in db.iterator_cf(cf, IteratorMode::Start) {
        let (key, value) = item?;
        let name = std::str::from_utf8(&key)
            .map_err(|_| Error::CorruptBytes {
                cf: CF_CONSTRAINT_META,
                expected: key.len(),
                actual: key.len(),
            })?
            .to_string();
        specs.push(constraint_meta_decode(CF_CONSTRAINT_META, name, &value)?);
    }
    Ok(specs)
}

/// Rehydrate the property-index registry from [`CF_INDEX_META`] at
/// [`RocksDbStorageEngine::open`] time. Runs once per process, so
/// walking the CF sequentially is fine even for large index counts.
fn load_index_meta(db: &DB) -> Result<Vec<PropertyIndexSpec>> {
    let cf = db
        .cf_handle(CF_INDEX_META)
        .ok_or(Error::MissingColumnFamily(CF_INDEX_META))?;
    let mut specs = Vec::new();
    for item in db.iterator_cf(cf, IteratorMode::Start) {
        let (key, _) = item?;
        specs.push(index_meta_key_decode(&key)?);
    }
    Ok(specs)
}

impl StorageEngine for RocksDbStorageEngine {
    fn put_node(&self, node: &Node) -> Result<()> {
        RocksDbStorageEngine::put_node(self, node)
    }

    fn get_node(&self, id: NodeId) -> Result<Option<Node>> {
        RocksDbStorageEngine::get_node(self, id)
    }

    fn detach_delete_node(&self, id: NodeId) -> Result<()> {
        RocksDbStorageEngine::detach_delete_node(self, id)
    }

    fn put_edge(&self, edge: &Edge) -> Result<()> {
        RocksDbStorageEngine::put_edge(self, edge)
    }

    fn get_edge(&self, id: EdgeId) -> Result<Option<Edge>> {
        RocksDbStorageEngine::get_edge(self, id)
    }

    fn delete_edge(&self, id: EdgeId) -> Result<()> {
        RocksDbStorageEngine::delete_edge(self, id)
    }

    fn apply_batch(&self, mutations: &[GraphMutation]) -> Result<()> {
        RocksDbStorageEngine::apply_batch(self, mutations)
    }

    fn all_nodes(&self) -> Result<Vec<Node>> {
        RocksDbStorageEngine::all_nodes(self)
    }

    fn all_edges(&self) -> Result<Vec<Edge>> {
        RocksDbStorageEngine::all_edges(self)
    }

    fn all_node_ids(&self) -> Result<Vec<NodeId>> {
        RocksDbStorageEngine::all_node_ids(self)
    }

    fn outgoing(&self, source: NodeId) -> Result<Vec<(EdgeId, NodeId)>> {
        RocksDbStorageEngine::outgoing(self, source)
    }

    fn incoming(&self, target: NodeId) -> Result<Vec<(EdgeId, NodeId)>> {
        RocksDbStorageEngine::incoming(self, target)
    }

    fn nodes_by_label(&self, label: &str) -> Result<Vec<NodeId>> {
        RocksDbStorageEngine::nodes_by_label(self, label)
    }

    fn edges_by_type(&self, edge_type: &str) -> Result<Vec<EdgeId>> {
        RocksDbStorageEngine::edges_by_type(self, edge_type)
    }

    fn nodes_by_property(
        &self,
        label: &str,
        property: &str,
        value: &Property,
    ) -> Result<Vec<NodeId>> {
        RocksDbStorageEngine::nodes_by_property(self, label, property, value)
    }

    fn create_property_index(&self, label: &str, property: &str) -> Result<()> {
        RocksDbStorageEngine::create_property_index(self, label, property)
    }

    fn drop_property_index(&self, label: &str, property: &str) -> Result<()> {
        RocksDbStorageEngine::drop_property_index(self, label, property)
    }

    fn list_property_indexes(&self) -> Vec<PropertyIndexSpec> {
        RocksDbStorageEngine::list_property_indexes(self)
    }

    fn create_property_constraint(
        &self,
        name: Option<&str>,
        label: &str,
        property: &str,
        kind: PropertyConstraintKind,
        if_not_exists: bool,
    ) -> Result<PropertyConstraintSpec> {
        RocksDbStorageEngine::create_property_constraint(
            self,
            name,
            label,
            property,
            kind,
            if_not_exists,
        )
    }

    fn drop_property_constraint(&self, name: &str, if_exists: bool) -> Result<()> {
        RocksDbStorageEngine::drop_property_constraint(self, name, if_exists)
    }

    fn list_property_constraints(&self) -> Vec<PropertyConstraintSpec> {
        RocksDbStorageEngine::list_property_constraints(self)
    }

    fn create_checkpoint(&self, path: &Path) -> Result<()> {
        RocksDbStorageEngine::create_checkpoint(self, path)
    }

    fn clear_all(&self) -> Result<()> {
        RocksDbStorageEngine::clear_all(self)
    }
}
