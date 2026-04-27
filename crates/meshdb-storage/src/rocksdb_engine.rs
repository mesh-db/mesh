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
        ConstraintScope, EdgePointIndexSpec, EdgePropertyIndexSpec, GraphMutation, PointIndexSpec,
        PropertyConstraintKind, PropertyConstraintSpec, PropertyIndexSpec, PropertyType,
        StorageEngine,
    },
    error::{Error, Result},
    keys::{
        adj_key, cell_from_point_index_key, constraint_meta_decode, constraint_meta_encode,
        decode_point_index_value, edge_from_adj_key, edge_id_from_property_index_key,
        edge_property_index_composite_key, edge_property_index_composite_value_prefix,
        encode_index_tuple, encode_index_value, id_from_str_index_key, label_index_key,
        label_index_prefix, node_from_adj_value, node_id_from_point_index_key,
        node_id_from_property_index_key, parse_property_index_entry_props, point_cell,
        point_cell_range, point_index_key, point_index_label_prop_prefix, point_index_srid_prefix,
        point_index_value, property_index_composite_key, property_index_composite_value_prefix,
        property_index_label_prefix, type_index_key, type_index_prefix, ID_LEN,
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
const CF_EDGE_PROPERTY_INDEX: &str = "edge_property_index";
const CF_EDGE_INDEX_META: &str = "edge_index_meta";
const CF_CONSTRAINT_META: &str = "constraint_meta";
const CF_POINT_INDEX: &str = "point_index";
const CF_POINT_INDEX_META: &str = "point_index_meta";
const CF_EDGE_POINT_INDEX: &str = "edge_point_index";
const CF_EDGE_POINT_INDEX_META: &str = "edge_point_index_meta";
/// Persisted `apoc.trigger.*` registry. Keyed by trigger name
/// (UTF-8 bytes), valued by the JSON-encoded trigger spec
/// (cypher / selector / config / installed_at). Local-only in
/// this release — the trigger registry doesn't replicate
/// across cluster peers, so each node holds its own set.
const CF_TRIGGER_META: &str = "trigger_meta";
/// Persisted multi-raft pending-tx staging. Keyed by partition-
/// namespaced txid (4-byte LE partition + txid bytes), valued by
/// the serde-encoded `Vec<GraphMutation>` to apply on `CommitTx`.
/// Lets the partition applier reconstruct `pending_txs` after a
/// restart instead of relying on openraft to re-replay entries
/// past `last_applied` (which it doesn't).
const CF_PENDING_TX_META: &str = "pending_tx_meta";

const ALL_CFS: &[&str] = &[
    CF_NODES,
    CF_EDGES,
    CF_ADJ_OUT,
    CF_ADJ_IN,
    CF_LABEL_INDEX,
    CF_TYPE_INDEX,
    CF_PROPERTY_INDEX,
    CF_INDEX_META,
    CF_EDGE_PROPERTY_INDEX,
    CF_EDGE_INDEX_META,
    CF_CONSTRAINT_META,
    CF_POINT_INDEX,
    CF_POINT_INDEX_META,
    CF_EDGE_POINT_INDEX,
    CF_EDGE_POINT_INDEX_META,
    CF_TRIGGER_META,
    CF_PENDING_TX_META,
];

const EMPTY: &[u8] = &[];

/// Encode a [`PropertyIndexSpec`] as a stable `CF_INDEX_META` key:
/// `<label>\0<prop1>\0<prop2>\0...\0<propN>`. No component can contain
/// a NUL (identifier-only grammar), so splitting on NUL decodes
/// unambiguously. Length-1 specs encode byte-identically to the
/// pre-composite-refactor format (`<label>\0<prop>`), so existing
/// on-disk data survives the upgrade.
fn index_meta_key(spec: &PropertyIndexSpec) -> Vec<u8> {
    let cap = spec.label.len()
        + spec.properties.iter().map(|p| p.len()).sum::<usize>()
        + spec.properties.len();
    let mut k = Vec::with_capacity(cap);
    k.extend_from_slice(spec.label.as_bytes());
    for p in &spec.properties {
        k.push(0);
        k.extend_from_slice(p.as_bytes());
    }
    k
}

fn index_meta_key_decode(key: &[u8]) -> Result<PropertyIndexSpec> {
    let mut parts = key.split(|b| *b == 0);
    let label_bytes = parts.next().ok_or(Error::CorruptBytes {
        cf: CF_INDEX_META,
        expected: 1,
        actual: 0,
    })?;
    let label = std::str::from_utf8(label_bytes)
        .map_err(|_| Error::CorruptBytes {
            cf: CF_INDEX_META,
            expected: label_bytes.len(),
            actual: label_bytes.len(),
        })?
        .to_string();
    let mut properties: Vec<String> = Vec::new();
    for part in parts {
        let s = std::str::from_utf8(part)
            .map_err(|_| Error::CorruptBytes {
                cf: CF_INDEX_META,
                expected: part.len(),
                actual: part.len(),
            })?
            .to_string();
        properties.push(s);
    }
    if properties.is_empty() {
        return Err(Error::CorruptBytes {
            cf: CF_INDEX_META,
            expected: 1,
            actual: 0,
        });
    }
    Ok(PropertyIndexSpec { label, properties })
}

/// Encode an [`EdgePropertyIndexSpec`] as a stable `CF_EDGE_INDEX_META`
/// key: `<edge_type>\0<prop1>\0<prop2>\0...\0<propN>`. Same NUL-split
/// shape as the node form, same backward-compat guarantee for
/// length-1 specs.
fn edge_index_meta_key(spec: &EdgePropertyIndexSpec) -> Vec<u8> {
    let cap = spec.edge_type.len()
        + spec.properties.iter().map(|p| p.len()).sum::<usize>()
        + spec.properties.len();
    let mut k = Vec::with_capacity(cap);
    k.extend_from_slice(spec.edge_type.as_bytes());
    for p in &spec.properties {
        k.push(0);
        k.extend_from_slice(p.as_bytes());
    }
    k
}

fn edge_index_meta_key_decode(key: &[u8]) -> Result<EdgePropertyIndexSpec> {
    let mut parts = key.split(|b| *b == 0);
    let type_bytes = parts.next().ok_or(Error::CorruptBytes {
        cf: CF_EDGE_INDEX_META,
        expected: 1,
        actual: 0,
    })?;
    let edge_type = std::str::from_utf8(type_bytes)
        .map_err(|_| Error::CorruptBytes {
            cf: CF_EDGE_INDEX_META,
            expected: type_bytes.len(),
            actual: type_bytes.len(),
        })?
        .to_string();
    let mut properties: Vec<String> = Vec::new();
    for part in parts {
        let s = std::str::from_utf8(part)
            .map_err(|_| Error::CorruptBytes {
                cf: CF_EDGE_INDEX_META,
                expected: part.len(),
                actual: part.len(),
            })?
            .to_string();
        properties.push(s);
    }
    if properties.is_empty() {
        return Err(Error::CorruptBytes {
            cf: CF_EDGE_INDEX_META,
            expected: 1,
            actual: 0,
        });
    }
    Ok(EdgePropertyIndexSpec {
        edge_type,
        properties,
    })
}

/// Encode a [`PointIndexSpec`] as a stable `CF_POINT_INDEX_META`
/// key: `<label>\0<property>`. Single-property on purpose — the
/// spec only carries one property. NUL-split, same shape as the
/// non-spatial index meta format so tooling that inspects CFs by
/// hand stays predictable.
fn point_index_meta_key(spec: &PointIndexSpec) -> Vec<u8> {
    let mut k = Vec::with_capacity(spec.label.len() + spec.property.len() + 1);
    k.extend_from_slice(spec.label.as_bytes());
    k.push(0);
    k.extend_from_slice(spec.property.as_bytes());
    k
}

fn point_index_meta_key_decode(key: &[u8]) -> Result<PointIndexSpec> {
    let mut parts = key.splitn(2, |b| *b == 0);
    let label_bytes = parts.next().ok_or(Error::CorruptBytes {
        cf: CF_POINT_INDEX_META,
        expected: 1,
        actual: 0,
    })?;
    let property_bytes = parts.next().ok_or(Error::CorruptBytes {
        cf: CF_POINT_INDEX_META,
        expected: label_bytes.len() + 2,
        actual: key.len(),
    })?;
    let label = std::str::from_utf8(label_bytes)
        .map_err(|_| Error::CorruptBytes {
            cf: CF_POINT_INDEX_META,
            expected: label_bytes.len(),
            actual: label_bytes.len(),
        })?
        .to_string();
    let property = std::str::from_utf8(property_bytes)
        .map_err(|_| Error::CorruptBytes {
            cf: CF_POINT_INDEX_META,
            expected: property_bytes.len(),
            actual: property_bytes.len(),
        })?
        .to_string();
    if property.is_empty() {
        return Err(Error::CorruptBytes {
            cf: CF_POINT_INDEX_META,
            expected: 1,
            actual: 0,
        });
    }
    Ok(PointIndexSpec { label, property })
}

/// Relationship-scope analogue of [`point_index_meta_key`]. Same
/// `<edge_type>\0<property>` shape.
fn edge_point_index_meta_key(spec: &EdgePointIndexSpec) -> Vec<u8> {
    let mut k = Vec::with_capacity(spec.edge_type.len() + spec.property.len() + 1);
    k.extend_from_slice(spec.edge_type.as_bytes());
    k.push(0);
    k.extend_from_slice(spec.property.as_bytes());
    k
}

fn edge_point_index_meta_key_decode(key: &[u8]) -> Result<EdgePointIndexSpec> {
    let mut parts = key.splitn(2, |b| *b == 0);
    let type_bytes = parts.next().ok_or(Error::CorruptBytes {
        cf: CF_EDGE_POINT_INDEX_META,
        expected: 1,
        actual: 0,
    })?;
    let property_bytes = parts.next().ok_or(Error::CorruptBytes {
        cf: CF_EDGE_POINT_INDEX_META,
        expected: type_bytes.len() + 2,
        actual: key.len(),
    })?;
    let edge_type = std::str::from_utf8(type_bytes)
        .map_err(|_| Error::CorruptBytes {
            cf: CF_EDGE_POINT_INDEX_META,
            expected: type_bytes.len(),
            actual: type_bytes.len(),
        })?
        .to_string();
    let property = std::str::from_utf8(property_bytes)
        .map_err(|_| Error::CorruptBytes {
            cf: CF_EDGE_POINT_INDEX_META,
            expected: property_bytes.len(),
            actual: property_bytes.len(),
        })?
        .to_string();
    if property.is_empty() {
        return Err(Error::CorruptBytes {
            cf: CF_EDGE_POINT_INDEX_META,
            expected: 1,
            actual: 0,
        });
    }
    Ok(EdgePointIndexSpec {
        edge_type,
        property,
    })
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
    /// Relationship-scope analogue of `indexes`. Populated from
    /// [`CF_EDGE_INDEX_META`] at open time and kept in sync by
    /// [`RocksDbStorageEngine::create_edge_property_index`] /
    /// [`RocksDbStorageEngine::drop_edge_property_index`]. Consulted on
    /// every edge write path (`append_put_edge`, `append_delete_edge`,
    /// `append_detach_delete_node`) and by UNIQUE edge-constraint
    /// enforcement, so the same "must not hit the DB on the hot path"
    /// contract applies.
    edge_indexes: RwLock<Vec<EdgePropertyIndexSpec>>,
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
    /// In-memory view of the registered point indexes. Loaded from
    /// [`CF_POINT_INDEX_META`] at open and kept in sync by
    /// [`RocksDbStorageEngine::create_point_index`] /
    /// [`RocksDbStorageEngine::drop_point_index`]. `append_put_node`
    /// and `append_detach_delete_node` walk this list to maintain
    /// entries, so — as with the other in-memory index catalogs — it
    /// must not hit the DB on the hot path.
    point_indexes: RwLock<Vec<PointIndexSpec>>,
    /// Relationship-scope analogue of `point_indexes`. Consulted by
    /// every edge write (`append_put_edge`, `append_delete_edge`,
    /// `append_detach_delete_node`) to maintain
    /// [`CF_EDGE_POINT_INDEX`] entries.
    edge_point_indexes: RwLock<Vec<EdgePointIndexSpec>>,
}

/// Operator-tunable rocksdb options. Defaults match what
/// [`RocksDbStorageEngine::open`] applied historically (modest
/// `max_open_files`, capped LOG retention) — production
/// deployments override via the [`storage]` config section.
#[derive(Debug, Clone)]
pub struct StorageOptions {
    /// Cap on open SST files. RocksDB's default of -1 keeps
    /// every SST in the table cache forever, which blows past
    /// typical FD soft limits in CI / test setups. 64 is plenty
    /// for the hot set in the foundational test suites; bump
    /// for production read-heavy workloads.
    pub max_open_files: i32,
    /// Maximum number of historical INFO log files to keep on
    /// disk. RocksDB's default is 1000 which produces a long
    /// tail of LOG.old.* files that compounds disk pressure
    /// when many databases run in parallel.
    pub keep_log_file_num: usize,
    /// Per-column-family memtable size (write buffer). Bigger
    /// = fewer L0 files + more memory per CF. RocksDB's default
    /// is 64MB; tuning below that helps small / many-tenant
    /// deployments, above for write-heavy single-tenant ones.
    /// `None` keeps the upstream default.
    pub write_buffer_size_bytes: Option<usize>,
    /// Maximum number of memtables before writes stall. Bigger
    /// = more headroom for write bursts at the cost of memory.
    /// RocksDB's default is 2; 4-8 is common for write-heavy
    /// loads. `None` keeps the upstream default.
    pub max_write_buffer_number: Option<i32>,
    /// Bloom filter bits per key on the block-based table
    /// format. Bigger = lower false-positive rate on point
    /// reads at the cost of memory. RocksDB's default builds
    /// no bloom filter unless explicitly configured; 10 bits
    /// gives ~1% FP rate and is the typical recommendation.
    /// `None` keeps the upstream default (no bloom filter).
    pub bloom_filter_bits_per_key: Option<i32>,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            max_open_files: 64,
            keep_log_file_num: 4,
            write_buffer_size_bytes: None,
            max_write_buffer_number: None,
            bloom_filter_bits_per_key: None,
        }
    }
}

impl RocksDbStorageEngine {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, StorageOptions::default())
    }

    /// Open with operator-tunable rocksdb knobs. Applied to both
    /// the database-level options (FD cap, LOG retention) and
    /// every column family's table options (bloom filter, write
    /// buffer). Defaults match [`RocksDbStorageEngine::open`].
    pub fn open_with_options(path: impl AsRef<Path>, opts: StorageOptions) -> Result<Self> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_max_open_files(opts.max_open_files);
        db_opts.set_keep_log_file_num(opts.keep_log_file_num);

        // Per-CF options. Same options applied to every column
        // family — none of them have wildly different access
        // patterns, and the cost of tuning each independently
        // exceeds the win for v1.
        let mut cf_opts = Options::default();
        if let Some(sz) = opts.write_buffer_size_bytes {
            cf_opts.set_write_buffer_size(sz);
        }
        if let Some(n) = opts.max_write_buffer_number {
            cf_opts.set_max_write_buffer_number(n);
        }
        if let Some(bits) = opts.bloom_filter_bits_per_key {
            let mut block_opts = rocksdb::BlockBasedOptions::default();
            block_opts.set_bloom_filter(bits as f64, false);
            cf_opts.set_block_based_table_factory(&block_opts);
        }

        let cfs: Vec<ColumnFamilyDescriptor> = ALL_CFS
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, cf_opts.clone()))
            .collect();

        let db = DB::open_cf_descriptors(&db_opts, path, cfs)?;
        let indexes = load_index_meta(&db)?;
        let edge_indexes = load_edge_index_meta(&db)?;
        let constraints = load_constraint_meta(&db)?;
        let point_indexes = load_point_index_meta(&db)?;
        let edge_point_indexes = load_edge_point_index_meta(&db)?;
        Ok(Self {
            db,
            indexes: RwLock::new(indexes),
            edge_indexes: RwLock::new(edge_indexes),
            constraints: RwLock::new(constraints),
            point_indexes: RwLock::new(point_indexes),
            edge_point_indexes: RwLock::new(edge_point_indexes),
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
                    .and_then(|n| encode_index_tuple(&n.properties, &spec.properties))
            } else {
                None
            };
            let new_encoded = if now_indexed {
                encode_index_tuple(&node.properties, &spec.properties)
            } else {
                None
            };
            if old_encoded == new_encoded {
                continue;
            }
            if let Some(values) = &old_encoded {
                batch.delete_cf(
                    prop_cf,
                    property_index_composite_key(&spec.label, &spec.properties, values, node.id),
                );
            }
            if let Some(values) = &new_encoded {
                batch.put_cf(
                    prop_cf,
                    property_index_composite_key(&spec.label, &spec.properties, values, node.id),
                    EMPTY,
                );
            }
        }

        // Point-index maintenance. Same delta shape as the
        // property-index loop: compare old vs new indexed point and
        // emit the minimum entry set. Cheap-bail when neither the
        // label membership nor the point value has changed.
        let point_indexes = self
            .point_indexes
            .read()
            .expect("point_indexes lock poisoned");
        if !point_indexes.is_empty() {
            let point_cf = self.cf(CF_POINT_INDEX)?;
            for spec in point_indexes.iter() {
                let was_indexed = existing_labels.iter().any(|l| l == &spec.label);
                let now_indexed = node.labels.iter().any(|l| l == &spec.label);
                let old_point = if was_indexed {
                    existing
                        .as_ref()
                        .and_then(|n| extract_indexable_point(&n.properties, &spec.property))
                } else {
                    None
                };
                let new_point = if now_indexed {
                    extract_indexable_point(&node.properties, &spec.property)
                } else {
                    None
                };
                if old_point == new_point {
                    continue;
                }
                if let Some(p) = &old_point {
                    let cell = point_cell(p.srid, p.x, p.y);
                    batch.delete_cf(
                        point_cf,
                        point_index_key(
                            &spec.label,
                            &spec.property,
                            p.srid,
                            cell,
                            node.id.as_bytes(),
                        ),
                    );
                }
                if let Some(p) = &new_point {
                    let cell = point_cell(p.srid, p.x, p.y);
                    batch.put_cf(
                        point_cf,
                        point_index_key(
                            &spec.label,
                            &spec.property,
                            p.srid,
                            cell,
                            node.id.as_bytes(),
                        ),
                        point_index_value(p.x, p.y, p.z),
                    );
                }
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
        let edge_prop_cf = self.cf(CF_EDGE_PROPERTY_INDEX)?;

        // Relationship-scope constraint enforcement. Same short-
        // circuit pattern as `append_put_node` — if a constraint
        // would be violated, abort before touching the batch so the
        // write never lands.
        let existing_edge = self.get_edge(edge.id)?;
        self.enforce_edge_constraints(edge, existing_edge.as_ref())?;

        let bytes = serde_json::to_vec(edge)?;
        batch.put_cf(edges_cf, edge.id.as_bytes(), bytes);
        batch.put_cf(
            out_cf,
            adj_key(edge.source, edge.id),
            edge.target.as_bytes(),
        );
        batch.put_cf(in_cf, adj_key(edge.target, edge.id), edge.source.as_bytes());
        batch.put_cf(type_cf, type_index_key(&edge.edge_type, edge.id), EMPTY);

        // Edge-property-index maintenance: for each registered
        // (edge_type, prop) index, delta the previous entry against
        // the new one and emit the minimal set of put/delete
        // operations. Mirrors the node path in `append_put_node`.
        // An existing edge whose type doesn't match the registered
        // index contributes no work — edges never change their type,
        // so a type-mismatched entry can't exist to clean up.
        let edge_indexes = self
            .edge_indexes
            .read()
            .expect("edge_indexes lock poisoned");
        for spec in edge_indexes.iter() {
            if spec.edge_type != edge.edge_type {
                continue;
            }
            let old_encoded = existing_edge
                .as_ref()
                .and_then(|e| encode_index_tuple(&e.properties, &spec.properties));
            let new_encoded = encode_index_tuple(&edge.properties, &spec.properties);
            if old_encoded == new_encoded {
                continue;
            }
            if let Some(values) = &old_encoded {
                batch.delete_cf(
                    edge_prop_cf,
                    edge_property_index_composite_key(
                        &spec.edge_type,
                        &spec.properties,
                        values,
                        edge.id,
                    ),
                );
            }
            if let Some(values) = &new_encoded {
                batch.put_cf(
                    edge_prop_cf,
                    edge_property_index_composite_key(
                        &spec.edge_type,
                        &spec.properties,
                        values,
                        edge.id,
                    ),
                    EMPTY,
                );
            }
        }

        // Edge-point-index maintenance — relationship-scope mirror
        // of the node-point-index loop in `append_put_node`. Same
        // delta shape: diff the indexed point across the previous
        // and new edge state and emit the minimum put/delete set.
        let edge_point_indexes = self
            .edge_point_indexes
            .read()
            .expect("edge_point_indexes lock poisoned");
        if !edge_point_indexes.is_empty() {
            let point_cf = self.cf(CF_EDGE_POINT_INDEX)?;
            for spec in edge_point_indexes.iter() {
                if spec.edge_type != edge.edge_type {
                    continue;
                }
                let old_point = existing_edge
                    .as_ref()
                    .and_then(|e| extract_indexable_point(&e.properties, &spec.property));
                let new_point = extract_indexable_point(&edge.properties, &spec.property);
                if old_point == new_point {
                    continue;
                }
                if let Some(p) = &old_point {
                    let cell = point_cell(p.srid, p.x, p.y);
                    batch.delete_cf(
                        point_cf,
                        point_index_key(
                            &spec.edge_type,
                            &spec.property,
                            p.srid,
                            cell,
                            edge.id.as_bytes(),
                        ),
                    );
                }
                if let Some(p) = &new_point {
                    let cell = point_cell(p.srid, p.x, p.y);
                    batch.put_cf(
                        point_cf,
                        point_index_key(
                            &spec.edge_type,
                            &spec.property,
                            p.srid,
                            cell,
                            edge.id.as_bytes(),
                        ),
                        point_index_value(p.x, p.y, p.z),
                    );
                }
            }
        }
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
        let edge_prop_cf = self.cf(CF_EDGE_PROPERTY_INDEX)?;

        batch.delete_cf(edges_cf, id.as_bytes());
        batch.delete_cf(out_cf, adj_key(edge.source, id));
        batch.delete_cf(in_cf, adj_key(edge.target, id));
        batch.delete_cf(type_cf, type_index_key(&edge.edge_type, id));

        // Remove any edge-property-index entries this edge owned.
        // Mirrors the put path: for each registered index whose type
        // matches, the encoded value (if any) pinned an index key we
        // now need to delete.
        let edge_indexes = self
            .edge_indexes
            .read()
            .expect("edge_indexes lock poisoned");
        for spec in edge_indexes.iter() {
            if spec.edge_type != edge.edge_type {
                continue;
            }
            if let Some(values) = encode_index_tuple(&edge.properties, &spec.properties) {
                batch.delete_cf(
                    edge_prop_cf,
                    edge_property_index_composite_key(
                        &spec.edge_type,
                        &spec.properties,
                        &values,
                        id,
                    ),
                );
            }
        }

        // Edge-point-index sweep for the deleted edge. Mirrors the
        // node-point-index sweep in `append_detach_delete_node`.
        let edge_point_indexes = self
            .edge_point_indexes
            .read()
            .expect("edge_point_indexes lock poisoned");
        if !edge_point_indexes.is_empty() {
            let point_cf = self.cf(CF_EDGE_POINT_INDEX)?;
            for spec in edge_point_indexes.iter() {
                if spec.edge_type != edge.edge_type {
                    continue;
                }
                if let Some(p) = extract_indexable_point(&edge.properties, &spec.property) {
                    let cell = point_cell(p.srid, p.x, p.y);
                    batch.delete_cf(
                        point_cf,
                        point_index_key(
                            &spec.edge_type,
                            &spec.property,
                            p.srid,
                            cell,
                            id.as_bytes(),
                        ),
                    );
                }
            }
        }
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
        let edge_prop_cf = self.cf(CF_EDGE_PROPERTY_INDEX)?;

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
                if let Some(values) = encode_index_tuple(&n.properties, &spec.properties) {
                    batch.delete_cf(
                        prop_cf,
                        property_index_composite_key(&spec.label, &spec.properties, &values, id),
                    );
                }
            }
            // Point-index entries held by this node, mirror of the
            // property-index sweep above.
            let point_indexes = self
                .point_indexes
                .read()
                .expect("point_indexes lock poisoned");
            if !point_indexes.is_empty() {
                let point_cf = self.cf(CF_POINT_INDEX)?;
                for spec in point_indexes.iter() {
                    if !n.labels.iter().any(|l| l == &spec.label) {
                        continue;
                    }
                    if let Some(p) = extract_indexable_point(&n.properties, &spec.property) {
                        let cell = point_cell(p.srid, p.x, p.y);
                        batch.delete_cf(
                            point_cf,
                            point_index_key(
                                &spec.label,
                                &spec.property,
                                p.srid,
                                cell,
                                id.as_bytes(),
                            ),
                        );
                    }
                }
            }
        }

        // Edges detached by this delete also need their
        // edge-property-index entries swept. Read the catalog once so
        // the inner loop doesn't reacquire the lock per edge.
        let edge_indexes_snapshot = self
            .edge_indexes
            .read()
            .expect("edge_indexes lock poisoned")
            .clone();
        let edge_point_indexes_snapshot = self
            .edge_point_indexes
            .read()
            .expect("edge_point_indexes lock poisoned")
            .clone();
        let edge_point_cf_opt = if edge_point_indexes_snapshot.is_empty() {
            None
        } else {
            Some(self.cf(CF_EDGE_POINT_INDEX)?)
        };

        for (edge_id, target) in &outgoing {
            if let Some(e) = self.get_edge(*edge_id)? {
                batch.delete_cf(type_cf, type_index_key(&e.edge_type, *edge_id));
                for spec in &edge_indexes_snapshot {
                    if spec.edge_type != e.edge_type {
                        continue;
                    }
                    if let Some(values) = encode_index_tuple(&e.properties, &spec.properties) {
                        batch.delete_cf(
                            edge_prop_cf,
                            edge_property_index_composite_key(
                                &spec.edge_type,
                                &spec.properties,
                                &values,
                                *edge_id,
                            ),
                        );
                    }
                }
                if let Some(point_cf) = edge_point_cf_opt {
                    for spec in &edge_point_indexes_snapshot {
                        if spec.edge_type != e.edge_type {
                            continue;
                        }
                        if let Some(p) = extract_indexable_point(&e.properties, &spec.property) {
                            let cell = point_cell(p.srid, p.x, p.y);
                            batch.delete_cf(
                                point_cf,
                                point_index_key(
                                    &spec.edge_type,
                                    &spec.property,
                                    p.srid,
                                    cell,
                                    edge_id.as_bytes(),
                                ),
                            );
                        }
                    }
                }
            }
            batch.delete_cf(edges_cf, edge_id.as_bytes());
            batch.delete_cf(out_cf, adj_key(id, *edge_id));
            batch.delete_cf(in_cf, adj_key(*target, *edge_id));
        }
        for (edge_id, source) in &incoming {
            if let Some(e) = self.get_edge(*edge_id)? {
                batch.delete_cf(type_cf, type_index_key(&e.edge_type, *edge_id));
                for spec in &edge_indexes_snapshot {
                    if spec.edge_type != e.edge_type {
                        continue;
                    }
                    if let Some(values) = encode_index_tuple(&e.properties, &spec.properties) {
                        batch.delete_cf(
                            edge_prop_cf,
                            edge_property_index_composite_key(
                                &spec.edge_type,
                                &spec.properties,
                                &values,
                                *edge_id,
                            ),
                        );
                    }
                }
                if let Some(point_cf) = edge_point_cf_opt {
                    for spec in &edge_point_indexes_snapshot {
                        if spec.edge_type != e.edge_type {
                            continue;
                        }
                        if let Some(p) = extract_indexable_point(&e.properties, &spec.property) {
                            let cell = point_cell(p.srid, p.x, p.y);
                            batch.delete_cf(
                                point_cf,
                                point_index_key(
                                    &spec.edge_type,
                                    &spec.property,
                                    p.srid,
                                    cell,
                                    edge_id.as_bytes(),
                                ),
                            );
                        }
                    }
                }
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
        self.create_property_index_composite(label, &[property.to_string()])
    }

    /// Composite form of [`create_property_index`]. Declares a
    /// `(label, properties)` tuple index and backfills it by scanning
    /// every node currently carrying `label`. A node that's missing
    /// any of the properties, or carries an unindexable value for
    /// any of them, contributes no entry — composite indexes are
    /// all-or-nothing on the tuple.
    ///
    /// Single-property calls delegate here with a length-1 slice;
    /// the on-disk key format is byte-identical to the pre-composite
    /// layout in that case.
    pub fn create_property_index_composite(
        &self,
        label: &str,
        properties: &[String],
    ) -> Result<()> {
        assert!(
            !properties.is_empty(),
            "create_property_index_composite requires at least one property"
        );
        let spec = PropertyIndexSpec {
            label: label.to_string(),
            properties: properties.to_vec(),
        };
        let mut guard = self.indexes.write().expect("indexes lock poisoned");
        if guard.contains(&spec) {
            return Ok(());
        }

        let meta_cf = self.cf(CF_INDEX_META)?;
        let prop_cf = self.cf(CF_PROPERTY_INDEX)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(meta_cf, index_meta_key(&spec), EMPTY);

        for node_id in self.nodes_by_label(label)? {
            let node = match self.get_node(node_id)? {
                Some(n) => n,
                None => continue,
            };
            if let Some(values) = encode_index_tuple(&node.properties, &spec.properties) {
                batch.put_cf(
                    prop_cf,
                    property_index_composite_key(label, properties, &values, node_id),
                    EMPTY,
                );
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
        scope: &ConstraintScope,
        properties: &[String],
        kind: PropertyConstraintKind,
        if_not_exists: bool,
    ) -> Result<PropertyConstraintSpec> {
        // Arity validation: single-property kinds get exactly one
        // property; NodeKey accepts any positive count. Empty lists
        // are always invalid. NodeKey on relationship scope isn't
        // allowed — the "NODE" in the name is specifically about
        // node semantics; relationship equivalents would need their
        // own variant.
        if properties.is_empty() {
            return Err(Error::ConstraintArity {
                kind: kind.as_string(),
                details: "at least one property is required".into(),
            });
        }
        if !kind.allows_multi_property() && properties.len() != 1 {
            return Err(Error::ConstraintArity {
                kind: kind.as_string(),
                details: format!("expects exactly one property, got {}", properties.len()),
            });
        }
        if matches!(kind, PropertyConstraintKind::NodeKey)
            && matches!(scope, ConstraintScope::Relationship(_))
        {
            return Err(Error::ConstraintArity {
                kind: kind.as_string(),
                details: "NODE KEY cannot be applied to a relationship scope".into(),
            });
        }
        let resolved_name = match name {
            Some(n) => n.to_string(),
            None => default_constraint_name(scope, properties, kind),
        };
        let spec = PropertyConstraintSpec {
            name: resolved_name.clone(),
            scope: scope.clone(),
            properties: properties.to_vec(),
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

        // UNIQUE needs a backing single-property equality index on
        // both scopes so enforcement stays O(log N) per insert. The
        // create calls are idempotent, mirroring Neo4j's "a unique
        // constraint also provides an index" contract. The backing
        // index is not torn down on `DROP CONSTRAINT`; users who
        // want it gone issue a separate `DROP INDEX`. NODE KEY gets
        // the same auto-provisioning against a composite tuple
        // index so its uniqueness check is a single seek instead of
        // an O(M * K) label walk.
        match (kind, scope) {
            (PropertyConstraintKind::Unique, ConstraintScope::Node(label)) => {
                self.create_property_index(label, &properties[0])?;
            }
            (PropertyConstraintKind::Unique, ConstraintScope::Relationship(edge_type)) => {
                self.create_edge_property_index(edge_type, &properties[0])?;
            }
            (PropertyConstraintKind::NodeKey, ConstraintScope::Node(label)) => {
                self.create_property_index_composite(label, properties)?;
            }
            _ => {}
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
    /// value". Relationship-scope constraints are ignored here — the
    /// edge write path has its own entry point.
    fn enforce_constraints(&self, node: &Node, existing: Option<&Node>) -> Result<()> {
        let constraints = self.constraints.read().expect("constraints lock poisoned");
        if constraints.is_empty() {
            return Ok(());
        }
        let now_has_label = |label: &str| node.labels.iter().any(|l| l == label);
        for spec in constraints.iter() {
            let label = match &spec.scope {
                ConstraintScope::Node(l) => l,
                // Relationship-scope constraints apply to edges, not
                // nodes. Skip here; `enforce_edge_constraints` runs
                // on the edge write path.
                ConstraintScope::Relationship(_) => continue,
            };
            if !now_has_label(label) {
                continue;
            }
            // Single-property kinds store the target in
            // `properties[0]`; NodeKey walks the whole list.
            let primary = spec.primary_property();
            match spec.kind {
                PropertyConstraintKind::NotNull => {
                    let present = node
                        .properties
                        .get(primary)
                        .is_some_and(|v| !matches!(v, Property::Null));
                    if !present {
                        return Err(Error::ConstraintViolation {
                            name: spec.name.clone(),
                            kind: spec.kind.as_string(),
                            label: label.clone(),
                            property: primary.to_string(),
                            details: format!(
                                "node {} is missing required property `{}`",
                                node.id, primary
                            ),
                        });
                    }
                }
                PropertyConstraintKind::Unique => {
                    let Some(value) = node.properties.get(primary) else {
                        // No value → no uniqueness collision possible.
                        // Matches Neo4j: UNIQUE alone allows NULL;
                        // combine with NOT NULL for strict presence.
                        continue;
                    };
                    if matches!(value, Property::Null) {
                        continue;
                    }
                    if encode_index_value(value).is_none() {
                        continue;
                    }
                    let holders = self.nodes_by_property(label, primary, value)?;
                    for other in holders {
                        if other == node.id {
                            continue;
                        }
                        let was_self = existing
                            .and_then(|n| n.properties.get(primary))
                            .is_some_and(|v| v == value);
                        let _ = was_self;
                        return Err(Error::ConstraintViolation {
                            name: spec.name.clone(),
                            kind: spec.kind.as_string(),
                            label: label.clone(),
                            property: primary.to_string(),
                            details: format!("value already held by node {}", other),
                        });
                    }
                }
                PropertyConstraintKind::PropertyType(target) => {
                    let Some(value) = node.properties.get(primary) else {
                        continue;
                    };
                    if matches!(value, Property::Null) {
                        continue;
                    }
                    if !property_matches_type(value, target) {
                        return Err(Error::ConstraintViolation {
                            name: spec.name.clone(),
                            kind: spec.kind.as_string(),
                            label: label.clone(),
                            property: primary.to_string(),
                            details: format!(
                                "node {} has value of type {} (expected {})",
                                node.id,
                                value.type_name(),
                                target.as_str()
                            ),
                        });
                    }
                }
                PropertyConstraintKind::NodeKey => {
                    // Presence check first: every property must be
                    // present and non-null. Fails fast before the
                    // seek so a missing-property error takes
                    // precedence over a (potentially) conflicting
                    // existing tuple.
                    let mut my_tuple: Vec<Property> = Vec::with_capacity(spec.properties.len());
                    for prop in &spec.properties {
                        match node.properties.get(prop) {
                            Some(v) if !matches!(v, Property::Null) => my_tuple.push(v.clone()),
                            _ => {
                                return Err(Error::ConstraintViolation {
                                    name: spec.name.clone(),
                                    kind: spec.kind.as_string(),
                                    label: label.clone(),
                                    property: prop.clone(),
                                    details: format!(
                                        "node {} is missing required property `{}`",
                                        node.id, prop
                                    ),
                                });
                            }
                        }
                    }
                    // Uniqueness via the auto-provisioned composite
                    // index (see `create_property_constraint`). O(log N)
                    // per insert instead of the old O(M × K) label walk.
                    let hits = self.nodes_by_properties(label, &spec.properties, &my_tuple)?;
                    if let Some(other_id) = hits.iter().find(|&&id| id != node.id) {
                        return Err(Error::ConstraintViolation {
                            name: spec.name.clone(),
                            kind: spec.kind.as_string(),
                            label: label.clone(),
                            property: spec.properties.join(","),
                            details: format!("tuple already held by node {}", other_id),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Enforce registered relationship-scope constraints against the
    /// post-write state of `edge`. Invoked by the `put_edge` path
    /// before the batch is constructed. Walks every constraint whose
    /// scope matches the edge's type and runs the kind-specific
    /// check. `existing` is the prior on-disk snapshot of the same
    /// edge (if any) so the uniqueness check can allow self-updates.
    fn enforce_edge_constraints(&self, edge: &Edge, existing: Option<&Edge>) -> Result<()> {
        let constraints = self.constraints.read().expect("constraints lock poisoned");
        if constraints.is_empty() {
            return Ok(());
        }
        for spec in constraints.iter() {
            let edge_type = match &spec.scope {
                ConstraintScope::Relationship(t) => t,
                // Node-scope constraints don't concern edges.
                ConstraintScope::Node(_) => continue,
            };
            if edge_type != &edge.edge_type {
                continue;
            }
            let primary = spec.primary_property();
            match spec.kind {
                PropertyConstraintKind::NotNull => {
                    let present = edge
                        .properties
                        .get(primary)
                        .is_some_and(|v| !matches!(v, Property::Null));
                    if !present {
                        return Err(Error::ConstraintViolation {
                            name: spec.name.clone(),
                            kind: spec.kind.as_string(),
                            label: edge_type.clone(),
                            property: primary.to_string(),
                            details: format!(
                                "edge {} is missing required property `{}`",
                                edge.id, primary
                            ),
                        });
                    }
                }
                PropertyConstraintKind::Unique => {
                    let Some(value) = edge.properties.get(primary) else {
                        // No value → no uniqueness collision possible.
                        // Matches the node path: UNIQUE alone allows
                        // NULL / missing; combine with NOT NULL for
                        // strict presence.
                        continue;
                    };
                    if matches!(value, Property::Null) {
                        continue;
                    }
                    if encode_index_value(value).is_none() {
                        // Value type doesn't support equality in the
                        // index (Float, List, Map, etc.). Skip — the
                        // node path makes the same call for
                        // consistency, and the index can't help us
                        // here anyway.
                        continue;
                    }
                    // Probe the backing edge-property index (created
                    // by `create_property_constraint` above) for
                    // rival holders. O(log N) + O(matching edges)
                    // vs. the previous O(E_type) per-insert scan.
                    let holders = self.edges_by_property(edge_type, primary, value)?;
                    for other_id in holders {
                        if other_id == edge.id {
                            continue;
                        }
                        let was_self = existing
                            .and_then(|e| e.properties.get(primary))
                            .is_some_and(|v| v == value);
                        let _ = was_self;
                        return Err(Error::ConstraintViolation {
                            name: spec.name.clone(),
                            kind: spec.kind.as_string(),
                            label: edge_type.clone(),
                            property: primary.to_string(),
                            details: format!("value already held by edge {}", other_id),
                        });
                    }
                }
                PropertyConstraintKind::PropertyType(target) => {
                    let Some(value) = edge.properties.get(primary) else {
                        continue;
                    };
                    if matches!(value, Property::Null) {
                        continue;
                    }
                    if !property_matches_type(value, target) {
                        return Err(Error::ConstraintViolation {
                            name: spec.name.clone(),
                            kind: spec.kind.as_string(),
                            label: edge_type.clone(),
                            property: primary.to_string(),
                            details: format!(
                                "edge {} has value of type {} (expected {})",
                                edge.id,
                                value.type_name(),
                                target.as_str()
                            ),
                        });
                    }
                }
                PropertyConstraintKind::NodeKey => {
                    // Disallowed at create time — see
                    // `create_property_constraint`.
                    unreachable!(
                        "NODE KEY on relationship scope should have been rejected at create time"
                    );
                }
            }
        }
        Ok(())
    }

    /// Tear down a property index: removes the meta entry and every
    /// entry under the `(label, prop)` prefix. Idempotent: dropping a
    /// non-existent index is a no-op. Atomic via one [`WriteBatch`].
    pub fn drop_property_index(&self, label: &str, property: &str) -> Result<()> {
        self.drop_property_index_composite(label, &[property.to_string()])
    }

    pub fn drop_property_index_composite(&self, label: &str, properties: &[String]) -> Result<()> {
        assert!(
            !properties.is_empty(),
            "drop_property_index_composite requires at least one property"
        );
        let spec = PropertyIndexSpec {
            label: label.to_string(),
            properties: properties.to_vec(),
        };
        let mut guard = self.indexes.write().expect("indexes lock poisoned");
        if !guard.contains(&spec) {
            return Ok(());
        }

        // Reject the drop if a UNIQUE or NODE KEY constraint on the
        // same `(label, properties)` is registered — enforcement for
        // those kinds runs a seek through this exact index, so
        // silently removing it would collapse the uniqueness check
        // to "always passes" without producing any user-visible
        // signal. `DROP CONSTRAINT` is the right prerequisite.
        if let Some(constraint) = find_constraint_backing_node_index(
            &self.constraints.read().expect("constraints lock poisoned"),
            label,
            properties,
        ) {
            return Err(Error::IndexInUse { constraint });
        }

        let meta_cf = self.cf(CF_INDEX_META)?;
        let prop_cf = self.cf(CF_PROPERTY_INDEX)?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(meta_cf, index_meta_key(&spec));

        // Interleaved key layout means no clean `[label][props...]`
        // byte-prefix scopes a composite index's entries on its own —
        // an index `(a, b)` would share a `[label][a]` prefix with
        // single-property index `(a)`. Iterate by the label-only
        // prefix and parse each entry's property sequence, deleting
        // those that exactly match this spec's list.
        let label_prefix = property_index_label_prefix(label);
        let iter = self.db.iterator_cf(
            prop_cf,
            IteratorMode::From(&label_prefix, Direction::Forward),
        );
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(&label_prefix) {
                break;
            }
            let Some(parsed) = parse_property_index_entry_props(&key) else {
                continue;
            };
            if parsed.len() == properties.len()
                && parsed.iter().zip(properties.iter()).all(|(a, b)| a == b)
            {
                batch.delete_cf(prop_cf, key);
            }
        }

        self.db.write(batch)?;
        guard.retain(|s| s != &spec);
        Ok(())
    }

    /// Snapshot the currently-registered edge property indexes.
    /// Mirror of [`RocksDbStorageEngine::list_property_indexes`] for
    /// relationship scope. Cheap clone — specs are tiny.
    pub fn list_edge_property_indexes(&self) -> Vec<EdgePropertyIndexSpec> {
        self.edge_indexes
            .read()
            .expect("edge_indexes lock poisoned")
            .clone()
    }

    /// Declare a new `(edge_type, property)` edge property index and
    /// backfill it by scanning every edge currently carrying
    /// `edge_type`. Idempotent: re-creating an already-registered
    /// index is a no-op. Mirror of
    /// [`RocksDbStorageEngine::create_property_index`] for
    /// relationship scope; the same "same WriteBatch" atomicity
    /// guarantee applies — a crash mid-backfill leaves the store with
    /// either zero entries or a fully-populated index, never a
    /// partially-built one.
    pub fn create_edge_property_index(&self, edge_type: &str, property: &str) -> Result<()> {
        self.create_edge_property_index_composite(edge_type, &[property.to_string()])
    }

    pub fn create_edge_property_index_composite(
        &self,
        edge_type: &str,
        properties: &[String],
    ) -> Result<()> {
        assert!(
            !properties.is_empty(),
            "create_edge_property_index_composite requires at least one property"
        );
        let spec = EdgePropertyIndexSpec {
            edge_type: edge_type.to_string(),
            properties: properties.to_vec(),
        };
        let mut guard = self
            .edge_indexes
            .write()
            .expect("edge_indexes lock poisoned");
        if guard.contains(&spec) {
            return Ok(());
        }

        let meta_cf = self.cf(CF_EDGE_INDEX_META)?;
        let prop_cf = self.cf(CF_EDGE_PROPERTY_INDEX)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(meta_cf, edge_index_meta_key(&spec), EMPTY);

        for edge_id in self.edges_by_type(edge_type)? {
            let edge = match self.get_edge(edge_id)? {
                Some(e) => e,
                None => continue,
            };
            if let Some(values) = encode_index_tuple(&edge.properties, &spec.properties) {
                batch.put_cf(
                    prop_cf,
                    edge_property_index_composite_key(edge_type, properties, &values, edge_id),
                    EMPTY,
                );
            }
        }

        self.db.write(batch)?;
        guard.push(spec);
        Ok(())
    }

    pub fn drop_edge_property_index(&self, edge_type: &str, property: &str) -> Result<()> {
        self.drop_edge_property_index_composite(edge_type, &[property.to_string()])
    }

    pub fn drop_edge_property_index_composite(
        &self,
        edge_type: &str,
        properties: &[String],
    ) -> Result<()> {
        assert!(
            !properties.is_empty(),
            "drop_edge_property_index_composite requires at least one property"
        );
        let spec = EdgePropertyIndexSpec {
            edge_type: edge_type.to_string(),
            properties: properties.to_vec(),
        };
        let mut guard = self
            .edge_indexes
            .write()
            .expect("edge_indexes lock poisoned");
        if !guard.contains(&spec) {
            return Ok(());
        }

        // Same rationale as the node-side drop guard: a UNIQUE
        // constraint on this relationship type + property seeks
        // through this exact index during enforcement, so silently
        // removing it would defeat the check. Require
        // `DROP CONSTRAINT` first.
        if let Some(constraint) = find_constraint_backing_edge_index(
            &self.constraints.read().expect("constraints lock poisoned"),
            edge_type,
            properties,
        ) {
            return Err(Error::IndexInUse { constraint });
        }

        let meta_cf = self.cf(CF_EDGE_INDEX_META)?;
        let prop_cf = self.cf(CF_EDGE_PROPERTY_INDEX)?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(meta_cf, edge_index_meta_key(&spec));

        // See the node-side drop for why DROP parses entries rather
        // than scoping with a property-name byte prefix.
        let label_prefix = property_index_label_prefix(edge_type);
        let iter = self.db.iterator_cf(
            prop_cf,
            IteratorMode::From(&label_prefix, Direction::Forward),
        );
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(&label_prefix) {
                break;
            }
            let Some(parsed) = parse_property_index_entry_props(&key) else {
                continue;
            };
            if parsed.len() == properties.len()
                && parsed.iter().zip(properties.iter()).all(|(a, b)| a == b)
            {
                batch.delete_cf(prop_cf, key);
            }
        }

        self.db.write(batch)?;
        guard.retain(|s| s != &spec);
        Ok(())
    }

    // ---------------------------------------------------------------
    // Point / spatial index (node scope).
    //
    // Z-order (Morton) cell encoding over a fixed per-SRID domain.
    // Each indexed `Property::Point` produces one row keyed by
    // `<label>\0<prop>\0<srid:4><cell:8><node_id:16>` with the point's
    // coordinates stored in the value. Cross-SRID queries are
    // impossible by construction — the SRID is in the key prefix, so
    // a bbox scan is naturally scoped to one CRS.
    //
    // Single-property only for now. Extending to composite spatial
    // or to relationship scope is additive — separate CFs and
    // separate spec shapes.
    // ---------------------------------------------------------------

    pub fn list_point_indexes(&self) -> Vec<PointIndexSpec> {
        self.point_indexes
            .read()
            .expect("point_indexes lock poisoned")
            .clone()
    }

    /// Declare a new point index on `(label, property)` and backfill
    /// it by scanning every node that currently carries the label.
    /// Idempotent: re-creating a registered index is a no-op. Same
    /// "single WriteBatch" atomicity guarantee as the property-index
    /// create path — a crash mid-backfill leaves the store with
    /// either zero entries or a fully-populated index.
    pub fn create_point_index(&self, label: &str, property: &str) -> Result<()> {
        let spec = PointIndexSpec {
            label: label.to_string(),
            property: property.to_string(),
        };
        let mut guard = self
            .point_indexes
            .write()
            .expect("point_indexes lock poisoned");
        if guard.contains(&spec) {
            return Ok(());
        }

        let meta_cf = self.cf(CF_POINT_INDEX_META)?;
        let point_cf = self.cf(CF_POINT_INDEX)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(meta_cf, point_index_meta_key(&spec), EMPTY);

        for node_id in self.nodes_by_label(label)? {
            let node = match self.get_node(node_id)? {
                Some(n) => n,
                None => continue,
            };
            if let Some(Property::Point(p)) = node.properties.get(property) {
                let cell = point_cell(p.srid, p.x, p.y);
                batch.put_cf(
                    point_cf,
                    point_index_key(label, property, p.srid, cell, node_id.as_bytes()),
                    point_index_value(p.x, p.y, p.z),
                );
            }
        }

        self.db.write(batch)?;
        guard.push(spec);
        Ok(())
    }

    /// Tear down a point index. Removes the meta entry and every
    /// entry under the `<label>\0<prop>\0` prefix — that sweep
    /// covers all SRID variants at once, since SRID is to the right
    /// of the label+property header. Idempotent.
    pub fn drop_point_index(&self, label: &str, property: &str) -> Result<()> {
        let spec = PointIndexSpec {
            label: label.to_string(),
            property: property.to_string(),
        };
        let mut guard = self
            .point_indexes
            .write()
            .expect("point_indexes lock poisoned");
        if !guard.contains(&spec) {
            return Ok(());
        }

        let meta_cf = self.cf(CF_POINT_INDEX_META)?;
        let point_cf = self.cf(CF_POINT_INDEX)?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(meta_cf, point_index_meta_key(&spec));

        let prefix = point_index_label_prop_prefix(label, property);
        let iter = self
            .db
            .iterator_cf(point_cf, IteratorMode::From(&prefix, Direction::Forward));
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            batch.delete_cf(point_cf, key);
        }

        self.db.write(batch)?;
        guard.retain(|s| s != &spec);
        Ok(())
    }

    /// Range query: node ids whose indexed point falls inside the
    /// axis-aligned bounding box `[(xlo, ylo), (xhi, yhi)]` under
    /// `srid`. The scan walks the Morton-code cell range that
    /// covers the bbox and applies a precise per-row filter against
    /// the coordinates stored in the value — the cell range is a
    /// superset, so the filter is what guarantees correctness.
    ///
    /// A missing index returns empty rather than erroring, matching
    /// the property-seek convention (lets the planner race a DROP
    /// without a fatal). Results are sorted by `NodeId` for
    /// determinism across runs.
    pub fn nodes_in_bbox(
        &self,
        label: &str,
        property: &str,
        srid: i32,
        xlo: f64,
        ylo: f64,
        xhi: f64,
        yhi: f64,
    ) -> Result<Vec<NodeId>> {
        let spec = PointIndexSpec {
            label: label.to_string(),
            property: property.to_string(),
        };
        {
            let guard = self
                .point_indexes
                .read()
                .expect("point_indexes lock poisoned");
            if !guard.contains(&spec) {
                return Ok(Vec::new());
            }
        }

        let (lo_x, hi_x) = if xlo <= xhi { (xlo, xhi) } else { (xhi, xlo) };
        let (lo_y, hi_y) = if ylo <= yhi { (ylo, yhi) } else { (yhi, ylo) };
        let (min_cell, max_cell) = point_cell_range(srid, lo_x, lo_y, hi_x, hi_y);

        let prefix = point_index_srid_prefix(label, property, srid);
        let header_len = prefix.len();
        let mut seek = prefix.clone();
        seek.extend_from_slice(&min_cell.to_be_bytes());

        let cf = self.cf(CF_POINT_INDEX)?;
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&seek, Direction::Forward));
        let mut results = Vec::new();
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let cell = cell_from_point_index_key(CF_POINT_INDEX, &key, header_len)?;
            if cell > max_cell {
                break;
            }
            let (x, y, _z) = decode_point_index_value(CF_POINT_INDEX, &value)?;
            if x < lo_x || x > hi_x || y < lo_y || y > hi_y {
                continue;
            }
            let id_bytes = node_id_from_point_index_key(CF_POINT_INDEX, &key)?;
            results.push(NodeId::from_bytes(id_bytes));
        }
        results.sort();
        Ok(results)
    }

    // ---------------------------------------------------------------
    // Point / spatial index (relationship scope).
    //
    // Mirror of the node-scope block above. Lives in its own CF so
    // node and edge spatial entries can't alias and the two can be
    // dropped independently — matches how non-spatial property
    // indexes are split between `CF_PROPERTY_INDEX` and
    // `CF_EDGE_PROPERTY_INDEX`.
    // ---------------------------------------------------------------

    pub fn list_edge_point_indexes(&self) -> Vec<EdgePointIndexSpec> {
        self.edge_point_indexes
            .read()
            .expect("edge_point_indexes lock poisoned")
            .clone()
    }

    /// Declare a new edge point index and backfill it by scanning
    /// every edge currently carrying `edge_type`. Idempotent.
    /// Single-property / relationship-scope analogue of
    /// [`RocksDbStorageEngine::create_point_index`].
    pub fn create_edge_point_index(&self, edge_type: &str, property: &str) -> Result<()> {
        let spec = EdgePointIndexSpec {
            edge_type: edge_type.to_string(),
            property: property.to_string(),
        };
        let mut guard = self
            .edge_point_indexes
            .write()
            .expect("edge_point_indexes lock poisoned");
        if guard.contains(&spec) {
            return Ok(());
        }

        let meta_cf = self.cf(CF_EDGE_POINT_INDEX_META)?;
        let point_cf = self.cf(CF_EDGE_POINT_INDEX)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(meta_cf, edge_point_index_meta_key(&spec), EMPTY);

        for edge_id in self.edges_by_type(edge_type)? {
            let edge = match self.get_edge(edge_id)? {
                Some(e) => e,
                None => continue,
            };
            if let Some(Property::Point(p)) = edge.properties.get(property) {
                let cell = point_cell(p.srid, p.x, p.y);
                batch.put_cf(
                    point_cf,
                    // Reuse the node-scope key builder: the key layout
                    // `<label>\0<prop>\0<srid:4><cell:8><id:16>` doesn't
                    // care whether `id` is a node or edge id — both are
                    // 16-byte UUIDs. The separate CF is what scopes
                    // relationship entries away from node entries.
                    point_index_key(edge_type, property, p.srid, cell, edge_id.as_bytes()),
                    point_index_value(p.x, p.y, p.z),
                );
            }
        }

        self.db.write(batch)?;
        guard.push(spec);
        Ok(())
    }

    /// Tear down an edge point index. Idempotent.
    pub fn drop_edge_point_index(&self, edge_type: &str, property: &str) -> Result<()> {
        let spec = EdgePointIndexSpec {
            edge_type: edge_type.to_string(),
            property: property.to_string(),
        };
        let mut guard = self
            .edge_point_indexes
            .write()
            .expect("edge_point_indexes lock poisoned");
        if !guard.contains(&spec) {
            return Ok(());
        }

        let meta_cf = self.cf(CF_EDGE_POINT_INDEX_META)?;
        let point_cf = self.cf(CF_EDGE_POINT_INDEX)?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(meta_cf, edge_point_index_meta_key(&spec));

        let prefix = point_index_label_prop_prefix(edge_type, property);
        let iter = self
            .db
            .iterator_cf(point_cf, IteratorMode::From(&prefix, Direction::Forward));
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            batch.delete_cf(point_cf, key);
        }

        self.db.write(batch)?;
        guard.retain(|s| s != &spec);
        Ok(())
    }

    /// Relationship-scope analogue of
    /// [`RocksDbStorageEngine::nodes_in_bbox`]. Same Z-order cell
    /// range + precise bbox filter via stored coords, just over
    /// `CF_EDGE_POINT_INDEX`.
    pub fn edges_in_bbox(
        &self,
        edge_type: &str,
        property: &str,
        srid: i32,
        xlo: f64,
        ylo: f64,
        xhi: f64,
        yhi: f64,
    ) -> Result<Vec<EdgeId>> {
        let spec = EdgePointIndexSpec {
            edge_type: edge_type.to_string(),
            property: property.to_string(),
        };
        {
            let guard = self
                .edge_point_indexes
                .read()
                .expect("edge_point_indexes lock poisoned");
            if !guard.contains(&spec) {
                return Ok(Vec::new());
            }
        }

        let (lo_x, hi_x) = if xlo <= xhi { (xlo, xhi) } else { (xhi, xlo) };
        let (lo_y, hi_y) = if ylo <= yhi { (ylo, yhi) } else { (yhi, ylo) };
        let (min_cell, max_cell) = point_cell_range(srid, lo_x, lo_y, hi_x, hi_y);

        let prefix = point_index_srid_prefix(edge_type, property, srid);
        let header_len = prefix.len();
        let mut seek = prefix.clone();
        seek.extend_from_slice(&min_cell.to_be_bytes());

        let cf = self.cf(CF_EDGE_POINT_INDEX)?;
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&seek, Direction::Forward));
        let mut results = Vec::new();
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let cell = cell_from_point_index_key(CF_EDGE_POINT_INDEX, &key, header_len)?;
            if cell > max_cell {
                break;
            }
            let (x, y, _z) = decode_point_index_value(CF_EDGE_POINT_INDEX, &value)?;
            if x < lo_x || x > hi_x || y < lo_y || y > hi_y {
                continue;
            }
            let id_bytes = node_id_from_point_index_key(CF_EDGE_POINT_INDEX, &key)?;
            results.push(EdgeId::from_bytes(id_bytes));
        }
        results.sort();
        Ok(results)
    }

    /// Look up edge ids for a `(edge_type, property, value)` equality
    /// via the edge-property-index CF. Mirror of
    /// [`RocksDbStorageEngine::nodes_by_property`] for relationship
    /// scope; unindexable value types surface as
    /// [`Error::UnindexableValue`] so callers can fall back to a
    /// type-wide scan rather than silently returning empty.
    pub fn edges_by_property(
        &self,
        edge_type: &str,
        property: &str,
        value: &Property,
    ) -> Result<Vec<EdgeId>> {
        self.edges_by_properties(
            edge_type,
            std::slice::from_ref(&property.to_string()),
            std::slice::from_ref(value),
        )
    }

    /// Composite form of [`edges_by_property`]. Same contract as
    /// [`RocksDbStorageEngine::nodes_by_properties`] on the
    /// relationship side.
    pub fn edges_by_properties(
        &self,
        edge_type: &str,
        properties: &[String],
        values: &[Property],
    ) -> Result<Vec<EdgeId>> {
        assert_eq!(
            properties.len(),
            values.len(),
            "composite edge seek: properties and values must have equal length"
        );
        let mut encoded: Vec<Vec<u8>> = Vec::with_capacity(values.len());
        for (p, v) in properties.iter().zip(values.iter()) {
            let bytes = encode_index_value(v).ok_or_else(|| Error::UnindexableValue {
                property: p.clone(),
                kind: v.type_name(),
            })?;
            encoded.push(bytes);
        }
        let cf = self.cf(CF_EDGE_PROPERTY_INDEX)?;
        let prefix = edge_property_index_composite_value_prefix(edge_type, properties, &encoded);
        let mut results = Vec::new();
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let bytes = edge_id_from_property_index_key(&key, prefix.len())?;
            results.push(EdgeId::from_bytes(bytes));
        }
        Ok(results)
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
        self.nodes_by_properties(
            label,
            std::slice::from_ref(&property.to_string()),
            std::slice::from_ref(value),
        )
    }

    /// Composite form of [`nodes_by_property`]. Seeks every node
    /// whose `(label, property_i = value_i)` tuple matches in order.
    /// The two slices must have equal length. Unindexable value
    /// types surface as [`Error::UnindexableValue`] the same way
    /// the single-property form does — composite indexes are
    /// all-or-nothing and an unindexable slot can never match a
    /// stored tuple.
    pub fn nodes_by_properties(
        &self,
        label: &str,
        properties: &[String],
        values: &[Property],
    ) -> Result<Vec<NodeId>> {
        assert_eq!(
            properties.len(),
            values.len(),
            "composite seek: properties and values must have equal length"
        );
        let mut encoded: Vec<Vec<u8>> = Vec::with_capacity(values.len());
        for (p, v) in properties.iter().zip(values.iter()) {
            let bytes = encode_index_value(v).ok_or_else(|| Error::UnindexableValue {
                property: p.clone(),
                kind: v.type_name(),
            })?;
            encoded.push(bytes);
        }
        let cf = self.cf(CF_PROPERTY_INDEX)?;
        let prefix = property_index_composite_value_prefix(label, properties, &encoded);
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

/// Return the [`meshdb_core::Point`] at `properties[key]` when the
/// property is present and of point type. Used by point-index
/// maintenance to decide whether a node's indexed property changed
/// across a put. Non-point or missing values yield `None`, which the
/// index maintenance treats as "no entry" — the same way
/// `encode_index_tuple` treats missing / unindexable values for
/// property indexes.
fn extract_indexable_point(
    properties: &std::collections::HashMap<String, Property>,
    key: &str,
) -> Option<meshdb_core::Point> {
    match properties.get(key)? {
        Property::Point(p) => Some(*p),
        _ => None,
    }
}

/// Deterministic auto-name for a constraint the user didn't name.
/// Shape: `constraint_<label>_<property_list>_<kind>` where the
/// property list joins the property names with `_` and `<kind>` is
/// `unique`, `not_null`, `type_<t>`, or `node_key`. Stable across
/// restarts — users can target the auto-name with `DROP CONSTRAINT`
/// without having to inspect `SHOW CONSTRAINTS` first.
fn default_constraint_name(
    scope: &ConstraintScope,
    properties: &[String],
    kind: PropertyConstraintKind,
) -> String {
    let joined = properties.join("_");
    format!(
        "constraint_{scope_tag}_{target}_{joined}_{kind_tag}",
        scope_tag = scope.name_tag(),
        target = scope.target(),
        kind_tag = kind.name_tag(),
    )
}

/// Return the name of a UNIQUE or NODE KEY constraint whose backing
/// index is exactly `(label, properties)`, or `None` if no such
/// constraint is registered. The enforcement paths for both kinds seek
/// through that index — dropping it silently defeats the check — so
/// `drop_property_index_composite` consults this helper before
/// removing any entries.
fn find_constraint_backing_node_index(
    constraints: &[PropertyConstraintSpec],
    label: &str,
    properties: &[String],
) -> Option<String> {
    constraints
        .iter()
        .filter(|c| matches!(&c.scope, ConstraintScope::Node(l) if l == label))
        .filter(|c| {
            matches!(
                c.kind,
                PropertyConstraintKind::Unique | PropertyConstraintKind::NodeKey
            )
        })
        .find(|c| c.properties.as_slice() == properties)
        .map(|c| c.name.clone())
}

/// Relationship-scope mirror of
/// [`find_constraint_backing_node_index`]. NODE KEY is node-only, so
/// only UNIQUE can back an edge index.
fn find_constraint_backing_edge_index(
    constraints: &[PropertyConstraintSpec],
    edge_type: &str,
    properties: &[String],
) -> Option<String> {
    constraints
        .iter()
        .filter(|c| matches!(&c.scope, ConstraintScope::Relationship(t) if t == edge_type))
        .filter(|c| matches!(c.kind, PropertyConstraintKind::Unique))
        .find(|c| c.properties.as_slice() == properties)
        .map(|c| c.name.clone())
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
    match &spec.scope {
        ConstraintScope::Node(label) => validate_existing_nodes(engine, spec, label),
        ConstraintScope::Relationship(edge_type) => {
            validate_existing_edges(engine, spec, edge_type)
        }
    }
}

fn validate_existing_nodes(
    engine: &RocksDbStorageEngine,
    spec: &PropertyConstraintSpec,
    label: &str,
) -> Result<()> {
    let label_members = engine.nodes_by_label(label)?;
    let primary = spec.primary_property();
    match spec.kind {
        PropertyConstraintKind::NotNull => {
            for id in label_members {
                let node = match engine.get_node(id)? {
                    Some(n) => n,
                    None => continue,
                };
                let present = node
                    .properties
                    .get(primary)
                    .is_some_and(|v| !matches!(v, Property::Null));
                if !present {
                    return Err(Error::ConstraintViolation {
                        name: spec.name.clone(),
                        kind: spec.kind.as_string(),
                        label: label.to_string(),
                        property: primary.to_string(),
                        details: format!("node {id} is missing required property"),
                    });
                }
            }
        }
        PropertyConstraintKind::Unique => {
            use std::collections::HashMap;
            let mut seen: HashMap<Vec<u8>, meshdb_core::NodeId> = HashMap::new();
            for id in label_members {
                let node = match engine.get_node(id)? {
                    Some(n) => n,
                    None => continue,
                };
                let Some(value) = node.properties.get(primary) else {
                    continue;
                };
                let Some(encoded) = encode_index_value(value) else {
                    continue;
                };
                if let Some(&first) = seen.get(&encoded) {
                    return Err(Error::ConstraintViolation {
                        name: spec.name.clone(),
                        kind: spec.kind.as_string(),
                        label: label.to_string(),
                        property: primary.to_string(),
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
                let Some(value) = node.properties.get(primary) else {
                    continue;
                };
                if matches!(value, Property::Null) {
                    continue;
                }
                if !property_matches_type(value, target) {
                    return Err(Error::ConstraintViolation {
                        name: spec.name.clone(),
                        kind: spec.kind.as_string(),
                        label: label.to_string(),
                        property: primary.to_string(),
                        details: format!(
                            "node {id} has value of type {} (expected {})",
                            value.type_name(),
                            target.as_str()
                        ),
                    });
                }
            }
        }
        PropertyConstraintKind::NodeKey => {
            use std::collections::HashMap;
            let mut seen: HashMap<Vec<Vec<u8>>, meshdb_core::NodeId> = HashMap::new();
            for id in label_members {
                let node = match engine.get_node(id)? {
                    Some(n) => n,
                    None => continue,
                };
                let mut tuple: Vec<Vec<u8>> = Vec::with_capacity(spec.properties.len());
                let mut missing: Option<&str> = None;
                for prop in &spec.properties {
                    match node.properties.get(prop) {
                        Some(v) if !matches!(v, Property::Null) => {
                            if let Some(encoded) = encode_index_value(v) {
                                tuple.push(encoded);
                            } else {
                                missing = Some(prop);
                                break;
                            }
                        }
                        _ => {
                            missing = Some(prop.as_str());
                            break;
                        }
                    }
                }
                if let Some(prop) = missing {
                    return Err(Error::ConstraintViolation {
                        name: spec.name.clone(),
                        kind: spec.kind.as_string(),
                        label: label.to_string(),
                        property: prop.to_string(),
                        details: format!("node {id} is missing required property `{prop}`"),
                    });
                }
                if let Some(&first) = seen.get(&tuple) {
                    return Err(Error::ConstraintViolation {
                        name: spec.name.clone(),
                        kind: spec.kind.as_string(),
                        label: label.to_string(),
                        property: spec.properties.join(","),
                        details: format!("duplicate tuple held by nodes {first} and {id}"),
                    });
                }
                seen.insert(tuple, id);
            }
        }
    }
    Ok(())
}

/// Relationship-scope analogue to `validate_existing_nodes`. Walks
/// `edges_by_type(edge_type)` because we don't have an edge property
/// index; complexity is O(E_type) per newly-declared constraint. `NodeKey` is rejected at create time
/// for relationship scope, so this path doesn't handle it.
fn validate_existing_edges(
    engine: &RocksDbStorageEngine,
    spec: &PropertyConstraintSpec,
    edge_type: &str,
) -> Result<()> {
    let edge_ids = engine.edges_by_type(edge_type)?;
    let primary = spec.primary_property();
    match spec.kind {
        PropertyConstraintKind::NotNull => {
            for id in edge_ids {
                let edge = match engine.get_edge(id)? {
                    Some(e) => e,
                    None => continue,
                };
                let present = edge
                    .properties
                    .get(primary)
                    .is_some_and(|v| !matches!(v, Property::Null));
                if !present {
                    return Err(Error::ConstraintViolation {
                        name: spec.name.clone(),
                        kind: spec.kind.as_string(),
                        label: edge_type.to_string(),
                        property: primary.to_string(),
                        details: format!("edge {id} is missing required property"),
                    });
                }
            }
        }
        PropertyConstraintKind::Unique => {
            use std::collections::HashMap;
            let mut seen: HashMap<Vec<u8>, meshdb_core::EdgeId> = HashMap::new();
            for id in edge_ids {
                let edge = match engine.get_edge(id)? {
                    Some(e) => e,
                    None => continue,
                };
                let Some(value) = edge.properties.get(primary) else {
                    continue;
                };
                let Some(encoded) = encode_index_value(value) else {
                    continue;
                };
                if let Some(&first) = seen.get(&encoded) {
                    return Err(Error::ConstraintViolation {
                        name: spec.name.clone(),
                        kind: spec.kind.as_string(),
                        label: edge_type.to_string(),
                        property: primary.to_string(),
                        details: format!("duplicate value held by edges {first} and {id}"),
                    });
                }
                seen.insert(encoded, id);
            }
        }
        PropertyConstraintKind::PropertyType(target) => {
            for id in edge_ids {
                let edge = match engine.get_edge(id)? {
                    Some(e) => e,
                    None => continue,
                };
                let Some(value) = edge.properties.get(primary) else {
                    continue;
                };
                if matches!(value, Property::Null) {
                    continue;
                }
                if !property_matches_type(value, target) {
                    return Err(Error::ConstraintViolation {
                        name: spec.name.clone(),
                        kind: spec.kind.as_string(),
                        label: edge_type.to_string(),
                        property: primary.to_string(),
                        details: format!(
                            "edge {id} has value of type {} (expected {})",
                            value.type_name(),
                            target.as_str()
                        ),
                    });
                }
            }
        }
        PropertyConstraintKind::NodeKey => {
            unreachable!("NODE KEY on relationship scope rejected at create time")
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

/// Relationship-scope analogue of [`load_index_meta`]. Rehydrates the
/// edge-property-index registry from [`CF_EDGE_INDEX_META`] at open
/// time.
fn load_edge_index_meta(db: &DB) -> Result<Vec<EdgePropertyIndexSpec>> {
    let cf = db
        .cf_handle(CF_EDGE_INDEX_META)
        .ok_or(Error::MissingColumnFamily(CF_EDGE_INDEX_META))?;
    let mut specs = Vec::new();
    for item in db.iterator_cf(cf, IteratorMode::Start) {
        let (key, _) = item?;
        specs.push(edge_index_meta_key_decode(&key)?);
    }
    Ok(specs)
}

/// Rehydrate the point-index registry from [`CF_POINT_INDEX_META`]
/// at open. Same once-per-process walk as the other meta loaders.
fn load_point_index_meta(db: &DB) -> Result<Vec<PointIndexSpec>> {
    let cf = db
        .cf_handle(CF_POINT_INDEX_META)
        .ok_or(Error::MissingColumnFamily(CF_POINT_INDEX_META))?;
    let mut specs = Vec::new();
    for item in db.iterator_cf(cf, IteratorMode::Start) {
        let (key, _) = item?;
        specs.push(point_index_meta_key_decode(&key)?);
    }
    Ok(specs)
}

/// Relationship-scope analogue of [`load_point_index_meta`].
fn load_edge_point_index_meta(db: &DB) -> Result<Vec<EdgePointIndexSpec>> {
    let cf = db
        .cf_handle(CF_EDGE_POINT_INDEX_META)
        .ok_or(Error::MissingColumnFamily(CF_EDGE_POINT_INDEX_META))?;
    let mut specs = Vec::new();
    for item in db.iterator_cf(cf, IteratorMode::Start) {
        let (key, _) = item?;
        specs.push(edge_point_index_meta_key_decode(&key)?);
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

    fn nodes_by_properties(
        &self,
        label: &str,
        properties: &[String],
        values: &[Property],
    ) -> Result<Vec<NodeId>> {
        RocksDbStorageEngine::nodes_by_properties(self, label, properties, values)
    }

    fn edges_by_property(
        &self,
        edge_type: &str,
        property: &str,
        value: &Property,
    ) -> Result<Vec<EdgeId>> {
        RocksDbStorageEngine::edges_by_property(self, edge_type, property, value)
    }

    fn create_property_index(&self, label: &str, property: &str) -> Result<()> {
        RocksDbStorageEngine::create_property_index(self, label, property)
    }

    fn drop_property_index(&self, label: &str, property: &str) -> Result<()> {
        RocksDbStorageEngine::drop_property_index(self, label, property)
    }

    fn create_property_index_composite(&self, label: &str, properties: &[String]) -> Result<()> {
        RocksDbStorageEngine::create_property_index_composite(self, label, properties)
    }

    fn drop_property_index_composite(&self, label: &str, properties: &[String]) -> Result<()> {
        RocksDbStorageEngine::drop_property_index_composite(self, label, properties)
    }

    fn list_property_indexes(&self) -> Vec<PropertyIndexSpec> {
        RocksDbStorageEngine::list_property_indexes(self)
    }

    fn create_edge_property_index(&self, edge_type: &str, property: &str) -> Result<()> {
        RocksDbStorageEngine::create_edge_property_index(self, edge_type, property)
    }

    fn drop_edge_property_index(&self, edge_type: &str, property: &str) -> Result<()> {
        RocksDbStorageEngine::drop_edge_property_index(self, edge_type, property)
    }

    fn create_edge_property_index_composite(
        &self,
        edge_type: &str,
        properties: &[String],
    ) -> Result<()> {
        RocksDbStorageEngine::create_edge_property_index_composite(self, edge_type, properties)
    }

    fn drop_edge_property_index_composite(
        &self,
        edge_type: &str,
        properties: &[String],
    ) -> Result<()> {
        RocksDbStorageEngine::drop_edge_property_index_composite(self, edge_type, properties)
    }

    fn list_edge_property_indexes(&self) -> Vec<EdgePropertyIndexSpec> {
        RocksDbStorageEngine::list_edge_property_indexes(self)
    }

    fn create_point_index(&self, label: &str, property: &str) -> Result<()> {
        RocksDbStorageEngine::create_point_index(self, label, property)
    }

    fn drop_point_index(&self, label: &str, property: &str) -> Result<()> {
        RocksDbStorageEngine::drop_point_index(self, label, property)
    }

    fn list_point_indexes(&self) -> Vec<PointIndexSpec> {
        RocksDbStorageEngine::list_point_indexes(self)
    }

    fn nodes_in_bbox(
        &self,
        label: &str,
        property: &str,
        srid: i32,
        xlo: f64,
        ylo: f64,
        xhi: f64,
        yhi: f64,
    ) -> Result<Vec<NodeId>> {
        RocksDbStorageEngine::nodes_in_bbox(self, label, property, srid, xlo, ylo, xhi, yhi)
    }

    fn create_edge_point_index(&self, edge_type: &str, property: &str) -> Result<()> {
        RocksDbStorageEngine::create_edge_point_index(self, edge_type, property)
    }

    fn drop_edge_point_index(&self, edge_type: &str, property: &str) -> Result<()> {
        RocksDbStorageEngine::drop_edge_point_index(self, edge_type, property)
    }

    fn list_edge_point_indexes(&self) -> Vec<EdgePointIndexSpec> {
        RocksDbStorageEngine::list_edge_point_indexes(self)
    }

    fn edges_in_bbox(
        &self,
        edge_type: &str,
        property: &str,
        srid: i32,
        xlo: f64,
        ylo: f64,
        xhi: f64,
        yhi: f64,
    ) -> Result<Vec<EdgeId>> {
        RocksDbStorageEngine::edges_in_bbox(self, edge_type, property, srid, xlo, ylo, xhi, yhi)
    }

    fn create_property_constraint(
        &self,
        name: Option<&str>,
        scope: &ConstraintScope,
        properties: &[String],
        kind: PropertyConstraintKind,
        if_not_exists: bool,
    ) -> Result<PropertyConstraintSpec> {
        RocksDbStorageEngine::create_property_constraint(
            self,
            name,
            scope,
            properties,
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

    fn put_trigger(&self, name: &str, value: &[u8]) -> Result<()> {
        let cf = self.cf(CF_TRIGGER_META)?;
        self.db
            .put_cf(cf, name.as_bytes(), value)
            .map_err(Error::from)
    }

    fn delete_trigger(&self, name: &str) -> Result<()> {
        let cf = self.cf(CF_TRIGGER_META)?;
        self.db.delete_cf(cf, name.as_bytes()).map_err(Error::from)
    }

    fn list_triggers(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let cf = self.cf(CF_TRIGGER_META)?;
        let mut out: Vec<(String, Vec<u8>)> = Vec::new();
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);
        for entry in iter {
            let (k, v) = entry.map_err(Error::from)?;
            // Names are stored as raw UTF-8 bytes — anything else
            // would be a corruption since `put_trigger` only
            // accepts `&str`. Lossy decode is defensive.
            let name = String::from_utf8_lossy(&k).into_owned();
            out.push((name, v.to_vec()));
        }
        Ok(out)
    }

    fn put_pending_tx(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let cf = self.cf(CF_PENDING_TX_META)?;
        self.db.put_cf(cf, key, value).map_err(Error::from)
    }

    fn delete_pending_tx(&self, key: &[u8]) -> Result<()> {
        let cf = self.cf(CF_PENDING_TX_META)?;
        self.db.delete_cf(cf, key).map_err(Error::from)
    }

    fn list_pending_txs(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let cf = self.cf(CF_PENDING_TX_META)?;
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);
        for entry in iter {
            let (k, v) = entry.map_err(Error::from)?;
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    fn create_checkpoint(&self, path: &Path) -> Result<()> {
        RocksDbStorageEngine::create_checkpoint(self, path)
    }

    fn clear_all(&self) -> Result<()> {
        RocksDbStorageEngine::clear_all(self)
    }
}
