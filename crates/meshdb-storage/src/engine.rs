//! Backend-agnostic graph storage interface.
//!
//! Defines [`StorageEngine`], the object-safe trait that every concrete
//! storage backend must implement, plus the neutral types that flow
//! through the trait's API ([`PropertyIndexSpec`] and [`GraphMutation`]).
//! All non-executor consumers (`meshdb-rpc`, `meshdb-server`) hold the engine
//! as `Arc<dyn StorageEngine>` so swapping backends is a one-line change
//! at the `open` site — everything else flows through the trait.
//!
//! The trait deliberately mirrors the public surface of the RocksDB
//! backend 1:1 rather than inventing a more abstract vocabulary. Any
//! would-be second backend should be able to implement it without
//! fighting a RocksDB-shaped API, because the methods name graph
//! operations, not rocksdb CFs.
//!
//! Not part of the trait:
//! - `open(path)` — each backend takes different construction arguments.
//! - Raw batch types (`WriteBatch`, etc.) — [`GraphMutation`] is the
//!   portable sequencing primitive instead.
//!
//! The concrete RocksDB impl lives in [`crate::rocksdb_engine`].

use crate::{Error, Result};
use meshdb_core::{Edge, EdgeId, Node, NodeId, Property};
use std::path::Path;

/// Declarative spec for a single-property equality index. An index is
/// uniquely identified by its `(label, properties)` pair — users don't
/// name them, which keeps DROP/SHOW behavior simple and matches the
/// way the planner looks them up when deciding whether to emit
/// `IndexSeek`.
///
/// `properties` is a `Vec<String>` so composite indexes (tuple keys
/// over multiple properties, `CREATE INDEX FOR (n:Label) ON (n.a, n.b)`)
/// fit the same shape as the single-property form. Single-property
/// specs encode byte-identically to the pre-composite layout on
/// disk, so existing data is readable after upgrade.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PropertyIndexSpec {
    pub label: String,
    pub properties: Vec<String>,
}

/// Relationship-scope analogue of [`PropertyIndexSpec`]. Same
/// `properties: Vec<String>` shape, same composite semantics, same
/// byte-level on-disk compatibility for length-1 entries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EdgePropertyIndexSpec {
    pub edge_type: String,
    pub properties: Vec<String>,
}

/// Declarative spec for a point / spatial index on a single
/// `Property::Point` property. A point index accelerates bounding-box
/// containment and distance-radius queries via a Z-order (Morton)
/// cell quantizer — points map to sortable u64 cell IDs, so bbox
/// queries reduce to a cell-range scan plus a per-row precision
/// filter.
///
/// Single-property for now; a future "composite point index" would
/// have to pick a curve that extends cleanly to N spatial axes and
/// so gets its own spec shape when it lands.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PointIndexSpec {
    pub label: String,
    pub property: String,
}

/// Relationship-scope analogue of [`PointIndexSpec`]. Same
/// single-property shape; the index lives under a separate CF so
/// node and edge spatial entries can't alias and the two can be
/// dropped independently.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EdgePointIndexSpec {
    pub edge_type: String,
    pub property: String,
}

/// Kind of single-property constraint. `Unique` comes from
/// `REQUIRE n.prop IS UNIQUE`, `NotNull` from `REQUIRE n.prop IS NOT
/// NULL`, and `PropertyType(t)` from `REQUIRE n.prop IS :: <TYPE>`.
/// `PropertyType` carries the target type so a single constraint-kind
/// enum covers all four flavours without blowing up into one variant
/// per type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PropertyConstraintKind {
    Unique,
    NotNull,
    PropertyType(PropertyType),
    /// `REQUIRE (n.a, n.b) IS NODE KEY` — composite uniqueness + NOT
    /// NULL. The constraint holds across the tuple of values the
    /// spec's `properties` list names; each property must be present
    /// and the tuple must be unique across every node carrying the
    /// constrained label.
    NodeKey,
}

/// Property types recognised by `IS :: <TYPE>`. Covers the four
/// primitive Cypher types used in practice; temporal / spatial / list
/// / map type constraints are a follow-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PropertyType {
    String,
    Integer,
    Float,
    Boolean,
}

impl PropertyType {
    /// User-facing token used by `SHOW CONSTRAINTS` output.
    pub fn as_str(&self) -> &'static str {
        match self {
            PropertyType::String => "STRING",
            PropertyType::Integer => "INTEGER",
            PropertyType::Float => "FLOAT",
            PropertyType::Boolean => "BOOLEAN",
        }
    }

    /// Lowercase tag used when generating a default constraint name —
    /// must match the tag the cluster crate uses on the Raft-log side
    /// so auto-names resolve identically across replicas.
    pub fn name_tag(&self) -> &'static str {
        match self {
            PropertyType::String => "string",
            PropertyType::Integer => "integer",
            PropertyType::Float => "float",
            PropertyType::Boolean => "boolean",
        }
    }
}

impl PropertyConstraintKind {
    /// Short, human-facing token used in `SHOW CONSTRAINTS` output.
    /// For `PropertyType` the string carries the target type inside
    /// parentheses so operators reading the output can see what the
    /// constraint enforces at a glance. Stable — do not rename
    /// without a migration, because the auto-name the storage layer
    /// derives from this tag is part of the user-visible DROP surface.
    pub fn as_string(&self) -> String {
        match self {
            PropertyConstraintKind::Unique => "UNIQUE".into(),
            PropertyConstraintKind::NotNull => "NOT NULL".into(),
            PropertyConstraintKind::PropertyType(t) => format!("IS :: {}", t.as_str()),
            PropertyConstraintKind::NodeKey => "NODE KEY".into(),
        }
    }

    /// Lowercase tag used when generating a default constraint name.
    /// `PropertyType(T)` contributes `type_<t>` so every distinct type
    /// gets its own auto-name and two type constraints on the same
    /// `(label, property)` can coexist if they ever need to.
    pub fn name_tag(&self) -> String {
        match self {
            PropertyConstraintKind::Unique => "unique".into(),
            PropertyConstraintKind::NotNull => "not_null".into(),
            PropertyConstraintKind::PropertyType(t) => format!("type_{}", t.name_tag()),
            PropertyConstraintKind::NodeKey => "node_key".into(),
        }
    }

    /// True iff the kind accepts more than one property in its spec.
    /// Used by the storage layer to validate the `properties` list
    /// length at create time — single-property kinds must not receive
    /// a multi-element list.
    pub fn allows_multi_property(&self) -> bool {
        matches!(self, PropertyConstraintKind::NodeKey)
    }
}

/// Scope a constraint applies to. `Node(label)` covers every node
/// carrying the label; `Relationship(edge_type)` covers every edge
/// with the given type. Kept as an enum rather than a plain string
/// so future scope extensions (e.g. specific pattern shapes) land
/// additively.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConstraintScope {
    Node(String),
    Relationship(String),
}

impl ConstraintScope {
    /// The scope's target string — label for node scope, edge type
    /// for relationship scope. Handy when the caller has already
    /// dispatched on the variant and just needs the name.
    pub fn target(&self) -> &str {
        match self {
            ConstraintScope::Node(l) => l,
            ConstraintScope::Relationship(t) => t,
        }
    }

    /// Short tag used to disambiguate auto-generated constraint
    /// names so a node and a relationship constraint on the same
    /// target-and-property don't collide.
    pub fn name_tag(&self) -> &'static str {
        match self {
            ConstraintScope::Node(_) => "node",
            ConstraintScope::Relationship(_) => "rel",
        }
    }
}

/// Declarative spec for a property constraint. Unlike
/// [`PropertyIndexSpec`], constraints carry a user-facing `name` —
/// `DROP CONSTRAINT` takes a name, not a `(scope, properties, kind)`
/// tuple, so the registry keys on name for lookup. When the user
/// omits the name, [`StorageEngine::create_property_constraint`] fills
/// in a deterministic one derived from the other fields.
///
/// `properties` is a list so that [`PropertyConstraintKind::NodeKey`]
/// can carry its composite tuple here alongside single-property kinds;
/// the single-property kinds (`Unique`, `NotNull`, `PropertyType`)
/// enforce a length-one invariant at creation time.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PropertyConstraintSpec {
    pub name: String,
    pub scope: ConstraintScope,
    pub properties: Vec<String>,
    pub kind: PropertyConstraintKind,
}

impl PropertyConstraintSpec {
    /// The constraint's first (and for single-property kinds, only)
    /// property name. Handy shorthand for call sites that already
    /// know the spec is single-property.
    pub fn primary_property(&self) -> &str {
        self.properties.first().map(String::as_str).unwrap_or("")
    }
}

/// A single mutation that can be combined with others into an atomic
/// [`StorageEngine::apply_batch`] call. Backend-neutral: the enum only
/// names what to do to the graph, not how the backend persists it.
/// Lets callers commit a sequence of mutations as one atomic write —
/// useful for giving multi-write Cypher queries crash-atomic local
/// persistence.
#[derive(Debug, Clone)]
pub enum GraphMutation {
    PutNode(Node),
    PutEdge(Edge),
    /// Idempotent within a batch: missing edges are skipped silently so a
    /// log replay that re-applies a partially-committed batch is safe.
    DeleteEdge(EdgeId),
    /// Idempotent within a batch: missing nodes contribute no operations.
    DetachDeleteNode(NodeId),
}

/// Object-safe interface for a graph storage backend. See module docs for
/// design notes.
pub trait StorageEngine: Send + Sync {
    // --- Node CRUD ---

    fn put_node(&self, node: &Node) -> Result<()>;
    fn get_node(&self, id: NodeId) -> Result<Option<Node>>;
    fn detach_delete_node(&self, id: NodeId) -> Result<()>;

    // --- Edge CRUD ---

    fn put_edge(&self, edge: &Edge) -> Result<()>;
    fn get_edge(&self, id: EdgeId) -> Result<Option<Edge>>;
    fn delete_edge(&self, id: EdgeId) -> Result<()>;

    /// Apply a sequence of mutations atomically. Either every mutation
    /// lands or none does. See [`GraphMutation`] for the variant set.
    fn apply_batch(&self, mutations: &[GraphMutation]) -> Result<()>;

    // --- Full-graph scans ---

    fn all_nodes(&self) -> Result<Vec<Node>>;
    fn all_edges(&self) -> Result<Vec<Edge>>;
    fn all_node_ids(&self) -> Result<Vec<NodeId>>;

    // --- Adjacency ---

    fn outgoing(&self, source: NodeId) -> Result<Vec<(EdgeId, NodeId)>>;
    fn incoming(&self, target: NodeId) -> Result<Vec<(EdgeId, NodeId)>>;

    // --- Label / type / property indexes ---

    fn nodes_by_label(&self, label: &str) -> Result<Vec<NodeId>>;
    fn edges_by_type(&self, edge_type: &str) -> Result<Vec<EdgeId>>;
    fn nodes_by_property(
        &self,
        label: &str,
        property: &str,
        value: &Property,
    ) -> Result<Vec<NodeId>>;
    /// Composite form of [`Self::nodes_by_property`]. `properties`
    /// and `values` are parallel slices of equal length — the tuple
    /// must match an equivalent stored index entry. Single-property
    /// callers should use [`Self::nodes_by_property`] which delegates
    /// here with length-1 slices.
    fn nodes_by_properties(
        &self,
        label: &str,
        properties: &[String],
        values: &[Property],
    ) -> Result<Vec<NodeId>>;

    /// Equality lookup through an edge property index. Mirrors
    /// [`StorageEngine::nodes_by_property`] for the relationship side.
    /// The caller is responsible for verifying the `(edge_type, property)`
    /// index exists before dispatching — a missing index returns an
    /// empty result (no entries maintained) rather than erroring so
    /// the planner's race with a concurrent DROP stays safe.
    fn edges_by_property(
        &self,
        edge_type: &str,
        property: &str,
        value: &Property,
    ) -> Result<Vec<EdgeId>>;

    // --- Index DDL ---

    fn create_property_index(&self, label: &str, property: &str) -> Result<()>;
    fn drop_property_index(&self, label: &str, property: &str) -> Result<()>;
    /// Composite form of [`Self::create_property_index`] — declares
    /// a tuple index over `(label, properties...)`. Single-property
    /// callers should use [`Self::create_property_index`] which
    /// delegates here with a length-1 slice.
    fn create_property_index_composite(&self, label: &str, properties: &[String]) -> Result<()>;
    /// Composite form of [`Self::drop_property_index`].
    fn drop_property_index_composite(&self, label: &str, properties: &[String]) -> Result<()>;
    fn list_property_indexes(&self) -> Vec<PropertyIndexSpec>;

    /// Declare a new `(edge_type, property)` single-property equality
    /// index and backfill it from every edge currently carrying the
    /// type. Idempotent: re-creating an already-registered index is a
    /// no-op. Matches [`StorageEngine::create_property_index`] for
    /// node scope.
    fn create_edge_property_index(&self, edge_type: &str, property: &str) -> Result<()>;
    /// Tear down an edge property index. Removes the meta entry and
    /// every entry under the `(edge_type, prop)` prefix. Idempotent.
    fn drop_edge_property_index(&self, edge_type: &str, property: &str) -> Result<()>;
    /// Composite form of [`Self::create_edge_property_index`].
    fn create_edge_property_index_composite(
        &self,
        edge_type: &str,
        properties: &[String],
    ) -> Result<()>;
    /// Composite form of [`Self::drop_edge_property_index`].
    fn drop_edge_property_index_composite(
        &self,
        edge_type: &str,
        properties: &[String],
    ) -> Result<()>;
    /// Snapshot the currently-registered edge property indexes.
    fn list_edge_property_indexes(&self) -> Vec<EdgePropertyIndexSpec>;

    // --- Point / spatial index DDL ---

    /// Declare a point index on `(label, property)` and backfill it
    /// by scanning every node carrying `label`. Idempotent:
    /// re-creating a registered index is a no-op. Single-property
    /// and node-scope only — composite / relationship-scope spatial
    /// indexes are follow-ups.
    fn create_point_index(&self, label: &str, property: &str) -> Result<()>;

    /// Tear down a point index. Removes the meta entry and sweeps
    /// every stored entry under the `(label, property)` header — the
    /// SRID-keyed sub-prefixes all fall inside that header, so one
    /// range scan covers all coordinate systems. Idempotent.
    fn drop_point_index(&self, label: &str, property: &str) -> Result<()>;

    /// Snapshot the currently-registered point indexes.
    fn list_point_indexes(&self) -> Vec<PointIndexSpec>;

    /// Axis-aligned bounding-box range query over the point index
    /// `(label, property)` under `srid`. Entries tagged with a
    /// different SRID are scoped out by the index key prefix, so
    /// cross-SRID rows never leak. A missing index returns empty
    /// rather than erroring — matches the soft-fail contract of
    /// [`Self::nodes_by_property`].
    fn nodes_in_bbox(
        &self,
        label: &str,
        property: &str,
        srid: i32,
        xlo: f64,
        ylo: f64,
        xhi: f64,
        yhi: f64,
    ) -> Result<Vec<NodeId>>;

    // --- Edge point / spatial index DDL ---

    /// Relationship-scope analogue of [`Self::create_point_index`].
    /// Declares an edge point index on `(edge_type, property)` and
    /// backfills by scanning every edge currently carrying
    /// `edge_type`. Idempotent.
    fn create_edge_point_index(&self, edge_type: &str, property: &str) -> Result<()>;

    /// Tear down an edge point index. Idempotent. Mirrors
    /// [`Self::drop_point_index`].
    fn drop_edge_point_index(&self, edge_type: &str, property: &str) -> Result<()>;

    /// Snapshot the currently-registered edge point indexes.
    fn list_edge_point_indexes(&self) -> Vec<EdgePointIndexSpec>;

    /// Relationship-scope analogue of [`Self::nodes_in_bbox`].
    fn edges_in_bbox(
        &self,
        edge_type: &str,
        property: &str,
        srid: i32,
        xlo: f64,
        ylo: f64,
        xhi: f64,
        yhi: f64,
    ) -> Result<Vec<EdgeId>>;

    // --- Constraint DDL ---

    /// Declare a new property constraint. If `name` is `None`, the
    /// backend derives a deterministic name from the other fields so
    /// `DROP CONSTRAINT` can still target it. When `if_not_exists` is
    /// true, re-declaring a constraint with the same name is a no-op
    /// (including when the existing constraint has different
    /// label/property/kind — matches Neo4j's name-first semantics).
    /// Returns the final [`PropertyConstraintSpec`] that was either
    /// installed or already present.
    ///
    /// `UNIQUE` constraints implicitly require a backing property
    /// index; implementations should ensure one exists on the relevant
    /// `(label, property)` pair. The backing index is NOT torn down
    /// when the constraint is dropped — users who want it gone must
    /// issue a separate `DROP INDEX`.
    fn create_property_constraint(
        &self,
        name: Option<&str>,
        scope: &ConstraintScope,
        properties: &[String],
        kind: PropertyConstraintKind,
        if_not_exists: bool,
    ) -> Result<PropertyConstraintSpec>;

    /// Tear down a constraint identified by `name`. When `if_exists`
    /// is true, dropping a non-existent constraint is a no-op.
    fn drop_property_constraint(&self, name: &str, if_exists: bool) -> Result<()>;

    /// Snapshot every registered constraint. Order is insertion order
    /// for deterministic `SHOW CONSTRAINTS` output across restarts.
    fn list_property_constraints(&self) -> Vec<PropertyConstraintSpec>;

    // --- apoc.trigger.* registry ---

    /// Persist an `apoc.trigger.*` registration. The value is a
    /// JSON-encoded blob owned by `meshdb-executor`'s
    /// `TriggerSpec` — storage stays format-agnostic so the
    /// schema can evolve without bumping the storage trait.
    /// Default impl errors loudly so backends that haven't
    /// wired triggers in yet surface the gap immediately.
    fn put_trigger(&self, _name: &str, _value: &[u8]) -> Result<()> {
        Err(Error::Unsupported(
            "trigger registry is not supported by this backend".into(),
        ))
    }

    /// Remove a registered trigger by name. Idempotent — dropping
    /// a non-existent name is a no-op.
    fn delete_trigger(&self, _name: &str) -> Result<()> {
        Err(Error::Unsupported(
            "trigger registry is not supported by this backend".into(),
        ))
    }

    /// Snapshot every registered trigger as `(name, value)` pairs
    /// in insertion-order by name. The value bytes are passed
    /// straight back to the caller for format-side decoding.
    fn list_triggers(&self) -> Result<Vec<(String, Vec<u8>)>> {
        Ok(Vec::new())
    }

    // --- Snapshot / restore hooks ---

    /// Persist a point-in-time copy of the backend's on-disk state into
    /// `path`. The shape of what lands at `path` is backend-specific — the
    /// caller is expected to package it in a backend-aware way (see
    /// `meshdb-rpc::raft_applier` for the RocksDB path). For RocksDB this is
    /// a `Checkpoint` directory of SST files; a different backend is free
    /// to write a single file, a directory of segments, etc., as long as
    /// its own `open(path)` can later rehydrate from the same layout.
    fn create_checkpoint(&self, path: &Path) -> Result<()>;

    /// Drop every entry from every part of the backend. Used by the Raft
    /// snapshot-install path to wipe local state before applying the
    /// leader's snapshot.
    fn clear_all(&self) -> Result<()>;
}
