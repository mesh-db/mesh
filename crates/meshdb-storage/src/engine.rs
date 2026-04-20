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

use crate::Result;
use meshdb_core::{Edge, EdgeId, Node, NodeId, Property};
use std::path::Path;

/// Declarative spec for a single-property equality index. An index is
/// uniquely identified by its `(label, property)` pair — users don't
/// name them, which keeps DROP/SHOW behavior simple and matches the
/// way the planner looks them up when deciding whether to emit
/// `IndexSeek`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PropertyIndexSpec {
    pub label: String,
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

    // --- Index DDL ---

    fn create_property_index(&self, label: &str, property: &str) -> Result<()>;
    fn drop_property_index(&self, label: &str, property: &str) -> Result<()>;
    fn list_property_indexes(&self) -> Vec<PropertyIndexSpec>;

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
