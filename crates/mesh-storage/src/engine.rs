//! Backend-agnostic graph storage interface.
//!
//! Defines [`StorageEngine`], the object-safe trait that every concrete
//! storage backend must implement, plus the neutral types that flow
//! through the trait's API ([`PropertyIndexSpec`] and [`GraphMutation`]).
//! All non-executor consumers (`mesh-rpc`, `mesh-server`) hold the engine
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
use mesh_core::{Edge, EdgeId, Node, NodeId, Property};
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

    // --- Snapshot / restore hooks ---

    /// Persist a point-in-time copy of the backend's on-disk state into
    /// `path`. The shape of what lands at `path` is backend-specific — the
    /// caller is expected to package it in a backend-aware way (see
    /// `mesh-rpc::raft_applier` for the RocksDB path). For RocksDB this is
    /// a `Checkpoint` directory of SST files; a different backend is free
    /// to write a single file, a directory of segments, etc., as long as
    /// its own `open(path)` can later rehydrate from the same layout.
    fn create_checkpoint(&self, path: &Path) -> Result<()>;

    /// Drop every entry from every part of the backend. Used by the Raft
    /// snapshot-install path to wipe local state before applying the
    /// leader's snapshot.
    fn clear_all(&self) -> Result<()>;
}
