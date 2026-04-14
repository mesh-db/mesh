use crate::{
    error::{Error, Result},
    keys::{
        adj_key, edge_from_adj_key, id_from_str_index_key, label_index_key, label_index_prefix,
        node_from_adj_value, type_index_key, type_index_prefix, ID_LEN,
    },
};
use mesh_core::{Edge, EdgeId, Node, NodeId};
use rocksdb::{ColumnFamilyDescriptor, Direction, IteratorMode, Options, WriteBatch, DB};
use std::path::Path;

const CF_NODES: &str = "nodes";
const CF_EDGES: &str = "edges";
const CF_ADJ_OUT: &str = "adj_out";
const CF_ADJ_IN: &str = "adj_in";
const CF_LABEL_INDEX: &str = "label_index";
const CF_TYPE_INDEX: &str = "type_index";

const ALL_CFS: &[&str] = &[
    CF_NODES,
    CF_EDGES,
    CF_ADJ_OUT,
    CF_ADJ_IN,
    CF_LABEL_INDEX,
    CF_TYPE_INDEX,
];

/// A single mutation that can be combined with others into an atomic
/// [`Store::apply_batch`] call. Mirrors the shape of the per-op `Store`
/// methods but lets the caller commit a sequence of mutations as one
/// rocksdb [`WriteBatch`] — useful for giving multi-write Cypher queries
/// crash-atomic local persistence.
#[derive(Debug, Clone)]
pub enum StoreMutation {
    PutNode(Node),
    PutEdge(Edge),
    /// Idempotent within a batch: missing edges are skipped silently so a
    /// log replay that re-applies a partially-committed batch is safe.
    DeleteEdge(EdgeId),
    /// Idempotent within a batch: missing nodes contribute no operations.
    DetachDeleteNode(NodeId),
}

pub struct Store {
    db: DB,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cfs: Vec<ColumnFamilyDescriptor> = ALL_CFS
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect();

        let db = DB::open_cf_descriptors(&db_opts, path, cfs)?;
        Ok(Self { db })
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

        let existing_labels: Vec<String> = match self.db.get_cf(nodes_cf, node.id.as_bytes())? {
            Some(bytes) => {
                let existing: Node = serde_json::from_slice(&bytes)?;
                existing.labels
            }
            None => Vec::new(),
        };

        let bytes = serde_json::to_vec(node)?;
        batch.put_cf(nodes_cf, node.id.as_bytes(), bytes);

        for old in &existing_labels {
            if !node.labels.contains(old) {
                batch.delete_cf(label_cf, label_index_key(old, node.id));
            }
        }
        for new in &node.labels {
            if !existing_labels.contains(new) {
                batch.put_cf(label_cf, label_index_key(new, node.id), EMPTY);
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
        batch.put_cf(
            in_cf,
            adj_key(edge.target, edge.id),
            edge.source.as_bytes(),
        );
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

    fn append_delete_edge(
        &self,
        batch: &mut WriteBatch,
        id: EdgeId,
        edge: &Edge,
    ) -> Result<()> {
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

    fn append_detach_delete_node(
        &self,
        batch: &mut WriteBatch,
        id: NodeId,
    ) -> Result<()> {
        let node = self.get_node(id)?;
        let outgoing = self.outgoing(id)?;
        let incoming = self.incoming(id)?;

        let nodes_cf = self.cf(CF_NODES)?;
        let edges_cf = self.cf(CF_EDGES)?;
        let out_cf = self.cf(CF_ADJ_OUT)?;
        let in_cf = self.cf(CF_ADJ_IN)?;
        let label_cf = self.cf(CF_LABEL_INDEX)?;
        let type_cf = self.cf(CF_TYPE_INDEX)?;

        if let Some(n) = &node {
            for label in &n.labels {
                batch.delete_cf(label_cf, label_index_key(label, id));
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
    pub fn apply_batch(&self, mutations: &[StoreMutation]) -> Result<()> {
        let mut batch = WriteBatch::default();
        for m in mutations {
            match m {
                StoreMutation::PutNode(n) => self.append_put_node(&mut batch, n)?,
                StoreMutation::PutEdge(e) => self.append_put_edge(&mut batch, e)?,
                StoreMutation::DeleteEdge(id) => {
                    if let Some(edge) = self.get_edge(*id)? {
                        self.append_delete_edge(&mut batch, *id, &edge)?;
                    }
                }
                StoreMutation::DetachDeleteNode(id) => {
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

const EMPTY: &[u8] = &[];
