use crate::{
    error::{Error, Result},
    keys::{adj_key, edge_from_adj_key, node_from_adj_value},
};
use mesh_core::{Edge, EdgeId, Node, NodeId};
use rocksdb::{ColumnFamilyDescriptor, Direction, IteratorMode, Options, WriteBatch, DB};
use std::path::Path;

const CF_NODES: &str = "nodes";
const CF_EDGES: &str = "edges";
const CF_ADJ_OUT: &str = "adj_out";
const CF_ADJ_IN: &str = "adj_in";

const ALL_CFS: &[&str] = &[CF_NODES, CF_EDGES, CF_ADJ_OUT, CF_ADJ_IN];

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
        let cf = self.cf(CF_NODES)?;
        let bytes = serde_json::to_vec(node)?;
        self.db.put_cf(cf, node.id.as_bytes(), bytes)?;
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
        let edges_cf = self.cf(CF_EDGES)?;
        let out_cf = self.cf(CF_ADJ_OUT)?;
        let in_cf = self.cf(CF_ADJ_IN)?;

        let bytes = serde_json::to_vec(edge)?;
        let mut batch = WriteBatch::default();
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
        self.db.write(batch)?;
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
        let edges_cf = self.cf(CF_EDGES)?;
        let out_cf = self.cf(CF_ADJ_OUT)?;
        let in_cf = self.cf(CF_ADJ_IN)?;

        let mut batch = WriteBatch::default();
        batch.delete_cf(edges_cf, id.as_bytes());
        batch.delete_cf(out_cf, adj_key(edge.source, id));
        batch.delete_cf(in_cf, adj_key(edge.target, id));
        self.db.write(batch)?;
        Ok(())
    }

    pub fn detach_delete_node(&self, id: NodeId) -> Result<()> {
        let outgoing = self.outgoing(id)?;
        let incoming = self.incoming(id)?;

        let nodes_cf = self.cf(CF_NODES)?;
        let edges_cf = self.cf(CF_EDGES)?;
        let out_cf = self.cf(CF_ADJ_OUT)?;
        let in_cf = self.cf(CF_ADJ_IN)?;

        let mut batch = WriteBatch::default();
        for (edge_id, target) in &outgoing {
            batch.delete_cf(edges_cf, edge_id.as_bytes());
            batch.delete_cf(out_cf, adj_key(id, *edge_id));
            batch.delete_cf(in_cf, adj_key(*target, *edge_id));
        }
        for (edge_id, source) in &incoming {
            batch.delete_cf(edges_cf, edge_id.as_bytes());
            batch.delete_cf(out_cf, adj_key(*source, *edge_id));
            batch.delete_cf(in_cf, adj_key(id, *edge_id));
        }
        batch.delete_cf(nodes_cf, id.as_bytes());
        self.db.write(batch)?;
        Ok(())
    }

    pub fn outgoing(&self, source: NodeId) -> Result<Vec<(EdgeId, NodeId)>> {
        self.scan_adj(CF_ADJ_OUT, source)
    }

    pub fn incoming(&self, target: NodeId) -> Result<Vec<(EdgeId, NodeId)>> {
        self.scan_adj(CF_ADJ_IN, target)
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
