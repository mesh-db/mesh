use meshdb_cluster::raft::RaftCluster;
use meshdb_cluster::{
    ConstraintKind as ClusterConstraintKind, ConstraintScope as ClusterConstraintScope,
    GraphCommand, PropertyType as ClusterPropertyType,
};
use meshdb_core::{Edge, EdgeId, Node, NodeId};
use meshdb_executor::{Error as ExecError, GraphWriter, Result as ExecResult};
use meshdb_storage::{
    ConstraintScope, PropertyConstraintKind, PropertyConstraintSpec, PropertyType,
};
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;

/// Bridge the storage-crate `ConstraintScope` into the cluster-crate
/// enum so proposals can be serialized without the Raft log depending
/// on the storage crate.
fn cluster_scope(scope: &ConstraintScope) -> ClusterConstraintScope {
    match scope {
        ConstraintScope::Node(l) => ClusterConstraintScope::Node(l.clone()),
        ConstraintScope::Relationship(t) => ClusterConstraintScope::Relationship(t.clone()),
    }
}

/// Bridge the storage-crate `PropertyConstraintKind` back into the
/// cluster-crate enum. The cluster enum is the "wire type" for DDL
/// replication; every writer that accepts a storage-side kind has to
/// translate on the way into the Raft log / routing fan-out.
fn cluster_kind(kind: PropertyConstraintKind) -> ClusterConstraintKind {
    match kind {
        PropertyConstraintKind::Unique => ClusterConstraintKind::Unique,
        PropertyConstraintKind::NotNull => ClusterConstraintKind::NotNull,
        PropertyConstraintKind::PropertyType(t) => {
            ClusterConstraintKind::PropertyType(cluster_property_type(t))
        }
        PropertyConstraintKind::NodeKey => ClusterConstraintKind::NodeKey,
    }
}

fn cluster_property_type(t: PropertyType) -> ClusterPropertyType {
    match t {
        PropertyType::String => ClusterPropertyType::String,
        PropertyType::Integer => ClusterPropertyType::Integer,
        PropertyType::Float => ClusterPropertyType::Float,
        PropertyType::Boolean => ClusterPropertyType::Boolean,
    }
}

/// Executor write sink that proposes every mutation through Raft. Must be
/// invoked from inside a `spawn_blocking` context, because the methods
/// bridge the sync executor to async Raft RPCs via `Handle::block_on`.
pub struct RaftGraphWriter {
    raft: Arc<RaftCluster>,
    handle: Handle,
}

impl RaftGraphWriter {
    pub fn new(raft: Arc<RaftCluster>) -> Self {
        Self {
            raft,
            handle: Handle::current(),
        }
    }

    fn propose(&self, cmd: GraphCommand) -> ExecResult<()> {
        let raft = self.raft.clone();
        self.handle
            .block_on(async move { raft.propose_graph(cmd).await })
            .map(|_| ())
            .map_err(|e| ExecError::Write(e.to_string()))
    }
}

impl GraphWriter for RaftGraphWriter {
    fn put_node(&self, node: &Node) -> ExecResult<()> {
        self.propose(GraphCommand::PutNode(node.clone()))
    }

    fn put_edge(&self, edge: &Edge) -> ExecResult<()> {
        self.propose(GraphCommand::PutEdge(edge.clone()))
    }

    fn delete_edge(&self, id: EdgeId) -> ExecResult<()> {
        self.propose(GraphCommand::DeleteEdge(id))
    }

    fn detach_delete_node(&self, id: NodeId) -> ExecResult<()> {
        self.propose(GraphCommand::DetachDeleteNode(id))
    }

    fn create_property_index(&self, label: &str, properties: &[String]) -> ExecResult<()> {
        self.propose(GraphCommand::CreateIndex {
            label: label.to_string(),
            properties: properties.iter().map(|p| p.to_string()).collect(),
        })
    }

    fn drop_property_index(&self, label: &str, properties: &[String]) -> ExecResult<()> {
        self.propose(GraphCommand::DropIndex {
            label: label.to_string(),
            properties: properties.iter().map(|p| p.to_string()).collect(),
        })
    }

    fn create_edge_property_index(&self, edge_type: &str, properties: &[String]) -> ExecResult<()> {
        self.propose(GraphCommand::CreateEdgeIndex {
            edge_type: edge_type.to_string(),
            properties: properties.iter().map(|p| p.to_string()).collect(),
        })
    }

    fn drop_edge_property_index(&self, edge_type: &str, properties: &[String]) -> ExecResult<()> {
        self.propose(GraphCommand::DropEdgeIndex {
            edge_type: edge_type.to_string(),
            properties: properties.iter().map(|p| p.to_string()).collect(),
        })
    }

    fn create_property_constraint(
        &self,
        name: Option<&str>,
        scope: &ConstraintScope,
        properties: &[String],
        kind: PropertyConstraintKind,
        if_not_exists: bool,
    ) -> ExecResult<PropertyConstraintSpec> {
        // Propose the creation through Raft. The state machine on
        // every replica applies the same command and returns a spec;
        // this writer has no way to thread that return value back
        // through `propose_graph`, so we synthesize the spec locally
        // from the supplied fields. The resolved name uses the same
        // deterministic formula as the storage layer.
        let cluster_scope = cluster_scope(scope);
        let resolved = meshdb_cluster::resolved_constraint_name(
            &name.map(str::to_string),
            &cluster_scope,
            properties,
            cluster_kind(kind),
        );
        self.propose(GraphCommand::CreateConstraint {
            name: name.map(str::to_string),
            scope: cluster_scope,
            properties: properties.to_vec(),
            kind: cluster_kind(kind),
            if_not_exists,
        })?;
        Ok(PropertyConstraintSpec {
            name: resolved,
            scope: scope.clone(),
            properties: properties.to_vec(),
            kind,
        })
    }

    fn drop_property_constraint(&self, name: &str, if_exists: bool) -> ExecResult<()> {
        self.propose(GraphCommand::DropConstraint {
            name: name.to_string(),
            if_exists,
        })
    }

    fn list_property_constraints(&self) -> ExecResult<Vec<PropertyConstraintSpec>> {
        // RaftGraphWriter can't read; snapshotting the registry is
        // the reader's job. Returning empty matches the default trait
        // impl — the executor only calls this on read paths, which
        // funnel through a reader instead of this sink.
        Ok(Vec::new())
    }
}

/// Executor write sink that accumulates mutations in memory instead of
/// proposing them one-by-one. Lets the caller commit a multi-write Cypher
/// query as a single Raft entry (`GraphCommand::Batch`) so a crash mid-query
/// can't leave behind a partial result on any replica.
pub struct BufferingGraphWriter {
    buffer: Mutex<Vec<GraphCommand>>,
}

impl Default for BufferingGraphWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferingGraphWriter {
    pub fn new() -> Self {
        Self {
            buffer: Mutex::new(Vec::new()),
        }
    }

    /// Drain the buffer, returning everything the executor wrote.
    pub fn into_commands(self) -> Vec<GraphCommand> {
        self.buffer.into_inner().unwrap_or_default()
    }
}

impl GraphWriter for BufferingGraphWriter {
    fn put_node(&self, node: &Node) -> ExecResult<()> {
        self.buffer
            .lock()
            .unwrap()
            .push(GraphCommand::PutNode(node.clone()));
        Ok(())
    }

    fn put_edge(&self, edge: &Edge) -> ExecResult<()> {
        self.buffer
            .lock()
            .unwrap()
            .push(GraphCommand::PutEdge(edge.clone()));
        Ok(())
    }

    fn delete_edge(&self, id: EdgeId) -> ExecResult<()> {
        self.buffer
            .lock()
            .unwrap()
            .push(GraphCommand::DeleteEdge(id));
        Ok(())
    }

    fn detach_delete_node(&self, id: NodeId) -> ExecResult<()> {
        self.buffer
            .lock()
            .unwrap()
            .push(GraphCommand::DetachDeleteNode(id));
        Ok(())
    }

    fn create_property_index(&self, label: &str, properties: &[String]) -> ExecResult<()> {
        self.buffer.lock().unwrap().push(GraphCommand::CreateIndex {
            label: label.to_string(),
            properties: properties.iter().map(|p| p.to_string()).collect(),
        });
        Ok(())
    }

    fn drop_property_index(&self, label: &str, properties: &[String]) -> ExecResult<()> {
        self.buffer.lock().unwrap().push(GraphCommand::DropIndex {
            label: label.to_string(),
            properties: properties.iter().map(|p| p.to_string()).collect(),
        });
        Ok(())
    }

    fn create_edge_property_index(&self, edge_type: &str, properties: &[String]) -> ExecResult<()> {
        self.buffer
            .lock()
            .unwrap()
            .push(GraphCommand::CreateEdgeIndex {
                edge_type: edge_type.to_string(),
                properties: properties.iter().map(|p| p.to_string()).collect(),
            });
        Ok(())
    }

    fn drop_edge_property_index(&self, edge_type: &str, properties: &[String]) -> ExecResult<()> {
        self.buffer
            .lock()
            .unwrap()
            .push(GraphCommand::DropEdgeIndex {
                edge_type: edge_type.to_string(),
                properties: properties.iter().map(|p| p.to_string()).collect(),
            });
        Ok(())
    }

    fn create_property_constraint(
        &self,
        name: Option<&str>,
        scope: &ConstraintScope,
        properties: &[String],
        kind: PropertyConstraintKind,
        if_not_exists: bool,
    ) -> ExecResult<PropertyConstraintSpec> {
        // Same synthesis trick as `RaftGraphWriter`: the real apply
        // happens later via `apply_prepared_batch` or the Raft log;
        // the caller only needs a plausible `PropertyConstraintSpec`
        // for the DDL acknowledgement row. Name resolution is
        // deterministic so the spec we return matches what the
        // applier will install on every replica.
        let cluster_scope = cluster_scope(scope);
        let resolved = meshdb_cluster::resolved_constraint_name(
            &name.map(str::to_string),
            &cluster_scope,
            properties,
            cluster_kind(kind),
        );
        self.buffer
            .lock()
            .unwrap()
            .push(GraphCommand::CreateConstraint {
                name: name.map(str::to_string),
                scope: cluster_scope,
                properties: properties.to_vec(),
                kind: cluster_kind(kind),
                if_not_exists,
            });
        Ok(PropertyConstraintSpec {
            name: resolved,
            scope: scope.clone(),
            properties: properties.to_vec(),
            kind,
        })
    }

    fn drop_property_constraint(&self, name: &str, if_exists: bool) -> ExecResult<()> {
        self.buffer
            .lock()
            .unwrap()
            .push(GraphCommand::DropConstraint {
                name: name.to_string(),
                if_exists,
            });
        Ok(())
    }

    fn list_property_constraints(&self) -> ExecResult<Vec<PropertyConstraintSpec>> {
        // Writers never read — the executor consults the reader
        // for `SHOW CONSTRAINTS` / `db.constraints()`. Returning
        // empty mirrors the index-side default.
        Ok(Vec::new())
    }
}
