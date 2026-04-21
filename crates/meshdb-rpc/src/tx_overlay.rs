//! Read-your-writes overlay used by the Bolt explicit-transaction path.
//!
//! Bolt transactions buffer mutations until COMMIT, which means
//! between-RUN reads would otherwise see the pre-BEGIN store — a
//! `CREATE (n)` followed by `MATCH (n)` in the same `BEGIN` / `COMMIT`
//! block would return zero rows until COMMIT. That's surprising, and
//! it blocks any driver pattern that does read-after-write inside a
//! transaction.
//!
//! The overlay closes the gap: at every in-tx `RUN` boundary, we fold
//! the accumulated write buffer into a [`TxOverlayState`], wrap the
//! normal [`GraphReader`] base in an [`OverlayGraphReader`], and pass
//! that to the executor. The overlay applies the buffered mutations on
//! top of every read:
//!
//! - `PutNode` / `PutEdge` entries shadow the base store's values and
//!   appear in scans and adjacency lookups.
//! - `DeleteEdge` hides the matching edge from every read method.
//! - `DetachDeleteNode` hides the node *and* implicitly filters any
//!   edge whose source or target is in the deleted set (the executor
//!   never sees an edge whose endpoint the overlay says is gone).
//!
//! The overlay is purely in-memory and read-only against the base. The
//! real commit still happens in `commit_buffered_commands` at COMMIT
//! time, through the same 2PC / Raft / local-apply machinery the
//! auto-commit single-RUN path uses. The overlay's job is to give
//! *this RUN* a view of the graph as if the tx had already committed.

use meshdb_cluster::GraphCommand;
use meshdb_core::{Edge, EdgeId, Node, NodeId};
use meshdb_executor::{GraphReader, Result as ExecResult};
use std::collections::{HashMap, HashSet};

/// Pre-folded view of a Bolt transaction's accumulated mutations.
/// Build once per in-tx `RUN` from the caller-held buffer; the
/// overlay reader consults it to answer reads.
#[derive(Debug, Default, Clone)]
pub struct TxOverlayState {
    /// Nodes created or updated by buffered commands. Keyed by id so
    /// repeated `PutNode(n)` calls on the same id collapse to the
    /// last-writer-wins value.
    put_nodes: HashMap<NodeId, Node>,
    /// Edges created or updated by buffered commands.
    put_edges: HashMap<EdgeId, Edge>,
    /// Node ids that `DetachDeleteNode` has removed. The overlay
    /// hides these from every read method and also implicitly hides
    /// any edge incident to one of them — matching detach-delete's
    /// cascade semantics without having to enumerate the incident
    /// edges at overlay-build time.
    deleted_nodes: HashSet<NodeId>,
    /// Edge ids that `DeleteEdge` has explicitly removed.
    deleted_edges: HashSet<EdgeId>,
}

impl TxOverlayState {
    /// Fold an accumulated command buffer into an overlay state.
    /// Idempotent under repeated calls with the same buffer — the
    /// state is a pure function of the command sequence.
    pub fn from_commands(commands: &[GraphCommand]) -> Self {
        let mut state = Self::default();
        state.apply_commands(commands);
        state
    }

    fn apply_commands(&mut self, commands: &[GraphCommand]) {
        for cmd in commands {
            self.apply_command(cmd);
        }
    }

    fn apply_command(&mut self, cmd: &GraphCommand) {
        match cmd {
            GraphCommand::PutNode(n) => {
                // A Put resurrects any prior delete for the same id —
                // idempotent with respect to command replay and
                // matches the Cypher semantics of `CREATE` followed
                // by a later `SET` on the same variable.
                self.deleted_nodes.remove(&n.id);
                self.put_nodes.insert(n.id, n.clone());
            }
            GraphCommand::PutEdge(e) => {
                self.deleted_edges.remove(&e.id);
                self.put_edges.insert(e.id, e.clone());
            }
            GraphCommand::DeleteEdge(id) => {
                self.put_edges.remove(id);
                self.deleted_edges.insert(*id);
            }
            GraphCommand::DetachDeleteNode(id) => {
                self.put_nodes.remove(id);
                // Also drop any buffered edges touching this node —
                // they'd be orphaned by the detach-delete anyway.
                self.put_edges
                    .retain(|_, e| e.source != *id && e.target != *id);
                self.deleted_nodes.insert(*id);
            }
            GraphCommand::Batch(inner) => self.apply_commands(inner),
            // DDL commands have no representation in the per-tx
            // overlay — they change the schema of the underlying
            // store rather than introducing transient node/edge
            // state. A Bolt BEGIN/COMMIT block that mixes DDL with
            // graph writes will still see the DDL effect on commit
            // via the applier path; the overlay just doesn't have
            // to pretend the index is already there mid-tx.
            GraphCommand::CreateIndex { .. }
            | GraphCommand::DropIndex { .. }
            | GraphCommand::CreateEdgeIndex { .. }
            | GraphCommand::DropEdgeIndex { .. }
            | GraphCommand::CreateConstraint { .. }
            | GraphCommand::DropConstraint { .. } => {}
        }
    }

    /// True iff `edge` should be visible through the overlay. An
    /// edge is hidden if the overlay explicitly deleted it or if
    /// either endpoint is in the deleted-nodes set (implicit
    /// cascade from `DetachDeleteNode`).
    fn is_edge_visible(&self, edge: &Edge) -> bool {
        !self.deleted_edges.contains(&edge.id)
            && !self.deleted_nodes.contains(&edge.source)
            && !self.deleted_nodes.contains(&edge.target)
    }
}

/// Read-your-writes overlay over a base [`GraphReader`]. Holds
/// references rather than owning anything so it can be cheaply
/// constructed per-RUN and dropped at RUN end without allocating.
pub struct OverlayGraphReader<'a> {
    base: &'a dyn GraphReader,
    overlay: &'a TxOverlayState,
}

impl<'a> OverlayGraphReader<'a> {
    pub fn new(base: &'a dyn GraphReader, overlay: &'a TxOverlayState) -> Self {
        Self { base, overlay }
    }
}

impl<'a> GraphReader for OverlayGraphReader<'a> {
    fn get_node(&self, id: NodeId) -> ExecResult<Option<Node>> {
        if self.overlay.deleted_nodes.contains(&id) {
            return Ok(None);
        }
        if let Some(n) = self.overlay.put_nodes.get(&id) {
            return Ok(Some(n.clone()));
        }
        self.base.get_node(id)
    }

    fn get_edge(&self, id: EdgeId) -> ExecResult<Option<Edge>> {
        if self.overlay.deleted_edges.contains(&id) {
            return Ok(None);
        }
        if let Some(e) = self.overlay.put_edges.get(&id) {
            if self.overlay.is_edge_visible(e) {
                return Ok(Some(e.clone()));
            }
            return Ok(None);
        }
        let base_edge = self.base.get_edge(id)?;
        if let Some(e) = &base_edge {
            if !self.overlay.is_edge_visible(e) {
                return Ok(None);
            }
        }
        Ok(base_edge)
    }

    fn all_node_ids(&self) -> ExecResult<Vec<NodeId>> {
        let mut seen: HashSet<NodeId> = self.base.all_node_ids()?.into_iter().collect();
        for id in &self.overlay.deleted_nodes {
            seen.remove(id);
        }
        for id in self.overlay.put_nodes.keys() {
            seen.insert(*id);
        }
        Ok(seen.into_iter().collect())
    }

    fn nodes_by_label(&self, label: &str) -> ExecResult<Vec<NodeId>> {
        let mut result: HashSet<NodeId> = self.base.nodes_by_label(label)?.into_iter().collect();
        // Drop deleted nodes and anything the overlay has a fresh
        // put for — we'll decide inclusion based on the put's labels
        // below, not the base's stale labels.
        for id in &self.overlay.deleted_nodes {
            result.remove(id);
        }
        for id in self.overlay.put_nodes.keys() {
            result.remove(id);
        }
        for (id, node) in &self.overlay.put_nodes {
            if node.labels.iter().any(|l| l == label) {
                result.insert(*id);
            }
        }
        Ok(result.into_iter().collect())
    }

    fn list_property_indexes(&self) -> ExecResult<Vec<(String, String)>> {
        // Index DDL lives on the store, not in the per-tx overlay,
        // so read-through to the base is correct even mid-tx.
        self.base.list_property_indexes()
    }

    fn list_edge_property_indexes(&self) -> ExecResult<Vec<(String, String)>> {
        // Same rationale as `list_property_indexes`: edge-index
        // DDL is a store-level concern; the overlay never shadows
        // it, so read-through is correct.
        self.base.list_edge_property_indexes()
    }

    fn list_property_constraints(&self) -> ExecResult<Vec<meshdb_storage::PropertyConstraintSpec>> {
        // Same rationale as `list_property_indexes`: constraint
        // registry lives on the store, so reading through to the
        // base is correct even mid-tx.
        self.base.list_property_constraints()
    }

    fn nodes_by_property(
        &self,
        label: &str,
        property: &str,
        value: &meshdb_core::Property,
    ) -> ExecResult<Vec<NodeId>> {
        // Same overlay pattern as nodes_by_label: start from the base
        // result, drop anything the overlay has shadowed, then re-add
        // from the overlay's puts when the current row matches. This
        // preserves read-your-writes across in-tx property mutations.
        let mut result: HashSet<NodeId> = self
            .base
            .nodes_by_property(label, property, value)?
            .into_iter()
            .collect();
        for id in &self.overlay.deleted_nodes {
            result.remove(id);
        }
        for id in self.overlay.put_nodes.keys() {
            result.remove(id);
        }
        for (id, node) in &self.overlay.put_nodes {
            if !node.labels.iter().any(|l| l == label) {
                continue;
            }
            if node.properties.get(property) == Some(value) {
                result.insert(*id);
            }
        }
        Ok(result.into_iter().collect())
    }

    fn outgoing(&self, source: NodeId) -> ExecResult<Vec<(EdgeId, NodeId)>> {
        // A deleted source has no outgoing edges in the overlay view.
        if self.overlay.deleted_nodes.contains(&source) {
            return Ok(Vec::new());
        }
        let mut out: Vec<(EdgeId, NodeId)> = self
            .base
            .outgoing(source)?
            .into_iter()
            .filter(|(eid, target)| {
                !self.overlay.deleted_edges.contains(eid)
                    && !self.overlay.deleted_nodes.contains(target)
                    // Shadow the base entry if the overlay has a
                    // fresh put for this edge id — we'll emit the
                    // put's version below.
                    && !self.overlay.put_edges.contains_key(eid)
            })
            .collect();
        for (eid, edge) in &self.overlay.put_edges {
            if edge.source == source && !self.overlay.deleted_nodes.contains(&edge.target) {
                out.push((*eid, edge.target));
            }
        }
        Ok(out)
    }

    fn incoming(&self, target: NodeId) -> ExecResult<Vec<(EdgeId, NodeId)>> {
        if self.overlay.deleted_nodes.contains(&target) {
            return Ok(Vec::new());
        }
        let mut out: Vec<(EdgeId, NodeId)> = self
            .base
            .incoming(target)?
            .into_iter()
            .filter(|(eid, src)| {
                !self.overlay.deleted_edges.contains(eid)
                    && !self.overlay.deleted_nodes.contains(src)
                    && !self.overlay.put_edges.contains_key(eid)
            })
            .collect();
        for (eid, edge) in &self.overlay.put_edges {
            if edge.target == target && !self.overlay.deleted_nodes.contains(&edge.source) {
                out.push((*eid, edge.source));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meshdb_core::{Edge, Node};
    use std::sync::Mutex;

    /// Tiny synthetic base reader that lets the overlay tests drive a
    /// known-good "store" without pulling in rocksdb. Holds HashMaps
    /// in a Mutex so the reader can be shared across threads.
    #[derive(Default)]
    struct FakeBase {
        nodes: Mutex<HashMap<NodeId, Node>>,
        edges: Mutex<HashMap<EdgeId, Edge>>,
    }

    impl FakeBase {
        fn with_node(self, node: Node) -> Self {
            self.nodes.lock().unwrap().insert(node.id, node);
            self
        }

        fn with_edge(self, edge: Edge) -> Self {
            self.edges.lock().unwrap().insert(edge.id, edge);
            self
        }
    }

    impl GraphReader for FakeBase {
        fn get_node(&self, id: NodeId) -> ExecResult<Option<Node>> {
            Ok(self.nodes.lock().unwrap().get(&id).cloned())
        }
        fn get_edge(&self, id: EdgeId) -> ExecResult<Option<Edge>> {
            Ok(self.edges.lock().unwrap().get(&id).cloned())
        }
        fn all_node_ids(&self) -> ExecResult<Vec<NodeId>> {
            Ok(self.nodes.lock().unwrap().keys().copied().collect())
        }
        fn nodes_by_label(&self, label: &str) -> ExecResult<Vec<NodeId>> {
            Ok(self
                .nodes
                .lock()
                .unwrap()
                .values()
                .filter(|n| n.labels.iter().any(|l| l == label))
                .map(|n| n.id)
                .collect())
        }
        fn nodes_by_property(
            &self,
            label: &str,
            property: &str,
            value: &meshdb_core::Property,
        ) -> ExecResult<Vec<NodeId>> {
            Ok(self
                .nodes
                .lock()
                .unwrap()
                .values()
                .filter(|n| n.labels.iter().any(|l| l == label))
                .filter(|n| n.properties.get(property) == Some(value))
                .map(|n| n.id)
                .collect())
        }
        fn outgoing(&self, source: NodeId) -> ExecResult<Vec<(EdgeId, NodeId)>> {
            Ok(self
                .edges
                .lock()
                .unwrap()
                .values()
                .filter(|e| e.source == source)
                .map(|e| (e.id, e.target))
                .collect())
        }
        fn incoming(&self, target: NodeId) -> ExecResult<Vec<(EdgeId, NodeId)>> {
            Ok(self
                .edges
                .lock()
                .unwrap()
                .values()
                .filter(|e| e.target == target)
                .map(|e| (e.id, e.source))
                .collect())
        }
    }

    #[test]
    fn put_node_is_visible_via_overlay() {
        let base = FakeBase::default();
        let new_node = Node::new().with_label("Fresh");
        let state = TxOverlayState::from_commands(&[GraphCommand::PutNode(new_node.clone())]);
        let reader = OverlayGraphReader::new(&base, &state);

        assert_eq!(
            reader.get_node(new_node.id).unwrap().unwrap().labels,
            vec!["Fresh"]
        );
        assert_eq!(reader.all_node_ids().unwrap(), vec![new_node.id]);
        assert_eq!(reader.nodes_by_label("Fresh").unwrap(), vec![new_node.id]);
    }

    #[test]
    fn put_node_shadows_base_node_with_same_id() {
        let original = Node::new().with_label("Stale");
        let id = original.id;
        let base = FakeBase::default().with_node(original);

        // Build an overlay that updates the same id with a new label.
        let mut updated = Node::new().with_label("Fresh");
        updated.id = id;
        let state = TxOverlayState::from_commands(&[GraphCommand::PutNode(updated)]);
        let reader = OverlayGraphReader::new(&base, &state);

        // get_node returns the overlay's version.
        let fetched = reader.get_node(id).unwrap().unwrap();
        assert_eq!(fetched.labels, vec!["Fresh"]);

        // nodes_by_label respects the shadowed labels: "Stale" no
        // longer returns this id, "Fresh" does.
        assert!(reader.nodes_by_label("Stale").unwrap().is_empty());
        assert_eq!(reader.nodes_by_label("Fresh").unwrap(), vec![id]);
    }

    #[test]
    fn delete_edge_hides_base_edge_from_all_read_methods() {
        let src = Node::new().with_label("N");
        let dst = Node::new().with_label("N");
        let edge = Edge::new("KNOWS", src.id, dst.id);
        let eid = edge.id;
        let base = FakeBase::default()
            .with_node(src.clone())
            .with_node(dst.clone())
            .with_edge(edge);

        let state = TxOverlayState::from_commands(&[GraphCommand::DeleteEdge(eid)]);
        let reader = OverlayGraphReader::new(&base, &state);

        assert!(reader.get_edge(eid).unwrap().is_none());
        assert!(reader.outgoing(src.id).unwrap().is_empty());
        assert!(reader.incoming(dst.id).unwrap().is_empty());
    }

    #[test]
    fn detach_delete_node_hides_node_and_cascades_to_incident_edges() {
        let a = Node::new().with_label("N");
        let b = Node::new().with_label("N");
        let c = Node::new().with_label("N");
        let a_id = a.id;
        let b_id = b.id;
        let c_id = c.id;
        let ab = Edge::new("K", a_id, b_id);
        let bc = Edge::new("K", b_id, c_id);
        let bc_id = bc.id;
        let base = FakeBase::default()
            .with_node(a)
            .with_node(b)
            .with_node(c)
            .with_edge(ab)
            .with_edge(bc);

        // DetachDeleteNode(b) should remove b and both edges.
        let state = TxOverlayState::from_commands(&[GraphCommand::DetachDeleteNode(b_id)]);
        let reader = OverlayGraphReader::new(&base, &state);

        assert!(reader.get_node(b_id).unwrap().is_none());
        // a still has b as its only outgoing target → filtered because
        // b is in deleted_nodes.
        assert!(reader.outgoing(a_id).unwrap().is_empty());
        // c still has b as incoming → also filtered.
        assert!(reader.incoming(c_id).unwrap().is_empty());
        // get_edge on bc returns None because b is deleted.
        assert!(reader.get_edge(bc_id).unwrap().is_none());
        // a and c are still visible in all_node_ids; b is not.
        let mut ids = reader.all_node_ids().unwrap();
        ids.sort();
        let mut expected = vec![a_id, c_id];
        expected.sort();
        assert_eq!(ids, expected);
    }

    #[test]
    fn put_edge_is_visible_via_outgoing_and_incoming() {
        let src = Node::new().with_label("N");
        let dst = Node::new().with_label("N");
        let base = FakeBase::default()
            .with_node(src.clone())
            .with_node(dst.clone());
        let edge = Edge::new("LINK", src.id, dst.id);
        let eid = edge.id;

        let state = TxOverlayState::from_commands(&[GraphCommand::PutEdge(edge)]);
        let reader = OverlayGraphReader::new(&base, &state);

        let out = reader.outgoing(src.id).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], (eid, dst.id));

        let inc = reader.incoming(dst.id).unwrap();
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0], (eid, src.id));
    }

    #[test]
    fn put_then_delete_node_collapses_to_no_visible_state() {
        // PutNode followed by DetachDeleteNode on the same id should
        // leave the overlay with no visible trace of the node — both
        // operations should cancel out.
        let n = Node::new().with_label("Temp");
        let id = n.id;
        let base = FakeBase::default();
        let state = TxOverlayState::from_commands(&[
            GraphCommand::PutNode(n),
            GraphCommand::DetachDeleteNode(id),
        ]);
        let reader = OverlayGraphReader::new(&base, &state);

        assert!(reader.get_node(id).unwrap().is_none());
        assert!(reader.all_node_ids().unwrap().is_empty());
        assert!(reader.nodes_by_label("Temp").unwrap().is_empty());
    }

    #[test]
    fn delete_then_put_resurrects_the_id() {
        let original = Node::new().with_label("Original");
        let id = original.id;
        let base = FakeBase::default().with_node(original);

        let mut replacement = Node::new().with_label("Replacement");
        replacement.id = id;
        // Delete first, then put a fresh version — this is the
        // rare but legal "replace node" sequence inside a tx.
        let state = TxOverlayState::from_commands(&[
            GraphCommand::DetachDeleteNode(id),
            GraphCommand::PutNode(replacement),
        ]);
        let reader = OverlayGraphReader::new(&base, &state);

        let fetched = reader.get_node(id).unwrap().unwrap();
        assert_eq!(fetched.labels, vec!["Replacement"]);
    }

    #[test]
    fn nested_batch_commands_are_flattened_into_state() {
        let a = Node::new().with_label("Outer");
        let b = Node::new().with_label("Inner");
        let base = FakeBase::default();
        let state = TxOverlayState::from_commands(&[GraphCommand::Batch(vec![
            GraphCommand::PutNode(a.clone()),
            GraphCommand::Batch(vec![GraphCommand::PutNode(b.clone())]),
        ])]);
        let reader = OverlayGraphReader::new(&base, &state);

        let mut ids = reader.all_node_ids().unwrap();
        ids.sort();
        let mut expected = vec![a.id, b.id];
        expected.sort();
        assert_eq!(ids, expected);
    }
}
