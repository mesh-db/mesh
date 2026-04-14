use mesh_core::{Edge, Node, NodeId};
use mesh_storage::{Store, StoreMutation};
use tempfile::TempDir;

fn tmp_store() -> (Store, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    (store, dir)
}

#[test]
fn put_get_node_roundtrips() {
    let (store, _dir) = tmp_store();
    let node = Node::new()
        .with_label("Person")
        .with_property("name", "Ada")
        .with_property("age", 37_i64);

    store.put_node(&node).unwrap();
    let fetched = store.get_node(node.id).unwrap().unwrap();
    assert_eq!(fetched, node);
}

#[test]
fn get_missing_node_returns_none() {
    let (store, _dir) = tmp_store();
    assert!(store.get_node(NodeId::new()).unwrap().is_none());
}

#[test]
fn edge_adjacency_is_bidirectional() {
    let (store, _dir) = tmp_store();
    let a = Node::new();
    let b = Node::new();
    store.put_node(&a).unwrap();
    store.put_node(&b).unwrap();

    let e = Edge::new("KNOWS", a.id, b.id).with_property("since", 2020_i64);
    store.put_edge(&e).unwrap();

    assert_eq!(store.outgoing(a.id).unwrap(), vec![(e.id, b.id)]);
    assert_eq!(store.incoming(b.id).unwrap(), vec![(e.id, a.id)]);
    assert!(store.outgoing(b.id).unwrap().is_empty());
    assert!(store.incoming(a.id).unwrap().is_empty());
}

#[test]
fn outgoing_isolates_per_node() {
    let (store, _dir) = tmp_store();
    let a = Node::new();
    let b = Node::new();
    let c = Node::new();
    for n in [&a, &b, &c] {
        store.put_node(n).unwrap();
    }
    let e1 = Edge::new("T", a.id, b.id);
    let e2 = Edge::new("T", a.id, c.id);
    let e3 = Edge::new("T", b.id, c.id);
    store.put_edge(&e1).unwrap();
    store.put_edge(&e2).unwrap();
    store.put_edge(&e3).unwrap();

    let mut out_a: Vec<_> = store
        .outgoing(a.id)
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    out_a.sort();
    let mut expected = vec![e1.id, e2.id];
    expected.sort();
    assert_eq!(out_a, expected);

    assert_eq!(store.outgoing(b.id).unwrap(), vec![(e3.id, c.id)]);
}

#[test]
fn delete_edge_removes_both_adjacency_sides() {
    let (store, _dir) = tmp_store();
    let a = Node::new();
    let b = Node::new();
    store.put_node(&a).unwrap();
    store.put_node(&b).unwrap();
    let e = Edge::new("T", a.id, b.id);
    store.put_edge(&e).unwrap();

    store.delete_edge(e.id).unwrap();

    assert!(store.get_edge(e.id).unwrap().is_none());
    assert!(store.outgoing(a.id).unwrap().is_empty());
    assert!(store.incoming(b.id).unwrap().is_empty());
}

#[test]
fn detach_delete_removes_incident_edges_on_both_ends() {
    let (store, _dir) = tmp_store();
    let a = Node::new();
    let b = Node::new();
    let c = Node::new();
    for n in [&a, &b, &c] {
        store.put_node(n).unwrap();
    }
    let e_ab = Edge::new("T", a.id, b.id);
    let e_cb = Edge::new("T", c.id, b.id);
    let e_ba = Edge::new("T", b.id, a.id);
    store.put_edge(&e_ab).unwrap();
    store.put_edge(&e_cb).unwrap();
    store.put_edge(&e_ba).unwrap();

    store.detach_delete_node(b.id).unwrap();

    assert!(store.get_node(b.id).unwrap().is_none());
    assert!(store.get_edge(e_ab.id).unwrap().is_none());
    assert!(store.get_edge(e_cb.id).unwrap().is_none());
    assert!(store.get_edge(e_ba.id).unwrap().is_none());
    assert!(store.outgoing(a.id).unwrap().is_empty());
    assert!(store.incoming(a.id).unwrap().is_empty());
    assert!(store.outgoing(c.id).unwrap().is_empty());
}

#[test]
fn nodes_by_label_finds_all_matching_nodes() {
    let (store, _dir) = tmp_store();
    let ada = Node::new()
        .with_label("Person")
        .with_property("name", "Ada");
    let alan = Node::new()
        .with_label("Person")
        .with_property("name", "Alan");
    let py = Node::new()
        .with_label("Language")
        .with_property("name", "Python");
    store.put_node(&ada).unwrap();
    store.put_node(&alan).unwrap();
    store.put_node(&py).unwrap();

    let mut people = store.nodes_by_label("Person").unwrap();
    people.sort();
    let mut expected = vec![ada.id, alan.id];
    expected.sort();
    assert_eq!(people, expected);

    assert_eq!(store.nodes_by_label("Language").unwrap(), vec![py.id]);
    assert!(store.nodes_by_label("Nonexistent").unwrap().is_empty());
}

#[test]
fn multi_label_node_appears_under_each_label() {
    let (store, _dir) = tmp_store();
    let n = Node::new().with_label("Person").with_label("Employee");
    store.put_node(&n).unwrap();

    assert_eq!(store.nodes_by_label("Person").unwrap(), vec![n.id]);
    assert_eq!(store.nodes_by_label("Employee").unwrap(), vec![n.id]);
}

#[test]
fn put_node_diffs_labels_on_overwrite() {
    let (store, _dir) = tmp_store();
    let mut n = Node::new().with_label("Draft");
    store.put_node(&n).unwrap();
    assert_eq!(store.nodes_by_label("Draft").unwrap(), vec![n.id]);

    n.labels = vec!["Published".to_string()];
    store.put_node(&n).unwrap();

    assert!(store.nodes_by_label("Draft").unwrap().is_empty());
    assert_eq!(store.nodes_by_label("Published").unwrap(), vec![n.id]);
}

#[test]
fn detach_delete_removes_label_entries() {
    let (store, _dir) = tmp_store();
    let n = Node::new().with_label("Doomed");
    store.put_node(&n).unwrap();
    store.detach_delete_node(n.id).unwrap();
    assert!(store.nodes_by_label("Doomed").unwrap().is_empty());
}

#[test]
fn edges_by_type_finds_all_matching_edges() {
    let (store, _dir) = tmp_store();
    let a = Node::new();
    let b = Node::new();
    let c = Node::new();
    for n in [&a, &b, &c] {
        store.put_node(n).unwrap();
    }
    let knows_ab = Edge::new("KNOWS", a.id, b.id);
    let knows_ac = Edge::new("KNOWS", a.id, c.id);
    let likes_bc = Edge::new("LIKES", b.id, c.id);
    store.put_edge(&knows_ab).unwrap();
    store.put_edge(&knows_ac).unwrap();
    store.put_edge(&likes_bc).unwrap();

    let mut knows = store.edges_by_type("KNOWS").unwrap();
    knows.sort();
    let mut expected = vec![knows_ab.id, knows_ac.id];
    expected.sort();
    assert_eq!(knows, expected);

    assert_eq!(store.edges_by_type("LIKES").unwrap(), vec![likes_bc.id]);
    assert!(store.edges_by_type("HATES").unwrap().is_empty());
}

#[test]
fn delete_edge_removes_type_index_entry() {
    let (store, _dir) = tmp_store();
    let a = Node::new();
    let b = Node::new();
    store.put_node(&a).unwrap();
    store.put_node(&b).unwrap();
    let e = Edge::new("KNOWS", a.id, b.id);
    store.put_edge(&e).unwrap();

    store.delete_edge(e.id).unwrap();
    assert!(store.edges_by_type("KNOWS").unwrap().is_empty());
}

#[test]
fn detach_delete_removes_type_index_for_incident_edges() {
    let (store, _dir) = tmp_store();
    let a = Node::new();
    let b = Node::new();
    store.put_node(&a).unwrap();
    store.put_node(&b).unwrap();
    let e = Edge::new("KNOWS", a.id, b.id);
    store.put_edge(&e).unwrap();

    store.detach_delete_node(b.id).unwrap();
    assert!(store.edges_by_type("KNOWS").unwrap().is_empty());
}

#[test]
fn indexes_share_no_state_across_similar_prefixes() {
    let (store, _dir) = tmp_store();
    let p = Node::new().with_label("Pers");
    let person = Node::new().with_label("Person");
    store.put_node(&p).unwrap();
    store.put_node(&person).unwrap();

    assert_eq!(store.nodes_by_label("Pers").unwrap(), vec![p.id]);
    assert_eq!(store.nodes_by_label("Person").unwrap(), vec![person.id]);
}

#[test]
fn apply_batch_commits_multi_op_atomically() {
    let (store, _dir) = tmp_store();
    let a = Node::new()
        .with_label("Person")
        .with_property("name", "Ada");
    let b = Node::new()
        .with_label("Person")
        .with_property("name", "Grace");
    let edge = Edge::new("KNOWS", a.id, b.id);

    store
        .apply_batch(&[
            StoreMutation::PutNode(a.clone()),
            StoreMutation::PutNode(b.clone()),
            StoreMutation::PutEdge(edge.clone()),
        ])
        .unwrap();

    assert_eq!(store.get_node(a.id).unwrap().unwrap(), a);
    assert_eq!(store.get_node(b.id).unwrap().unwrap(), b);
    assert_eq!(store.get_edge(edge.id).unwrap().unwrap(), edge);

    // Both indexes should reflect the post-batch state.
    let person_ids = store.nodes_by_label("Person").unwrap();
    assert!(person_ids.contains(&a.id) && person_ids.contains(&b.id));
    assert_eq!(store.edges_by_type("KNOWS").unwrap(), vec![edge.id]);
}

#[test]
fn apply_batch_delete_edge_is_idempotent_for_missing_target() {
    let (store, _dir) = tmp_store();
    let a = Node::new();
    let b = Node::new();
    let edge = Edge::new("KNOWS", a.id, b.id);
    store.put_node(&a).unwrap();
    store.put_node(&b).unwrap();
    store.put_edge(&edge).unwrap();

    // Deleting twice in one batch must not error — the second sees the
    // edge already gone (live read) and skips.
    store
        .apply_batch(&[
            StoreMutation::DeleteEdge(edge.id),
            StoreMutation::DeleteEdge(edge.id),
        ])
        .unwrap();
    assert!(store.get_edge(edge.id).unwrap().is_none());
}

#[test]
fn apply_batch_detach_delete_then_reinsert_in_one_commit() {
    let (store, _dir) = tmp_store();
    let original = Node::new().with_label("Person").with_property("v", 1_i64);
    let id = original.id;
    store.put_node(&original).unwrap();

    // Replace an existing node atomically: delete then reinsert with a
    // different label set. The post-state must reflect the second put,
    // and label indexes must not leak the old label.
    let replacement = Node {
        id,
        labels: vec!["Hero".into()],
        properties: Default::default(),
    };
    store
        .apply_batch(&[
            StoreMutation::DetachDeleteNode(id),
            StoreMutation::PutNode(replacement.clone()),
        ])
        .unwrap();

    let fetched = store.get_node(id).unwrap().unwrap();
    assert_eq!(fetched.labels, vec!["Hero"]);
    assert!(store.nodes_by_label("Person").unwrap().is_empty());
    assert_eq!(store.nodes_by_label("Hero").unwrap(), vec![id]);
}

#[test]
fn apply_batch_empty_is_a_noop() {
    let (store, _dir) = tmp_store();
    store.apply_batch(&[]).unwrap();
    assert!(store.all_node_ids().unwrap().is_empty());
}

#[test]
fn store_reopens_and_data_persists() {
    let dir = TempDir::new().unwrap();
    let node = Node::new().with_label("Persistent");
    let node_id = node.id;
    {
        let store = Store::open(dir.path()).unwrap();
        store.put_node(&node).unwrap();
    }
    let store = Store::open(dir.path()).unwrap();
    let fetched = store.get_node(node_id).unwrap().unwrap();
    assert_eq!(fetched.labels, vec!["Persistent"]);
}

#[test]
fn create_checkpoint_produces_a_consistent_clone() {
    // Populate a source store, checkpoint it into a sibling dir,
    // then open that dir as a fresh Store and confirm the data is
    // visible. The checkpoint must reflect the source's state at
    // `create_checkpoint` call time, independent of any subsequent
    // mutations to the source.
    let src_dir = TempDir::new().unwrap();
    let cp_parent = TempDir::new().unwrap();
    let cp_path = cp_parent.path().join("snap");

    let src = Store::open(src_dir.path()).unwrap();
    let node = Node::new().with_label("Snapshotted");
    let node_id = node.id;
    src.put_node(&node).unwrap();

    src.create_checkpoint(&cp_path).unwrap();

    // After the checkpoint, mutate the source to prove the clone is
    // decoupled from subsequent writes.
    src.put_node(&Node::new().with_label("AfterSnapshot"))
        .unwrap();

    // Drop the source handle so the checkpoint dir isn't sharing any
    // open FDs with the live DB when we open it.
    drop(src);

    let cp = Store::open(&cp_path).unwrap();
    // The snapshotted node is visible via the checkpoint.
    let fetched = cp.get_node(node_id).unwrap().unwrap();
    assert_eq!(fetched.labels, vec!["Snapshotted"]);
    // The post-checkpoint write is NOT visible.
    let after: Vec<Vec<String>> = cp
        .all_nodes()
        .unwrap()
        .into_iter()
        .map(|n| n.labels)
        .collect();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0], vec!["Snapshotted"]);
}
