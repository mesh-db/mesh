use mesh_core::{Edge, Node, NodeId};
use mesh_storage::Store;
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
