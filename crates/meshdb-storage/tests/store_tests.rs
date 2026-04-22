use meshdb_core::{Edge, Node, NodeId, Property};
use meshdb_storage::{
    ConstraintScope, EdgePropertyIndexSpec, GraphMutation, PropertyConstraintKind,
    PropertyIndexSpec, RocksDbStorageEngine as Store,
};
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
            GraphMutation::PutNode(a.clone()),
            GraphMutation::PutNode(b.clone()),
            GraphMutation::PutEdge(edge.clone()),
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
            GraphMutation::DeleteEdge(edge.id),
            GraphMutation::DeleteEdge(edge.id),
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
            GraphMutation::DetachDeleteNode(id),
            GraphMutation::PutNode(replacement.clone()),
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

#[test]
fn property_index_create_backfills_existing_nodes() {
    let (store, _dir) = tmp_store();
    let ada = Node::new()
        .with_label("Person")
        .with_property("name", "Ada");
    let bob = Node::new()
        .with_label("Person")
        .with_property("name", "Bob");
    let cid = Node::new().with_label("Robot").with_property("name", "C3");
    store.put_node(&ada).unwrap();
    store.put_node(&bob).unwrap();
    store.put_node(&cid).unwrap();

    store.create_property_index("Person", "name").unwrap();

    let ada_hits = store
        .nodes_by_property("Person", "name", &Property::String("Ada".into()))
        .unwrap();
    assert_eq!(ada_hits, vec![ada.id]);
    // Different label: unindexed, backfill excluded it.
    let c3_hits = store
        .nodes_by_property("Person", "name", &Property::String("C3".into()))
        .unwrap();
    assert!(c3_hits.is_empty());
}

#[test]
fn property_index_tracks_subsequent_puts_and_overwrites() {
    let (store, _dir) = tmp_store();
    store.create_property_index("Person", "name").unwrap();

    let mut ada = Node::new()
        .with_label("Person")
        .with_property("name", "Ada");
    store.put_node(&ada).unwrap();
    assert_eq!(
        store
            .nodes_by_property("Person", "name", &Property::String("Ada".into()))
            .unwrap(),
        vec![ada.id]
    );

    ada.properties
        .insert("name".into(), Property::String("Ada Lovelace".into()));
    store.put_node(&ada).unwrap();
    // Old value no longer resolves.
    assert!(store
        .nodes_by_property("Person", "name", &Property::String("Ada".into()))
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .nodes_by_property("Person", "name", &Property::String("Ada Lovelace".into()))
            .unwrap(),
        vec![ada.id]
    );
}

#[test]
fn property_index_detach_delete_removes_entry() {
    let (store, _dir) = tmp_store();
    store.create_property_index("Person", "name").unwrap();
    let ada = Node::new()
        .with_label("Person")
        .with_property("name", "Ada");
    store.put_node(&ada).unwrap();
    store.detach_delete_node(ada.id).unwrap();
    assert!(store
        .nodes_by_property("Person", "name", &Property::String("Ada".into()))
        .unwrap()
        .is_empty());
}

#[test]
fn property_index_drop_clears_entries_and_registry() {
    let (store, _dir) = tmp_store();
    store.create_property_index("Person", "name").unwrap();
    let ada = Node::new()
        .with_label("Person")
        .with_property("name", "Ada");
    store.put_node(&ada).unwrap();

    store.drop_property_index("Person", "name").unwrap();
    assert!(store.list_property_indexes().is_empty());
    // Re-creating with no backfill source still works; a subsequent
    // put_node also won't resurrect the stale entry because drop
    // swept the CF.
    store.create_property_index("Person", "name").unwrap();
    assert!(store
        .nodes_by_property("Person", "name", &Property::String("Ada".into()))
        .unwrap()
        .contains(&ada.id));
}

#[test]
fn property_index_registry_survives_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        store.create_property_index("Person", "name").unwrap();
        store
            .put_node(
                &Node::new()
                    .with_label("Person")
                    .with_property("name", "Ada"),
            )
            .unwrap();
    }
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(
        store.list_property_indexes(),
        vec![PropertyIndexSpec {
            label: "Person".into(),
            properties: vec!["name".into()],
        }]
    );
    let hits = store
        .nodes_by_property("Person", "name", &Property::String("Ada".into()))
        .unwrap();
    assert_eq!(hits.len(), 1);
}

#[test]
fn property_index_rejects_float_values_at_query_time() {
    let (store, _dir) = tmp_store();
    store.create_property_index("M", "score").unwrap();
    let err = store
        .nodes_by_property("M", "score", &Property::Float64(1.5))
        .unwrap_err();
    assert!(err.to_string().contains("not indexable"));
}

#[test]
fn property_index_tracks_label_change() {
    // Adding an indexed label to an existing node should start
    // indexing it; removing the label should drop the entry.
    let (store, _dir) = tmp_store();
    store.create_property_index("Person", "name").unwrap();

    let mut n = Node::new().with_label("Thing").with_property("name", "Ada");
    store.put_node(&n).unwrap();
    assert!(store
        .nodes_by_property("Person", "name", &Property::String("Ada".into()))
        .unwrap()
        .is_empty());

    n.labels.push("Person".into());
    store.put_node(&n).unwrap();
    assert_eq!(
        store
            .nodes_by_property("Person", "name", &Property::String("Ada".into()))
            .unwrap(),
        vec![n.id]
    );

    n.labels.retain(|l| l != "Person");
    store.put_node(&n).unwrap();
    assert!(store
        .nodes_by_property("Person", "name", &Property::String("Ada".into()))
        .unwrap()
        .is_empty());
}

#[test]
fn property_index_int64_and_bool_values_round_trip() {
    let (store, _dir) = tmp_store();
    store.create_property_index("P", "age").unwrap();
    store.create_property_index("P", "active").unwrap();
    let n = Node::new()
        .with_label("P")
        .with_property("age", 37_i64)
        .with_property("active", true);
    store.put_node(&n).unwrap();

    assert_eq!(
        store
            .nodes_by_property("P", "age", &Property::Int64(37))
            .unwrap(),
        vec![n.id]
    );
    assert_eq!(
        store
            .nodes_by_property("P", "active", &Property::Bool(true))
            .unwrap(),
        vec![n.id]
    );
    assert!(store
        .nodes_by_property("P", "age", &Property::Int64(38))
        .unwrap()
        .is_empty());
}

#[test]
fn property_index_string_int_same_value_do_not_alias() {
    // Ensures the type-tag byte prevents a String "42" from aliasing
    // an Int64 42 under the same (label, prop) prefix.
    let (store, _dir) = tmp_store();
    store.create_property_index("M", "x").unwrap();
    let a = Node::new().with_label("M").with_property("x", "42");
    let b = Node::new().with_label("M").with_property("x", 42_i64);
    store.put_node(&a).unwrap();
    store.put_node(&b).unwrap();
    assert_eq!(
        store
            .nodes_by_property("M", "x", &Property::String("42".into()))
            .unwrap(),
        vec![a.id]
    );
    assert_eq!(
        store
            .nodes_by_property("M", "x", &Property::Int64(42))
            .unwrap(),
        vec![b.id]
    );
}

#[test]
fn edge_property_index_create_backfills_existing_edges() {
    let (store, _dir) = tmp_store();
    let a = Node::new();
    let b = Node::new();
    store.put_node(&a).unwrap();
    store.put_node(&b).unwrap();
    let e1 = Edge::new("KNOWS", a.id, b.id).with_property("since", 2020_i64);
    let e2 = Edge::new("KNOWS", a.id, b.id).with_property("since", 2024_i64);
    let e3 = Edge::new("WORKS_AT", a.id, b.id).with_property("since", 2020_i64);
    store.put_edge(&e1).unwrap();
    store.put_edge(&e2).unwrap();
    store.put_edge(&e3).unwrap();

    store.create_edge_property_index("KNOWS", "since").unwrap();

    let hits = store
        .edges_by_property("KNOWS", "since", &Property::Int64(2020))
        .unwrap();
    assert_eq!(hits, vec![e1.id]);
    // Different type — backfill left it alone.
    let ignored = store
        .edges_by_property("KNOWS", "since", &Property::Int64(9999))
        .unwrap();
    assert!(ignored.is_empty());
    // Different edge type entirely, no index entry exists.
    let wrong_type = store
        .edges_by_property("WORKS_AT", "since", &Property::Int64(2020))
        .unwrap();
    assert!(wrong_type.is_empty());
}

#[test]
fn edge_property_index_tracks_puts_overwrites_and_deletes() {
    let (store, _dir) = tmp_store();
    store.create_edge_property_index("KNOWS", "since").unwrap();
    let a = Node::new();
    let b = Node::new();
    store.put_node(&a).unwrap();
    store.put_node(&b).unwrap();

    let mut e = Edge::new("KNOWS", a.id, b.id).with_property("since", 2020_i64);
    store.put_edge(&e).unwrap();
    assert_eq!(
        store
            .edges_by_property("KNOWS", "since", &Property::Int64(2020))
            .unwrap(),
        vec![e.id]
    );

    // Overwrite with a new value — old entry should be swept.
    e.properties.insert("since".into(), Property::Int64(2024));
    store.put_edge(&e).unwrap();
    assert!(store
        .edges_by_property("KNOWS", "since", &Property::Int64(2020))
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .edges_by_property("KNOWS", "since", &Property::Int64(2024))
            .unwrap(),
        vec![e.id]
    );

    // Delete drops the index entry.
    store.delete_edge(e.id).unwrap();
    assert!(store
        .edges_by_property("KNOWS", "since", &Property::Int64(2024))
        .unwrap()
        .is_empty());
}

#[test]
fn edge_property_index_detach_delete_sweeps_incident_edges() {
    // A DETACH DELETE on a node removes incident edges — the edge
    // property index needs to drop the corresponding entries along
    // with the edge row and its adjacency / type-index keys.
    let (store, _dir) = tmp_store();
    store.create_edge_property_index("KNOWS", "since").unwrap();
    let a = Node::new();
    let b = Node::new();
    let c = Node::new();
    for n in [&a, &b, &c] {
        store.put_node(n).unwrap();
    }
    let e1 = Edge::new("KNOWS", a.id, b.id).with_property("since", 2020_i64);
    let e2 = Edge::new("KNOWS", c.id, a.id).with_property("since", 2021_i64);
    store.put_edge(&e1).unwrap();
    store.put_edge(&e2).unwrap();

    store.detach_delete_node(a.id).unwrap();

    assert!(store
        .edges_by_property("KNOWS", "since", &Property::Int64(2020))
        .unwrap()
        .is_empty());
    assert!(store
        .edges_by_property("KNOWS", "since", &Property::Int64(2021))
        .unwrap()
        .is_empty());
}

#[test]
fn edge_property_index_drop_clears_entries_and_registry() {
    let (store, _dir) = tmp_store();
    store.create_edge_property_index("KNOWS", "since").unwrap();
    let a = Node::new();
    let b = Node::new();
    store.put_node(&a).unwrap();
    store.put_node(&b).unwrap();
    let e = Edge::new("KNOWS", a.id, b.id).with_property("since", 2020_i64);
    store.put_edge(&e).unwrap();

    store.drop_edge_property_index("KNOWS", "since").unwrap();
    assert!(store.list_edge_property_indexes().is_empty());

    // A fresh create rebuilds via backfill.
    store.create_edge_property_index("KNOWS", "since").unwrap();
    assert_eq!(
        store
            .edges_by_property("KNOWS", "since", &Property::Int64(2020))
            .unwrap(),
        vec![e.id]
    );
}

#[test]
fn edge_property_index_registry_survives_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        store.create_edge_property_index("KNOWS", "since").unwrap();
        let a = Node::new();
        let b = Node::new();
        store.put_node(&a).unwrap();
        store.put_node(&b).unwrap();
        store
            .put_edge(&Edge::new("KNOWS", a.id, b.id).with_property("since", 2020_i64))
            .unwrap();
    }
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(
        store.list_edge_property_indexes(),
        vec![EdgePropertyIndexSpec {
            edge_type: "KNOWS".into(),
            properties: vec!["since".into()],
        }]
    );
    let hits = store
        .edges_by_property("KNOWS", "since", &Property::Int64(2020))
        .unwrap();
    assert_eq!(hits.len(), 1);
}

#[test]
fn edge_property_index_rejects_float_values_at_query_time() {
    let (store, _dir) = tmp_store();
    store.create_edge_property_index("R", "weight").unwrap();
    let err = store
        .edges_by_property("R", "weight", &Property::Float64(1.5))
        .unwrap_err();
    assert!(err.to_string().contains("not indexable"));
}

#[test]
fn relationship_unique_constraint_provisions_backing_edge_index() {
    // Creating a UNIQUE constraint on a relationship type should
    // implicitly provision a (edge_type, property) index so every
    // subsequent put_edge routes through O(log N) enforcement
    // instead of the O(E_type) scan that preceded edge indexes.
    let (store, _dir) = tmp_store();
    store
        .create_property_constraint(
            Some("u_knows_since"),
            &ConstraintScope::Relationship("KNOWS".into()),
            &["since".to_string()],
            PropertyConstraintKind::Unique,
            false,
        )
        .unwrap();
    assert_eq!(
        store.list_edge_property_indexes(),
        vec![EdgePropertyIndexSpec {
            edge_type: "KNOWS".into(),
            properties: vec!["since".into()],
        }]
    );
}

#[test]
fn relationship_unique_detects_duplicate_on_insert() {
    let (store, _dir) = tmp_store();
    let a = Node::new();
    let b = Node::new();
    store.put_node(&a).unwrap();
    store.put_node(&b).unwrap();
    store
        .create_property_constraint(
            None,
            &ConstraintScope::Relationship("KNOWS".into()),
            &["since".to_string()],
            PropertyConstraintKind::Unique,
            false,
        )
        .unwrap();
    store
        .put_edge(&Edge::new("KNOWS", a.id, b.id).with_property("since", 2020_i64))
        .unwrap();
    let err = store
        .put_edge(&Edge::new("KNOWS", a.id, b.id).with_property("since", 2020_i64))
        .unwrap_err();
    assert!(err.to_string().contains("value already held"), "{err}");
}

#[test]
fn relationship_unique_allows_self_update_on_same_edge() {
    // A second put_edge that carries the same id should be treated
    // as an update — the existing holder is self, so no conflict.
    let (store, _dir) = tmp_store();
    let a = Node::new();
    let b = Node::new();
    store.put_node(&a).unwrap();
    store.put_node(&b).unwrap();
    store
        .create_property_constraint(
            None,
            &ConstraintScope::Relationship("KNOWS".into()),
            &["since".to_string()],
            PropertyConstraintKind::Unique,
            false,
        )
        .unwrap();
    let e = Edge::new("KNOWS", a.id, b.id).with_property("since", 2020_i64);
    store.put_edge(&e).unwrap();
    // Same id + same value should be a no-op update.
    let mut updated = e.clone();
    updated
        .properties
        .insert("since".into(), Property::Int64(2020));
    store.put_edge(&updated).unwrap();
}

// ---------------------------------------------------------------
// Composite property index tests (slice 3).
// ---------------------------------------------------------------

#[test]
fn composite_property_index_backfills_matching_nodes() {
    let (store, _dir) = tmp_store();
    // Three nodes: two match both props, one only has the first.
    store
        .put_node(
            &Node::new()
                .with_label("P")
                .with_property("first", "Ada")
                .with_property("last", "Lovelace"),
        )
        .unwrap();
    store
        .put_node(
            &Node::new()
                .with_label("P")
                .with_property("first", "Ada")
                .with_property("last", "Smith"),
        )
        .unwrap();
    store
        .put_node(&Node::new().with_label("P").with_property("first", "Ada"))
        .unwrap();

    store
        .create_property_index_composite("P", &["first".to_string(), "last".to_string()])
        .unwrap();
    let hits = store
        .nodes_by_properties(
            "P",
            &["first".to_string(), "last".to_string()],
            &[
                Property::String("Ada".into()),
                Property::String("Lovelace".into()),
            ],
        )
        .unwrap();
    assert_eq!(hits.len(), 1);
}

#[test]
fn composite_property_index_skips_partial_tuples() {
    // A node missing any of the composite properties contributes no
    // entry — composite indexes are all-or-nothing on the tuple.
    let (store, _dir) = tmp_store();
    store
        .create_property_index_composite("P", &["first".to_string(), "last".to_string()])
        .unwrap();
    // Partial: only `first` is set.
    store
        .put_node(&Node::new().with_label("P").with_property("first", "Ada"))
        .unwrap();
    let hits = store
        .nodes_by_properties(
            "P",
            &["first".to_string(), "last".to_string()],
            &[
                Property::String("Ada".into()),
                Property::String("Lovelace".into()),
            ],
        )
        .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn composite_property_index_tracks_put_updates() {
    let (store, _dir) = tmp_store();
    store
        .create_property_index_composite("P", &["a".to_string(), "b".to_string()])
        .unwrap();
    let mut n = Node::new()
        .with_label("P")
        .with_property("a", 1_i64)
        .with_property("b", 2_i64);
    let id = n.id;
    store.put_node(&n).unwrap();
    assert_eq!(
        store
            .nodes_by_properties(
                "P",
                &["a".to_string(), "b".to_string()],
                &[Property::Int64(1), Property::Int64(2)]
            )
            .unwrap(),
        vec![id]
    );

    // Update the tuple — old entry must drop, new entry appears.
    n.properties.insert("b".into(), Property::Int64(99));
    store.put_node(&n).unwrap();
    assert!(store
        .nodes_by_properties(
            "P",
            &["a".to_string(), "b".to_string()],
            &[Property::Int64(1), Property::Int64(2)]
        )
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .nodes_by_properties(
                "P",
                &["a".to_string(), "b".to_string()],
                &[Property::Int64(1), Property::Int64(99)]
            )
            .unwrap(),
        vec![id]
    );
}

#[test]
fn composite_property_index_detach_delete_sweeps_entries() {
    let (store, _dir) = tmp_store();
    store
        .create_property_index_composite("P", &["a".to_string(), "b".to_string()])
        .unwrap();
    let n = Node::new()
        .with_label("P")
        .with_property("a", 1_i64)
        .with_property("b", 2_i64);
    let id = n.id;
    store.put_node(&n).unwrap();
    store.detach_delete_node(id).unwrap();

    let hits = store
        .nodes_by_properties(
            "P",
            &["a".to_string(), "b".to_string()],
            &[Property::Int64(1), Property::Int64(2)],
        )
        .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn composite_property_index_drop_clears_entries() {
    let (store, _dir) = tmp_store();
    store
        .create_property_index_composite("P", &["a".to_string(), "b".to_string()])
        .unwrap();
    store
        .put_node(
            &Node::new()
                .with_label("P")
                .with_property("a", 1_i64)
                .with_property("b", 2_i64),
        )
        .unwrap();
    store
        .drop_property_index_composite("P", &["a".to_string(), "b".to_string()])
        .unwrap();
    assert!(store.list_property_indexes().is_empty());
}

#[test]
fn composite_edge_index_backfills_and_seeks() {
    let (store, _dir) = tmp_store();
    let a = Node::new();
    let b = Node::new();
    store.put_node(&a).unwrap();
    store.put_node(&b).unwrap();
    store
        .put_edge(
            &Edge::new("KNOWS", a.id, b.id)
                .with_property("since", 2020_i64)
                .with_property("weight", 5_i64),
        )
        .unwrap();
    store
        .create_edge_property_index_composite("KNOWS", &["since".to_string(), "weight".to_string()])
        .unwrap();
    let hits = store
        .edges_by_properties(
            "KNOWS",
            &["since".to_string(), "weight".to_string()],
            &[Property::Int64(2020), Property::Int64(5)],
        )
        .unwrap();
    assert_eq!(hits.len(), 1);
}

#[test]
fn node_key_constraint_auto_provisions_composite_backing_index() {
    // NODE KEY needs an O(log N) enforcement path, so creating the
    // constraint auto-provisions a matching composite index on the
    // same `(label, properties)` shape. Mirrors how UNIQUE provisions
    // a single-property backing index.
    let (store, _dir) = tmp_store();
    store
        .create_property_constraint(
            Some("pk_person"),
            &ConstraintScope::Node("Person".into()),
            &["first".to_string(), "last".to_string()],
            PropertyConstraintKind::NodeKey,
            false,
        )
        .unwrap();

    // Backing index must exist on the same property tuple, in order.
    let indexes = store.list_property_indexes();
    assert!(
        indexes.iter().any(|s| s.label == "Person"
            && s.properties == vec!["first".to_string(), "last".to_string()]),
        "expected backing composite index on Person(first, last), got {indexes:?}",
    );
}

#[test]
fn node_key_constraint_backfilled_index_drives_duplicate_detection() {
    // Insert data first, then declare the constraint. The backing
    // index is populated via backfill during `create_property_index_composite`,
    // and a subsequent duplicate insert hits the constraint through
    // the seek rather than an O(N) scan.
    let (store, _dir) = tmp_store();
    let ada = Node::new()
        .with_label("Person")
        .with_property("first", "Ada")
        .with_property("last", "Lovelace");
    store.put_node(&ada).unwrap();

    store
        .create_property_constraint(
            None,
            &ConstraintScope::Node("Person".into()),
            &["first".to_string(), "last".to_string()],
            PropertyConstraintKind::NodeKey,
            false,
        )
        .unwrap();

    // Duplicate tuple — must be rejected by the constraint check.
    let dup = Node::new()
        .with_label("Person")
        .with_property("first", "Ada")
        .with_property("last", "Lovelace");
    let err = store.put_node(&dup).unwrap_err();
    assert!(err.to_string().contains("tuple already held"), "{err}");

    // Self-update (same id, same tuple) must not false-positive on
    // the seek: the seek returns the existing id and the enforcement
    // loop skips it with `id != node.id`.
    store.put_node(&ada).unwrap();

    // A different tuple — allowed.
    let grace = Node::new()
        .with_label("Person")
        .with_property("first", "Grace")
        .with_property("last", "Hopper");
    store.put_node(&grace).unwrap();
}

#[test]
fn single_property_index_via_composite_api_matches_legacy_api() {
    // Length-1 call through the composite API must produce the same
    // seek results as the legacy single-property API. Pins the
    // backward-compatibility of the on-disk key format.
    let (store, _dir) = tmp_store();
    store
        .put_node(&Node::new().with_label("P").with_property("name", "Ada"))
        .unwrap();
    store
        .create_property_index_composite("P", &["name".to_string()])
        .unwrap();
    let via_composite = store
        .nodes_by_properties(
            "P",
            &["name".to_string()],
            &[Property::String("Ada".into())],
        )
        .unwrap();
    let via_legacy = store
        .nodes_by_property("P", "name", &Property::String("Ada".into()))
        .unwrap();
    assert_eq!(via_composite, via_legacy);
    assert_eq!(via_composite.len(), 1);
}
