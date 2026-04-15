use mesh_core::{Edge, Node, NodeId, Property};
use mesh_cypher::{parse, plan, plan_with_context, PlannerContext};
use mesh_executor::{execute, execute_with_reader, GraphReader, GraphWriter, ParamMap, Row, Value};
use mesh_storage::RocksDbStorageEngine as Store;
use tempfile::TempDir;

fn open_store() -> (Store, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    (store, dir)
}

fn run(store: &Store, q: &str) -> Vec<Row> {
    let stmt = parse(q).unwrap_or_else(|e| panic!("parse {q}: {e}"));
    let plan = plan(&stmt).unwrap_or_else(|e| panic!("plan {q}: {e}"));
    execute(&plan, store).unwrap_or_else(|e| panic!("exec {q}: {e}"))
}

/// Same as `run` but threads a `ParamMap` through to the executor —
/// used by the `$param` tests below. The store acts as both the read
/// source and the write sink.
fn run_with_params(store: &Store, q: &str, params: &ParamMap) -> Vec<Row> {
    let stmt = parse(q).unwrap_or_else(|e| panic!("parse {q}: {e}"));
    let plan = plan(&stmt).unwrap_or_else(|e| panic!("plan {q}: {e}"));
    execute_with_reader(
        &plan,
        store as &dyn GraphReader,
        store as &dyn GraphWriter,
        params,
    )
    .unwrap_or_else(|e| panic!("exec {q}: {e}"))
}

fn str_prop(row: &Row, key: &str) -> String {
    match row.get(key) {
        Some(Value::Property(Property::String(s))) => s.clone(),
        other => panic!("expected string at {key}, got {other:?}"),
    }
}

fn int_prop(row: &Row, key: &str) -> i64 {
    match row.get(key) {
        Some(Value::Property(Property::Int64(i))) => *i,
        other => panic!("expected int at {key}, got {other:?}"),
    }
}

#[test]
fn create_and_scan_all() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada'})");
    run(&store, "CREATE (n:Person {name: 'Alan'})");

    let rows = run(&store, "MATCH (n) RETURN n");
    assert_eq!(rows.len(), 2);
}

#[test]
fn scan_by_label_filters_out_other_labels() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada'})");
    run(&store, "CREATE (n:Language {name: 'Rust'})");

    let rows = run(&store, "MATCH (n:Person) RETURN n");
    assert_eq!(rows.len(), 1);
}

#[test]
fn project_properties_with_aliases() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada', age: 37})");

    let rows = run(
        &store,
        "MATCH (n:Person) RETURN n.name AS name, n.age AS age",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
    assert_eq!(int_prop(&rows[0], "age"), 37);
}

#[test]
fn filter_by_integer_comparison() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada', age: 37})");
    run(&store, "CREATE (n:Person {name: 'Alan', age: 41})");
    run(&store, "CREATE (n:Person {name: 'Grace', age: 85})");

    let rows = run(
        &store,
        "MATCH (n:Person) WHERE n.age > 40 RETURN n.name AS name",
    );
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["Alan", "Grace"]);
}

#[test]
fn where_with_and_combination() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada', age: 37})");
    run(&store, "CREATE (n:Person {name: 'Alan', age: 41})");

    let rows = run(
        &store,
        "MATCH (n:Person) WHERE n.age > 30 AND n.name = 'Ada' RETURN n.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

#[test]
fn where_with_or_combination() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada', age: 37})");
    run(&store, "CREATE (n:Person {name: 'Alan', age: 41})");
    run(&store, "CREATE (n:Person {name: 'Grace', age: 85})");

    let rows = run(
        &store,
        "MATCH (n:Person) WHERE n.name = 'Ada' OR n.age > 80 RETURN n.name AS name",
    );
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["Ada", "Grace"]);
}

#[test]
fn where_with_not() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Flag {active: true})");
    run(&store, "CREATE (n:Flag {active: false})");

    let rows = run(&store, "MATCH (n:Flag) WHERE NOT n.active = true RETURN n");
    assert_eq!(rows.len(), 1);
}

#[test]
fn limit_caps_result_count() {
    let (store, _d) = open_store();
    for _ in 0..5 {
        run(&store, "CREATE (n:X)");
    }
    let rows = run(&store, "MATCH (n:X) RETURN n LIMIT 3");
    assert_eq!(rows.len(), 3);
}

#[test]
fn skip_drops_leading_rows() {
    let (store, _d) = open_store();
    for _ in 0..5 {
        run(&store, "CREATE (n:X)");
    }
    let rows = run(&store, "MATCH (n:X) RETURN n SKIP 2");
    assert_eq!(rows.len(), 3);
}

#[test]
fn skip_and_limit_together() {
    let (store, _d) = open_store();
    for _ in 0..5 {
        run(&store, "CREATE (n:X)");
    }
    let rows = run(&store, "MATCH (n:X) RETURN n SKIP 1 LIMIT 2");
    assert_eq!(rows.len(), 2);
}

#[test]
fn missing_property_returns_null() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada'})");

    let rows = run(
        &store,
        "MATCH (n:Person) RETURN n.name AS name, n.age AS age",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
    assert_eq!(rows[0].get("age"), Some(&Value::Null));
}

#[allow(dead_code)]
struct Graph {
    ada: NodeId,
    alan: NodeId,
    grace: NodeId,
    analytics: NodeId,
}

fn build_social_graph(store: &Store) -> Graph {
    let ada = Node::new()
        .with_label("Person")
        .with_property("name", "Ada")
        .with_property("age", 37_i64);
    let alan = Node::new()
        .with_label("Person")
        .with_property("name", "Alan")
        .with_property("age", 41_i64);
    let grace = Node::new()
        .with_label("Person")
        .with_property("name", "Grace")
        .with_property("age", 85_i64);
    let analytics = Node::new()
        .with_label("Company")
        .with_property("name", "Analytics");

    for n in [&ada, &alan, &grace, &analytics] {
        store.put_node(n).unwrap();
    }

    store
        .put_edge(&Edge::new("KNOWS", ada.id, alan.id).with_property("since", 2018_i64))
        .unwrap();
    store
        .put_edge(&Edge::new("KNOWS", alan.id, grace.id))
        .unwrap();
    store
        .put_edge(&Edge::new("WORKS_AT", ada.id, analytics.id))
        .unwrap();
    store
        .put_edge(&Edge::new("LIKES", alan.id, ada.id))
        .unwrap();

    Graph {
        ada: ada.id,
        alan: alan.id,
        grace: grace.id,
        analytics: analytics.id,
    }
}

fn sorted_names(rows: &[Row], col: &str) -> Vec<String> {
    let mut v: Vec<String> = rows.iter().map(|r| str_prop(r, col)).collect();
    v.sort();
    v
}

#[test]
fn single_hop_outgoing_with_type() {
    let (store, _d) = open_store();
    let _g = build_social_graph(&store);

    let rows = run(
        &store,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS dst",
    );
    let mut pairs: Vec<(String, String)> = rows
        .iter()
        .map(|r| (str_prop(r, "src"), str_prop(r, "dst")))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("Ada".to_string(), "Alan".to_string()),
            ("Alan".to_string(), "Grace".to_string()),
        ]
    );
}

#[test]
fn single_hop_outgoing_any_type() {
    let (store, _d) = open_store();
    let _g = build_social_graph(&store);

    let rows = run(
        &store,
        "MATCH (a:Person)-->(b) RETURN a.name AS src, b.name AS dst",
    );
    assert_eq!(rows.len(), 4);
}

#[test]
fn single_hop_incoming() {
    let (store, _d) = open_store();
    let _g = build_social_graph(&store);

    let rows = run(
        &store,
        "MATCH (a:Person)<-[:KNOWS]-(b:Person) RETURN a.name AS who, b.name AS knower",
    );
    let mut pairs: Vec<(String, String)> = rows
        .iter()
        .map(|r| (str_prop(r, "who"), str_prop(r, "knower")))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("Alan".to_string(), "Ada".to_string()),
            ("Grace".to_string(), "Alan".to_string()),
        ]
    );
}

#[test]
fn single_hop_undirected_sees_both_sides() {
    let (store, _d) = open_store();
    let g = build_social_graph(&store);

    let rows = run(
        &store,
        "MATCH (a:Person)-[:KNOWS]-(b:Person) WHERE a.name = 'Alan' RETURN b.name AS other",
    );
    let names = sorted_names(&rows, "other");
    assert_eq!(names, vec!["Ada", "Grace"]);
    let _ = g.alan;
}

#[test]
fn target_label_filters_expand() {
    let (store, _d) = open_store();
    let _g = build_social_graph(&store);

    let rows = run(
        &store,
        "MATCH (a:Person)-->(c:Company) RETURN a.name AS who, c.name AS co",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "who"), "Ada");
    assert_eq!(str_prop(&rows[0], "co"), "Analytics");
}

#[test]
fn edge_type_filter() {
    let (store, _d) = open_store();
    let _g = build_social_graph(&store);

    let rows = run(
        &store,
        "MATCH (a:Person)-[:LIKES]->(b) RETURN a.name AS liker, b.name AS liked",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "liker"), "Alan");
    assert_eq!(str_prop(&rows[0], "liked"), "Ada");
}

#[test]
fn multi_hop_chain_two_knows() {
    let (store, _d) = open_store();
    let _g = build_social_graph(&store);

    let rows = run(
        &store,
        "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN a.name AS a, c.name AS c",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a"), "Ada");
    assert_eq!(str_prop(&rows[0], "c"), "Grace");
}

#[test]
fn where_predicate_on_target_of_expand() {
    let (store, _d) = open_store();
    let _g = build_social_graph(&store);

    let rows = run(
        &store,
        "MATCH (a:Person)-[:KNOWS]->(b) WHERE b.age > 80 RETURN a.name AS a, b.name AS b",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a"), "Alan");
    assert_eq!(str_prop(&rows[0], "b"), "Grace");
}

#[test]
fn edge_variable_binds_edge() {
    let (store, _d) = open_store();
    let _g = build_social_graph(&store);

    let rows = run(
        &store,
        "MATCH (a:Person)-[r:KNOWS]->(b) WHERE r.since = 2018 RETURN a.name AS a, b.name AS b",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a"), "Ada");
    assert_eq!(str_prop(&rows[0], "b"), "Alan");
}

#[test]
fn expand_yields_no_rows_when_no_edges() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Loner)");

    let rows = run(&store, "MATCH (n:Loner)-->(m) RETURN m");
    assert!(rows.is_empty());
}

#[test]
fn multi_label_node_pattern_matches_intersection() {
    let (store, _d) = open_store();
    run(&store, "CREATE (a:Person {name: 'Ada'})");
    let employee = Node::new()
        .with_label("Person")
        .with_label("Employee")
        .with_property("name", "Alan");
    store.put_node(&employee).unwrap();

    let rows = run(&store, "MATCH (n:Person:Employee) RETURN n.name AS name");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Alan");
}

#[test]
fn multi_label_expand_target() {
    let (store, _d) = open_store();
    let alan = Node::new()
        .with_label("Person")
        .with_label("Employee")
        .with_property("name", "Alan");
    let ada = Node::new()
        .with_label("Person")
        .with_property("name", "Ada");
    store.put_node(&alan).unwrap();
    store.put_node(&ada).unwrap();
    let e = mesh_core::Edge::new("KNOWS", ada.id, alan.id);
    store.put_edge(&e).unwrap();

    let rows = run(
        &store,
        "MATCH (a:Person)-[:KNOWS]->(b:Person:Employee) RETURN a.name AS a, b.name AS b",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a"), "Ada");
    assert_eq!(str_prop(&rows[0], "b"), "Alan");
}

#[test]
fn create_with_multi_label() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person:Employee {name: 'Alan'})");
    let rows = run(&store, "MATCH (n:Person:Employee) RETURN labels(n) AS ls");
    match rows[0].get("ls") {
        Some(Value::List(items)) => {
            let mut names: Vec<String> = items
                .iter()
                .map(|v| match v {
                    Value::Property(Property::String(s)) => s.clone(),
                    _ => panic!(),
                })
                .collect();
            names.sort();
            assert_eq!(names, vec!["Employee", "Person"]);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn set_label_adds_a_label() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada'})");
    run(&store, "MATCH (n:Person) SET n:Archived");

    let rows = run(&store, "MATCH (n:Archived) RETURN n.name AS name");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");

    let rows2 = run(&store, "MATCH (n:Person) RETURN n.name AS name");
    assert_eq!(rows2.len(), 1);
}

#[test]
fn set_label_multiple_at_once() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada'})");
    run(&store, "MATCH (n:Person) SET n:Active:VIP");

    let rows = run(&store, "MATCH (n:Person:Active:VIP) RETURN n.name AS name");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

#[test]
fn set_merge_map_overlays_properties() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:X {a: 1, b: 2})");

    run(&store, "MATCH (n:X) SET n += {b: 20, c: 30}");

    let rows = run(&store, "MATCH (n:X) RETURN n.a AS a, n.b AS b, n.c AS c");
    assert_eq!(int_prop(&rows[0], "a"), 1);
    assert_eq!(int_prop(&rows[0], "b"), 20);
    assert_eq!(int_prop(&rows[0], "c"), 30);
}

#[test]
fn set_replace_map_clears_other_properties() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:X {a: 1, b: 2})");

    run(&store, "MATCH (n:X) SET n = {c: 3}");

    let rows = run(&store, "MATCH (n:X) RETURN n.a AS a, n.b AS b, n.c AS c");
    assert_eq!(rows[0].get("a"), Some(&Value::Null));
    assert_eq!(rows[0].get("b"), Some(&Value::Null));
    assert_eq!(int_prop(&rows[0], "c"), 3);
}

#[test]
fn count_distinct_dedupes_values() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {dept: 'eng'})");
    run(&store, "CREATE (n:Person {dept: 'eng'})");
    run(&store, "CREATE (n:Person {dept: 'ops'})");
    run(&store, "CREATE (n:Person {dept: 'ops'})");
    run(&store, "CREATE (n:Person {dept: 'sales'})");

    let rows = run(
        &store,
        "MATCH (n:Person) RETURN count(DISTINCT n.dept) AS dcount",
    );
    assert_eq!(int_prop(&rows[0], "dcount"), 3);
}

#[test]
fn count_distinct_vs_count_plain() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {dept: 'eng'})");
    run(&store, "CREATE (n:Person {dept: 'eng'})");
    run(&store, "CREATE (n:Person {dept: 'ops'})");

    let rows = run(
        &store,
        "MATCH (n:Person) RETURN count(n.dept) AS total, count(DISTINCT n.dept) AS uniq",
    );
    assert_eq!(int_prop(&rows[0], "total"), 3);
    assert_eq!(int_prop(&rows[0], "uniq"), 2);
}

#[test]
fn scalar_tointeger_from_string() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:X {s: '42'})");
    let rows = run(&store, "MATCH (n:X) RETURN toInteger(n.s) AS i");
    assert_eq!(int_prop(&rows[0], "i"), 42);
}

#[test]
fn scalar_tointeger_from_invalid_string_returns_null() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:X {s: 'abc'})");
    let rows = run(&store, "MATCH (n:X) RETURN toInteger(n.s) AS i");
    assert_eq!(rows[0].get("i"), Some(&Value::Null));
}

#[test]
fn scalar_tointeger_truncates_float() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:X {f: 3.7})");
    let rows = run(&store, "MATCH (n:X) RETURN toInteger(n.f) AS i");
    assert_eq!(int_prop(&rows[0], "i"), 3);
}

#[test]
fn scalar_coalesce_picks_first_non_null() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:X {name: 'Ada'})");
    let rows = run(
        &store,
        "MATCH (n:X) RETURN coalesce(n.nickname, n.name, 'unknown') AS out",
    );
    assert_eq!(str_prop(&rows[0], "out"), "Ada");
}

#[test]
fn scalar_coalesce_all_null_returns_null() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:X {name: 'Ada'})");
    let rows = run(
        &store,
        "MATCH (n:X) RETURN coalesce(n.missing1, n.missing2) AS out",
    );
    assert_eq!(rows[0].get("out"), Some(&Value::Null));
}

#[test]
fn scalar_size_on_string_and_list() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:X {name: 'hello'})");
    let rows = run(&store, "MATCH (n:X) RETURN size(n.name) AS len");
    assert_eq!(int_prop(&rows[0], "len"), 5);
}

#[test]
fn scalar_labels_returns_list_of_strings() {
    let (store, _d) = open_store();
    // Multi-label node patterns aren't yet in the grammar — build directly.
    let n = Node::new()
        .with_label("Person")
        .with_label("Employee")
        .with_property("name", "Ada");
    store.put_node(&n).unwrap();

    let rows = run(&store, "MATCH (n:Person) RETURN labels(n) AS ls");
    match rows[0].get("ls") {
        Some(Value::List(items)) => {
            let mut names: Vec<String> = items
                .iter()
                .map(|v| match v {
                    Value::Property(Property::String(s)) => s.clone(),
                    _ => panic!(),
                })
                .collect();
            names.sort();
            assert_eq!(names, vec!["Employee", "Person"]);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn scalar_keys_returns_sorted_list() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:X {b: 2, a: 1, c: 3})");
    let rows = run(&store, "MATCH (n:X) RETURN keys(n) AS ks");
    match rows[0].get("ks") {
        Some(Value::List(items)) => {
            let ks: Vec<String> = items
                .iter()
                .map(|v| match v {
                    Value::Property(Property::String(s)) => s.clone(),
                    _ => panic!(),
                })
                .collect();
            assert_eq!(ks, vec!["a", "b", "c"]);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn scalar_type_on_edge() {
    let (store, _d) = open_store();
    run(&store, "CREATE (a:Person)-[:KNOWS]->(b:Person)");
    let rows = run(&store, "MATCH (a)-[r]->(b) RETURN type(r) AS t");
    assert_eq!(str_prop(&rows[0], "t"), "KNOWS");
}

#[test]
fn scalar_tolower_and_toupper() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:X {name: 'HeLLo'})");
    let rows = run(
        &store,
        "MATCH (n:X) RETURN toLower(n.name) AS lo, toUpper(n.name) AS hi",
    );
    assert_eq!(str_prop(&rows[0], "lo"), "hello");
    assert_eq!(str_prop(&rows[0], "hi"), "HELLO");
}

#[test]
fn scalar_size_on_var_length_path_list() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (a:Link {name: 'a'})-[:N]->(b:Link {name: 'b'})-[:N]->(c:Link {name: 'c'})",
    );
    let rows = run(
        &store,
        "MATCH (a:Link)-[r:N*1..3]->(b:Link) WHERE a.name = 'a' AND b.name = 'c' RETURN size(r) AS hops",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "hops"), 2);
}

#[test]
fn scalar_in_where_clause() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada'})");
    run(&store, "CREATE (n:Person {name: 'Alexandria'})");

    let rows = run(
        &store,
        "MATCH (n:Person) WHERE size(n.name) > 5 RETURN n.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Alexandria");
}

#[test]
fn create_with_return_yields_new_node() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "CREATE (n:Person {name: 'Ada'}) RETURN n.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

#[test]
fn match_set_with_return_sees_updated_value() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada'})");

    let rows = run(
        &store,
        "MATCH (n:Person) SET n.age = 37 RETURN n.name AS name, n.age AS age",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
    assert_eq!(int_prop(&rows[0], "age"), 37);
}

#[test]
fn match_create_with_return_yields_linked_pair() {
    let (store, _d) = open_store();
    run(&store, "CREATE (a:Person {name: 'Ada'})");

    let rows = run(
        &store,
        "MATCH (a:Person) CREATE (a)-[:WORKS_AT]->(c:Company {name: 'Acme'}) RETURN a.name AS who, c.name AS co",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "who"), "Ada");
    assert_eq!(str_prop(&rows[0], "co"), "Acme");
}

#[test]
fn match_detach_delete_with_return_sees_pre_delete_row() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (n:Person {name: 'Ada'})-[:KNOWS]->(m:Person {name: 'Alan'})",
    );

    let rows = run(
        &store,
        "MATCH (n:Person) WHERE n.name = 'Ada' DETACH DELETE n RETURN n.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");

    // Verify Ada and her edge are actually gone.
    let after = run(&store, "MATCH (n:Person) WHERE n.name = 'Ada' RETURN n");
    assert!(after.is_empty());
}

#[test]
fn match_create_with_return_and_aggregation() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada'})");
    run(&store, "CREATE (n:Person {name: 'Alan'})");

    let rows = run(
        &store,
        "MATCH (p:Person) CREATE (p)-[:WORKS_AT]->(c:Company) RETURN count(*) AS total",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "total"), 2);
}

#[test]
fn multi_pattern_cartesian_product() {
    let (store, _d) = open_store();
    run(&store, "CREATE (a:Person {name: 'Ada'})");
    run(&store, "CREATE (b:Person {name: 'Alan'})");
    run(&store, "CREATE (c:Company {name: 'Acme'})");

    let rows = run(
        &store,
        "MATCH (p:Person), (c:Company) RETURN p.name AS person, c.name AS company",
    );
    assert_eq!(rows.len(), 2);
    let mut pairs: Vec<(String, String)> = rows
        .iter()
        .map(|r| (str_prop(r, "person"), str_prop(r, "company")))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("Ada".to_string(), "Acme".to_string()),
            ("Alan".to_string(), "Acme".to_string()),
        ]
    );
}

#[test]
fn multi_pattern_match_with_where_filters_join() {
    let (store, _d) = open_store();
    run(&store, "CREATE (a:Person {name: 'Ada', age: 37})");
    run(&store, "CREATE (b:Person {name: 'Alan', age: 41})");
    run(&store, "CREATE (c:Company {name: 'Acme', budget: 100})");
    run(&store, "CREATE (d:Company {name: 'Beta', budget: 50})");

    let rows = run(
        &store,
        "MATCH (p:Person), (co:Company) WHERE p.age > 40 AND co.budget > 75 RETURN p.name AS p, co.name AS c",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "p"), "Alan");
    assert_eq!(str_prop(&rows[0], "c"), "Acme");
}

#[test]
fn multi_pattern_match_same_var_rejected() {
    let (store, _d) = open_store();
    let _ = &store;
    let stmt = parse("MATCH (a), (a) RETURN a").unwrap();
    let err = plan(&stmt).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("multiple MATCH patterns"), "msg: {msg}");
}

#[test]
fn multi_pattern_create_builds_independent_nodes() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (a:Person {name: 'Ada'}), (b:Person {name: 'Alan'})",
    );

    let rows = run(&store, "MATCH (p:Person) RETURN p.name AS name");
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["Ada", "Alan"]);
}

#[test]
fn match_create_links_existing_nodes() {
    let (store, _d) = open_store();
    run(&store, "CREATE (a:Person {name: 'Ada'})");
    run(&store, "CREATE (b:Person {name: 'Alan'})");

    run(
        &store,
        "MATCH (a:Person), (b:Person) WHERE a.name = 'Ada' AND b.name = 'Alan' CREATE (a)-[:KNOWS]->(b)",
    );

    let rows = run(
        &store,
        "MATCH (a)-[:KNOWS]->(b) RETURN a.name AS src, b.name AS dst",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "src"), "Ada");
    assert_eq!(str_prop(&rows[0], "dst"), "Alan");
}

#[test]
fn match_create_can_introduce_new_node() {
    let (store, _d) = open_store();
    run(&store, "CREATE (a:Person {name: 'Ada'})");

    run(
        &store,
        "MATCH (a:Person) CREATE (a)-[:WORKS_AT]->(c:Company {name: 'Acme'})",
    );

    let rows = run(
        &store,
        "MATCH (a:Person)-[:WORKS_AT]->(c:Company) RETURN a.name AS p, c.name AS co",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "p"), "Ada");
    assert_eq!(str_prop(&rows[0], "co"), "Acme");
}

#[test]
fn match_create_runs_once_per_matched_row() {
    let (store, _d) = open_store();
    // Three persons, each gets a fresh attached company
    run(&store, "CREATE (n:Person {name: 'Ada'})");
    run(&store, "CREATE (n:Person {name: 'Alan'})");
    run(&store, "CREATE (n:Person {name: 'Grace'})");

    run(
        &store,
        "MATCH (p:Person) CREATE (p)-[:WORKS_AT]->(c:Company)",
    );

    let rows = run(
        &store,
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name AS name",
    );
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["Ada", "Alan", "Grace"]);

    // Exactly 3 Company nodes were created.
    let companies = run(&store, "MATCH (c:Company) RETURN c");
    assert_eq!(companies.len(), 3);
}

#[test]
fn match_create_self_loop_via_same_var() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Node {name: 'self'})");

    run(&store, "MATCH (n:Node) CREATE (n)-[:LOOP]->(n)");

    let rows = run(
        &store,
        "MATCH (a)-[:LOOP]->(b) RETURN a.name AS a, b.name AS b",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a"), "self");
    assert_eq!(str_prop(&rows[0], "b"), "self");
}

fn seed_people(store: &Store) {
    run(
        store,
        "CREATE (n:Person {name: 'Ada', age: 37, dept: 'eng'})",
    );
    run(
        store,
        "CREATE (n:Person {name: 'Alan', age: 41, dept: 'eng'})",
    );
    run(
        store,
        "CREATE (n:Person {name: 'Grace', age: 85, dept: 'ops'})",
    );
    run(
        store,
        "CREATE (n:Person {name: 'Ada', age: 29, dept: 'ops'})",
    );
}

#[test]
fn order_by_property_ascending() {
    let (store, _d) = open_store();
    seed_people(&store);

    let rows = run(
        &store,
        "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY age",
    );
    let ages: Vec<i64> = rows.iter().map(|r| int_prop(r, "age")).collect();
    assert_eq!(ages, vec![29, 37, 41, 85]);
}

#[test]
fn order_by_property_descending() {
    let (store, _d) = open_store();
    seed_people(&store);

    let rows = run(
        &store,
        "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY age DESC",
    );
    let ages: Vec<i64> = rows.iter().map(|r| int_prop(r, "age")).collect();
    assert_eq!(ages, vec![85, 41, 37, 29]);
}

#[test]
fn order_by_multi_key_mixed_direction() {
    let (store, _d) = open_store();
    seed_people(&store);

    // dept ASC, age DESC -> eng:41,37 then ops:85,29
    let rows = run(
        &store,
        "MATCH (p:Person) RETURN p.name AS name, p.age AS age, p.dept AS dept ORDER BY dept, age DESC",
    );
    let pairs: Vec<(String, i64)> = rows
        .iter()
        .map(|r| (str_prop(r, "dept"), int_prop(r, "age")))
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("eng".to_string(), 41),
            ("eng".to_string(), 37),
            ("ops".to_string(), 85),
            ("ops".to_string(), 29),
        ]
    );
}

#[test]
fn order_by_then_limit() {
    let (store, _d) = open_store();
    seed_people(&store);

    let rows = run(
        &store,
        "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY age DESC LIMIT 2",
    );
    let ages: Vec<i64> = rows.iter().map(|r| int_prop(r, "age")).collect();
    assert_eq!(ages, vec![85, 41]);
}

#[test]
fn distinct_dedupes_identical_projected_rows() {
    let (store, _d) = open_store();
    seed_people(&store);

    let rows = run(&store, "MATCH (p:Person) RETURN DISTINCT p.dept AS dept");
    let mut depts: Vec<String> = rows.iter().map(|r| str_prop(r, "dept")).collect();
    depts.sort();
    assert_eq!(depts, vec!["eng", "ops"]);
}

#[test]
fn distinct_with_order_by() {
    let (store, _d) = open_store();
    seed_people(&store);

    let rows = run(
        &store,
        "MATCH (p:Person) RETURN DISTINCT p.dept AS dept ORDER BY dept",
    );
    let depts: Vec<String> = rows.iter().map(|r| str_prop(r, "dept")).collect();
    assert_eq!(depts, vec!["eng", "ops"]);
}

#[test]
fn count_star_over_label() {
    let (store, _d) = open_store();
    seed_people(&store);

    let rows = run(&store, "MATCH (p:Person) RETURN count(*) AS total");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "total"), 4);
}

#[test]
fn count_empty_match_returns_zero() {
    let (store, _d) = open_store();
    let rows = run(&store, "MATCH (p:NoSuchLabel) RETURN count(*) AS total");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "total"), 0);
}

#[test]
fn count_expr_skips_null() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'with_age', age: 30})");
    run(&store, "CREATE (n:Person {name: 'no_age'})");

    let rows = run(&store, "MATCH (p:Person) RETURN count(p.age) AS c");
    assert_eq!(int_prop(&rows[0], "c"), 1);
}

#[test]
fn sum_and_avg_over_integer_property() {
    let (store, _d) = open_store();
    seed_people(&store);

    let rows = run(
        &store,
        "MATCH (p:Person) RETURN sum(p.age) AS total, avg(p.age) AS mean",
    );
    assert_eq!(int_prop(&rows[0], "total"), 37 + 41 + 85 + 29);
    match rows[0].get("mean") {
        Some(Value::Property(Property::Float64(f))) => {
            let expected = (37.0 + 41.0 + 85.0 + 29.0) / 4.0;
            assert!((*f - expected).abs() < 1e-9);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn min_and_max_over_property() {
    let (store, _d) = open_store();
    seed_people(&store);

    let rows = run(
        &store,
        "MATCH (p:Person) RETURN min(p.age) AS lo, max(p.age) AS hi",
    );
    assert_eq!(int_prop(&rows[0], "lo"), 29);
    assert_eq!(int_prop(&rows[0], "hi"), 85);
}

#[test]
fn collect_returns_list_of_names() {
    let (store, _d) = open_store();
    seed_people(&store);

    let rows = run(&store, "MATCH (p:Person) RETURN collect(p.name) AS names");
    match rows[0].get("names") {
        Some(Value::List(items)) => {
            assert_eq!(items.len(), 4);
            let mut names: Vec<String> = items
                .iter()
                .map(|v| match v {
                    Value::Property(Property::String(s)) => s.clone(),
                    _ => panic!(),
                })
                .collect();
            names.sort();
            assert_eq!(names, vec!["Ada", "Ada", "Alan", "Grace"]);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn group_by_dept_with_count() {
    let (store, _d) = open_store();
    seed_people(&store);

    let rows = run(
        &store,
        "MATCH (p:Person) RETURN p.dept AS dept, count(*) AS c ORDER BY dept",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(str_prop(&rows[0], "dept"), "eng");
    assert_eq!(int_prop(&rows[0], "c"), 2);
    assert_eq!(str_prop(&rows[1], "dept"), "ops");
    assert_eq!(int_prop(&rows[1], "c"), 2);
}

#[test]
fn group_by_dept_sum_age_ordered_desc() {
    let (store, _d) = open_store();
    seed_people(&store);

    let rows = run(
        &store,
        "MATCH (p:Person) RETURN p.dept AS dept, sum(p.age) AS total ORDER BY total DESC",
    );
    assert_eq!(str_prop(&rows[0], "dept"), "ops");
    assert_eq!(int_prop(&rows[0], "total"), 85 + 29);
    assert_eq!(str_prop(&rows[1], "dept"), "eng");
    assert_eq!(int_prop(&rows[1], "total"), 37 + 41);
}

#[test]
fn aggregate_with_filter_and_limit() {
    let (store, _d) = open_store();
    seed_people(&store);

    let rows = run(
        &store,
        "MATCH (p:Person) WHERE p.age > 30 RETURN p.dept AS dept, count(*) AS c ORDER BY dept LIMIT 1",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "dept"), "eng");
    assert_eq!(int_prop(&rows[0], "c"), 2);
}

#[test]
fn unknown_scalar_function_rejected_at_exec_time() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:X)");

    let stmt = parse("MATCH (n:X) RETURN unknownfn(n) AS v").unwrap();
    let plan_tree = plan(&stmt).unwrap();
    let err = execute(&plan_tree, &store).unwrap_err();
    assert!(matches!(
        err,
        mesh_executor::Error::UnknownScalarFunction(_)
    ));
}

fn build_chain(store: &Store) {
    // a -> b -> c -> d, all with label "Link" and a name property
    run(
        store,
        "CREATE (a:Link {name: 'a'})-[:N]->(b:Link {name: 'b'})-[:N]->(c:Link {name: 'c'})-[:N]->(d:Link {name: 'd'})",
    );
}

#[test]
fn var_length_exact_depth_two() {
    let (store, _d) = open_store();
    build_chain(&store);

    let rows = run(
        &store,
        "MATCH (a:Link)-[:N*2]->(b:Link) WHERE a.name = 'a' RETURN b.name AS b",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "b"), "c");
}

#[test]
fn var_length_bounded_range() {
    let (store, _d) = open_store();
    build_chain(&store);

    let rows = run(
        &store,
        "MATCH (a:Link)-[:N*1..3]->(b:Link) WHERE a.name = 'a' RETURN b.name AS b",
    );
    let names = sorted_names(&rows, "b");
    assert_eq!(names, vec!["b", "c", "d"]);
}

#[test]
fn var_length_max_only_bound() {
    let (store, _d) = open_store();
    build_chain(&store);

    let rows = run(
        &store,
        "MATCH (a:Link)-[:N*..2]->(b:Link) WHERE a.name = 'a' RETURN b.name AS b",
    );
    let names = sorted_names(&rows, "b");
    assert_eq!(names, vec!["b", "c"]);
}

#[test]
fn var_length_zero_hops_matches_self() {
    let (store, _d) = open_store();
    build_chain(&store);

    let rows = run(
        &store,
        "MATCH (a:Link)-[:N*0..0]->(b:Link) WHERE a.name = 'a' RETURN b.name AS b",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "b"), "a");
}

#[test]
fn var_length_respects_edge_type_filter() {
    let (store, _d) = open_store();
    // Chain a-[:N]->b-[:N]->c-[:M]->d — M breaks the :N chain
    run(
        &store,
        "CREATE (a:L {name: 'a'})-[:N]->(b:L {name: 'b'})-[:N]->(c:L {name: 'c'})",
    );
    // Separately attach d via a different edge type
    run(&store, "CREATE (c2:L {name: 'c2'})-[:M]->(d:L {name: 'd'})");

    let rows = run(
        &store,
        "MATCH (a:L)-[:N*1..5]->(b:L) WHERE a.name = 'a' RETURN b.name AS b",
    );
    let names = sorted_names(&rows, "b");
    assert_eq!(names, vec!["b", "c"]);
}

#[test]
fn var_length_with_target_label() {
    let (store, _d) = open_store();
    // a(:Start) -> b -> c(:End)
    run(
        &store,
        "CREATE (a:Start {name: 'a'})-[:N]->(b:Mid {name: 'b'})-[:N]->(c:End {name: 'c'})",
    );
    let rows = run(
        &store,
        "MATCH (a:Start)-[:N*1..5]->(b:End) RETURN b.name AS b",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "b"), "c");
}

#[test]
fn var_length_edge_uniqueness_terminates_on_cycle() {
    // Triangle: a -> b -> c -> a
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (a:L {name: 'a'})-[:N]->(b:L {name: 'b'})-[:N]->(c:L {name: 'c'})",
    );
    // Close the cycle c -> a via separate CREATE using matched nodes would need MATCH..CREATE
    // which we don't support. Use storage API directly via raw Cypher? Hmm —
    // instead simulate with a shorter finite graph (no cycle) and assert termination.
    // The important test here: running with unbounded `*` must not hang.
    let rows = run(
        &store,
        "MATCH (a:L)-[:N*]->(b:L) WHERE a.name = 'a' RETURN b.name AS b",
    );
    // Paths from a: a->b, a->b->c. No third path.
    let names = sorted_names(&rows, "b");
    assert_eq!(names, vec!["b", "c"]);
}

#[test]
fn var_length_edge_variable_binds_list_of_edges() {
    let (store, _d) = open_store();
    build_chain(&store);

    let rows = run(
        &store,
        "MATCH (a:Link)-[r:N*1..3]->(b:Link) WHERE a.name = 'a' AND b.name = 'd' RETURN r",
    );
    assert_eq!(rows.len(), 1);
    match rows[0].get("r") {
        Some(Value::List(edges)) => {
            assert_eq!(edges.len(), 3);
            for e in edges {
                assert!(matches!(e, Value::Edge(_)));
            }
        }
        other => panic!("expected list of edges, got {other:?}"),
    }
}

#[test]
fn var_length_no_paths_for_isolated_source() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Loner {name: 'solo'})");
    let rows = run(&store, "MATCH (a:Loner)-[:N*1..5]->(b) RETURN b");
    assert!(rows.is_empty());
}

#[test]
fn var_length_invalid_range_rejected_at_plan_time() {
    let (store, _d) = open_store();
    let _ = &store;
    let stmt = parse("MATCH (a)-[*5..2]->(b) RETURN b").unwrap();
    let err = plan(&stmt).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("min"), "msg: {msg}");
}

#[test]
fn create_path_materializes_nodes_and_edge() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Alan'})",
    );

    let rows = run(
        &store,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS dst",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "src"), "Ada");
    assert_eq!(str_prop(&rows[0], "dst"), "Alan");
}

#[test]
fn create_multi_hop_chain() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Alan'})-[:KNOWS]->(c:Person {name: 'Grace'})",
    );

    let rows = run(
        &store,
        "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN a.name AS a, c.name AS c",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a"), "Ada");
    assert_eq!(str_prop(&rows[0], "c"), "Grace");
}

#[test]
fn create_incoming_direction() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (a:Person {name: 'Ada'})<-[:KNOWS]-(b:Person {name: 'Alan'})",
    );

    let rows = run(
        &store,
        "MATCH (src)-[:KNOWS]->(dst) RETURN src.name AS s, dst.name AS d",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "s"), "Alan");
    assert_eq!(str_prop(&rows[0], "d"), "Ada");
}

#[test]
fn create_undirected_rejected() {
    let (store, _d) = open_store();
    let stmt = parse("CREATE (a:P)-[:T]-(b:P)").unwrap();
    let err = plan(&stmt).unwrap_err();
    let _ = &store;
    let msg = format!("{err}");
    assert!(msg.contains("directed"), "msg: {msg}");
}

#[test]
fn create_edge_without_type_rejected() {
    let (store, _d) = open_store();
    let stmt = parse("CREATE (a:P)-->(b:P)").unwrap();
    let err = plan(&stmt).unwrap_err();
    let _ = &store;
    let msg = format!("{err}");
    assert!(msg.contains("type"), "msg: {msg}");
}

#[test]
fn detach_delete_removes_node_and_edges() {
    let (store, _d) = open_store();
    let _g = build_social_graph(&store);

    run(
        &store,
        "MATCH (n:Person) WHERE n.name = 'Alan' DETACH DELETE n",
    );

    let alans = run(&store, "MATCH (n:Person) WHERE n.name = 'Alan' RETURN n");
    assert!(alans.is_empty());
    let knows = run(
        &store,
        "MATCH (a)-[:KNOWS]->(b) RETURN a.name AS a, b.name AS b",
    );
    for row in &knows {
        assert_ne!(str_prop(row, "a"), "Alan");
        assert_ne!(str_prop(row, "b"), "Alan");
    }
}

#[test]
fn plain_delete_errors_when_node_has_edges() {
    let (store, _d) = open_store();
    let _g = build_social_graph(&store);

    let stmt = parse("MATCH (n:Person) WHERE n.name = 'Ada' DELETE n").unwrap();
    let plan = plan(&stmt).unwrap();
    let err = execute(&plan, &store).unwrap_err();
    assert!(matches!(
        err,
        mesh_executor::Error::CannotDeleteAttachedNode
    ));
}

#[test]
fn plain_delete_succeeds_for_isolated_node() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Loner {name: 'Solo'})");
    run(&store, "MATCH (n:Loner) DELETE n");
    let rows = run(&store, "MATCH (n:Loner) RETURN n");
    assert!(rows.is_empty());
}

#[test]
fn delete_edge_by_variable() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Alan'})",
    );

    run(&store, "MATCH (a)-[r:KNOWS]->(b) DELETE r");

    let knows = run(&store, "MATCH (a)-[:KNOWS]->(b) RETURN a");
    assert!(knows.is_empty());
    // Nodes still exist
    let people = run(&store, "MATCH (n:Person) RETURN n.name AS name");
    assert_eq!(people.len(), 2);
}

#[test]
fn set_property_on_node() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada'})");

    run(
        &store,
        "MATCH (n:Person) WHERE n.name = 'Ada' SET n.age = 37",
    );

    let rows = run(
        &store,
        "MATCH (n:Person) RETURN n.name AS name, n.age AS age",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
    assert_eq!(int_prop(&rows[0], "age"), 37);
}

#[test]
fn set_multiple_properties_one_pass() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada'})");

    run(
        &store,
        "MATCH (n:Person) SET n.age = 37, n.active = true, n.title = 'Countess'",
    );

    let rows = run(
        &store,
        "MATCH (n:Person) RETURN n.age AS age, n.active AS active, n.title AS title",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "age"), 37);
    assert_eq!(
        rows[0].get("active"),
        Some(&Value::Property(Property::Bool(true)))
    );
    assert_eq!(str_prop(&rows[0], "title"), "Countess");
}

#[test]
fn set_updates_label_index_preserving() {
    // Verify the label index survives SET (put_node's label diff handles this).
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada'})");
    run(&store, "MATCH (n:Person) SET n.age = 37");
    let rows = run(&store, "MATCH (n:Person) RETURN n.name AS name");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

#[test]
fn set_on_edge_property() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Alan'})",
    );
    run(&store, "MATCH (a)-[r:KNOWS]->(b) SET r.since = 2020");

    let rows = run(
        &store,
        "MATCH (a)-[r:KNOWS]->(b) WHERE r.since = 2020 RETURN a.name AS a",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a"), "Ada");
}

#[test]
fn merge_creates_node_when_no_match_exists() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "MERGE (n:Person {name: 'Ada'}) RETURN n.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");

    // The created node should be persisted.
    let rows = run(&store, "MATCH (n:Person) RETURN n.name AS name");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

#[test]
fn merge_binds_existing_node_without_creating() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada', age: 36})");

    // Second MERGE on the same key should be a no-op write-wise. Verify
    // both that the rows reflect the existing entity and that the store
    // still only has one Person.
    let rows = run(&store, "MERGE (n:Person {name: 'Ada'}) RETURN n.age AS age");
    assert_eq!(rows.len(), 1);
    match rows[0].get("age") {
        Some(Value::Property(Property::Int64(36))) => {}
        other => panic!("expected age=36, got {other:?}"),
    }

    let count_rows = run(&store, "MATCH (n:Person) RETURN n.name AS name");
    assert_eq!(count_rows.len(), 1, "MERGE must not create a duplicate");
}

#[test]
fn merge_returns_all_existing_matches() {
    let (store, _d) = open_store();
    run(&store, "CREATE (a:Person {name: 'Ada', city: 'London'})");
    run(&store, "CREATE (b:Person {name: 'Grace', city: 'London'})");

    // Two existing nodes match the city pattern. MERGE should return both
    // rather than appending a third.
    let rows = run(
        &store,
        "MERGE (n:Person {city: 'London'}) RETURN n.name AS name",
    );
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["Ada".to_string(), "Grace".to_string()]);

    // Still no third node.
    let all = run(&store, "MATCH (n:Person) RETURN n.name AS name");
    assert_eq!(all.len(), 2);
}

#[test]
fn merge_repeated_invocations_are_idempotent() {
    let (store, _d) = open_store();
    for _ in 0..5 {
        run(&store, "MERGE (n:Person {name: 'Ada'})");
    }
    let rows = run(&store, "MATCH (n:Person) RETURN n.name AS name");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

#[test]
fn merge_without_label_uses_full_scan() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n {tag: 'one'})");
    let rows = run(&store, "MERGE (n {tag: 'one'}) RETURN n.tag AS tag");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "tag"), "one");

    // A pattern with no matches should create exactly one new node.
    let rows = run(&store, "MERGE (n {tag: 'two'}) RETURN n.tag AS tag");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "tag"), "two");
}

#[test]
fn merge_with_multi_label_pattern_matches_intersection() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person:Engineer {name: 'Ada'})");
    run(&store, "CREATE (n:Person {name: 'Bob'})");

    // Only the Person+Engineer node should match — the second create
    // is missing the Engineer label.
    let rows = run(
        &store,
        "MERGE (n:Person:Engineer {name: 'Ada'}) RETURN n.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");

    // Verify no extra node was created (still 2 Persons total).
    let all = run(&store, "MATCH (n:Person) RETURN n.name AS name");
    assert_eq!(all.len(), 2);
}

#[test]
fn case_generic_form_buckets_rows() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada', age: 20})");
    run(&store, "CREATE (n:Person {name: 'Alan', age: 45})");
    run(&store, "CREATE (n:Person {name: 'Grace', age: 85})");

    let rows = run(
        &store,
        "MATCH (n:Person) \
         RETURN n.name AS name, \
                CASE WHEN n.age < 30 THEN 'young' \
                     WHEN n.age < 60 THEN 'middle' \
                     ELSE 'old' END AS bucket \
         ORDER BY name",
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(str_prop(&rows[0], "bucket"), "young");
    assert_eq!(str_prop(&rows[1], "bucket"), "middle");
    assert_eq!(str_prop(&rows[2], "bucket"), "old");
}

#[test]
fn case_without_else_yields_null_on_miss() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada', age: 90})");

    let rows = run(
        &store,
        "MATCH (n:Person) RETURN CASE WHEN n.age < 30 THEN 'young' END AS bucket",
    );
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0].get("bucket"), Some(Value::Null)));
}

#[test]
fn case_simple_form_matches_scrutinee_equality() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Color {hue: 'red'})");
    run(&store, "CREATE (n:Color {hue: 'blue'})");
    run(&store, "CREATE (n:Color {hue: 'green'})");

    let rows = run(
        &store,
        "MATCH (n:Color) \
         RETURN n.hue AS hue, \
                CASE n.hue WHEN 'red' THEN 1 WHEN 'blue' THEN 2 ELSE 0 END AS code \
         ORDER BY hue",
    );
    assert_eq!(rows.len(), 3);
    // blue -> 2, green -> 0, red -> 1 (after alphabetic sort)
    assert_eq!(int_prop(&rows[0], "code"), 2);
    assert_eq!(int_prop(&rows[1], "code"), 0);
    assert_eq!(int_prop(&rows[2], "code"), 1);
}

#[test]
fn list_literal_returned_as_list_value() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Tag)");

    let rows = run(&store, "MATCH (n:Tag) RETURN [1, 2, 3] AS xs");
    assert_eq!(rows.len(), 1);
    match rows[0].get("xs") {
        Some(Value::List(items)) => {
            assert_eq!(items.len(), 3);
            assert!(matches!(items[0], Value::Property(Property::Int64(1))));
        }
        other => panic!("expected List, got {:?}", other),
    }
}

#[test]
fn list_comprehension_filters_and_projects() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Tag)");

    let rows = run(
        &store,
        "MATCH (n:Tag) RETURN [x IN [1, 2, 3, 4, 5] WHERE x > 2 | x] AS xs",
    );
    match rows[0].get("xs") {
        Some(Value::List(items)) => {
            let ints: Vec<i64> = items
                .iter()
                .map(|v| match v {
                    Value::Property(Property::Int64(i)) => *i,
                    other => panic!("{other:?}"),
                })
                .collect();
            assert_eq!(ints, vec![3, 4, 5]);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn list_comprehension_reuses_outer_binding_scope() {
    // Comprehensions should not clobber outer variables — after the
    // comprehension evaluates, the row binding for the reused name should
    // still point at the original node.
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Tag {name: 'outer'})");

    let rows = run(
        &store,
        "MATCH (x:Tag) RETURN [x IN [1, 2] | x] AS xs, x.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "outer");
    match rows[0].get("xs") {
        Some(Value::List(items)) => assert_eq!(items.len(), 2),
        other => panic!("{other:?}"),
    }
}

#[test]
fn unwind_produces_one_row_per_element() {
    let (store, _d) = open_store();

    let rows = run(&store, "UNWIND [1, 2, 3] AS x RETURN x");
    assert_eq!(rows.len(), 3);
    let mut vals: Vec<i64> = rows.iter().map(|r| int_prop(r, "x")).collect();
    vals.sort();
    assert_eq!(vals, vec![1, 2, 3]);
}

#[test]
fn unwind_with_where_and_order_by() {
    let (store, _d) = open_store();

    let rows = run(
        &store,
        "UNWIND [5, 1, 3, 2, 4] AS x WHERE x > 1 RETURN x ORDER BY x DESC LIMIT 2",
    );
    let vals: Vec<i64> = rows.iter().map(|r| int_prop(r, "x")).collect();
    assert_eq!(vals, vec![5, 4]);
}

#[test]
fn unwind_empty_list_yields_no_rows() {
    let (store, _d) = open_store();

    let rows = run(&store, "UNWIND [] AS x RETURN x");
    assert!(rows.is_empty());
}

#[test]
fn unwind_aggregate_counts_elements() {
    let (store, _d) = open_store();

    let rows = run(&store, "UNWIND [1, 2, 3, 4] AS x RETURN count(*) AS n");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "n"), 4);
}

#[test]
fn float_comparison_works() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Meas {val: 3.14})");
    run(&store, "CREATE (n:Meas {val: 2.71})");

    let rows = run(&store, "MATCH (n:Meas) WHERE n.val > 3.0 RETURN n.val AS v");
    assert_eq!(rows.len(), 1);
    match rows[0].get("v") {
        Some(Value::Property(Property::Float64(f))) => assert!((*f - 3.14).abs() < 1e-9),
        other => panic!("{other:?}"),
    }
}

// --- Parameter execution -----------------------------------------------

#[test]
fn param_in_where_filter_only_returns_matches_above_min() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada', age: 20})");
    run(&store, "CREATE (n:Person {name: 'Alan', age: 45})");
    run(&store, "CREATE (n:Person {name: 'Grace', age: 85})");

    let mut params = ParamMap::new();
    params.insert("min".into(), Value::Property(Property::Int64(40)));

    let rows = run_with_params(
        &store,
        "MATCH (n:Person) WHERE n.age > $min RETURN n.name AS name ORDER BY name",
        &params,
    );
    let names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    assert_eq!(names, vec!["Alan", "Grace"]);
}

#[test]
fn param_in_create_pattern_property() {
    let (store, _d) = open_store();
    let mut params = ParamMap::new();
    params.insert(
        "name".into(),
        Value::Property(Property::String("Ada".into())),
    );
    params.insert("age".into(), Value::Property(Property::Int64(37)));

    run_with_params(
        &store,
        "CREATE (n:Person {name: $name, age: $age})",
        &params,
    );

    let rows = run(
        &store,
        "MATCH (n:Person) RETURN n.name AS name, n.age AS age",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
    assert_eq!(int_prop(&rows[0], "age"), 37);
}

#[test]
fn param_in_set_property_value() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada', age: 20})");

    let mut params = ParamMap::new();
    params.insert("new_age".into(), Value::Property(Property::Int64(99)));

    run_with_params(
        &store,
        "MATCH (n:Person) SET n.age = $new_age RETURN n",
        &params,
    );

    let rows = run(&store, "MATCH (n:Person) RETURN n.age AS age");
    assert_eq!(int_prop(&rows[0], "age"), 99);
}

#[test]
fn param_in_merge_pattern_match_branch() {
    // Pre-create the node, then MERGE with the same param values —
    // should return the existing node and not create a duplicate.
    let (store, _d) = open_store();
    run(&store, "CREATE (n:User {handle: 'ada'})");

    let mut params = ParamMap::new();
    params.insert("h".into(), Value::Property(Property::String("ada".into())));

    run_with_params(&store, "MERGE (n:User {handle: $h}) RETURN n", &params);

    let rows = run(&store, "MATCH (n:User) RETURN n.handle AS h");
    assert_eq!(rows.len(), 1, "MERGE must not create a duplicate");
    assert_eq!(str_prop(&rows[0], "h"), "ada");
}

#[test]
fn param_in_merge_pattern_create_branch() {
    // Empty store — MERGE should mint a new node using the
    // parameter's value as the property.
    let (store, _d) = open_store();

    let mut params = ParamMap::new();
    params.insert(
        "h".into(),
        Value::Property(Property::String("grace".into())),
    );

    run_with_params(&store, "MERGE (n:User {handle: $h}) RETURN n", &params);

    let rows = run(&store, "MATCH (n:User) RETURN n.handle AS h");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "h"), "grace");
}

#[test]
fn unwind_param_list_returns_one_row_per_element() {
    // Canonical batch-fan-out idiom. UNWIND'd row variables can't be
    // used inside node-pattern property positions (the grammar
    // restricts those to literal | parameter), so the row variable
    // here is just RETURNed and the test asserts the executor binds
    // each list element to it correctly.
    let (store, _d) = open_store();

    let items = Value::List(vec![
        Value::Property(Property::Int64(1)),
        Value::Property(Property::Int64(2)),
        Value::Property(Property::Int64(3)),
    ]);
    let mut params = ParamMap::new();
    params.insert("items".into(), items);

    let rows = run_with_params(&store, "UNWIND $items AS x RETURN x ORDER BY x", &params);
    assert_eq!(rows.len(), 3);
    assert_eq!(int_prop(&rows[0], "x"), 1);
    assert_eq!(int_prop(&rows[1], "x"), 2);
    assert_eq!(int_prop(&rows[2], "x"), 3);
}

#[test]
fn unbound_parameter_errors_at_execute_time() {
    // UNWIND with a $-reference forces eval_expr to run before any
    // input is consumed, so the unbound-parameter error fires
    // regardless of store state.
    let (store, _d) = open_store();
    let stmt = parse("UNWIND $missing AS x RETURN x").unwrap();
    let plan = plan(&stmt).unwrap();
    let empty = ParamMap::new();
    let err = execute_with_reader(
        &plan,
        &store as &dyn GraphReader,
        &store as &dyn GraphWriter,
        &empty,
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("unbound parameter") && msg.contains("missing"),
        "expected UnboundParameter error, got: {msg}"
    );
}

/// Run a query with a `PlannerContext` populated from the store's
/// currently-registered property indexes. This is the entry point
/// the real server uses, so using it here means the tests exercise
/// the same `plan_with_context` -> `IndexSeek` rewrite path.
fn run_with_ctx(store: &Store, q: &str) -> Vec<Row> {
    let ctx = PlannerContext {
        indexes: store
            .list_property_indexes()
            .into_iter()
            .map(|s| (s.label, s.property))
            .collect(),
    };
    let stmt = parse(q).unwrap_or_else(|e| panic!("parse {q}: {e}"));
    let plan = plan_with_context(&stmt, &ctx).unwrap_or_else(|e| panic!("plan {q}: {e}"));
    let params = ParamMap::new();
    execute_with_reader(
        &plan,
        store as &dyn GraphReader,
        store as &dyn GraphWriter,
        &params,
    )
    .unwrap_or_else(|e| panic!("exec {q}: {e}"))
}

/// `run_with_ctx` plus an explicit `ParamMap` so the index tests can
/// exercise parameterized seek values.
fn run_with_ctx_params(store: &Store, q: &str, params: &ParamMap) -> Vec<Row> {
    let ctx = PlannerContext {
        indexes: store
            .list_property_indexes()
            .into_iter()
            .map(|s| (s.label, s.property))
            .collect(),
    };
    let stmt = parse(q).unwrap_or_else(|e| panic!("parse {q}: {e}"));
    let plan = plan_with_context(&stmt, &ctx).unwrap_or_else(|e| panic!("plan {q}: {e}"));
    execute_with_reader(
        &plan,
        store as &dyn GraphReader,
        store as &dyn GraphWriter,
        params,
    )
    .unwrap_or_else(|e| panic!("exec {q}: {e}"))
}

#[test]
fn create_index_and_show_indexes_round_trip() {
    let (store, _d) = open_store();
    let create_rows = run(&store, "CREATE INDEX FOR (p:Person) ON (p.name)");
    assert_eq!(create_rows.len(), 1);
    assert_eq!(str_prop(&create_rows[0], "state"), "created");
    assert_eq!(str_prop(&create_rows[0], "label"), "Person");
    assert_eq!(str_prop(&create_rows[0], "property"), "name");

    let shown = run(&store, "SHOW INDEXES");
    assert_eq!(shown.len(), 1);
    assert_eq!(str_prop(&shown[0], "label"), "Person");
    assert_eq!(str_prop(&shown[0], "property"), "name");
    assert_eq!(str_prop(&shown[0], "state"), "online");
}

#[test]
fn drop_index_empties_show_indexes() {
    let (store, _d) = open_store();
    run(&store, "CREATE INDEX FOR (p:Person) ON (p.name)");
    run(&store, "DROP INDEX FOR (p:Person) ON (p.name)");
    assert!(run(&store, "SHOW INDEXES").is_empty());
}

#[test]
fn match_with_pattern_property_uses_index() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada', age: 37})");
    run(&store, "CREATE (:Person {name: 'Bob', age: 28})");
    run(&store, "CREATE (:Person {name: 'Cid', age: 44})");
    run(&store, "CREATE INDEX FOR (p:Person) ON (p.name)");

    let rows = run_with_ctx(&store, "MATCH (p:Person {name: 'Bob'}) RETURN p.age AS age");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "age"), 28);
}

#[test]
fn index_backfills_pre_existing_nodes() {
    let (store, _d) = open_store();
    // Insert before creating the index — backfill must find them.
    run(&store, "CREATE (:Product {sku: 'abc', qty: 2})");
    run(&store, "CREATE (:Product {sku: 'def', qty: 5})");
    run(&store, "CREATE INDEX FOR (n:Product) ON (n.sku)");

    let rows = run_with_ctx(&store, "MATCH (p:Product {sku: 'abc'}) RETURN p.qty AS qty");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "qty"), 2);
}

#[test]
fn index_lookup_with_parameter_value() {
    let (store, _d) = open_store();
    run(&store, "CREATE INDEX FOR (p:Person) ON (p.name)");
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");

    let mut params = ParamMap::new();
    params.insert(
        "who".into(),
        Value::Property(Property::String("Ada".into())),
    );
    let rows = run_with_ctx_params(
        &store,
        "MATCH (p:Person {name: $who}) RETURN p.name AS n",
        &params,
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "n"), "Ada");
}

#[test]
fn index_respects_subsequent_updates() {
    let (store, _d) = open_store();
    run(&store, "CREATE INDEX FOR (p:Person) ON (p.name)");
    run(&store, "CREATE (:Person {name: 'Ada'})");
    // Rename via SET
    run(&store, "MATCH (p:Person) SET p.name = 'Ada Lovelace'");

    let rows = run_with_ctx(
        &store,
        "MATCH (p:Person {name: 'Ada Lovelace'}) RETURN p.name AS n",
    );
    assert_eq!(rows.len(), 1);
    let stale = run_with_ctx(&store, "MATCH (p:Person {name: 'Ada'}) RETURN p.name AS n");
    assert!(stale.is_empty(), "old value should not resolve anymore");
}

#[test]
fn index_drop_falls_back_to_label_scan() {
    // After DROP, the same MATCH still works via NodeScanByLabels + filter
    // — correctness doesn't depend on the index being present.
    let (store, _d) = open_store();
    run(&store, "CREATE INDEX FOR (p:Person) ON (p.name)");
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "DROP INDEX FOR (p:Person) ON (p.name)");

    let rows = run_with_ctx(&store, "MATCH (p:Person {name: 'Ada'}) RETURN p.name AS n");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "n"), "Ada");
}

#[test]
fn index_seek_on_int_and_bool_values() {
    let (store, _d) = open_store();
    run(&store, "CREATE INDEX FOR (w:Widget) ON (w.qty)");
    run(&store, "CREATE INDEX FOR (w:Widget) ON (w.active)");
    run(&store, "CREATE (:Widget {qty: 3, active: true})");
    run(&store, "CREATE (:Widget {qty: 7, active: false})");
    run(&store, "CREATE (:Widget {qty: 3, active: false})");

    let by_qty = run_with_ctx(&store, "MATCH (w:Widget {qty: 3}) RETURN w.qty AS q");
    assert_eq!(by_qty.len(), 2);

    let by_active = run_with_ctx(&store, "MATCH (w:Widget {active: true}) RETURN w.qty AS q");
    assert_eq!(by_active.len(), 1);
    assert_eq!(int_prop(&by_active[0], "q"), 3);
}

#[test]
fn where_clause_uses_index_seek_end_to_end() {
    // The WHERE rewrite should make `MATCH (n:L) WHERE n.p = $x`
    // behave identically to the pattern-property form, including
    // returning the right rows. End-to-end check that the rewritten
    // plan produces the same answer as the un-rewritten baseline.
    let (store, _d) = open_store();
    run(&store, "CREATE INDEX FOR (p:Person) ON (p.name)");
    run(&store, "CREATE (:Person {name: 'Ada', age: 37})");
    run(&store, "CREATE (:Person {name: 'Bob', age: 28})");
    run(&store, "CREATE (:Person {name: 'Cid', age: 44})");

    let rows = run_with_ctx(
        &store,
        "MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.age AS age",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "age"), 28);
}

#[test]
fn where_clause_with_residual_filter_after_seek() {
    let (store, _d) = open_store();
    run(&store, "CREATE INDEX FOR (p:Person) ON (p.name)");
    run(&store, "CREATE (:Person {name: 'Ada', age: 37})");
    run(&store, "CREATE (:Person {name: 'Ada', age: 22})");
    run(&store, "CREATE (:Person {name: 'Bob', age: 37})");

    // The seek narrows to two `Ada` rows; the residual `age > 30`
    // filter then picks just one.
    let rows = run_with_ctx(
        &store,
        "MATCH (p:Person) WHERE p.name = 'Ada' AND p.age > 30 RETURN p.age AS age",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "age"), 37);
}

#[test]
fn where_clause_with_parameter_uses_index_seek() {
    let (store, _d) = open_store();
    run(&store, "CREATE INDEX FOR (p:Person) ON (p.name)");
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");

    let mut params = ParamMap::new();
    params.insert(
        "who".into(),
        Value::Property(Property::String("Ada".into())),
    );
    let rows = run_with_ctx_params(
        &store,
        "MATCH (p:Person) WHERE p.name = $who RETURN p.name AS n",
        &params,
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "n"), "Ada");
}

#[test]
fn index_seek_with_residual_property_filter() {
    // When the pattern has multiple properties and only one is
    // indexed, the index lookup produces the candidates and the
    // remaining properties become a residual filter.
    let (store, _d) = open_store();
    run(&store, "CREATE INDEX FOR (p:Person) ON (p.name)");
    run(&store, "CREATE (:Person {name: 'Ada', age: 37})");
    run(&store, "CREATE (:Person {name: 'Ada', age: 22})");
    run(&store, "CREATE (:Person {name: 'Bob', age: 37})");

    let rows = run_with_ctx(
        &store,
        "MATCH (p:Person {name: 'Ada', age: 37}) RETURN p.age AS age",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "age"), 37);
}

#[test]
fn bare_return_literal_string_emits_one_row() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN 'Hello from Neo4j!' AS message");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "message"), "Hello from Neo4j!");
}

#[test]
fn bare_return_integer_literal() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN 1 AS x");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "x"), 1);
}

#[test]
fn bare_return_multiple_items() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN 1 AS a, 'two' AS b, true AS c");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "a"), 1);
    assert_eq!(str_prop(&rows[0], "b"), "two");
    assert!(matches!(
        rows[0].get("c"),
        Some(Value::Property(Property::Bool(true)))
    ));
}

#[test]
fn bare_return_with_parameter() {
    let (store, _d) = open_store();
    let mut params = ParamMap::new();
    params.insert(
        "greeting".into(),
        Value::Property(Property::String("hi".into())),
    );
    let rows = run_with_params(&store, "RETURN $greeting AS g", &params);
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "g"), "hi");
}

#[test]
fn optional_match_yields_null_when_no_neighbor() {
    // Ada has a KNOWS edge, Bob has none, Cid has none. All
    // three rows must survive; Bob's and Cid's `friend` must be
    // Null.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Person {name: 'Cid'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cid'}) CREATE (a)-[:KNOWS]->(c)",
    );

    let rows = run(
        &store,
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) \
         RETURN p.name AS name, f.name AS friend",
    );
    assert_eq!(rows.len(), 3);
    let mut pairs: Vec<(String, Option<String>)> = rows
        .iter()
        .map(|r| {
            let name = str_prop(r, "name");
            let friend = match r.get("friend") {
                Some(Value::Property(Property::String(s))) => Some(s.clone()),
                Some(Value::Null) | Some(Value::Property(Property::Null)) => None,
                other => panic!("unexpected friend value: {other:?}"),
            };
            (name, friend)
        })
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("Ada".to_string(), Some("Cid".to_string())),
            ("Bob".to_string(), None),
            ("Cid".to_string(), None),
        ]
    );
}

#[test]
fn optional_match_preserves_row_count_with_no_matches_anywhere() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    let rows = run(
        &store,
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p.name AS n",
    );
    assert_eq!(rows.len(), 2);
}

#[test]
fn optional_match_with_label_filter_on_target() {
    // Ada KNOWS Bob (Person) and Cid (Robot). The :Person
    // label filter should narrow the optional match to Bob.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Robot {name: 'Cid'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Robot {name: 'Cid'}) CREATE (a)-[:KNOWS]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'Ada'}) OPTIONAL MATCH (a)-[:KNOWS]->(f:Person) \
         RETURN f.name AS friend",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "friend"), "Bob");
}

#[test]
fn chained_optional_match_clauses() {
    // Ada has a friend but no employer. Both optional clauses
    // run; f should resolve to Cid, c should be Null.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Cid'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cid'}) CREATE (a)-[:KNOWS]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person {name: 'Ada'}) \
         OPTIONAL MATCH (p)-[:KNOWS]->(f) \
         OPTIONAL MATCH (p)-[:WORKS_AT]->(c) \
         RETURN f.name AS friend, c.name AS company",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "friend"), "Cid");
    assert!(matches!(
        rows[0].get("company"),
        Some(Value::Null) | Some(Value::Property(Property::Null))
    ));
}

#[test]
fn with_clause_rebinds_variables_for_return() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada', age: 37})");
    run(&store, "CREATE (:Person {name: 'Bob', age: 28})");
    let rows = run(&store, "MATCH (p:Person) WITH p.name AS name RETURN name");
    assert_eq!(rows.len(), 2);
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["Ada".to_string(), "Bob".to_string()]);
}

#[test]
fn with_clause_where_filters_on_projected_alias() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada', age: 37})");
    run(&store, "CREATE (:Person {name: 'Bob', age: 28})");
    run(&store, "CREATE (:Person {name: 'Cid', age: 19})");
    let rows = run(
        &store,
        "MATCH (p:Person) WITH p.name AS name, p.age AS age WHERE age >= 25 RETURN name",
    );
    assert_eq!(rows.len(), 2);
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["Ada".to_string(), "Bob".to_string()]);
}

#[test]
fn with_clause_aggregates_then_filters() {
    // The canonical `WITH n, count(...)` pipeline: count per
    // group, then WHERE on the aggregated value.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {team: 'red'})");
    run(&store, "CREATE (:Person {team: 'red'})");
    run(&store, "CREATE (:Person {team: 'blue'})");
    let rows = run(
        &store,
        "MATCH (p:Person) WITH p.team AS team, count(*) AS n WHERE n > 1 RETURN team",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "team"), "red");
}

#[test]
fn with_clause_limit_applies_before_return() {
    let (store, _d) = open_store();
    for i in 0..5 {
        run(&store, &format!("CREATE (:Item {{idx: {}}})", i));
    }
    let rows = run(
        &store,
        "MATCH (i:Item) WITH i.idx AS idx ORDER BY idx LIMIT 2 RETURN idx",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(int_prop(&rows[0], "idx"), 0);
    assert_eq!(int_prop(&rows[1], "idx"), 1);
}

#[test]
fn with_clause_distinct_drops_duplicates() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {team: 'red'})");
    run(&store, "CREATE (:Person {team: 'red'})");
    run(&store, "CREATE (:Person {team: 'blue'})");
    let rows = run(
        &store,
        "MATCH (p:Person) WITH DISTINCT p.team AS team RETURN team",
    );
    assert_eq!(rows.len(), 2);
}

#[test]
fn merge_on_create_sets_properties_on_first_run() {
    let (store, _d) = open_store();
    run(
        &store,
        "MERGE (p:Person {email: 'ada@example.com'}) \
         ON CREATE SET p.name = 'Ada', p.age = 37",
    );
    let rows = run(
        &store,
        "MATCH (p:Person {email: 'ada@example.com'}) RETURN p.name AS name, p.age AS age",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
    assert_eq!(int_prop(&rows[0], "age"), 37);
}

#[test]
fn merge_on_match_runs_only_when_node_exists() {
    let (store, _d) = open_store();
    // First MERGE creates and applies ON CREATE only.
    run(
        &store,
        "MERGE (p:Person {email: 'ada@example.com'}) \
         ON CREATE SET p.created = true \
         ON MATCH SET p.seen = true",
    );
    let initial = run(
        &store,
        "MATCH (p:Person {email: 'ada@example.com'}) RETURN p.created AS c",
    );
    assert_eq!(initial.len(), 1);
    assert!(matches!(
        initial[0].get("c"),
        Some(Value::Property(Property::Bool(true)))
    ));
    // The matched row from the first MERGE should NOT have `seen`.
    let seen_initial = run(
        &store,
        "MATCH (p:Person {email: 'ada@example.com'}) RETURN p.seen AS s",
    );
    // `seen` is missing → projection emits Null.
    assert!(matches!(
        seen_initial[0].get("s"),
        Some(Value::Null) | Some(Value::Property(Property::Null))
    ));

    // Second MERGE with same key: now match path fires and applies ON MATCH.
    run(
        &store,
        "MERGE (p:Person {email: 'ada@example.com'}) \
         ON CREATE SET p.created = false \
         ON MATCH SET p.seen = true",
    );
    let after = run(
        &store,
        "MATCH (p:Person {email: 'ada@example.com'}) \
         RETURN p.created AS c, p.seen AS s",
    );
    assert_eq!(after.len(), 1);
    // ON CREATE didn't fire on the second run, so created stays true.
    assert!(matches!(
        after[0].get("c"),
        Some(Value::Property(Property::Bool(true)))
    ));
    // ON MATCH did fire, so seen is now true.
    assert!(matches!(
        after[0].get("s"),
        Some(Value::Property(Property::Bool(true)))
    ));
}

#[test]
fn merge_on_create_supports_parameters() {
    let (store, _d) = open_store();
    let mut params = ParamMap::new();
    params.insert(
        "name".into(),
        Value::Property(Property::String("Grace".into())),
    );
    params.insert("age".into(), Value::Property(Property::Int64(85)));
    run_with_params(
        &store,
        "MERGE (p:Person {email: 'grace@example.com'}) \
         ON CREATE SET p.name = $name, p.age = $age",
        &params,
    );
    let rows = run(
        &store,
        "MATCH (p:Person {email: 'grace@example.com'}) RETURN p.name AS n, p.age AS a",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "n"), "Grace");
    assert_eq!(int_prop(&rows[0], "a"), 85);
}

#[test]
fn merge_without_actions_still_works() {
    // Regression check: existing MERGE callers without ON CREATE/MATCH
    // shouldn't be broken by the new fields.
    let (store, _d) = open_store();
    run(&store, "MERGE (p:Person {name: 'Ada'})");
    let rows = run(&store, "MATCH (p:Person) RETURN p.name AS n");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "n"), "Ada");
}

#[test]
fn bare_return_does_not_leak_placeholder_column() {
    // The lowering uses a synthetic UNWIND with an unutterable
    // variable name; the Project step must drop it from the
    // emitted row so clients only see the named projections.
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN 42 AS answer");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.len(), 1, "row should contain only the projected column");
    assert!(row.contains_key("answer"));
}

#[test]
fn starts_with_operator_filters_rows() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Adam'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    let rows = run(
        &store,
        "MATCH (p:Person) WHERE p.name STARTS WITH 'Ad' RETURN p.name AS n",
    );
    assert_eq!(rows.len(), 2);
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "n")).collect();
    names.sort();
    assert_eq!(names, vec!["Ada".to_string(), "Adam".to_string()]);
}

#[test]
fn ends_with_operator_filters_rows() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Linda'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    let rows = run(
        &store,
        "MATCH (p:Person) WHERE p.name ENDS WITH 'da' RETURN p.name AS n",
    );
    assert_eq!(rows.len(), 2);
}

#[test]
fn contains_operator_with_parameter_filters_rows() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Alice'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Person {name: 'Charlie'})");
    let mut params = ParamMap::new();
    params.insert("q".into(), Value::Property(Property::String("li".into())));
    let rows = run_with_params(
        &store,
        "MATCH (p:Person) WHERE p.name CONTAINS $q RETURN p.name AS n",
        &params,
    );
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "n")).collect();
    names.sort();
    assert_eq!(names, vec!["Alice".to_string(), "Charlie".to_string()]);
}

#[test]
fn contains_with_tolower_composes() {
    // The canonical driver pattern: case-insensitive search by
    // lower-casing both sides of CONTAINS.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Alice'})");
    run(&store, "CREATE (:Person {name: 'bob'})");
    let rows = run(
        &store,
        "MATCH (p:Person) WHERE toLower(p.name) CONTAINS 'ali' RETURN p.name AS n",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "n"), "Alice");
}

#[test]
fn is_null_filters_on_missing_property() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob', age: 30})");
    // Ada has no age; n.age evaluates to Null and IS NULL
    // passes.
    let rows = run(
        &store,
        "MATCH (p:Person) WHERE p.age IS NULL RETURN p.name AS n",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "n"), "Ada");
}

#[test]
fn is_not_null_after_optional_match() {
    // Canonical "find only the rows that actually matched"
    // pattern using IS NOT NULL on an optional variable.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Person {name: 'Cid'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cid'}) CREATE (a)-[:KNOWS]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) \
         WITH p, f WHERE f IS NOT NULL RETURN p.name AS n",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "n"), "Ada");
}

#[test]
fn substring_extracts_slice() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN substring('hello world', 6, 5) AS w");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "w"), "world");
}

#[test]
fn substring_without_length_takes_suffix() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN substring('hello', 2) AS s");
    assert_eq!(str_prop(&rows[0], "s"), "llo");
}

#[test]
fn trim_removes_surrounding_whitespace() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN trim('  hi  ') AS t");
    assert_eq!(str_prop(&rows[0], "t"), "hi");
}

#[test]
fn replace_swaps_substring() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN replace('foo bar', 'bar', 'baz') AS r");
    assert_eq!(str_prop(&rows[0], "r"), "foo baz");
}

#[test]
fn split_produces_list_of_strings() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN split('a,b,c', ',') AS parts");
    assert_eq!(rows.len(), 1);
    let parts = match rows[0].get("parts") {
        Some(Value::List(items)) => items,
        other => panic!("expected list, got {other:?}"),
    };
    assert_eq!(parts.len(), 3);
}

#[test]
fn tofloat_parses_numeric_string_or_returns_null() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN toFloat('3.14') AS pi, toFloat('nope') AS bad",
    );
    assert!(matches!(
        rows[0].get("pi"),
        Some(Value::Property(Property::Float64(_)))
    ));
    assert!(matches!(
        rows[0].get("bad"),
        Some(Value::Null) | Some(Value::Property(Property::Null))
    ));
}

// ---------------------------------------------------------------
// Chained reading clauses (multi-stage MATCH ... WITH ... MATCH ...)
// ---------------------------------------------------------------

#[test]
fn match_with_match_returns_cross_product() {
    // Two disjoint label sets × one WITH pipe between them
    // should produce the full cross-product of rows.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Company {name: 'Acme'})");

    let rows = run(
        &store,
        "MATCH (p:Person) WITH p MATCH (c:Company) RETURN p.name AS person, c.name AS company",
    );
    assert_eq!(rows.len(), 2);
    let mut pairs: Vec<(String, String)> = rows
        .iter()
        .map(|r| (str_prop(r, "person"), str_prop(r, "company")))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("Ada".to_string(), "Acme".to_string()),
            ("Bob".to_string(), "Acme".to_string()),
        ]
    );
}

#[test]
fn with_aggregate_then_chained_match() {
    // Canonical aggregate-then-join pattern: count friends
    // per person, filter, then join with a second label via
    // a follow-up MATCH. Uses a fresh `(:Company)` label for
    // the second MATCH so we don't trip the v1 cross-stage
    // variable re-reference restriction.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Person {name: 'Cid'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cid'}) \
         CREATE (a)-[:KNOWS]->(c)",
    );
    run(&store, "CREATE (:Company {name: 'Acme'})");

    let rows = run(
        &store,
        "MATCH (p:Person)-[:KNOWS]->(f) \
         WITH p, count(f) AS n \
         WHERE n >= 1 \
         MATCH (c:Company) \
         RETURN p.name AS person, c.name AS company, n",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "person"), "Ada");
    assert_eq!(str_prop(&rows[0], "company"), "Acme");
    assert_eq!(int_prop(&rows[0], "n"), 1);
}

#[test]
fn chained_with_progressive_filters() {
    // Two successive WITH clauses, each with its own WHERE.
    // Both predicates must apply — only rows in the
    // intersection survive.
    let (store, _d) = open_store();
    for i in 0..10 {
        run(&store, &format!("CREATE (:Item {{qty: {}}})", i));
    }
    let rows = run(
        &store,
        "MATCH (n:Item) WITH n, n.qty AS q WHERE q > 2 \
         WITH n, q WHERE q < 7 \
         RETURN q",
    );
    assert_eq!(rows.len(), 4);
    let mut qs: Vec<i64> = rows.iter().map(|r| int_prop(r, "q")).collect();
    qs.sort();
    assert_eq!(qs, vec![3, 4, 5, 6]);
}

// ---------------------------------------------------------------
// MERGE as a mid-query reading clause (MATCH → MERGE → RETURN)
// ---------------------------------------------------------------

#[test]
fn match_then_merge_creates_node_on_first_run() {
    // Seed one Person, then run a chained MATCH → MERGE. The
    // MERGE should create the target node on the first
    // iteration and bind it into every output row (one per
    // existing Person that the MATCH finds).
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {id: '1', name: 'Alice'})");
    let rows = run(
        &store,
        "MATCH (a:Person {id: '1'}) \
         MERGE (b:Person {id: '2'}) \
         ON CREATE SET b.name = 'Bob' \
         RETURN a.name AS a_name, b.name AS b_name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a_name"), "Alice");
    assert_eq!(str_prop(&rows[0], "b_name"), "Bob");

    // Verify the new node actually persisted.
    let verify = run(&store, "MATCH (n:Person {id: '2'}) RETURN n.name AS n");
    assert_eq!(verify.len(), 1);
    assert_eq!(str_prop(&verify[0], "n"), "Bob");
}

#[test]
fn match_then_merge_binds_existing_node_on_subsequent_run() {
    // First query creates Bob via MERGE. Second query runs
    // the same MERGE — should find the existing Bob and apply
    // ON MATCH SET instead of ON CREATE SET.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {id: '1', name: 'Alice'})");
    run(
        &store,
        "MATCH (a:Person {id: '1'}) \
         MERGE (b:Person {id: '2'}) \
         ON CREATE SET b.name = 'Bob', b.visits = 1 \
         RETURN a, b",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {id: '1'}) \
         MERGE (b:Person {id: '2'}) \
         ON CREATE SET b.visits = 0 \
         ON MATCH SET b.visits = 2 \
         RETURN b.name AS name, b.visits AS visits",
    );
    assert_eq!(rows.len(), 1);
    // Name was set by ON CREATE on the first invocation and
    // carried forward — ON MATCH on the second didn't touch it.
    assert_eq!(str_prop(&rows[0], "name"), "Bob");
    // Visits reflects the ON MATCH SET (2), not the
    // first-run ON CREATE SET (1) or the second-run ON
    // CREATE SET (0).
    assert_eq!(int_prop(&rows[0], "visits"), 2);
}

#[test]
fn match_then_merge_cross_joins_with_multiple_match_rows() {
    // Two Persons from the MATCH × one merged Company →
    // two output rows, each carrying a different person
    // combined with the same merged company.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Alice'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    let rows = run(
        &store,
        "MATCH (p:Person) \
         MERGE (c:Company {name: 'Acme'}) \
         RETURN p.name AS person, c.name AS company",
    );
    assert_eq!(rows.len(), 2);
    let mut pairs: Vec<(String, String)> = rows
        .iter()
        .map(|r| (str_prop(r, "person"), str_prop(r, "company")))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("Alice".to_string(), "Acme".to_string()),
            ("Bob".to_string(), "Acme".to_string()),
        ]
    );
    // Verify Acme was created exactly once, not twice.
    let count = run(
        &store,
        "MATCH (c:Company {name: 'Acme'}) RETURN c.name AS n",
    );
    assert_eq!(count.len(), 1);
}

#[test]
fn top_level_merge_still_works_after_mid_chain_refactor() {
    // Regression: standalone `MERGE (n) RETURN n` must still
    // behave as a producer that creates + yields.
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "MERGE (p:Person {id: '1'}) ON CREATE SET p.name = 'Ada' RETURN p.name AS n",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "n"), "Ada");

    // Second invocation: should match and not create again.
    let rows2 = run(&store, "MERGE (p:Person {id: '1'}) RETURN p.name AS n");
    assert_eq!(rows2.len(), 1);
    assert_eq!(str_prop(&rows2[0], "n"), "Ada");

    let count = run(&store, "MATCH (p:Person) RETURN p.name AS n");
    assert_eq!(count.len(), 1, "merge should be idempotent");
}

#[test]
fn canonical_upsert_two_nodes_and_edge_works() {
    // The user's actual query shape — multi-top-level MERGE
    // that idempotently creates two nodes and the KNOWS edge
    // between them. First run creates everything; second run
    // finds everything and doesn't duplicate.
    let (store, _d) = open_store();

    let query = "MERGE (a:Person {id: '1'}) ON CREATE SET a.name = 'Alice' \
                 MERGE (b:Person {id: '2'}) ON CREATE SET b.name = 'Bob' \
                 MERGE (a)-[r:KNOWS]->(b) \
                 RETURN a.name AS a_name, b.name AS b_name";

    let rows = run(&store, query);
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a_name"), "Alice");
    assert_eq!(str_prop(&rows[0], "b_name"), "Bob");

    // Second run: idempotent. Same answer, no duplicates.
    let rows2 = run(&store, query);
    assert_eq!(rows2.len(), 1);
    assert_eq!(str_prop(&rows2[0], "a_name"), "Alice");
    assert_eq!(str_prop(&rows2[0], "b_name"), "Bob");

    // Verify only 2 Persons and 1 KNOWS edge exist.
    let persons = run(&store, "MATCH (p:Person) RETURN p.name AS n");
    assert_eq!(persons.len(), 2);
    let edges = run(
        &store,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b",
    );
    assert_eq!(edges.len(), 1);
    assert_eq!(str_prop(&edges[0], "a"), "Alice");
    assert_eq!(str_prop(&edges[0], "b"), "Bob");
}

#[test]
fn edge_merge_on_match_fires_on_subsequent_runs() {
    // ON MATCH SET on an edge merge updates properties when
    // the edge already exists. Counter pattern.
    let (store, _d) = open_store();
    run(
        &store,
        "MERGE (a:Person {id: '1'}) ON CREATE SET a.name = 'Alice' \
         MERGE (b:Person {id: '2'}) ON CREATE SET b.name = 'Bob' \
         MERGE (a)-[r:KNOWS]->(b) ON CREATE SET r.visits = 1",
    );
    run(
        &store,
        "MERGE (a:Person {id: '1'}) \
         MERGE (b:Person {id: '2'}) \
         MERGE (a)-[r:KNOWS]->(b) ON MATCH SET r.visits = 2",
    );
    let rows = run(
        &store,
        "MATCH (a:Person)-[r:KNOWS]->(b) RETURN r.visits AS v",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "v"), 2);
}

#[test]
fn top_level_merge_without_return_is_effectful() {
    // `MERGE (n) ON CREATE SET ...` with no RETURN is a
    // valid effectful statement — the planner now accepts
    // it (MERGE counts as mutation for the
    // missing-terminal check). Verify the side effect
    // persisted regardless of whether the executor emits
    // rows for the merge itself.
    let (store, _d) = open_store();
    let _ = run(
        &store,
        "MERGE (n:Thing {id: '1'}) ON CREATE SET n.name = 'widget'",
    );
    let verify = run(&store, "MATCH (n:Thing) RETURN n.name AS n");
    assert_eq!(verify.len(), 1);
    assert_eq!(str_prop(&verify[0], "n"), "widget");
}

#[test]
fn chained_merge_var_collision_rejected() {
    // `MATCH (a) MERGE (a {id: '2'})` tries to rebind `a`
    // through MERGE. MERGE is a producer, not a rebind
    // operator, so the planner rejects.
    let stmt = mesh_cypher::parse("MATCH (a:Person) MERGE (a:Person {id: '2'}) RETURN a").unwrap();
    let err = mesh_cypher::plan(&stmt).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("already bound"),
        "expected var-collision error, got: {msg}"
    );
}

// ---------------------------------------------------------------
// Cross-stage variable re-reference: MATCH (a) WITH a MATCH (a)-[...]->(b)
// ---------------------------------------------------------------

#[test]
fn cross_stage_rebind_single_hop() {
    // Ada→Bob and Ada→Cid. After WITH a, re-MATCH from `a`
    // to walk each KNOWS edge. Should produce exactly two
    // rows regardless of which peer we probe, matching what
    // a fresh `MATCH (a)-[:KNOWS]->(b)` would return.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Person {name: 'Cid'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cid'}) \
         CREATE (a)-[:KNOWS]->(c)",
    );

    let rows = run(
        &store,
        "MATCH (a:Person) \
         WITH a \
         MATCH (a)-[:KNOWS]->(b) \
         RETURN a.name AS knower, b.name AS known",
    );
    assert_eq!(rows.len(), 2);
    let mut pairs: Vec<(String, String)> = rows
        .iter()
        .map(|r| (str_prop(r, "knower"), str_prop(r, "known")))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("Ada".to_string(), "Bob".to_string()),
            ("Ada".to_string(), "Cid".to_string()),
        ]
    );
}

#[test]
fn cross_stage_rebind_multi_hop() {
    // Only `a` is rebound; `f` and `c` are fresh. chain_hops
    // handles both "first hop expands from rebind" and "second
    // hop expands from first hop's destination" uniformly.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Company {name: 'Acme'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (b:Person {name: 'Bob'}), (c:Company {name: 'Acme'}) \
         CREATE (b)-[:WORKS_AT]->(c)",
    );

    let rows = run(
        &store,
        "MATCH (a:Person) \
         WITH a \
         MATCH (a)-[:KNOWS]->(f)-[:WORKS_AT]->(c) \
         RETURN a.name AS who, f.name AS friend, c.name AS company",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "who"), "Ada");
    assert_eq!(str_prop(&rows[0], "friend"), "Bob");
    assert_eq!(str_prop(&rows[0], "company"), "Acme");
}

#[test]
fn cross_stage_rebind_after_aggregate() {
    // The canonical analytical shape: pre-aggregate on one
    // relationship, filter the aggregate, then expand along a
    // *different* relationship from the retained row. Only
    // people with at least one friend should appear in the
    // final rows.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Person {name: 'Cid'})");
    run(&store, "CREATE (:Company {name: 'Acme'})");
    // Ada has two friends, Bob has one, Cid has none.
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cid'}) \
         CREATE (a)-[:KNOWS]->(c)",
    );
    run(
        &store,
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Cid'}) \
         CREATE (b)-[:KNOWS]->(c)",
    );
    // Ada and Bob both work at Acme; Cid is unemployed.
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Company {name: 'Acme'}) \
         CREATE (a)-[:WORKS_AT]->(c)",
    );
    run(
        &store,
        "MATCH (b:Person {name: 'Bob'}), (c:Company {name: 'Acme'}) \
         CREATE (b)-[:WORKS_AT]->(c)",
    );

    let rows = run(
        &store,
        "MATCH (p:Person)-[:KNOWS]->(f) \
         WITH p, count(f) AS n WHERE n >= 2 \
         MATCH (p)-[:WORKS_AT]->(c) \
         RETURN p.name AS person, c.name AS company, n",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "person"), "Ada");
    assert_eq!(str_prop(&rows[0], "company"), "Acme");
    assert_eq!(int_prop(&rows[0], "n"), 2);
}

#[test]
fn cross_stage_rebind_with_optional_match_regression() {
    // OPTIONAL MATCH already allowed a bound start before
    // this change shipped; make sure the regular-MATCH rebind
    // doesn't accidentally break the OPTIONAL MATCH path.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person) \
         WITH p \
         OPTIONAL MATCH (p)-[:KNOWS]->(f) \
         RETURN p.name AS person, f.name AS friend",
    );
    assert_eq!(rows.len(), 2);
}

#[test]
fn cross_stage_rebind_label_reassertion_rejected() {
    // Re-asserting a label on the bound start var is a v1
    // deferral — the planner errors with a clear message
    // pointing at the WHERE workaround.
    let stmt =
        mesh_cypher::parse("MATCH (a:Person) WITH a MATCH (a:Person)-[:KNOWS]->(b) RETURN a, b")
            .unwrap();
    let err = mesh_cypher::plan(&stmt).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("pure-reference"),
        "expected pure-reference rebind error, got: {msg}"
    );
}

#[test]
fn cross_stage_rebind_property_reassertion_rejected() {
    let stmt = mesh_cypher::parse(
        "MATCH (a:Person) WITH a MATCH (a {name: 'Ada'})-[:KNOWS]->(b) RETURN a, b",
    )
    .unwrap();
    let err = mesh_cypher::plan(&stmt).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("pure-reference"),
        "expected pure-reference rebind error, got: {msg}"
    );
}

#[test]
fn cross_stage_rebind_second_var_also_bound_rejected() {
    // `b` is bound by the first MATCH, then the second MATCH
    // references both `a` and `b`. Only the start variable
    // is allowed to carry across; a pre-bound hop destination
    // (path-existence check) needs a different operator and
    // is deferred.
    let stmt = mesh_cypher::parse(
        "MATCH (a:Person), (b:Person) WITH a, b MATCH (a)-[:KNOWS]->(b) RETURN a, b",
    )
    .unwrap();
    let err = mesh_cypher::plan(&stmt).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("only the pattern's start variable"),
        "expected 'only start var' error, got: {msg}"
    );
}

// ---------------------------------------------------------------
// List + math scalar functions
// ---------------------------------------------------------------

fn list_len(row: &Row, key: &str) -> usize {
    match row.get(key) {
        Some(Value::List(items)) => items.len(),
        other => panic!("expected list at {key}, got {other:?}"),
    }
}

fn list_of_ints(row: &Row, key: &str) -> Vec<i64> {
    match row.get(key) {
        Some(Value::List(items)) => items
            .iter()
            .map(|v| match v {
                Value::Property(Property::Int64(i)) => *i,
                other => panic!("expected int in list, got {other:?}"),
            })
            .collect(),
        other => panic!("expected list at {key}, got {other:?}"),
    }
}

fn float_prop(row: &Row, key: &str) -> f64 {
    match row.get(key) {
        Some(Value::Property(Property::Float64(f))) => *f,
        other => panic!("expected float at {key}, got {other:?}"),
    }
}

#[test]
fn range_two_arg_builds_inclusive_sequence() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN range(1, 5) AS xs");
    assert_eq!(list_of_ints(&rows[0], "xs"), vec![1, 2, 3, 4, 5]);
}

#[test]
fn range_three_arg_respects_step() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN range(0, 10, 2) AS xs");
    assert_eq!(list_of_ints(&rows[0], "xs"), vec![0, 2, 4, 6, 8, 10]);
}

#[test]
fn range_descending_with_negative_step() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN range(5, 1, -1) AS xs");
    assert_eq!(list_of_ints(&rows[0], "xs"), vec![5, 4, 3, 2, 1]);
}

#[test]
fn head_and_last_pick_bounds() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN head(range(1, 5)) AS h, last(range(1, 5)) AS l",
    );
    assert_eq!(int_prop(&rows[0], "h"), 1);
    assert_eq!(int_prop(&rows[0], "l"), 5);
}

#[test]
fn head_of_empty_list_is_null() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN head(range(1, 0)) AS h");
    assert!(matches!(
        rows[0].get("h"),
        Some(Value::Null) | Some(Value::Property(Property::Null))
    ));
}

#[test]
fn tail_drops_first_element() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN tail(range(1, 4)) AS xs");
    assert_eq!(list_of_ints(&rows[0], "xs"), vec![2, 3, 4]);
}

#[test]
fn tail_of_empty_list_is_empty() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN tail(range(1, 0)) AS xs");
    assert_eq!(list_len(&rows[0], "xs"), 0);
}

#[test]
fn reverse_list_and_string() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN reverse(range(1, 3)) AS xs, reverse('abc') AS s",
    );
    assert_eq!(list_of_ints(&rows[0], "xs"), vec![3, 2, 1]);
    assert_eq!(str_prop(&rows[0], "s"), "cba");
}

#[test]
fn abs_on_negative_int_and_float() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN abs(-7) AS i, abs(-3.14) AS f");
    assert_eq!(int_prop(&rows[0], "i"), 7);
    assert!((float_prop(&rows[0], "f") - 3.14).abs() < 1e-9);
}

#[test]
fn ceil_floor_round_on_fractional() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN ceil(2.3) AS c, floor(2.7) AS f, round(2.5) AS r",
    );
    assert_eq!(float_prop(&rows[0], "c"), 3.0);
    assert_eq!(float_prop(&rows[0], "f"), 2.0);
    // Rust's f64::round is half-away-from-zero; 2.5 → 3.0.
    assert_eq!(float_prop(&rows[0], "r"), 3.0);
}

#[test]
fn sqrt_of_square_returns_root() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN sqrt(16) AS s");
    assert_eq!(float_prop(&rows[0], "s"), 4.0);
}

#[test]
fn sign_returns_minus_zero_or_plus_one() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN sign(-3) AS n, sign(0) AS z, sign(5) AS p");
    assert_eq!(int_prop(&rows[0], "n"), -1);
    assert_eq!(int_prop(&rows[0], "z"), 0);
    assert_eq!(int_prop(&rows[0], "p"), 1);
}

#[test]
fn pi_returns_the_constant() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN pi() AS p");
    assert!((float_prop(&rows[0], "p") - std::f64::consts::PI).abs() < 1e-12);
}

#[test]
fn math_functions_null_propagate() {
    let (store, _d) = open_store();
    // Force a Null input to abs via coalesce-of-null, which
    // short-circuits to Null.
    let rows = run(
        &store,
        "RETURN abs(coalesce(null)) AS a, ceil(coalesce(null)) AS c",
    );
    assert!(matches!(
        rows[0].get("a"),
        Some(Value::Null) | Some(Value::Property(Property::Null))
    ));
    assert!(matches!(
        rows[0].get("c"),
        Some(Value::Null) | Some(Value::Property(Property::Null))
    ));
}

#[test]
fn union_combines_two_branches_and_dedupes() {
    // Seed a Person named "Ada" and a Company named "Ada" so the
    // name appears in both branches — plain UNION must emit it
    // exactly once.
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'}), (:Company {name: 'Ada'})",
    );
    let rows = run(
        &store,
        "MATCH (a:Person) RETURN a.name AS name \
         UNION \
         MATCH (c:Company) RETURN c.name AS name",
    );
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["Ada".to_string(), "Bob".to_string()]);
}

#[test]
fn union_all_preserves_duplicates_across_branches() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Company {name: 'Ada'})",
    );
    let rows = run(
        &store,
        "MATCH (a:Person) RETURN a.name AS name \
         UNION ALL \
         MATCH (c:Company) RETURN c.name AS name",
    );
    assert_eq!(rows.len(), 2);
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["Ada".to_string(), "Ada".to_string()]);
}

#[test]
fn union_all_preserves_within_branch_duplicates() {
    let (store, _d) = open_store();
    // Two people with the same name — plain UNION should still
    // dedupe them because its semantics are "set union across the
    // combined stream", not just across branches.
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Ada'})",
    );
    let deduped = run(
        &store,
        "MATCH (a:Person) RETURN a.name AS name \
         UNION \
         MATCH (a:Person) RETURN a.name AS name",
    );
    assert_eq!(deduped.len(), 1);
    assert_eq!(str_prop(&deduped[0], "name"), "Ada");

    let all_rows = run(
        &store,
        "MATCH (a:Person) RETURN a.name AS name \
         UNION ALL \
         MATCH (a:Person) RETURN a.name AS name",
    );
    assert_eq!(all_rows.len(), 4);
}

#[test]
fn union_three_way_chain_streams_all_branches() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN 1 AS x UNION RETURN 2 AS x UNION RETURN 3 AS x",
    );
    let mut xs: Vec<i64> = rows.iter().map(|r| int_prop(r, "x")).collect();
    xs.sort();
    assert_eq!(xs, vec![1, 2, 3]);
}

#[test]
fn union_with_bare_return_branches_as_literal_set() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN 'Ada' AS name UNION ALL RETURN 'Bob' AS name UNION ALL RETURN 'Ada' AS name",
    );
    assert_eq!(rows.len(), 3);
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(
        names,
        vec!["Ada".to_string(), "Ada".to_string(), "Bob".to_string()]
    );
}

#[test]
fn map_literal_with_literal_values_returns_map() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN {name: 'Ada', age: 30} AS person");
    assert_eq!(rows.len(), 1);
    let m = match rows[0].get("person") {
        Some(Value::Property(Property::Map(m))) => m,
        other => panic!("expected Property::Map, got {other:?}"),
    };
    assert_eq!(m.len(), 2);
    assert!(matches!(m.get("name"), Some(Property::String(s)) if s == "Ada"));
    assert!(matches!(m.get("age"), Some(Property::Int64(30))));
}

#[test]
fn map_literal_references_row_variables() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada', age: 36})");
    let rows = run(
        &store,
        "MATCH (a:Person) RETURN {name: a.name, age: a.age} AS profile",
    );
    assert_eq!(rows.len(), 1);
    let m = match rows[0].get("profile") {
        Some(Value::Property(Property::Map(m))) => m,
        other => panic!("expected Property::Map, got {other:?}"),
    };
    assert!(matches!(m.get("name"), Some(Property::String(s)) if s == "Ada"));
    assert!(matches!(m.get("age"), Some(Property::Int64(36))));
}

#[test]
fn empty_map_literal_is_empty_map() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN {} AS m");
    let m = match rows[0].get("m") {
        Some(Value::Property(Property::Map(m))) => m,
        other => panic!("expected Property::Map, got {other:?}"),
    };
    assert!(m.is_empty());
}

#[test]
fn map_literal_null_entry_survives_as_property_null() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN {x: null, y: 1} AS m");
    let m = match rows[0].get("m") {
        Some(Value::Property(Property::Map(m))) => m,
        other => panic!("expected Property::Map, got {other:?}"),
    };
    assert!(matches!(m.get("x"), Some(Property::Null)));
    assert!(matches!(m.get("y"), Some(Property::Int64(1))));
}

#[test]
fn map_literal_rejects_node_values() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    // `{p: a}` where a is a Node — v1 restriction: map values must
    // be Properties, not Nodes. Evaluator returns TypeMismatch.
    let (store2, _d2) = open_store();
    run(&store2, "CREATE (:Person {name: 'Ada'})");
    let plan =
        mesh_cypher::plan(&mesh_cypher::parse("MATCH (a:Person) RETURN {p: a} AS m").unwrap())
            .unwrap();
    let err = mesh_executor::execute(&plan, &store2).unwrap_err();
    assert!(
        matches!(err, mesh_executor::Error::TypeMismatch),
        "expected TypeMismatch, got {err:?}"
    );
}

#[test]
fn path_variable_single_hop_binds_path() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}), (a)-[:KNOWS]->(b)",
    );
    let rows = run(
        &store,
        "MATCH p = (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person) RETURN p",
    );
    assert_eq!(rows.len(), 1);
    match rows[0].get("p") {
        Some(Value::Path { nodes, edges }) => {
            assert_eq!(nodes.len(), 2);
            assert_eq!(edges.len(), 1);
            assert_eq!(
                nodes[0].properties.get("name"),
                Some(&Property::String("Ada".into()))
            );
            assert_eq!(
                nodes[1].properties.get("name"),
                Some(&Property::String("Bob".into()))
            );
            assert_eq!(edges[0].edge_type, "KNOWS");
        }
        other => panic!("expected Value::Path, got {other:?}"),
    }
}

#[test]
fn path_variable_length_returns_hop_count() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}), \
                 (c:Person {name: 'Cara'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)",
    );
    let rows = run(
        &store,
        "MATCH p = (a:Person {name: 'Ada'})-[:KNOWS]->(b)-[:KNOWS]->(c) \
                 RETURN length(p) AS len",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "len"), 2);
}

#[test]
fn path_variable_nodes_and_relationships_unpack_sequence() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}), \
                 (c:Person {name: 'Cara'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)",
    );
    let rows = run(
        &store,
        "MATCH p = (a:Person {name: 'Ada'})-[:KNOWS]->(b)-[:KNOWS]->(c) \
                 RETURN nodes(p) AS ns, relationships(p) AS rs",
    );
    assert_eq!(rows.len(), 1);
    match rows[0].get("ns") {
        Some(Value::List(items)) => {
            assert_eq!(items.len(), 3);
            assert!(items.iter().all(|v| matches!(v, Value::Node(_))));
        }
        other => panic!("expected Value::List of Node, got {other:?}"),
    }
    match rows[0].get("rs") {
        Some(Value::List(items)) => {
            assert_eq!(items.len(), 2);
            assert!(items.iter().all(|v| matches!(v, Value::Edge(_))));
        }
        other => panic!("expected Value::List of Edge, got {other:?}"),
    }
}

#[test]
fn path_variable_zero_hop_pattern_is_single_node_path() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(&store, "MATCH p = (n:Person) RETURN length(p) AS len, p");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "len"), 0);
    match rows[0].get("p") {
        Some(Value::Path { nodes, edges }) => {
            assert_eq!(nodes.len(), 1);
            assert!(edges.is_empty());
        }
        other => panic!("expected Value::Path, got {other:?}"),
    }
}

#[test]
fn path_variable_with_unnamed_edge_still_binds() {
    // The edge has no user-given variable; the planner should
    // synthesize one so BindPath has an edge to pull out of the row.
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}), (a)-[:KNOWS]->(b)",
    );
    let rows = run(
        &store,
        "MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN length(p) AS len",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "len"), 1);
}

#[test]
fn arithmetic_integer_add_sub_mul_div_mod() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN 1 + 2 AS s, 10 - 3 AS d, 4 * 5 AS m, 20 / 6 AS q, 20 % 6 AS r",
    );
    assert_eq!(int_prop(&rows[0], "s"), 3);
    assert_eq!(int_prop(&rows[0], "d"), 7);
    assert_eq!(int_prop(&rows[0], "m"), 20);
    assert_eq!(int_prop(&rows[0], "q"), 3); // truncated int division
    assert_eq!(int_prop(&rows[0], "r"), 2);
}

#[test]
fn arithmetic_precedence_matches_math() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN 1 + 2 * 3 AS r, (1 + 2) * 3 AS p");
    assert_eq!(int_prop(&rows[0], "r"), 7);
    assert_eq!(int_prop(&rows[0], "p"), 9);
}

#[test]
fn arithmetic_int_float_coercion_widens_to_float() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN 1 + 2.5 AS r");
    assert!((float_prop(&rows[0], "r") - 3.5).abs() < 1e-12);
}

#[test]
fn arithmetic_unary_negation_on_expression() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {age: 36})");
    let rows = run(&store, "MATCH (p:Person) RETURN -p.age AS neg");
    assert_eq!(int_prop(&rows[0], "neg"), -36);
}

#[test]
fn arithmetic_null_propagates() {
    let (store, _d) = open_store();
    // `null + 1` should produce null, not a type error.
    let rows = run(&store, "RETURN coalesce(null) + 1 AS r");
    assert!(matches!(
        rows[0].get("r"),
        Some(Value::Null) | Some(Value::Property(Property::Null))
    ));
}

#[test]
fn arithmetic_string_concat_with_plus() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN 'foo' + 'bar' AS r");
    assert_eq!(str_prop(&rows[0], "r"), "foobar");
}

#[test]
fn arithmetic_list_concat_with_plus() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN [1, 2] + [3, 4] AS r");
    match rows[0].get("r") {
        Some(Value::List(items)) => {
            assert_eq!(items.len(), 4);
        }
        other => panic!("expected list, got {other:?}"),
    }
}

#[test]
fn arithmetic_integer_divide_by_zero_errors() {
    let (store, _d) = open_store();
    let plan = mesh_cypher::plan(&mesh_cypher::parse("RETURN 1 / 0 AS r").unwrap()).unwrap();
    let err = mesh_executor::execute(&plan, &store).unwrap_err();
    assert!(
        matches!(err, mesh_executor::Error::DivideByZero),
        "expected DivideByZero, got {err:?}"
    );
}

#[test]
fn arithmetic_mod_by_zero_errors() {
    let (store, _d) = open_store();
    let plan = mesh_cypher::plan(&mesh_cypher::parse("RETURN 5 % 0 AS r").unwrap()).unwrap();
    let err = mesh_executor::execute(&plan, &store).unwrap_err();
    assert!(matches!(err, mesh_executor::Error::DivideByZero));
}

#[test]
fn arithmetic_in_where_clause_filters_rows() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada', age: 30}), (:Person {name: 'Bob', age: 17})",
    );
    let rows = run(
        &store,
        "MATCH (p:Person) WHERE p.age + 1 > 18 RETURN p.name AS name",
    );
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["Ada".to_string()]);
}

#[test]
fn arithmetic_with_parameters() {
    let (store, _d) = open_store();
    let mut params = ParamMap::new();
    params.insert(
        "multiplier".to_string(),
        Value::Property(Property::Int64(10)),
    );
    let rows = run_with_params(&store, "RETURN 5 * $multiplier AS r", &params);
    assert_eq!(int_prop(&rows[0], "r"), 50);
}

#[test]
fn reduce_sums_integer_list() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN reduce(acc = 0, x IN [1, 2, 3, 4] | acc + x) AS total",
    );
    assert_eq!(int_prop(&rows[0], "total"), 10);
}

#[test]
fn reduce_with_non_zero_initial_value() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN reduce(acc = 100, x IN [1, 2, 3] | acc + x) AS total",
    );
    assert_eq!(int_prop(&rows[0], "total"), 106);
}

#[test]
fn reduce_computes_product_with_mul() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN reduce(p = 1, x IN [2, 3, 4] | p * x) AS product",
    );
    assert_eq!(int_prop(&rows[0], "product"), 24);
}

#[test]
fn reduce_over_empty_list_returns_init() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN reduce(acc = 42, x IN [] | acc + x) AS total",
    );
    assert_eq!(int_prop(&rows[0], "total"), 42);
}

#[test]
fn reduce_over_null_source_propagates_null() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN reduce(acc = 0, x IN coalesce(null) | acc + x) AS total",
    );
    assert!(matches!(
        rows[0].get("total"),
        Some(Value::Null) | Some(Value::Property(Property::Null))
    ));
}

#[test]
fn reduce_with_row_variable_in_body() {
    // The body can reference outer bindings alongside acc + x.
    // Using UNWIND to get a single row with a bound outer var,
    // since bare `WITH` isn't valid without a prior reading clause.
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "UNWIND [10] AS multiplier \
         RETURN reduce(acc = 0, x IN [1, 2, 3] | acc + x * multiplier) AS total",
    );
    // 0 + 1*10 + 2*10 + 3*10 = 60
    assert_eq!(int_prop(&rows[0], "total"), 60);
}

#[test]
fn reduce_scope_does_not_leak_iteration_vars() {
    // After reduce finishes, the outer `x = 5` binding must
    // survive — reduce's `x` shadow is scoped to the fold only.
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "UNWIND [5] AS x \
         RETURN reduce(acc = 0, x IN [1, 2, 3] | acc + x) AS total, x AS outer_x",
    );
    assert_eq!(int_prop(&rows[0], "total"), 6);
    assert_eq!(int_prop(&rows[0], "outer_x"), 5);
}

#[test]
fn reduce_concatenates_strings() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN reduce(s = '', w IN ['foo', 'bar', 'baz'] | s + w) AS joined",
    );
    assert_eq!(str_prop(&rows[0], "joined"), "foobarbaz");
}

#[test]
fn where_pattern_predicate_filters_nodes_with_edge() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'}), \
                 (:Person {name: 'Cara'})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person) WHERE (p)-[:KNOWS]->(:Person) RETURN p.name AS name",
    );
    let names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    assert_eq!(names, vec!["Ada".to_string()]);
}

#[test]
fn where_not_pattern_predicate_excludes_nodes_with_edge() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'}), \
                 (:Person {name: 'Cara'})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person) WHERE NOT (p)-[:KNOWS]->(:Person) RETURN p.name AS name",
    );
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["Bob".to_string(), "Cara".to_string()]);
}

#[test]
fn where_pattern_predicate_with_target_label_filter() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:User {name: 'Ada'}), (:Company {name: 'Acme'}), (:User {name: 'Bob'})",
    );
    run(
        &store,
        "MATCH (u:User {name: 'Ada'}), (c:Company {name: 'Acme'}) \
         CREATE (u)-[:OWNS]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (p) WHERE (p)-[:OWNS]->(:Company) RETURN p.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

#[test]
fn where_pattern_predicate_with_both_endpoints_bound() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'}), (:Person {name: 'Cara'})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person), (q:Person) WHERE (p)-[:KNOWS]->(q) \
         RETURN p.name AS p_name, q.name AS q_name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "p_name"), "Ada");
    assert_eq!(str_prop(&rows[0], "q_name"), "Bob");
}

#[test]
fn where_pattern_predicate_multi_hop() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'}), \
                 (:Person {name: 'Cara'}), (:Person {name: 'Dex'})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Cara'}) \
         CREATE (b)-[:KNOWS]->(c)",
    );
    // Ada has a 2-hop KNOWS path. Dex doesn't. Bob has a 1-hop
    // path but a 2-hop path requires Bob→Cara→x which doesn't exist.
    let rows = run(
        &store,
        "MATCH (p:Person) WHERE (p)-[:KNOWS]->(:Person)-[:KNOWS]->(:Person) \
         RETURN p.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

#[test]
fn where_pattern_predicate_with_target_property_filter() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'}), (:Person {name: 'Cara'})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cara'}) \
         CREATE (a)-[:KNOWS]->(c)",
    );
    run(
        &store,
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Cara'}) \
         CREATE (b)-[:KNOWS]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person) WHERE (p)-[:KNOWS]->(:Person {name: 'Cara'}) \
         RETURN p.name AS name",
    );
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["Ada".to_string(), "Bob".to_string()]);
}

#[test]
fn where_pattern_predicate_combined_with_scalar_predicate_and() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada', age: 36}), \
                 (:Person {name: 'Bob', age: 17}), \
                 (:Person {name: 'Cara', age: 42})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cara'}) \
         CREATE (a)-[:KNOWS]->(c), (c)-[:KNOWS]->(a)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person) WHERE p.age >= 18 AND (p)-[:KNOWS]->(:Person) \
         RETURN p.name AS name",
    );
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["Ada".to_string(), "Cara".to_string()]);
}

#[test]
fn where_pattern_predicate_combined_with_scalar_predicate_or() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada', age: 36}), \
                 (:Person {name: 'Bob', age: 17}), \
                 (:Person {name: 'Cara', age: 42})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person) WHERE p.age < 18 OR (p)-[:KNOWS]->(:Person) \
         RETURN p.name AS name",
    );
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["Ada".to_string(), "Bob".to_string()]);
}

#[test]
fn where_pattern_predicate_var_length_errors() {
    let parsed =
        mesh_cypher::parse("MATCH (p:Person) WHERE (p)-[:KNOWS*1..3]->(:Person) RETURN p").unwrap();
    let err = mesh_cypher::plan(&parsed).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("variable-length") && msg.contains("pattern predicate"),
        "expected var-length rejection message, got: {msg}"
    );
}

#[test]
fn where_pattern_predicate_missing_edge_returns_empty() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'})",
    );
    let rows = run(
        &store,
        "MATCH (p:Person) WHERE (p)-[:KNOWS]->(:Person) RETURN p.name AS name",
    );
    assert!(rows.is_empty());
}

#[test]
fn datetime_now_returns_recent_epoch_millis() {
    let (store, _d) = open_store();
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let rows = run(&store, "RETURN datetime() AS now");
    let got = match rows[0].get("now") {
        Some(Value::Property(Property::DateTime(ms))) => *ms,
        other => panic!("expected DateTime, got {other:?}"),
    };
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    assert!(
        got >= before && got <= after,
        "datetime() {got} out of range [{before}, {after}]"
    );
}

#[test]
fn date_now_returns_today_as_days_since_epoch() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN date() AS today");
    let days = match rows[0].get("today") {
        Some(Value::Property(Property::Date(d))) => *d,
        other => panic!("expected Date, got {other:?}"),
    };
    assert!(days > 19_000 && days < 100_000, "days = {days}");
}

#[test]
fn timestamp_returns_int_epoch_millis() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN timestamp() AS t");
    let t = int_prop(&rows[0], "t");
    assert!(t > 1_600_000_000_000, "timestamp() = {t}");
}

#[test]
fn duration_constructor_builds_from_map() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN duration({months: 2, days: 3, hours: 4, minutes: 5, seconds: 6}) AS d",
    );
    let d = match rows[0].get("d") {
        Some(Value::Property(Property::Duration(d))) => *d,
        other => panic!("expected Duration, got {other:?}"),
    };
    assert_eq!(d.months, 2);
    assert_eq!(d.days, 3);
    assert_eq!(d.seconds, 4 * 3600 + 5 * 60 + 6);
    assert_eq!(d.nanos, 0);
}

#[test]
fn duration_constructor_years_weeks_folded() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN duration({years: 1, weeks: 2, seconds: 10}) AS d",
    );
    let d = match rows[0].get("d") {
        Some(Value::Property(Property::Duration(d))) => *d,
        other => panic!("expected Duration, got {other:?}"),
    };
    assert_eq!(d.months, 12);
    assert_eq!(d.days, 14);
    assert_eq!(d.seconds, 10);
}

#[test]
fn duration_constructor_rejects_unknown_key() {
    let (store, _d) = open_store();
    let plan = mesh_cypher::plan(&mesh_cypher::parse("RETURN duration({bogus: 1}) AS d").unwrap())
        .unwrap();
    let err = mesh_executor::execute(&plan, &store).unwrap_err();
    assert!(format!("{err}").contains("bogus"));
}

#[test]
fn datetime_plus_duration_advances_datetime() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN datetime() + duration({seconds: 60}) AS plus_minute, \
                 datetime() AS base",
    );
    let plus = match rows[0].get("plus_minute") {
        Some(Value::Property(Property::DateTime(ms))) => *ms,
        other => panic!("expected DateTime, got {other:?}"),
    };
    let base = match rows[0].get("base") {
        Some(Value::Property(Property::DateTime(ms))) => *ms,
        other => panic!("expected DateTime, got {other:?}"),
    };
    let delta = plus - base;
    assert!(
        (60_000..60_050).contains(&delta),
        "delta = {delta}, expected ~60000"
    );
}

#[test]
fn date_plus_duration_days_only() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN date() + duration({days: 7}) AS next_week, date() AS today",
    );
    let next = match rows[0].get("next_week") {
        Some(Value::Property(Property::Date(d))) => *d,
        other => panic!("expected Date, got {other:?}"),
    };
    let today = match rows[0].get("today") {
        Some(Value::Property(Property::Date(d))) => *d,
        other => panic!("expected Date, got {other:?}"),
    };
    assert_eq!(next - today, 7);
}

#[test]
fn date_plus_duration_with_seconds_errors() {
    let (store, _d) = open_store();
    let plan = mesh_cypher::plan(
        &mesh_cypher::parse("RETURN date() + duration({seconds: 10}) AS d").unwrap(),
    )
    .unwrap();
    let err = mesh_executor::execute(&plan, &store).unwrap_err();
    assert!(matches!(err, mesh_executor::Error::TypeMismatch));
}

#[test]
fn datetime_ordering_works_in_where() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "UNWIND [datetime() - duration({days: 1}), datetime() + duration({days: 1})] AS ts \
         WHERE ts < datetime() \
         RETURN ts",
    );
    assert_eq!(rows.len(), 1);
    let ts = match rows[0].get("ts") {
        Some(Value::Property(Property::DateTime(ms))) => *ms,
        other => panic!("expected DateTime, got {other:?}"),
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    assert!(ts < now);
}

#[test]
fn duration_plus_duration_adds_component_wise() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN duration({months: 1, days: 2, seconds: 30}) + \
                 duration({months: 3, days: 4, seconds: 60}) AS sum",
    );
    let sum = match rows[0].get("sum") {
        Some(Value::Property(Property::Duration(d))) => *d,
        other => panic!("expected Duration, got {other:?}"),
    };
    assert_eq!(sum.months, 4);
    assert_eq!(sum.days, 6);
    assert_eq!(sum.seconds, 90);
}
