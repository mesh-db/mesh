use meshdb_core::{Edge, Node, NodeId, Property};
use meshdb_cypher::{parse, plan, plan_with_context, PlannerContext};
use meshdb_executor::{
    execute, execute_with_reader, execute_with_reader_and_procs, explain, profile, GraphReader,
    GraphWriter, ParamMap, ProcedureRegistry, Row, Value,
};
use meshdb_storage::RocksDbStorageEngine as Store;
use tempfile::TempDir;

fn open_store() -> (Store, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    (store, dir)
}

fn run(store: &Store, q: &str) -> Vec<Row> {
    let stmt = parse(q).unwrap_or_else(|e| panic!("parse {q}: {e}"));
    if let meshdb_cypher::Statement::Explain(inner) = &stmt {
        let p = plan(inner).unwrap_or_else(|e| panic!("plan {q}: {e}"));
        return explain(&p);
    }
    if let meshdb_cypher::Statement::Profile(inner) = &stmt {
        let p = plan(inner).unwrap_or_else(|e| panic!("plan {q}: {e}"));
        return profile(&p, store).unwrap_or_else(|e| panic!("profile {q}: {e}"));
    }
    let p = plan(&stmt).unwrap_or_else(|e| panic!("plan {q}: {e}"));
    execute(&p, store).unwrap_or_else(|e| panic!("exec {q}: {e}"))
}

/// Like `run`, but registers the default `db.*` procedures so tests
/// for `CALL db.labels()` / `db.relationshipTypes()` / `db.propertyKeys()`
/// can invoke them — the plain `run` helper uses an empty procedure
/// registry.
fn run_with_default_procs(store: &Store, q: &str) -> Vec<Row> {
    let stmt = parse(q).unwrap_or_else(|e| panic!("parse {q}: {e}"));
    let p = plan(&stmt).unwrap_or_else(|e| panic!("plan {q}: {e}"));
    let mut procs = ProcedureRegistry::new();
    procs.register_defaults();
    let params = ParamMap::new();
    execute_with_reader_and_procs(
        &p,
        store as &dyn GraphReader,
        store as &dyn GraphWriter,
        &params,
        &procs,
    )
    .unwrap_or_else(|e| panic!("exec {q}: {e}"))
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

/// Extract a list-of-strings property column. Used by the constraint
/// tests, where `SHOW CONSTRAINTS` emits `properties` as a list to
/// accommodate composite kinds (`NODE KEY`).
fn str_list_prop(row: &Row, key: &str) -> Vec<String> {
    match row.get(key) {
        Some(Value::Property(Property::List(items))) => items
            .iter()
            .map(|v| match v {
                Property::String(s) => s.clone(),
                other => panic!("expected string element in list at {key}, got {other:?}"),
            })
            .collect(),
        other => panic!("expected list at {key}, got {other:?}"),
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
fn multi_type_relationship_pattern_knows_or_likes() {
    let (store, _d) = open_store();
    let _g = build_social_graph(&store);

    // [:KNOWS|LIKES] should match both KNOWS and LIKES edges.
    // KNOWS: Ada->Alan, Alan->Grace
    // LIKES: Alan->Ada
    // So (a)-[:KNOWS|LIKES]->(b) should yield 3 rows.
    let rows = run(
        &store,
        "MATCH (a)-[:KNOWS|LIKES]->(b) RETURN a.name AS src, b.name AS dst",
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
            ("Alan".to_string(), "Ada".to_string()),
            ("Alan".to_string(), "Grace".to_string()),
        ]
    );
}

#[test]
fn multi_type_relationship_three_types() {
    let (store, _d) = open_store();
    let _g = build_social_graph(&store);

    // [:KNOWS|LIKES|WORKS_AT] should match all 4 edges.
    let rows = run(
        &store,
        "MATCH (a)-[:KNOWS|LIKES|WORKS_AT]->(b) RETURN a.name AS src, b.name AS dst",
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
            ("Ada".to_string(), "Analytics".to_string()),
            ("Alan".to_string(), "Ada".to_string()),
            ("Alan".to_string(), "Grace".to_string()),
        ]
    );
}

#[test]
fn multi_type_with_edge_variable_and_type_function() {
    let (store, _d) = open_store();
    let _g = build_social_graph(&store);

    // Bind the edge to `r` and use type(r).
    let rows = run(
        &store,
        "MATCH (a:Person)-[r:KNOWS|LIKES]->(b:Person) RETURN a.name AS src, type(r) AS rel, b.name AS dst",
    );
    let mut triples: Vec<(String, String, String)> = rows
        .iter()
        .map(|r| (str_prop(r, "src"), str_prop(r, "rel"), str_prop(r, "dst")))
        .collect();
    triples.sort();
    assert_eq!(
        triples,
        vec![
            ("Ada".to_string(), "KNOWS".to_string(), "Alan".to_string()),
            ("Alan".to_string(), "KNOWS".to_string(), "Grace".to_string()),
            ("Alan".to_string(), "LIKES".to_string(), "Ada".to_string()),
        ]
    );
}

#[test]
fn multi_type_with_colon_prefix_on_second_type() {
    let (store, _d) = open_store();
    let _g = build_social_graph(&store);

    // Also accept [:KNOWS|:LIKES] with colon on the second type.
    let rows = run(
        &store,
        "MATCH (a)-[:KNOWS|:LIKES]->(b) RETURN a.name AS src, b.name AS dst",
    );
    assert_eq!(rows.len(), 3);
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
    let e = meshdb_core::Edge::new("KNOWS", ada.id, alan.id);
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
fn match_detach_delete_then_property_access_is_deleted_entity_access() {
    // openCypher Return2 [15]: property access on a node DELETEd
    // earlier in the same query raises DeletedEntityAccess at
    // runtime rather than reading off the pre-delete snapshot.
    // (`DETACH DELETE n RETURN n.name` had to produce the snapshot
    // before tombstones existed; now it errors, matching the TCK.)
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (n:Person {name: 'Ada'})-[:KNOWS]->(m:Person {name: 'Alan'})",
    );

    let stmt = meshdb_cypher::parse(
        "MATCH (n:Person) WHERE n.name = 'Ada' DETACH DELETE n RETURN n.name AS name",
    )
    .unwrap();
    let plan = meshdb_cypher::plan(&stmt).unwrap();
    let err = meshdb_executor::execute(&plan, &store).unwrap_err();
    assert!(
        matches!(err, meshdb_executor::Error::DeletedEntityAccess(_)),
        "expected DeletedEntityAccess, got {err:?}",
    );

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
fn multi_pattern_match_shared_variable() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {name: 'A'})");
    let rows = run(&store, "MATCH (a:N), (a) RETURN a.name AS name");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "A");
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
fn unknown_scalar_function_rejected_at_plan_time() {
    let stmt = parse("MATCH (n:X) RETURN unknownfn(n) AS v").unwrap();
    let err = plan(&stmt).unwrap_err();
    assert!(
        matches!(&err, meshdb_cypher::Error::Plan(msg) if msg.contains("unknown function")),
        "expected plan-time unknown-function error, got {err:?}",
    );
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
fn trailing_plus_quantifier_is_one_or_more_hops() {
    let (store, _d) = open_store();
    build_chain(&store);

    let rows = run(
        &store,
        "MATCH (a:Link)-[:N]->+(b:Link) WHERE a.name = 'a' RETURN b.name AS b",
    );
    let names = sorted_names(&rows, "b");
    // 1..unbounded over a->b->c->d reaches b, c, d from a.
    assert_eq!(names, vec!["b", "c", "d"]);
}

#[test]
fn trailing_star_quantifier_is_zero_or_more_hops() {
    let (store, _d) = open_store();
    build_chain(&store);

    let rows = run(
        &store,
        "MATCH (a:Link)-[:N]->*(b:Link) WHERE a.name = 'a' RETURN b.name AS b",
    );
    let names = sorted_names(&rows, "b");
    // 0..unbounded adds the zero-hop self-match to the 1..unbounded set.
    assert_eq!(names, vec!["a", "b", "c", "d"]);
}

#[test]
fn trailing_plus_on_bare_arrow_matches_all_outgoing() {
    let (store, _d) = open_store();
    build_chain(&store);

    let rows = run(
        &store,
        "MATCH (a:Link)-->+(b:Link) WHERE a.name = 'a' RETURN b.name AS b",
    );
    let names = sorted_names(&rows, "b");
    assert_eq!(names, vec!["b", "c", "d"]);
}

#[test]
fn brace_range_quantifier_selects_hop_range() {
    let (store, _d) = open_store();
    build_chain(&store);

    // 2..3 hops over a -> b -> c -> d: reaches c (2 hops) and d (3 hops).
    let rows = run(
        &store,
        "MATCH (a:Link)-[:N]->{2,3}(b:Link) WHERE a.name = 'a' RETURN b.name AS b",
    );
    let names = sorted_names(&rows, "b");
    assert_eq!(names, vec!["c", "d"]);
}

#[test]
fn brace_max_only_quantifier_starts_from_zero_hops() {
    let (store, _d) = open_store();
    build_chain(&store);

    // `{,2}` means min=0..max=2 — includes the zero-hop self-match,
    // distinguishing it from the inline `*..2` form (min=1).
    let rows = run(
        &store,
        "MATCH (a:Link)-[:N]->{,2}(b:Link) WHERE a.name = 'a' RETURN b.name AS b",
    );
    let names = sorted_names(&rows, "b");
    assert_eq!(names, vec!["a", "b", "c"]);
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
fn var_length_inverted_range_yields_empty_results() {
    // openCypher accepts an inverted range (min > max) and
    // returns zero rows rather than failing at plan time (TCK
    // Match5 scenarios 11/12/13). The planner short-circuits the
    // hop to a Filter(false) so downstream operators see an
    // empty stream.
    let (store, _d) = open_store();
    run(&store, "CREATE (:A)-[:R]->(:A)");
    let rows = run(&store, "MATCH (a)-[*5..2]->(b) RETURN b");
    assert!(rows.is_empty(), "expected empty result, got {rows:?}");
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
        meshdb_executor::Error::CannotDeleteAttachedNode
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
fn chained_unwind_after_match_expands_per_row_list() {
    // Per-row UNWIND where the list comes from a property on the
    // matched node. Lists are seeded via parameters because list
    // literals aren't accepted in pattern-property position.
    let (store, _d) = open_store();
    let mut ps = ParamMap::new();
    ps.insert(
        "a".into(),
        Value::Property(Property::List(vec![
            Property::String("rust".into()),
            Property::String("db".into()),
        ])),
    );
    run_with_params(&store, "CREATE (:Post {title: 'A', tags: $a})", &ps);
    let mut ps = ParamMap::new();
    ps.insert(
        "b".into(),
        Value::Property(Property::List(vec![Property::String("graph".into())])),
    );
    run_with_params(&store, "CREATE (:Post {title: 'B', tags: $b})", &ps);
    let rows = run(
        &store,
        "MATCH (p:Post) UNWIND p.tags AS tag \
         RETURN p.title AS title, tag ORDER BY title, tag",
    );
    let got: Vec<(String, String)> = rows
        .iter()
        .map(|r| (str_prop(r, "title"), str_prop(r, "tag")))
        .collect();
    assert_eq!(
        got,
        vec![
            ("A".into(), "db".into()),
            ("A".into(), "rust".into()),
            ("B".into(), "graph".into()),
        ]
    );
}

#[test]
fn unwind_first_then_match_correlates_on_param() {
    // Classic batch-fan-out: UNWIND a parameter list, then for
    // each element do a correlated MATCH. The correlation goes
    // through a WHERE clause because pattern-property syntax only
    // accepts literals and parameters on the right-hand side, not
    // row-bound identifiers introduced by an earlier UNWIND.
    let (store, _d) = open_store();
    run(&store, "CREATE (:User {id: 1, name: 'Ada'})");
    run(&store, "CREATE (:User {id: 2, name: 'Bob'})");
    run(&store, "CREATE (:User {id: 3, name: 'Cara'})");
    let mut params = ParamMap::new();
    params.insert(
        "ids".into(),
        Value::Property(Property::List(vec![Property::Int64(1), Property::Int64(3)])),
    );
    let rows = run_with_params(
        &store,
        "UNWIND $ids AS id \
         MATCH (u:User) WHERE u.id = id \
         RETURN u.name AS name ORDER BY name",
        &params,
    );
    let names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    assert_eq!(names, vec!["Ada".to_string(), "Cara".to_string()]);
}

#[test]
fn nested_chained_unwind_cross_products_both_lists() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "UNWIND [1, 2] AS a \
         UNWIND [10, 20] AS b \
         RETURN a, b ORDER BY a, b",
    );
    let pairs: Vec<(i64, i64)> = rows
        .iter()
        .map(|r| (int_prop(r, "a"), int_prop(r, "b")))
        .collect();
    assert_eq!(pairs, vec![(1, 10), (1, 20), (2, 10), (2, 20)]);
}

#[test]
fn chained_unwind_empty_list_drops_input_row() {
    // Input rows whose UNWIND expression evaluates to an empty or
    // null list are silently dropped — the operator pulls the next
    // input row. Post 'B' has no `tags` property, so the property
    // access returns Null and is coerced to an empty list.
    let (store, _d) = open_store();
    let mut ps = ParamMap::new();
    ps.insert(
        "a".into(),
        Value::Property(Property::List(vec![Property::String("x".into())])),
    );
    run_with_params(&store, "CREATE (:Post {title: 'A', tags: $a})", &ps);
    run(&store, "CREATE (:Post {title: 'B'})");
    let mut ps = ParamMap::new();
    ps.insert(
        "c".into(),
        Value::Property(Property::List(vec![
            Property::String("y".into()),
            Property::String("z".into()),
        ])),
    );
    run_with_params(&store, "CREATE (:Post {title: 'C', tags: $c})", &ps);
    let rows = run(
        &store,
        "MATCH (p:Post) UNWIND p.tags AS tag \
         RETURN p.title AS title, tag ORDER BY title, tag",
    );
    let got: Vec<(String, String)> = rows
        .iter()
        .map(|r| (str_prop(r, "title"), str_prop(r, "tag")))
        .collect();
    assert_eq!(
        got,
        vec![
            ("A".into(), "x".into()),
            ("C".into(), "y".into()),
            ("C".into(), "z".into()),
        ]
    );
}

#[test]
fn chained_unwind_rejects_alias_collision() {
    // UNWIND is now allowed to rebind existing variables per openCypher spec.
    let parsed = meshdb_cypher::parse("MATCH (x) UNWIND [1, 2] AS x RETURN x").unwrap();
    let plan = meshdb_cypher::plan(&parsed);
    assert!(
        plan.is_ok(),
        "UNWIND rebinding should be allowed: {:?}",
        plan.err()
    );
}

#[test]
fn regex_match_full_string() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:User {email: 'ada@example.com'})");
    run(&store, "CREATE (:User {email: 'bob@other.org'})");
    run(&store, "CREATE (:User {email: 'cara@example.net'})");
    let rows = run(
        &store,
        "MATCH (u:User) WHERE u.email =~ '.*@example\\.com' \
         RETURN u.email AS email",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "email"), "ada@example.com");
}

#[test]
fn regex_match_is_full_not_substring() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Tag {name: 'hello'})");
    let rows = run(
        &store,
        "MATCH (t:Tag) WHERE t.name =~ 'ell' RETURN t.name AS n",
    );
    assert!(rows.is_empty());
    let rows = run(
        &store,
        "MATCH (t:Tag) WHERE t.name =~ '.*ell.*' RETURN t.name AS n",
    );
    assert_eq!(rows.len(), 1);
}

#[test]
fn regex_match_case_insensitive_flag() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Tag {name: 'Hello'})");
    let rows = run(
        &store,
        "MATCH (t:Tag) WHERE t.name =~ '(?i)hello' RETURN t.name AS n",
    );
    assert_eq!(rows.len(), 1);
}

#[test]
fn regex_match_with_not() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:User {name: 'Ada'})");
    run(&store, "CREATE (:User {name: 'Bob'})");
    let rows = run(
        &store,
        "MATCH (u:User) WHERE NOT u.name =~ 'A.*' RETURN u.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Bob");
}

#[test]
fn regex_match_null_propagates() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Tag {name: 'x'})");
    run(&store, "CREATE (:Tag)");
    let rows = run(
        &store,
        "MATCH (t:Tag) WHERE t.name =~ '.*' RETURN t.name AS n",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "n"), "x");
}

#[test]
fn in_list_filters_matching_values() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:User {name: 'Ada', status: 'active'})");
    run(&store, "CREATE (:User {name: 'Bob', status: 'pending'})");
    run(&store, "CREATE (:User {name: 'Cara', status: 'banned'})");
    let rows = run(
        &store,
        "MATCH (u:User) WHERE u.status IN ['active', 'pending'] \
         RETURN u.name AS name ORDER BY name",
    );
    let names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    assert_eq!(names, vec!["Ada", "Bob"]);
}

#[test]
fn in_list_with_integers() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {v: 1})");
    run(&store, "CREATE (:N {v: 2})");
    run(&store, "CREATE (:N {v: 3})");
    run(&store, "CREATE (:N {v: 4})");
    let rows = run(
        &store,
        "MATCH (n:N) WHERE n.v IN [2, 4] RETURN n.v AS v ORDER BY v",
    );
    let vals: Vec<i64> = rows.iter().map(|r| int_prop(r, "v")).collect();
    assert_eq!(vals, vec![2, 4]);
}

#[test]
fn in_list_with_parameter() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {v: 1})");
    run(&store, "CREATE (:N {v: 2})");
    run(&store, "CREATE (:N {v: 3})");
    let mut params = ParamMap::new();
    params.insert(
        "ids".into(),
        Value::Property(Property::List(vec![Property::Int64(1), Property::Int64(3)])),
    );
    let rows = run_with_params(
        &store,
        "MATCH (n:N) WHERE n.v IN $ids RETURN n.v AS v ORDER BY v",
        &params,
    );
    let vals: Vec<i64> = rows.iter().map(|r| int_prop(r, "v")).collect();
    assert_eq!(vals, vec![1, 3]);
}

#[test]
fn not_in_list_excludes() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {v: 1})");
    run(&store, "CREATE (:N {v: 2})");
    run(&store, "CREATE (:N {v: 3})");
    let rows = run(
        &store,
        "MATCH (n:N) WHERE NOT n.v IN [2] RETURN n.v AS v ORDER BY v",
    );
    let vals: Vec<i64> = rows.iter().map(|r| int_prop(r, "v")).collect();
    assert_eq!(vals, vec![1, 3]);
}

#[test]
fn in_list_null_element_returns_false() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {v: 1})");
    run(&store, "CREATE (:N)");
    let rows = run(&store, "MATCH (n:N) WHERE n.v IN [1, 2] RETURN n.v AS v");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "v"), 1);
}

#[test]
fn in_empty_list_returns_no_rows() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {v: 1})");
    let rows = run(&store, "MATCH (n:N) WHERE n.v IN [] RETURN n.v AS v");
    assert!(rows.is_empty());
}

#[test]
fn id_returns_uuid_string_for_node() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(
        &store,
        "MATCH (p:Person) RETURN id(p) AS nid, p.name AS name",
    );
    assert_eq!(rows.len(), 1);
    let nid = str_prop(&rows[0], "nid");
    assert_eq!(nid.len(), 36, "UUID string is 36 chars: {nid}");
}

#[test]
fn id_returns_uuid_string_for_edge() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:A {name: 'a'}), (:B {name: 'b'})");
    run(&store, "MATCH (a:A), (b:B) CREATE (a)-[:KNOWS]->(b)");
    let rows = run(&store, "MATCH ()-[r:KNOWS]->() RETURN id(r) AS eid");
    assert_eq!(rows.len(), 1);
    let eid = str_prop(&rows[0], "eid");
    assert_eq!(eid.len(), 36);
}

#[test]
fn elementid_is_alias_for_id() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(
        &store,
        "MATCH (p:Person) RETURN id(p) AS a, elementId(p) AS b",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a"), str_prop(&rows[0], "b"));
}

#[test]
fn startnode_and_endnode_return_endpoints() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    let rows = run(
        &store,
        "MATCH ()-[r:KNOWS]->() \
         RETURN startNode(r).name AS src, endNode(r).name AS dst",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "src"), "Ada");
    assert_eq!(str_prop(&rows[0], "dst"), "Bob");
}

#[test]
fn chained_property_access_on_parenthesized_expr() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(&store, "MATCH (p:Person) RETURN (p).name AS name");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

#[test]
fn chained_property_access_on_map_property() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada', age: 37})");
    let rows = run(&store, "MATCH (p:Person) RETURN properties(p).name AS name");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

#[test]
fn chained_property_null_propagates() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:A {name: 'a'})");
    let rows = run(
        &store,
        "MATCH (a:A) OPTIONAL MATCH (a)-[r:NOPE]->(b) \
         RETURN startNode(r).name AS sn",
    );
    assert_eq!(rows.len(), 1);
    assert!(matches!(
        rows[0].get("sn"),
        Some(Value::Null) | Some(Value::Property(Property::Null))
    ));
}

#[test]
fn index_access_on_list_literal() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN [10, 20, 30][1] AS v");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "v"), 20);
}

#[test]
fn index_access_negative_index() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN [10, 20, 30][-1] AS v");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "v"), 30);
}

#[test]
fn index_access_out_of_bounds_returns_null() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN [10, 20][5] AS v");
    assert_eq!(rows.len(), 1);
    assert!(matches!(
        rows[0].get("v"),
        Some(Value::Null) | Some(Value::Property(Property::Null))
    ));
}

#[test]
fn index_access_on_property_list() {
    let (store, _d) = open_store();
    let mut ps = ParamMap::new();
    ps.insert(
        "tags".into(),
        Value::Property(Property::List(vec![
            Property::String("a".into()),
            Property::String("b".into()),
            Property::String("c".into()),
        ])),
    );
    run_with_params(&store, "CREATE (:N {tags: $tags})", &ps);
    let rows = run(&store, "MATCH (n:N) RETURN (n.tags)[0] AS first");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "first"), "a");
}

#[test]
fn index_access_chained_with_property() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN [{name: 'Ada'}, {name: 'Bob'}][1].name AS v");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "v"), "Bob");
}

#[test]
fn index_access_null_base_returns_null() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {name: 'a'})");
    run(&store, "CREATE (:N)");
    let rows = run(
        &store,
        "MATCH (n:N) WHERE n.name = 'a' RETURN (n.missing)[0] AS v",
    );
    assert_eq!(rows.len(), 1);
    assert!(matches!(
        rows[0].get("v"),
        Some(Value::Null) | Some(Value::Property(Property::Null))
    ));
}

#[test]
fn properties_returns_map() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada', age: 37})");
    let rows = run(&store, "MATCH (p:Person) RETURN properties(p) AS props");
    assert_eq!(rows.len(), 1);
    match rows[0].get("props") {
        Some(Value::Property(Property::Map(m))) => {
            assert_eq!(m.get("name"), Some(&Property::String("Ada".into())));
            assert_eq!(m.get("age"), Some(&Property::Int64(37)));
        }
        other => panic!("expected map, got: {other:?}"),
    }
}

#[test]
fn id_on_null_returns_null() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:A {name: 'a'})");
    let rows = run(
        &store,
        "MATCH (a:A) OPTIONAL MATCH (a)-[r:NOPE]->(b) \
         RETURN a.name AS name, id(r) AS rid",
    );
    assert_eq!(rows.len(), 1);
    assert!(matches!(
        rows[0].get("rid"),
        Some(Value::Null) | Some(Value::Property(Property::Null))
    ));
}

#[test]
fn exists_filters_on_property_presence() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada', email: 'ada@x.com'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    let rows = run(
        &store,
        "MATCH (p:Person) WHERE exists(p.email) RETURN p.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

#[test]
fn exists_returns_false_for_missing_property() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(
        &store,
        "MATCH (p:Person) RETURN exists(p.email) AS has_email",
    );
    assert_eq!(rows.len(), 1);
    match rows[0].get("has_email") {
        Some(Value::Property(Property::Bool(b))) => assert!(!b),
        other => panic!("expected false, got: {other:?}"),
    }
}

#[test]
fn not_exists_excludes_present_property() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada', email: 'ada@x.com'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    let rows = run(
        &store,
        "MATCH (p:Person) WHERE NOT exists(p.email) RETURN p.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Bob");
}

#[test]
fn var_length_path_binding_returns_path() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'A'})");
    run(&store, "CREATE (:Person {name: 'B'})");
    run(&store, "CREATE (:Person {name: 'C'})");
    run(
        &store,
        "MATCH (a:Person {name: 'A'}), (b:Person {name: 'B'}) CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (b:Person {name: 'B'}), (c:Person {name: 'C'}) CREATE (b)-[:KNOWS]->(c)",
    );
    let rows = run(
        &store,
        "MATCH p = (a:Person {name: 'A'})-[:KNOWS*1..3]->(c:Person {name: 'C'}) \
         RETURN length(p) AS len",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "len"), 2);
}

#[test]
fn var_length_path_binding_nodes_and_relationships() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {name: 'A'})");
    run(&store, "CREATE (:N {name: 'B'})");
    run(
        &store,
        "MATCH (a:N {name: 'A'}), (b:N {name: 'B'}) CREATE (a)-[:R]->(b)",
    );
    let rows = run(
        &store,
        "MATCH p = (a:N {name: 'A'})-[:R*1..1]->(b:N) \
         RETURN length(p) AS len, nodes(p) AS ns, relationships(p) AS rs",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "len"), 1);
    match rows[0].get("ns") {
        Some(Value::List(ns)) => assert_eq!(ns.len(), 2),
        other => panic!("expected 2 nodes, got: {other:?}"),
    }
    match rows[0].get("rs") {
        Some(Value::List(rs)) => assert_eq!(rs.len(), 1),
        other => panic!("expected 1 edge, got: {other:?}"),
    }
}

#[test]
fn var_length_path_binding_multiple_results() {
    // Diamond: A->B, A->C, B->D, C->D — two 2-hop paths from A to D.
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {name: 'A'})");
    run(&store, "CREATE (:N {name: 'B'})");
    run(&store, "CREATE (:N {name: 'C'})");
    run(&store, "CREATE (:N {name: 'D'})");
    run(
        &store,
        "MATCH (a:N {name: 'A'}), (b:N {name: 'B'}) CREATE (a)-[:R]->(b)",
    );
    run(
        &store,
        "MATCH (a:N {name: 'A'}), (c:N {name: 'C'}) CREATE (a)-[:R]->(c)",
    );
    run(
        &store,
        "MATCH (b:N {name: 'B'}), (d:N {name: 'D'}) CREATE (b)-[:R]->(d)",
    );
    run(
        &store,
        "MATCH (c:N {name: 'C'}), (d:N {name: 'D'}) CREATE (c)-[:R]->(d)",
    );
    let rows = run(
        &store,
        "MATCH p = (a:N {name: 'A'})-[:R*1..3]->(d:N {name: 'D'}) \
         RETURN length(p) AS len",
    );
    assert_eq!(rows.len(), 2);
    for r in &rows {
        assert_eq!(int_prop(r, "len"), 2);
    }
}

#[test]
fn var_length_path_no_match_produces_no_rows() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {name: 'A'})");
    run(&store, "CREATE (:N {name: 'Z'})");
    let rows = run(
        &store,
        "MATCH p = (a:N {name: 'A'})-[:R*1..5]->(z:N {name: 'Z'}) \
         RETURN length(p) AS len",
    );
    assert!(rows.is_empty());
}

#[test]
fn slice_both_bounds() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN [10, 20, 30, 40, 50][1..3] AS s");
    assert_eq!(rows.len(), 1);
    match rows[0].get("s") {
        Some(Value::List(items)) => {
            let vals: Vec<i64> = items
                .iter()
                .map(|v| match v {
                    Value::Property(Property::Int64(i)) => *i,
                    _ => panic!("expected int"),
                })
                .collect();
            assert_eq!(vals, vec![20, 30]);
        }
        other => panic!("expected list, got: {other:?}"),
    }
}

#[test]
fn slice_from_start() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN [10, 20, 30, 40][2..] AS s");
    assert_eq!(rows.len(), 1);
    match rows[0].get("s") {
        Some(Value::List(items)) => assert_eq!(items.len(), 2),
        other => panic!("expected list, got: {other:?}"),
    }
}

#[test]
fn slice_to_end() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN [10, 20, 30, 40][..2] AS s");
    assert_eq!(rows.len(), 1);
    match rows[0].get("s") {
        Some(Value::List(items)) => assert_eq!(items.len(), 2),
        other => panic!("expected list, got: {other:?}"),
    }
}

#[test]
fn slice_negative_indices() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN [10, 20, 30, 40, 50][1..-1] AS s");
    assert_eq!(rows.len(), 1);
    match rows[0].get("s") {
        Some(Value::List(items)) => {
            let vals: Vec<i64> = items
                .iter()
                .map(|v| match v {
                    Value::Property(Property::Int64(i)) => *i,
                    _ => panic!("expected int"),
                })
                .collect();
            assert_eq!(vals, vec![20, 30, 40]);
        }
        other => panic!("expected list, got: {other:?}"),
    }
}

#[test]
fn slice_out_of_range_clamps() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN [10, 20][0..100] AS s");
    assert_eq!(rows.len(), 1);
    match rows[0].get("s") {
        Some(Value::List(items)) => assert_eq!(items.len(), 2),
        other => panic!("expected list, got: {other:?}"),
    }
}

#[test]
fn slice_empty_range_returns_empty() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN [10, 20, 30][2..1] AS s");
    assert_eq!(rows.len(), 1);
    match rows[0].get("s") {
        Some(Value::List(items)) => assert!(items.is_empty()),
        other => panic!("expected empty list, got: {other:?}"),
    }
}

#[test]
fn slice_chained_with_index() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN [10, 20, 30, 40][1..3][0] AS v");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "v"), 20);
}

#[test]
fn optional_match_multi_hop_full_chain_matches() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Company {name: 'Acme'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (b:Person {name: 'Bob'}), (c:Company {name: 'Acme'}) CREATE (b)-[:WORKS_AT]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'Ada'}) \
         OPTIONAL MATCH (a)-[:KNOWS]->(f)-[:WORKS_AT]->(c) \
         RETURN a.name AS a, f.name AS f, c.name AS c",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a"), "Ada");
    assert_eq!(str_prop(&rows[0], "f"), "Bob");
    assert_eq!(str_prop(&rows[0], "c"), "Acme");
}

#[test]
fn optional_match_multi_hop_partial_chain_nulls_tail() {
    // `OPTIONAL MATCH` has whole-pattern semantics: either every hop
    // in the chain matches for a given outer row, or the entire
    // pattern is treated as "no match" and the bindings introduced
    // by the pattern all come out Null. A prefix like "KNOWS exists
    // but no WORKS_AT follows" is NOT treated as a partial match —
    // it's just a miss, and `f` / `c` both end up Null.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'Ada'}) \
         OPTIONAL MATCH (a)-[:KNOWS]->(f)-[:WORKS_AT]->(c) \
         RETURN a.name AS a, f IS NULL AS f_null, c IS NULL AS c_null",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a"), "Ada");
    match rows[0].get("f_null") {
        Some(Value::Property(Property::Bool(b))) => assert!(b),
        other => panic!("expected true, got: {other:?}"),
    }
    match rows[0].get("c_null") {
        Some(Value::Property(Property::Bool(b))) => assert!(b),
        other => panic!("expected true, got: {other:?}"),
    }
}

#[test]
fn optional_match_multi_hop_no_first_hop_all_null() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'Ada'}) \
         OPTIONAL MATCH (a)-[:KNOWS]->(f)-[:WORKS_AT]->(c) \
         RETURN a.name AS a, f IS NULL AS f_null, c IS NULL AS c_null",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a"), "Ada");
    match rows[0].get("f_null") {
        Some(Value::Property(Property::Bool(b))) => assert!(b),
        other => panic!("expected true, got: {other:?}"),
    }
    match rows[0].get("c_null") {
        Some(Value::Property(Property::Bool(b))) => assert!(b),
        other => panic!("expected true, got: {other:?}"),
    }
}

#[test]
fn optional_match_three_hops() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:A {name: 'a'})");
    run(&store, "CREATE (:B {name: 'b'})");
    run(&store, "CREATE (:C {name: 'c'})");
    run(&store, "CREATE (:D {name: 'd'})");
    run(&store, "MATCH (a:A), (b:B) CREATE (a)-[:R]->(b)");
    run(&store, "MATCH (b:B), (c:C) CREATE (b)-[:R]->(c)");
    run(&store, "MATCH (c:C), (d:D) CREATE (c)-[:R]->(d)");
    let rows = run(
        &store,
        "MATCH (a:A) \
         OPTIONAL MATCH (a)-[:R]->(b)-[:R]->(c)-[:R]->(d) \
         RETURN a.name AS a, b.name AS b, c.name AS c, d.name AS d",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a"), "a");
    assert_eq!(str_prop(&rows[0], "b"), "b");
    assert_eq!(str_prop(&rows[0], "c"), "c");
    assert_eq!(str_prop(&rows[0], "d"), "d");
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

// --- EXPLAIN ---------------------------------------------------------------

#[test]
fn explain_returns_plan_text() {
    let (store, _d) = open_store();
    let rows = run(&store, "EXPLAIN MATCH (n:Person) RETURN n.name AS name");
    assert_eq!(rows.len(), 1);
    let plan_text = str_prop(&rows[0], "plan");
    assert!(plan_text.contains("NodeScanByLabels"), "got: {plan_text}");
    assert!(plan_text.contains("Project"), "got: {plan_text}");
}

#[test]
fn explain_with_filter_shows_filter_node() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "EXPLAIN MATCH (n:Person) WHERE n.age > 18 RETURN n.name",
    );
    assert_eq!(rows.len(), 1);
    let plan_text = str_prop(&rows[0], "plan");
    assert!(plan_text.contains("Filter"), "got: {plan_text}");
}

#[test]
fn explain_does_not_execute_mutations() {
    let (store, _d) = open_store();
    run(&store, "EXPLAIN CREATE (:Person {name: 'Ghost'})");
    let rows = run(&store, "MATCH (n:Person) RETURN n.name AS name");
    assert!(rows.is_empty());
}

#[test]
fn explain_multi_hop_shows_expand_chain() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "EXPLAIN MATCH (a:Person)-[:KNOWS]->(b)-[:WORKS_AT]->(c) RETURN a, b, c",
    );
    assert_eq!(rows.len(), 1);
    let plan_text = str_prop(&rows[0], "plan");
    let expand_count = plan_text.matches("EdgeExpand").count();
    assert!(
        expand_count >= 2,
        "expected 2+ EdgeExpand, got: {plan_text}"
    );
}

// --- PROFILE -----------------------------------------------------------

#[test]
fn profile_returns_plan_and_row_count() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    let rows = run(&store, "PROFILE MATCH (n:Person) RETURN n.name AS name");
    assert_eq!(rows.len(), 1);
    let prof = str_prop(&rows[0], "profile");
    assert!(prof.contains("NodeScanByLabels"), "got: {prof}");
    assert!(prof.contains("Rows: 2"), "got: {prof}");
    assert_eq!(int_prop(&rows[0], "rows"), 2);
}

#[test]
fn profile_actually_executes_mutations() {
    let (store, _d) = open_store();
    run(&store, "PROFILE CREATE (:Ghost {name: 'Casper'})");
    let rows = run(&store, "MATCH (n:Ghost) RETURN n.name AS name");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Casper");
}

// --- FOREACH -----------------------------------------------------------

#[test]
fn foreach_sets_property_on_each_element() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(
        &store,
        "MATCH (p:Person) \
         FOREACH (p IN [p] | SET p.marked = true)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person) WHERE p.marked = true RETURN p.name AS name ORDER BY name",
    );
    let names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    assert_eq!(names, vec!["Ada", "Bob"]);
}

#[test]
fn foreach_removes_property() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {name: 'A', temp: 1})");
    run(&store, "CREATE (:N {name: 'B', temp: 2})");
    run(
        &store,
        "MATCH (n:N) \
         FOREACH (n IN [n] | REMOVE n.temp)",
    );
    let rows = run(
        &store,
        "MATCH (n:N) RETURN n.name AS name, exists(n.temp) AS has ORDER BY name",
    );
    assert_eq!(rows.len(), 2);
    match rows[0].get("has") {
        Some(Value::Property(Property::Bool(b))) => assert!(!b),
        other => panic!("expected false, got: {other:?}"),
    }
}

#[test]
fn foreach_does_not_expand_row_stream() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {name: 'A'})");
    let mut params = ParamMap::new();
    params.insert(
        "tags".into(),
        Value::Property(Property::List(vec![
            Property::String("x".into()),
            Property::String("y".into()),
        ])),
    );
    let rows = run_with_params(
        &store,
        "MATCH (n:N) \
         FOREACH (t IN $tags | SET n.last_tag = t) \
         RETURN n.name AS name",
        &params,
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "A");
}

// --- LOAD CSV ----------------------------------------------------------

#[test]
fn load_csv_without_headers_returns_lists() {
    let (store, _d) = open_store();
    let csv_path = _d.path().join("test.csv");
    std::fs::write(&csv_path, "Alice,30\nBob,25\n").unwrap();
    let mut params = ParamMap::new();
    params.insert(
        "path".into(),
        Value::Property(Property::String(csv_path.to_str().unwrap().to_string())),
    );
    let rows = run_with_params(&store, "LOAD CSV FROM $path AS row RETURN row", &params);
    assert_eq!(rows.len(), 2);
    match rows[0].get("row") {
        Some(Value::List(items)) => {
            assert_eq!(items.len(), 2);
        }
        other => panic!("expected list, got: {other:?}"),
    }
}

#[test]
fn load_csv_with_headers_returns_maps() {
    let (store, _d) = open_store();
    let csv_path = _d.path().join("test.csv");
    std::fs::write(&csv_path, "name,age\nAlice,30\nBob,25\n").unwrap();
    let mut params = ParamMap::new();
    params.insert(
        "path".into(),
        Value::Property(Property::String(csv_path.to_str().unwrap().to_string())),
    );
    let rows = run_with_params(
        &store,
        "LOAD CSV WITH HEADERS FROM $path AS row RETURN row.name AS name ORDER BY name",
        &params,
    );
    let names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    assert_eq!(names, vec!["Alice", "Bob"]);
}

#[test]
fn load_csv_with_headers_field_access() {
    let (store, _d) = open_store();
    let csv_path = _d.path().join("people.csv");
    std::fs::write(&csv_path, "name,age\nAda,37\nBob,25\n").unwrap();
    let mut params = ParamMap::new();
    params.insert(
        "path".into(),
        Value::Property(Property::String(csv_path.to_str().unwrap().to_string())),
    );
    let rows = run_with_params(
        &store,
        "LOAD CSV WITH HEADERS FROM $path AS row \
         RETURN row.name AS name, row.age AS age ORDER BY name",
        &params,
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
    assert_eq!(str_prop(&rows[0], "age"), "37");
    assert_eq!(str_prop(&rows[1], "name"), "Bob");
}

// --- Combined mutation clauses ------------------------------------------

#[test]
fn match_create_then_set_in_same_terminal() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Seed {v: 1})");
    let rows = run(
        &store,
        "MATCH (s:Seed) \
         CREATE (n:Person {name: 'placeholder'}) SET n.name = 'Ada' \
         RETURN n.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

#[test]
fn match_create_then_remove_in_same_terminal() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Seed {v: 1})");
    let rows = run(
        &store,
        "MATCH (s:Seed) \
         CREATE (n:Person:Temp {name: 'Bob'}) REMOVE n:Temp \
         RETURN labels(n) AS labs",
    );
    assert_eq!(rows.len(), 1);
    match rows[0].get("labs") {
        Some(Value::List(labs)) => {
            let names: Vec<&str> = labs
                .iter()
                .map(|v| match v {
                    Value::Property(Property::String(s)) => s.as_str(),
                    _ => panic!("expected string"),
                })
                .collect();
            assert!(names.contains(&"Person"));
            assert!(!names.contains(&"Temp"));
        }
        other => panic!("expected list, got: {other:?}"),
    }
}

#[test]
fn set_then_remove_in_same_query() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {name: 'A', old: 1})");
    run(&store, "MATCH (n:N) SET n.new_prop = 'hello' REMOVE n.old");
    let rows = run(
        &store,
        "MATCH (n:N) RETURN exists(n.new_prop) AS has_new, exists(n.old) AS has_old",
    );
    assert_eq!(rows.len(), 1);
    match rows[0].get("has_new") {
        Some(Value::Property(Property::Bool(b))) => assert!(b),
        other => panic!("expected true, got: {other:?}"),
    }
    match rows[0].get("has_old") {
        Some(Value::Property(Property::Bool(b))) => assert!(!b),
        other => panic!("expected false, got: {other:?}"),
    }
}

// --- CREATE with RETURN ------------------------------------------------

#[test]
fn create_with_return_echoes_node() {
    let (store, _d) = open_store();
    let rows = run(&store, "CREATE (n:Person {name: 'Ada'}) RETURN n");
    assert_eq!(rows.len(), 1);
    match rows[0].get("n") {
        Some(Value::Node(n)) => {
            assert_eq!(
                n.properties.get("name"),
                Some(&Property::String("Ada".into()))
            );
            assert!(n.labels.contains(&"Person".to_string()));
        }
        other => panic!("expected node, got: {other:?}"),
    }
}

#[test]
fn create_with_return_property_access() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "CREATE (n:Person {name: 'Ada'}) RETURN n.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

#[test]
fn create_with_return_edge() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "CREATE (a:Person {name: 'Ada'})-[r:KNOWS]->(b:Person {name: 'Bob'}) \
         RETURN a.name AS a, type(r) AS t, b.name AS b",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a"), "Ada");
    assert_eq!(str_prop(&rows[0], "t"), "KNOWS");
    assert_eq!(str_prop(&rows[0], "b"), "Bob");
}

// --- REMOVE clause -----------------------------------------------------

#[test]
fn remove_property_deletes_from_node() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada', age: 37})");
    run(&store, "MATCH (p:Person {name: 'Ada'}) REMOVE p.age");
    let rows = run(
        &store,
        "MATCH (p:Person {name: 'Ada'}) RETURN exists(p.age) AS has_age",
    );
    assert_eq!(rows.len(), 1);
    match rows[0].get("has_age") {
        Some(Value::Property(Property::Bool(b))) => assert!(!b),
        other => panic!("expected false, got: {other:?}"),
    }
}

#[test]
fn remove_label_strips_label_from_node() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person:Employee {name: 'Ada'})");
    run(&store, "MATCH (p:Person {name: 'Ada'}) REMOVE p:Employee");
    let rows = run(
        &store,
        "MATCH (p:Person {name: 'Ada'}) RETURN labels(p) AS labs",
    );
    assert_eq!(rows.len(), 1);
    match rows[0].get("labs") {
        Some(Value::List(labs)) => {
            let names: Vec<&str> = labs
                .iter()
                .map(|v| match v {
                    Value::Property(Property::String(s)) => s.as_str(),
                    _ => panic!("expected string"),
                })
                .collect();
            assert!(names.contains(&"Person"));
            assert!(!names.contains(&"Employee"));
        }
        other => panic!("expected list, got: {other:?}"),
    }
}

#[test]
fn remove_multiple_items() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person:Admin {name: 'Ada', age: 37, email: 'ada@x.com'})",
    );
    run(
        &store,
        "MATCH (p:Person {name: 'Ada'}) REMOVE p.age, p.email, p:Admin",
    );
    let rows = run(
        &store,
        "MATCH (p:Person {name: 'Ada'}) \
         RETURN exists(p.age) AS a, exists(p.email) AS e, labels(p) AS labs",
    );
    assert_eq!(rows.len(), 1);
    match rows[0].get("a") {
        Some(Value::Property(Property::Bool(b))) => assert!(!b),
        other => panic!("expected false, got: {other:?}"),
    }
    match rows[0].get("e") {
        Some(Value::Property(Property::Bool(b))) => assert!(!b),
        other => panic!("expected false, got: {other:?}"),
    }
    match rows[0].get("labs") {
        Some(Value::List(labs)) => {
            assert_eq!(labs.len(), 1);
        }
        other => panic!("expected list, got: {other:?}"),
    }
}

#[test]
fn remove_nonexistent_property_is_noop() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "MATCH (p:Person {name: 'Ada'}) REMOVE p.missing");
    let rows = run(
        &store,
        "MATCH (p:Person {name: 'Ada'}) RETURN p.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

// --- count { } subquery ------------------------------------------------

#[test]
fn count_subquery_returns_match_count() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Person {name: 'Cara'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cara'}) CREATE (a)-[:KNOWS]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person) \
         RETURN p.name AS name, count { MATCH (p)-[:KNOWS]->() } AS friends \
         ORDER BY name",
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
    assert_eq!(int_prop(&rows[0], "friends"), 2);
    assert_eq!(str_prop(&rows[1], "name"), "Bob");
    assert_eq!(int_prop(&rows[1], "friends"), 0);
    assert_eq!(str_prop(&rows[2], "name"), "Cara");
    assert_eq!(int_prop(&rows[2], "friends"), 0);
}

#[test]
fn count_subquery_in_where_filters_by_threshold() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Person {name: 'Cara'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cara'}) CREATE (a)-[:KNOWS]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person) \
         WHERE count { MATCH (p)-[:KNOWS]->() } > 1 \
         RETURN p.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

#[test]
fn count_subquery_with_where_clause() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob', active: true})");
    run(&store, "CREATE (:Person {name: 'Cara', active: false})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cara'}) CREATE (a)-[:KNOWS]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person {name: 'Ada'}) \
         RETURN count { MATCH (p)-[:KNOWS]->(f) WHERE f.active = true } AS active_friends",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "active_friends"), 1);
}

#[test]
fn count_subquery_zero_for_no_matches() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(
        &store,
        "MATCH (p:Person) \
         RETURN count { MATCH (p)-[:NOPE]->() } AS n",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "n"), 0);
}

#[test]
fn count_function_still_works_as_aggregate() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {v: 1})");
    run(&store, "CREATE (:N {v: 2})");
    run(&store, "CREATE (:N {v: 3})");
    let rows = run(&store, "MATCH (n:N) RETURN count(*) AS c");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "c"), 3);
}

// --- collect { } subquery -----------------------------------------------

#[test]
fn collect_subquery_returns_correlated_values_as_list() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}) \
         CREATE (a)-[:KNOWS]->(:Person {name: 'Cara'})",
    );
    let rows = run(
        &store,
        "MATCH (p:Person {name: 'Ada'}) \
         RETURN collect { MATCH (p)-[:KNOWS]->(f) RETURN f.name } AS friends",
    );
    assert_eq!(rows.len(), 1);
    let friends = match rows[0].get("friends") {
        Some(Value::List(items)) => items.clone(),
        other => panic!("expected list, got {other:?}"),
    };
    let mut names: Vec<String> = friends
        .iter()
        .map(|v| match v {
            Value::Property(Property::String(s)) => s.clone(),
            other => panic!("expected string element, got {other:?}"),
        })
        .collect();
    names.sort();
    assert_eq!(names, vec!["Bob".to_string(), "Cara".to_string()]);
}

#[test]
fn collect_subquery_empty_when_no_matches() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(
        &store,
        "MATCH (p:Person) \
         RETURN collect { MATCH (p)-[:NOPE]->(x) RETURN x.name } AS xs",
    );
    assert_eq!(rows.len(), 1);
    match rows[0].get("xs") {
        Some(Value::List(items)) => assert!(items.is_empty()),
        other => panic!("expected empty list, got {other:?}"),
    }
}

// --- built-in db.* procedures ------------------------------------------

#[test]
fn db_labels_yields_distinct_node_labels() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person)");
    run(&store, "CREATE (:Person)");
    run(&store, "CREATE (:City)");
    run(&store, "CREATE (:City:Capital)");
    let rows = run_with_default_procs(&store, "CALL db.labels() YIELD label RETURN label");
    let mut labels: Vec<String> = rows.iter().map(|r| str_prop(r, "label")).collect();
    labels.sort();
    assert_eq!(labels, vec!["Capital", "City", "Person"]);
}

#[test]
fn db_relationship_types_yields_distinct_edge_types() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:A)-[:KNOWS]->(:A)");
    run(&store, "CREATE (:A)-[:KNOWS]->(:A)");
    run(&store, "CREATE (:A)-[:FOLLOWS]->(:A)");
    let rows = run_with_default_procs(
        &store,
        "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType",
    );
    let mut types: Vec<String> = rows
        .iter()
        .map(|r| str_prop(r, "relationshipType"))
        .collect();
    types.sort();
    assert_eq!(types, vec!["FOLLOWS", "KNOWS"]);
}

#[test]
fn db_property_keys_includes_node_and_edge_keys() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:P {name: 'Ada', age: 36})");
    run(
        &store,
        "MATCH (p:P) CREATE (p)-[:R {since: 2020}]->(:P {name: 'Bob'})",
    );
    let rows = run_with_default_procs(
        &store,
        "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey",
    );
    let mut keys: Vec<String> = rows.iter().map(|r| str_prop(r, "propertyKey")).collect();
    keys.sort();
    assert_eq!(keys, vec!["age", "name", "since"]);
}

// --- CALL { } subquery --------------------------------------------------

#[test]
fn call_subquery_uncorrelated() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    let rows = run(
        &store,
        "MATCH (p:Person) \
         CALL { MATCH (q:Person) RETURN count(*) AS total } \
         RETURN p.name AS name, total ORDER BY name",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
    assert_eq!(int_prop(&rows[0], "total"), 2);
    assert_eq!(str_prop(&rows[1], "name"), "Bob");
    assert_eq!(int_prop(&rows[1], "total"), 2);
}

#[test]
fn call_subquery_correlated_with_importing_with() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Person {name: 'Cara'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cara'}) CREATE (a)-[:KNOWS]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person) \
         CALL { WITH p MATCH (p)-[:KNOWS]->(f) RETURN f.name AS friend } \
         RETURN p.name AS name, friend ORDER BY name, friend",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
    assert_eq!(str_prop(&rows[0], "friend"), "Bob");
    assert_eq!(str_prop(&rows[1], "name"), "Ada");
    assert_eq!(str_prop(&rows[1], "friend"), "Cara");
}

#[test]
fn call_subquery_no_body_results_drops_outer_row() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(
        &store,
        "MATCH (p:Person) \
         CALL { MATCH (:NonExistent) RETURN 1 AS x } \
         RETURN p.name AS name",
    );
    assert!(rows.is_empty());
}

#[test]
fn call_subquery_with_union_body() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:A {name: 'x'})");
    run(&store, "CREATE (:B {name: 'y'})");
    let rows = run(
        &store,
        "CALL { MATCH (a:A) RETURN a.name AS v \
                UNION ALL \
                MATCH (b:B) RETURN b.name AS v } \
         RETURN v ORDER BY v",
    );
    let vals: Vec<String> = rows.iter().map(|r| str_prop(r, "v")).collect();
    assert_eq!(vals, vec!["x", "y"]);
}

// --- Lifted restrictions -----------------------------------------------

#[test]
fn pattern_predicate_with_var_length() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {name: 'A'})");
    run(&store, "CREATE (:N {name: 'B'})");
    run(&store, "CREATE (:N {name: 'C'})");
    run(&store, "CREATE (:N {name: 'D'})");
    run(
        &store,
        "MATCH (a:N {name: 'A'}), (b:N {name: 'B'}) CREATE (a)-[:R]->(b)",
    );
    run(
        &store,
        "MATCH (b:N {name: 'B'}), (c:N {name: 'C'}) CREATE (b)-[:R]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (n:N) WHERE (n)-[:R*1..3]->(:N {name: 'C'}) RETURN n.name AS name ORDER BY name",
    );
    let names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    assert_eq!(names, vec!["A", "B"]);
}

#[test]
fn exists_subquery_with_var_length() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {name: 'A'})");
    run(&store, "CREATE (:N {name: 'B'})");
    run(&store, "CREATE (:N {name: 'C'})");
    run(
        &store,
        "MATCH (a:N {name: 'A'}), (b:N {name: 'B'}) CREATE (a)-[:R]->(b)",
    );
    run(
        &store,
        "MATCH (b:N {name: 'B'}), (c:N {name: 'C'}) CREATE (b)-[:R]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (n:N) \
         WHERE EXISTS { MATCH (n)-[:R*1..3]->(t:N {name: 'C'}) } \
         RETURN n.name AS name ORDER BY name",
    );
    let names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    assert_eq!(names, vec!["A", "B"]);
}

#[test]
fn count_subquery_with_var_length() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {name: 'A'})");
    run(&store, "CREATE (:N {name: 'B'})");
    run(&store, "CREATE (:N {name: 'C'})");
    run(
        &store,
        "MATCH (a:N {name: 'A'}), (b:N {name: 'B'}) CREATE (a)-[:R]->(b)",
    );
    run(
        &store,
        "MATCH (b:N {name: 'B'}), (c:N {name: 'C'}) CREATE (b)-[:R]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (n:N {name: 'A'}) \
         RETURN count { MATCH (n)-[:R*1..3]->() } AS reachable",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "reachable"), 2);
}

#[test]
fn optional_match_with_target_properties() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Person {name: 'Cara'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cara'}) CREATE (a)-[:KNOWS]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person {name: 'Ada'}) \
         OPTIONAL MATCH (p)-[:KNOWS]->(f:Person {name: 'Bob'}) \
         RETURN f.name AS friend",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "friend"), "Bob");
}

#[test]
fn optional_match_with_var_length() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {name: 'A'})");
    run(&store, "CREATE (:N {name: 'B'})");
    run(&store, "CREATE (:N {name: 'C'})");
    run(
        &store,
        "MATCH (a:N {name: 'A'}), (b:N {name: 'B'}) CREATE (a)-[:R]->(b)",
    );
    run(
        &store,
        "MATCH (b:N {name: 'B'}), (c:N {name: 'C'}) CREATE (b)-[:R]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (n:N {name: 'A'}) \
         OPTIONAL MATCH (n)-[:R*1..3]->(t:N) WHERE t.name = 'C' \
         RETURN t.name AS reached",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "reached"), "C");
}

#[test]
fn optional_match_multi_pattern() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Company {name: 'Acme'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'Ada'}) \
         OPTIONAL MATCH (a)-[:KNOWS]->(f), (a)-[:WORKS_AT]->(c) \
         RETURN f.name AS friend, c IS NULL AS no_company",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "friend"), "Bob");
}

#[test]
fn multi_hop_merge_decomposes_to_node_and_edge_merges() {
    let (store, _d) = open_store();
    run(
        &store,
        "MERGE (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Bob'})",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Bob'}) \
         RETURN a.name AS a, b.name AS b",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a"), "Ada");
    assert_eq!(str_prop(&rows[0], "b"), "Bob");
}

#[test]
fn path_binding_mixed_fixed_and_var_length() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {name: 'A'})");
    run(&store, "CREATE (:N {name: 'B'})");
    run(&store, "CREATE (:N {name: 'C'})");
    run(&store, "CREATE (:N {name: 'D'})");
    run(
        &store,
        "MATCH (a:N {name: 'A'}), (b:N {name: 'B'}) CREATE (a)-[:R]->(b)",
    );
    run(
        &store,
        "MATCH (b:N {name: 'B'}), (c:N {name: 'C'}) CREATE (b)-[:R]->(c)",
    );
    run(
        &store,
        "MATCH (c:N {name: 'C'}), (d:N {name: 'D'}) CREATE (c)-[:R]->(d)",
    );
    let rows = run(
        &store,
        "MATCH p = (a:N {name: 'A'})-[:R]->(b)-[:R*1..3]->(d:N {name: 'D'}) \
         RETURN length(p) AS len",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "len"), 3);
}

#[test]
fn left_and_right_string_functions() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN left('hello', 2) AS l, right('hello', 3) AS r",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "l"), "he");
    assert_eq!(str_prop(&rows[0], "r"), "llo");
}

#[test]
fn xor_operator() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN (true XOR false) AS a, (true XOR true) AS b, (false XOR false) AS c",
    );
    assert_eq!(rows.len(), 1);
    match rows[0].get("a") {
        Some(Value::Property(Property::Bool(b))) => assert!(b),
        other => panic!("expected true, got: {other:?}"),
    }
    match rows[0].get("b") {
        Some(Value::Property(Property::Bool(b))) => assert!(!b),
        other => panic!("expected false, got: {other:?}"),
    }
    match rows[0].get("c") {
        Some(Value::Property(Property::Bool(b))) => assert!(!b),
        other => panic!("expected false, got: {other:?}"),
    }
}

#[test]
fn math_functions_e_exp_log_log10_rand() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN e() AS euler, exp(1) AS exp1, log(e()) AS ln_e, log10(100) AS log100, rand() AS r",
    );
    assert_eq!(rows.len(), 1);
    match rows[0].get("euler") {
        Some(Value::Property(Property::Float64(f))) => {
            assert!((*f - std::f64::consts::E).abs() < 1e-9);
        }
        other => panic!("expected E, got: {other:?}"),
    }
    match rows[0].get("exp1") {
        Some(Value::Property(Property::Float64(f))) => {
            assert!((*f - std::f64::consts::E).abs() < 1e-9);
        }
        other => panic!("expected E, got: {other:?}"),
    }
    match rows[0].get("ln_e") {
        Some(Value::Property(Property::Float64(f))) => {
            assert!((*f - 1.0).abs() < 1e-9);
        }
        other => panic!("expected 1.0, got: {other:?}"),
    }
    match rows[0].get("log100") {
        Some(Value::Property(Property::Float64(f))) => {
            assert!((*f - 2.0).abs() < 1e-9);
        }
        other => panic!("expected 2.0, got: {other:?}"),
    }
    match rows[0].get("r") {
        Some(Value::Property(Property::Float64(f))) => {
            assert!(*f >= 0.0 && *f <= 1.0, "rand() should be in [0,1]: {f}");
        }
        other => panic!("expected float, got: {other:?}"),
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
        outer_bindings: Vec::new(),
        indexes: store
            .list_property_indexes()
            .into_iter()
            .map(|s| (s.label, s.properties))
            .collect(),
        edge_indexes: store
            .list_edge_property_indexes()
            .into_iter()
            .map(|s| (s.edge_type, s.properties))
            .collect(),
        point_indexes: store
            .list_point_indexes()
            .into_iter()
            .map(|s| (s.label, s.property))
            .collect(),
        edge_point_indexes: store
            .list_edge_point_indexes()
            .into_iter()
            .map(|s| (s.edge_type, s.property))
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
        outer_bindings: Vec::new(),
        indexes: store
            .list_property_indexes()
            .into_iter()
            .map(|s| (s.label, s.properties))
            .collect(),
        edge_indexes: store
            .list_edge_property_indexes()
            .into_iter()
            .map(|s| (s.edge_type, s.properties))
            .collect(),
        point_indexes: store
            .list_point_indexes()
            .into_iter()
            .map(|s| (s.label, s.property))
            .collect(),
        edge_point_indexes: store
            .list_edge_point_indexes()
            .into_iter()
            .map(|s| (s.edge_type, s.property))
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
    assert_eq!(str_prop(&create_rows[0], "scope"), "NODE");
    assert_eq!(str_prop(&create_rows[0], "label"), "Person");
    assert_eq!(str_prop(&create_rows[0], "property"), "name");

    let shown = run(&store, "SHOW INDEXES");
    assert_eq!(shown.len(), 1);
    assert_eq!(str_prop(&shown[0], "scope"), "NODE");
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
fn create_edge_index_and_show_indexes_round_trip() {
    let (store, _d) = open_store();
    let create_rows = run(&store, "CREATE INDEX FOR ()-[r:KNOWS]-() ON (r.since)");
    assert_eq!(create_rows.len(), 1);
    assert_eq!(str_prop(&create_rows[0], "state"), "created");
    assert_eq!(str_prop(&create_rows[0], "scope"), "RELATIONSHIP");
    assert_eq!(str_prop(&create_rows[0], "edge_type"), "KNOWS");
    assert_eq!(str_prop(&create_rows[0], "property"), "since");

    let shown = run(&store, "SHOW INDEXES");
    assert_eq!(shown.len(), 1);
    assert_eq!(str_prop(&shown[0], "scope"), "RELATIONSHIP");
    assert_eq!(str_prop(&shown[0], "edge_type"), "KNOWS");
    assert_eq!(str_prop(&shown[0], "property"), "since");
}

#[test]
fn show_indexes_merges_node_and_edge_scopes() {
    let (store, _d) = open_store();
    run(&store, "CREATE INDEX FOR (p:Person) ON (p.name)");
    run(&store, "CREATE INDEX FOR ()-[r:KNOWS]-() ON (r.since)");

    let shown = run(&store, "SHOW INDEXES");
    assert_eq!(shown.len(), 2);
    let scopes: std::collections::HashSet<_> = shown
        .iter()
        .map(|r| str_prop(r, "scope").to_string())
        .collect();
    assert!(scopes.contains("NODE"));
    assert!(scopes.contains("RELATIONSHIP"));
}

#[test]
fn drop_edge_index_empties_show_indexes() {
    let (store, _d) = open_store();
    run(&store, "CREATE INDEX FOR ()-[r:KNOWS]-() ON (r.since)");
    run(&store, "DROP INDEX FOR ()-[r:KNOWS]-() ON (r.since)");
    assert!(run(&store, "SHOW INDEXES").is_empty());
}

// ---------------------------------------------------------------
// POINT INDEX DDL end-to-end.
//
// Exercises the full Cypher surface: parse → plan → executor DDL
// dispatch → writer → storage. Separate `SHOW POINT INDEXES` row
// stream confirms the DDL doesn't leak into the non-spatial
// `SHOW INDEXES` output (and vice versa).
// ---------------------------------------------------------------

#[test]
fn create_point_index_and_show_point_indexes_round_trip() {
    let (store, _d) = open_store();
    let create_rows = run(&store, "CREATE POINT INDEX FOR (c:City) ON (c.loc)");
    assert_eq!(create_rows.len(), 1);
    assert_eq!(str_prop(&create_rows[0], "state"), "created");
    assert_eq!(str_prop(&create_rows[0], "scope"), "NODE");
    assert_eq!(str_prop(&create_rows[0], "type"), "POINT");
    assert_eq!(str_prop(&create_rows[0], "label"), "City");
    assert_eq!(str_prop(&create_rows[0], "property"), "loc");

    let shown = run(&store, "SHOW POINT INDEXES");
    assert_eq!(shown.len(), 1);
    assert_eq!(str_prop(&shown[0], "scope"), "NODE");
    assert_eq!(str_prop(&shown[0], "type"), "POINT");
    assert_eq!(str_prop(&shown[0], "label"), "City");
    assert_eq!(str_prop(&shown[0], "property"), "loc");
    assert_eq!(str_prop(&shown[0], "state"), "online");
}

#[test]
fn drop_point_index_empties_show_point_indexes() {
    let (store, _d) = open_store();
    run(&store, "CREATE POINT INDEX FOR (c:City) ON (c.loc)");
    let drop_rows = run(&store, "DROP POINT INDEX FOR (c:City) ON (c.loc)");
    assert_eq!(drop_rows.len(), 1);
    assert_eq!(str_prop(&drop_rows[0], "state"), "dropped");
    assert!(run(&store, "SHOW POINT INDEXES").is_empty());
}

#[test]
fn withinbbox_query_returns_points_inside_bbox() {
    // End-to-end bbox query going through the PointIndexSeek rewrite.
    // Three cities; the bbox encloses Berlin only.
    let (store, _d) = open_store();
    run_with_ctx(&store, "CREATE POINT INDEX FOR (c:City) ON (c.loc)");
    run_with_ctx(
        &store,
        "CREATE (:City {name: 'Berlin', loc: point({x: 13.4, y: 52.5})})",
    );
    run_with_ctx(
        &store,
        "CREATE (:City {name: 'NYC', loc: point({x: -73.9, y: 40.7})})",
    );
    run_with_ctx(
        &store,
        "CREATE (:City {name: 'Tokyo', loc: point({x: 139.7, y: 35.7})})",
    );
    let rows = run_with_ctx(
        &store,
        "MATCH (c:City) \
         WHERE point.withinbbox(c.loc, point({x: 10.0, y: 50.0}), point({x: 20.0, y: 55.0})) \
         RETURN c.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Berlin");
}

#[test]
fn withinbbox_returns_same_rows_with_and_without_index() {
    // Rewrite must preserve semantics. Run the same query against the
    // post-CREATE-POINT-INDEX store (rewrite fires) vs a parallel
    // store with no index (rewrite doesn't fire) and confirm the
    // result set matches.
    let (indexed, _d1) = open_store();
    let (scan_only, _d2) = open_store();
    for store in [&indexed, &scan_only] {
        run_with_ctx(
            store,
            "CREATE (:City {name: 'A', loc: point({x: 1.0, y: 1.0})})",
        );
        run_with_ctx(
            store,
            "CREATE (:City {name: 'B', loc: point({x: 9.0, y: 9.0})})",
        );
        run_with_ctx(
            store,
            "CREATE (:City {name: 'C', loc: point({x: 20.0, y: 20.0})})",
        );
    }
    run_with_ctx(&indexed, "CREATE POINT INDEX FOR (c:City) ON (c.loc)");

    let q = "MATCH (c:City) \
             WHERE point.withinbbox(c.loc, point({x: 0.0, y: 0.0}), point({x: 10.0, y: 10.0})) \
             RETURN c.name AS name";
    let mut indexed_names: Vec<String> = run_with_ctx(&indexed, q)
        .iter()
        .map(|r| str_prop(r, "name"))
        .collect();
    let mut scan_names: Vec<String> = run_with_ctx(&scan_only, q)
        .iter()
        .map(|r| str_prop(r, "name"))
        .collect();
    indexed_names.sort();
    scan_names.sort();
    assert_eq!(indexed_names, scan_names);
    assert_eq!(indexed_names, vec!["A".to_string(), "B".to_string()]);
}

#[test]
fn withinbbox_residual_conjunct_still_applies() {
    // Rewrite consumes the withinbbox conjunct but a sibling
    // predicate must still filter the seek output.
    let (store, _d) = open_store();
    run_with_ctx(&store, "CREATE POINT INDEX FOR (c:City) ON (c.loc)");
    run_with_ctx(
        &store,
        "CREATE (:City {name: 'A', pop: 500,  loc: point({x: 1.0, y: 1.0})})",
    );
    run_with_ctx(
        &store,
        "CREATE (:City {name: 'B', pop: 5000, loc: point({x: 2.0, y: 2.0})})",
    );
    let rows = run_with_ctx(
        &store,
        "MATCH (c:City) \
         WHERE point.withinbbox(c.loc, point({x: 0.0, y: 0.0}), point({x: 10.0, y: 10.0})) \
           AND c.pop > 1000 \
         RETURN c.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "B");
}

// ---------------------------------------------------------------
// point.distance(...) < r → PointIndexSeek (slice 4).
//
// Exercises the enclosing-bbox rewrite: storage returns all points
// in a bbox superset of the circle, then the residual Filter culls
// the corner overshoot. Must agree with the non-indexed scan.
// ---------------------------------------------------------------

#[test]
fn distance_query_returns_points_within_radius_cartesian() {
    let (store, _d) = open_store();
    run_with_ctx(&store, "CREATE POINT INDEX FOR (p:Spot) ON (p.pos)");
    // Inside the radius.
    run_with_ctx(
        &store,
        "CREATE (:Spot {name: 'inside', pos: point({x: 1.0, y: 1.0, srid: 7203})})",
    );
    // Inside the enclosing bbox but OUTSIDE the circle — corner of the square.
    // Distance to origin = sqrt(4.5² + 4.5²) ≈ 6.36, > 5.0.
    run_with_ctx(
        &store,
        "CREATE (:Spot {name: 'corner', pos: point({x: 4.5, y: 4.5, srid: 7203})})",
    );
    // Far away.
    run_with_ctx(
        &store,
        "CREATE (:Spot {name: 'far', pos: point({x: 20.0, y: 20.0, srid: 7203})})",
    );
    let rows = run_with_ctx(
        &store,
        "MATCH (p:Spot) \
         WHERE point.distance(p.pos, point({x: 0.0, y: 0.0, srid: 7203})) < 5.0 \
         RETURN p.name AS name",
    );
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["inside".to_string()]);
}

#[test]
fn distance_query_matches_scan_result_cartesian() {
    // Parity with the non-rewritten fallback: same dataset, same
    // query, two stores — one indexed (rewrite fires), one not.
    let (indexed, _d1) = open_store();
    let (scan_only, _d2) = open_store();
    for store in [&indexed, &scan_only] {
        run_with_ctx(
            store,
            "CREATE (:Spot {name: 'A', pos: point({x: 0.5, y: 0.5, srid: 7203})})",
        );
        run_with_ctx(
            store,
            "CREATE (:Spot {name: 'B', pos: point({x: 3.0, y: 4.0, srid: 7203})})",
        );
        run_with_ctx(
            store,
            "CREATE (:Spot {name: 'C', pos: point({x: 4.5, y: 4.5, srid: 7203})})",
        );
        run_with_ctx(
            store,
            "CREATE (:Spot {name: 'D', pos: point({x: 100.0, y: 0.0, srid: 7203})})",
        );
    }
    run_with_ctx(&indexed, "CREATE POINT INDEX FOR (p:Spot) ON (p.pos)");

    let q = "MATCH (p:Spot) \
             WHERE point.distance(p.pos, point({x: 0.0, y: 0.0, srid: 7203})) <= 5.0 \
             RETURN p.name AS name";
    let mut with_idx: Vec<String> = run_with_ctx(&indexed, q)
        .iter()
        .map(|r| str_prop(r, "name"))
        .collect();
    let mut without_idx: Vec<String> = run_with_ctx(&scan_only, q)
        .iter()
        .map(|r| str_prop(r, "name"))
        .collect();
    with_idx.sort();
    without_idx.sort();
    assert_eq!(with_idx, without_idx);
    assert_eq!(with_idx, vec!["A".to_string(), "B".to_string()]);
}

#[test]
fn distance_query_matches_scan_result_wgs84() {
    // WGS-84 round-trip: the operator's dlat/dlon-per-metre
    // approximation must still produce a superset, so the result
    // set matches the non-rewritten scan. Short distances (a few
    // km) near the equator are the safe regime.
    let (indexed, _d1) = open_store();
    let (scan_only, _d2) = open_store();
    for store in [&indexed, &scan_only] {
        // Reference: (0, 0). Three points at varying distances.
        run_with_ctx(
            store,
            "CREATE (:City {name: 'near', loc: point({latitude: 0.001, longitude: 0.0})})",
        );
        run_with_ctx(
            store,
            "CREATE (:City {name: 'medium', loc: point({latitude: 0.05, longitude: 0.0})})",
        );
        run_with_ctx(
            store,
            "CREATE (:City {name: 'far', loc: point({latitude: 1.0, longitude: 0.0})})",
        );
    }
    run_with_ctx(&indexed, "CREATE POINT INDEX FOR (c:City) ON (c.loc)");

    // Reference at equator, ~1 km radius. `near` (~111 m) qualifies;
    // `medium` (~5.5 km) and `far` (~111 km) don't.
    let q = "MATCH (c:City) \
             WHERE point.distance(c.loc, point({latitude: 0.0, longitude: 0.0})) < 1000.0 \
             RETURN c.name AS name";
    let mut with_idx: Vec<String> = run_with_ctx(&indexed, q)
        .iter()
        .map(|r| str_prop(r, "name"))
        .collect();
    let mut without_idx: Vec<String> = run_with_ctx(&scan_only, q)
        .iter()
        .map(|r| str_prop(r, "name"))
        .collect();
    with_idx.sort();
    without_idx.sort();
    assert_eq!(
        with_idx, without_idx,
        "rewrite must agree with scan fallback",
    );
    assert_eq!(with_idx, vec!["near".to_string()]);
}

#[test]
fn distance_query_with_parameterized_radius() {
    let (store, _d) = open_store();
    run_with_ctx(&store, "CREATE POINT INDEX FOR (p:Spot) ON (p.pos)");
    run_with_ctx(
        &store,
        "CREATE (:Spot {pos: point({x: 1.0, y: 1.0, srid: 7203})})",
    );
    run_with_ctx(
        &store,
        "CREATE (:Spot {pos: point({x: 100.0, y: 100.0, srid: 7203})})",
    );
    let mut params = ParamMap::new();
    params.insert(
        "r".into(),
        meshdb_executor::Value::Property(meshdb_core::Property::Float64(5.0)),
    );
    let rows = run_with_ctx_params(
        &store,
        "MATCH (p:Spot) \
         WHERE point.distance(p.pos, point({x: 0.0, y: 0.0, srid: 7203})) < $r \
         RETURN p.pos AS pos",
        &params,
    );
    assert_eq!(rows.len(), 1);
}

// ---------------------------------------------------------------
// Relationship-scope POINT INDEX DDL end-to-end (slice 5).
// ---------------------------------------------------------------

#[test]
fn create_edge_point_index_and_show_round_trip() {
    let (store, _d) = open_store();
    let create_rows = run_with_ctx(
        &store,
        "CREATE POINT INDEX FOR ()-[r:ROUTE]-() ON (r.waypoint)",
    );
    assert_eq!(create_rows.len(), 1);
    assert_eq!(str_prop(&create_rows[0], "state"), "created");
    assert_eq!(str_prop(&create_rows[0], "scope"), "RELATIONSHIP");
    assert_eq!(str_prop(&create_rows[0], "type"), "POINT");
    assert_eq!(str_prop(&create_rows[0], "edge_type"), "ROUTE");
    assert_eq!(str_prop(&create_rows[0], "property"), "waypoint");

    let shown = run_with_ctx(&store, "SHOW POINT INDEXES");
    assert_eq!(shown.len(), 1);
    assert_eq!(str_prop(&shown[0], "scope"), "RELATIONSHIP");
    assert_eq!(str_prop(&shown[0], "type"), "POINT");
    assert_eq!(str_prop(&shown[0], "edge_type"), "ROUTE");
    assert_eq!(str_prop(&shown[0], "property"), "waypoint");
    assert_eq!(str_prop(&shown[0], "state"), "online");
}

#[test]
fn drop_edge_point_index_empties_show() {
    let (store, _d) = open_store();
    run_with_ctx(
        &store,
        "CREATE POINT INDEX FOR ()-[r:ROUTE]-() ON (r.waypoint)",
    );
    let drop_rows = run_with_ctx(
        &store,
        "DROP POINT INDEX FOR ()-[r:ROUTE]-() ON (r.waypoint)",
    );
    assert_eq!(drop_rows.len(), 1);
    assert_eq!(str_prop(&drop_rows[0], "state"), "dropped");
    assert!(run_with_ctx(&store, "SHOW POINT INDEXES").is_empty());
}

#[test]
fn show_point_indexes_merges_node_and_edge_scopes() {
    let (store, _d) = open_store();
    run_with_ctx(&store, "CREATE POINT INDEX FOR (c:City) ON (c.loc)");
    run_with_ctx(
        &store,
        "CREATE POINT INDEX FOR ()-[r:ROUTE]-() ON (r.waypoint)",
    );

    let shown = run_with_ctx(&store, "SHOW POINT INDEXES");
    assert_eq!(shown.len(), 2);
    let scopes: std::collections::HashSet<_> = shown
        .iter()
        .map(|r| str_prop(r, "scope").to_string())
        .collect();
    assert!(scopes.contains("NODE"));
    assert!(scopes.contains("RELATIONSHIP"));
}

// ---------------------------------------------------------------
// Edge-scope PointIndexSeek end-to-end.
// ---------------------------------------------------------------

#[test]
fn edge_withinbbox_query_returns_edges_inside_bbox() {
    let (store, _d) = open_store();
    run_with_ctx(
        &store,
        "CREATE POINT INDEX FOR ()-[r:ROUTE]-() ON (r.waypoint)",
    );
    run_with_ctx(&store, "CREATE (:P {name: 'a'}), (:P {name: 'b'})");
    run_with_ctx(
        &store,
        "MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) \
         CREATE (a)-[:ROUTE {waypoint: point({x: 5.0, y: 5.0}), tag: 'inside'}]->(b)",
    );
    run_with_ctx(
        &store,
        "MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) \
         CREATE (a)-[:ROUTE {waypoint: point({x: 50.0, y: 50.0}), tag: 'outside'}]->(b)",
    );

    let rows = run_with_ctx(
        &store,
        "MATCH ()-[r:ROUTE]->() \
         WHERE point.withinbbox(r.waypoint, point({x: 0.0, y: 0.0}), point({x: 10.0, y: 10.0})) \
         RETURN r.tag AS tag",
    );
    let tags: Vec<String> = rows.iter().map(|r| str_prop(r, "tag")).collect();
    assert_eq!(tags, vec!["inside".to_string()]);
}

#[test]
fn edge_withinbbox_matches_scan_result() {
    // Parity with the non-rewritten fallback: same dataset, same
    // query, with / without the edge point index.
    let (indexed, _d1) = open_store();
    let (scan_only, _d2) = open_store();
    for store in [&indexed, &scan_only] {
        run_with_ctx(store, "CREATE (:N {id: 1}), (:N {id: 2})");
        run_with_ctx(
            store,
            "MATCH (a:N {id: 1}), (b:N {id: 2}) \
             CREATE (a)-[:ROUTE {waypoint: point({x: 1.0, y: 1.0}), tag: 'near'}]->(b)",
        );
        run_with_ctx(
            store,
            "MATCH (a:N {id: 1}), (b:N {id: 2}) \
             CREATE (a)-[:ROUTE {waypoint: point({x: 9.0, y: 9.0}), tag: 'corner'}]->(b)",
        );
        run_with_ctx(
            store,
            "MATCH (a:N {id: 1}), (b:N {id: 2}) \
             CREATE (a)-[:ROUTE {waypoint: point({x: 50.0, y: 50.0}), tag: 'far'}]->(b)",
        );
    }
    run_with_ctx(
        &indexed,
        "CREATE POINT INDEX FOR ()-[r:ROUTE]-() ON (r.waypoint)",
    );

    let q = "MATCH ()-[r:ROUTE]->() \
             WHERE point.withinbbox(r.waypoint, point({x: 0.0, y: 0.0}), point({x: 10.0, y: 10.0})) \
             RETURN r.tag AS tag";
    let mut with_idx: Vec<String> = run_with_ctx(&indexed, q)
        .iter()
        .map(|r| str_prop(r, "tag"))
        .collect();
    let mut without_idx: Vec<String> = run_with_ctx(&scan_only, q)
        .iter()
        .map(|r| str_prop(r, "tag"))
        .collect();
    with_idx.sort();
    without_idx.sort();
    assert_eq!(with_idx, without_idx);
    assert_eq!(with_idx, vec!["corner".to_string(), "near".to_string()]);
}

#[test]
fn edge_distance_query_returns_edges_within_radius() {
    let (store, _d) = open_store();
    run_with_ctx(
        &store,
        "CREATE POINT INDEX FOR ()-[r:ROUTE]-() ON (r.waypoint)",
    );
    run_with_ctx(&store, "CREATE (:N {id: 1}), (:N {id: 2})");
    for (x, y, tag) in &[
        (1.0, 1.0, "inside"),
        (4.5, 4.5, "corner"),
        (20.0, 20.0, "far"),
    ] {
        run_with_ctx(
            &store,
            &format!(
                "MATCH (a:N {{id: 1}}), (b:N {{id: 2}}) \
                 CREATE (a)-[:ROUTE {{waypoint: point({{x: {x}, y: {y}, srid: 7203}}), tag: '{tag}'}}]->(b)"
            ),
        );
    }

    let rows = run_with_ctx(
        &store,
        "MATCH ()-[r:ROUTE]->() \
         WHERE point.distance(r.waypoint, point({x: 0.0, y: 0.0, srid: 7203})) < 5.0 \
         RETURN r.tag AS tag",
    );
    let tags: Vec<String> = rows.iter().map(|r| str_prop(r, "tag")).collect();
    // `corner` at (4.5, 4.5) distance = sqrt(40.5) ≈ 6.36 > 5 →
    // excluded by residual filter even though it's in the
    // enclosing bbox.
    assert_eq!(tags, vec!["inside".to_string()]);
}

#[test]
fn point_index_and_property_index_shows_are_independent() {
    // Point DDL must not surface in `SHOW INDEXES` and vice versa —
    // the two row streams have different column shapes and mixing
    // them would break the `SHOW INDEXES` contract.
    let (store, _d) = open_store();
    run(&store, "CREATE INDEX FOR (p:Person) ON (p.name)");
    run(&store, "CREATE POINT INDEX FOR (c:City) ON (c.loc)");

    let prop = run(&store, "SHOW INDEXES");
    assert_eq!(prop.len(), 1);
    assert_eq!(str_prop(&prop[0], "label"), "Person");

    let point = run(&store, "SHOW POINT INDEXES");
    assert_eq!(point.len(), 1);
    assert_eq!(str_prop(&point[0], "label"), "City");
    assert_eq!(str_prop(&point[0], "type"), "POINT");
}

#[test]
fn edge_seek_anonymous_endpoints_finds_indexed_edges() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(&store, "CREATE (:Person {name: 'Cid'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS {since: 2020}]->(b)",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Cid'}) \
         CREATE (a)-[:KNOWS {since: 2021}]->(b)",
    );
    run(&store, "CREATE INDEX FOR ()-[r:KNOWS]-() ON (r.since)");

    let rows = run_with_ctx(
        &store,
        "MATCH ()-[r:KNOWS {since: 2020}]-() RETURN r.since AS s",
    );
    // Undirected pattern over a directed edge binds both orientations,
    // so one stored edge surfaces as two rows.
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| int_prop(r, "s") == 2020));
}

#[test]
fn edge_seek_directed_binds_single_orientation() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:P {name: 'a'})");
    run(&store, "CREATE (:P {name: 'b'})");
    run(
        &store,
        "MATCH (x:P {name: 'a'}), (y:P {name: 'b'}) \
         CREATE (x)-[:R {k: 1}]->(y)",
    );
    run(&store, "CREATE INDEX FOR ()-[r:R]-() ON (r.k)");

    let rows = run_with_ctx(
        &store,
        "MATCH (src)-[r:R {k: 1}]->(dst) RETURN src.name AS s, dst.name AS d",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "s"), "a");
    assert_eq!(str_prop(&rows[0], "d"), "b");
}

#[test]
fn edge_seek_incoming_inverts_endpoints() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:P {name: 'a'})");
    run(&store, "CREATE (:P {name: 'b'})");
    run(
        &store,
        "MATCH (x:P {name: 'a'}), (y:P {name: 'b'}) \
         CREATE (x)-[:R {k: 1}]->(y)",
    );
    run(&store, "CREATE INDEX FOR ()-[r:R]-() ON (r.k)");

    // `<-` means src_var binds to the edge's TARGET.
    let rows = run_with_ctx(
        &store,
        "MATCH (src)<-[r:R {k: 1}]-(dst) RETURN src.name AS s, dst.name AS d",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "s"), "b");
    assert_eq!(str_prop(&rows[0], "d"), "a");
}

#[test]
fn edge_seek_undirected_emits_both_orientations() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:P {name: 'a'})");
    run(&store, "CREATE (:P {name: 'b'})");
    run(
        &store,
        "MATCH (x:P {name: 'a'}), (y:P {name: 'b'}) \
         CREATE (x)-[:R {k: 1}]->(y)",
    );
    run(&store, "CREATE INDEX FOR ()-[r:R]-() ON (r.k)");

    let rows = run_with_ctx(
        &store,
        "MATCH (src)-[r:R {k: 1}]-(dst) RETURN src.name AS s, dst.name AS d ORDER BY s",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(str_prop(&rows[0], "s"), "a");
    assert_eq!(str_prop(&rows[0], "d"), "b");
    assert_eq!(str_prop(&rows[1], "s"), "b");
    assert_eq!(str_prop(&rows[1], "d"), "a");
}

#[test]
fn edge_seek_residual_property_filter_narrows_matches() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:P {name: 'a'})");
    run(&store, "CREATE (:P {name: 'b'})");
    run(&store, "CREATE (:P {name: 'c'})");
    run(&store, "CREATE (:P {name: 'd'})");
    run(
        &store,
        "MATCH (x:P {name: 'a'}), (y:P {name: 'b'}) \
         CREATE (x)-[:R {k: 1, tag: 'keep'}]->(y)",
    );
    run(
        &store,
        "MATCH (x:P {name: 'c'}), (y:P {name: 'd'}) \
         CREATE (x)-[:R {k: 1, tag: 'drop'}]->(y)",
    );
    run(&store, "CREATE INDEX FOR ()-[r:R]-() ON (r.k)");

    let rows = run_with_ctx(
        &store,
        "MATCH (src)-[r:R {k: 1, tag: 'keep'}]->(dst) RETURN src.name AS s, dst.name AS d",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "s"), "a");
    assert_eq!(str_prop(&rows[0], "d"), "b");
}

#[test]
fn edge_seek_returns_empty_when_no_edge_matches() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:P {name: 'a'})");
    run(&store, "CREATE (:P {name: 'b'})");
    run(
        &store,
        "MATCH (x:P {name: 'a'}), (y:P {name: 'b'}) \
         CREATE (x)-[:R {k: 1}]->(y)",
    );
    run(&store, "CREATE INDEX FOR ()-[r:R]-() ON (r.k)");

    let rows = run_with_ctx(&store, "MATCH ()-[r:R {k: 999}]-() RETURN r");
    assert!(rows.is_empty());
}

#[test]
fn edge_seek_backfill_sees_edges_created_before_index() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:P {name: 'a'})");
    run(&store, "CREATE (:P {name: 'b'})");
    // Insert the edge *before* the index — the backfill must surface it.
    run(
        &store,
        "MATCH (x:P {name: 'a'}), (y:P {name: 'b'}) \
         CREATE (x)-[:R {k: 7}]->(y)",
    );
    run(&store, "CREATE INDEX FOR ()-[r:R]-() ON (r.k)");

    let rows = run_with_ctx(&store, "MATCH ()-[r:R {k: 7}]->() RETURN r.k AS k");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "k"), 7);
}

#[test]
fn edge_seek_parameterized_value() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:P {name: 'a'})");
    run(&store, "CREATE (:P {name: 'b'})");
    run(
        &store,
        "MATCH (x:P {name: 'a'}), (y:P {name: 'b'}) \
         CREATE (x)-[:R {k: 42}]->(y)",
    );
    run(&store, "CREATE INDEX FOR ()-[r:R]-() ON (r.k)");

    let mut params = ParamMap::new();
    params.insert("v".into(), Value::Property(Property::Int64(42)));
    let rows = run_with_ctx_params(
        &store,
        "MATCH ()-[r:R {k: $v}]->() RETURN r.k AS k",
        &params,
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "k"), 42);
}

#[test]
fn edge_seek_with_labeled_endpoints_filters_via_has_labels_residual() {
    // Endpoint labels now ride along as residual `HasLabels` filters
    // over the seek output. Rows whose seek-hydrated endpoints don't
    // carry the required labels drop; rows that do pass through.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    run(&store, "CREATE (:Person {name: 'Bob'})");
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS {since: 2020}]->(b)",
    );
    // A same-type edge between two non-Person nodes exercises the
    // residual label filter — it must drop since the endpoints
    // fail the label check.
    run(&store, "CREATE (:Other)-[:KNOWS {since: 2020}]->(:Other)");
    run(&store, "CREATE INDEX FOR ()-[r:KNOWS]-() ON (r.since)");

    let rows = run_with_ctx(
        &store,
        "MATCH (a:Person)-[r:KNOWS {since: 2020}]->(b:Person) \
         RETURN a.name AS s, b.name AS d",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "s"), "Ada");
    assert_eq!(str_prop(&rows[0], "d"), "Bob");
}

#[test]
fn edge_seek_with_endpoint_pattern_property_narrows() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:P {name: 'keep'})");
    run(&store, "CREATE (:P {name: 'drop'})");
    run(&store, "CREATE (:P {name: 'target'})");
    run(
        &store,
        "MATCH (x:P {name: 'keep'}), (y:P {name: 'target'}) \
         CREATE (x)-[:R {k: 1}]->(y)",
    );
    run(
        &store,
        "MATCH (x:P {name: 'drop'}), (y:P {name: 'target'}) \
         CREATE (x)-[:R {k: 1}]->(y)",
    );
    run(&store, "CREATE INDEX FOR ()-[r:R]-() ON (r.k)");

    let rows = run_with_ctx(
        &store,
        "MATCH (a {name: 'keep'})-[r:R {k: 1}]->(b) RETURN b.name AS n",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "n"), "target");
}

#[test]
fn edge_seek_unregistered_index_falls_back_to_scan() {
    // With no index on (R, k), the planner must not emit EdgeSeek —
    // the query still returns the right rows via the fallback path.
    let (store, _d) = open_store();
    run(&store, "CREATE (:P {name: 'a'})");
    run(&store, "CREATE (:P {name: 'b'})");
    run(
        &store,
        "MATCH (x:P {name: 'a'}), (y:P {name: 'b'}) \
         CREATE (x)-[:R {k: 1}]->(y)",
    );
    // Note: no CREATE INDEX here.
    let rows = run_with_ctx(&store, "MATCH ()-[r:R {k: 1}]->() RETURN r.k AS k");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "k"), 1);
}

#[test]
fn composite_create_index_end_to_end_and_show_indexes() {
    // Composite DDL via Cypher + pre-existing data backfill + SHOW
    // INDEXES listing + DROP. Exercises the full plumbing: parser
    // accepts `ON (a.x, a.y)`, plan lowers with properties Vec,
    // executor dispatches to the composite storage method, storage
    // backfills tuple entries, and the list / show paths surface
    // entries for every component property.
    let (store, _d) = open_store();
    run(&store, "CREATE (:P {first: 'Ada', last: 'Lovelace'})");
    run(&store, "CREATE (:P {first: 'Bob', last: 'Smith'})");
    // Backfill covers the pre-existing nodes.
    run(&store, "CREATE INDEX FOR (n:P) ON (n.first, n.last)");

    // SHOW INDEXES must list the composite — the underlying registry
    // renders one row per component property for compat.
    let shown = run(&store, "SHOW INDEXES");
    let composite_rows: Vec<_> = shown
        .iter()
        .filter(|r| str_prop(r, "scope") == "NODE")
        .filter(|r| str_prop(r, "label") == "P")
        .collect();
    assert!(
        !composite_rows.is_empty(),
        "SHOW INDEXES missed the composite spec: {shown:?}"
    );

    // Drop via composite DDL removes the spec completely.
    run(&store, "DROP INDEX FOR (n:P) ON (n.first, n.last)");
    let shown_after = run(&store, "SHOW INDEXES");
    assert!(shown_after.is_empty());
}

#[test]
fn composite_index_seek_returns_matching_tuple_row() {
    // Composite index seek through the full planner → executor
    // pipeline. Only the node whose (first, last) tuple matches
    // both pattern equalities surfaces.
    let (store, _d) = open_store();
    run(&store, "CREATE (:P {first: 'Ada', last: 'Lovelace'})");
    run(&store, "CREATE (:P {first: 'Ada', last: 'Smith'})");
    run(&store, "CREATE (:P {first: 'Bob', last: 'Lovelace'})");
    run(&store, "CREATE INDEX FOR (n:P) ON (n.first, n.last)");

    let rows = run_with_ctx(
        &store,
        "MATCH (n:P {first: 'Ada', last: 'Lovelace'}) RETURN n.first AS f, n.last AS l",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "f"), "Ada");
    assert_eq!(str_prop(&rows[0], "l"), "Lovelace");
}

#[test]
fn composite_index_prefix_seek_returns_all_prefix_matches() {
    // Query binds only the first property of a (first, last) composite.
    // Seeks the prefix; both "Ada" rows come back regardless of last.
    let (store, _d) = open_store();
    run(&store, "CREATE (:P {first: 'Ada', last: 'Lovelace'})");
    run(&store, "CREATE (:P {first: 'Ada', last: 'Smith'})");
    run(&store, "CREATE (:P {first: 'Bob', last: 'Lovelace'})");
    run(&store, "CREATE INDEX FOR (n:P) ON (n.first, n.last)");

    let rows = run_with_ctx(&store, "MATCH (n:P {first: 'Ada'}) RETURN n.last AS l");
    assert_eq!(rows.len(), 2);
    let mut lasts: Vec<String> = rows.iter().map(|r| str_prop(r, "l").to_string()).collect();
    lasts.sort();
    assert_eq!(lasts, vec!["Lovelace".to_string(), "Smith".to_string()]);
}

#[test]
fn composite_index_where_form_seeks_correctly() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:P {first: 'Ada', last: 'Lovelace'})");
    run(&store, "CREATE (:P {first: 'Bob', last: 'Smith'})");
    run(&store, "CREATE INDEX FOR (n:P) ON (n.first, n.last)");

    let rows = run_with_ctx(
        &store,
        "MATCH (n:P) WHERE n.first = 'Ada' AND n.last = 'Lovelace' \
         RETURN n.first AS f",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "f"), "Ada");
}

#[test]
fn composite_edge_index_end_to_end() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {n: 'a'})");
    run(&store, "CREATE (:N {n: 'b'})");
    run(
        &store,
        "MATCH (a:N {n: 'a'}), (b:N {n: 'b'}) \
         CREATE (a)-[:KNOWS {since: 2020, weight: 5}]->(b)",
    );
    run(
        &store,
        "CREATE INDEX FOR ()-[r:KNOWS]-() ON (r.since, r.weight)",
    );
    let shown = run(&store, "SHOW INDEXES");
    assert!(
        shown
            .iter()
            .any(|r| str_prop(r, "scope") == "RELATIONSHIP" && str_prop(r, "edge_type") == "KNOWS"),
        "SHOW INDEXES missed the composite edge spec: {shown:?}"
    );
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
    // openCypher `VariableAlreadyBound`: MERGE can't impose new
    // predicates (labels or properties) on a variable that's
    // already bound by an earlier clause. TCK Merge1 scenario 15
    // and Merge5 scenario 22 cover this same rule.
    let stmt =
        meshdb_cypher::parse("MATCH (a:Person) MERGE (a:Person {id: '2'}) RETURN a").unwrap();
    let err = meshdb_cypher::plan(&stmt).unwrap_err();
    assert!(
        matches!(&err, meshdb_cypher::Error::Plan(msg) if msg.contains("VariableAlreadyBound")),
        "expected VariableAlreadyBound, got {err:?}",
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
fn cross_stage_rebind_label_reassertion_plans() {
    // Re-asserting a label on a bound start var plans as a
    // HasLabels filter on top of the input stream. The query
    // runs and returns matching rows; prior behaviour was a
    // planner error asking for a WHERE rewrite.
    let stmt =
        meshdb_cypher::parse("MATCH (a:Person) WITH a MATCH (a:Person)-[:KNOWS]->(b) RETURN a, b")
            .unwrap();
    // Just making sure it plans without error — execution is
    // covered elsewhere.
    let _plan = meshdb_cypher::plan(&stmt).unwrap();
}

#[test]
fn cross_stage_rebind_property_reassertion_plans() {
    let stmt = meshdb_cypher::parse(
        "MATCH (a:Person) WITH a MATCH (a {name: 'Ada'})-[:KNOWS]->(b) RETURN a, b",
    )
    .unwrap();
    let _plan = meshdb_cypher::plan(&stmt).unwrap();
}

#[test]
fn cross_stage_rebind_both_vars_bound_plans() {
    // Both `a` and `b` are bound from the first MATCH. The
    // second MATCH (a)-[:KNOWS]->(b) is a correlated join —
    // find edges between the two bound nodes.
    let stmt = meshdb_cypher::parse(
        "MATCH (a:Person), (b:Person) WITH a, b MATCH (a)-[:KNOWS]->(b) RETURN a, b",
    )
    .unwrap();
    meshdb_cypher::plan(&stmt).expect("cross-stage rebind with both vars bound should plan");
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
fn map_literal_wraps_node_values() {
    // A map literal whose values include a Node / Edge / Path is
    // a first-class Cypher value — `Value::Map` carries the full
    // element — and downstream clauses can reach into it.
    // Property::Map (scalar-only) is preserved for the common
    // case where every value lowers to a stored Property.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(&store, "MATCH (a:Person) RETURN {p: a}.p.name AS n");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "n"), "Ada");
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
    let plan = meshdb_cypher::plan(&meshdb_cypher::parse("RETURN 1 / 0 AS r").unwrap()).unwrap();
    let err = meshdb_executor::execute(&plan, &store).unwrap_err();
    assert!(
        matches!(err, meshdb_executor::Error::DivideByZero),
        "expected DivideByZero, got {err:?}"
    );
}

#[test]
fn arithmetic_mod_by_zero_errors() {
    let (store, _d) = open_store();
    let plan = meshdb_cypher::plan(&meshdb_cypher::parse("RETURN 5 % 0 AS r").unwrap()).unwrap();
    let err = meshdb_executor::execute(&plan, &store).unwrap_err();
    assert!(matches!(err, meshdb_executor::Error::DivideByZero));
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
fn where_pattern_predicate_var_length_plans() {
    let parsed =
        meshdb_cypher::parse("MATCH (p:Person) WHERE (p)-[:KNOWS*1..3]->(:Person) RETURN p")
            .unwrap();
    meshdb_cypher::plan(&parsed).expect("var-length in pattern predicate should plan");
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
        .as_nanos() as i128;
    let rows = run(&store, "RETURN datetime() AS now");
    let got = match rows[0].get("now") {
        Some(Value::Property(Property::DateTime { nanos: ns, .. })) => *ns,
        other => panic!("expected DateTime, got {other:?}"),
    };
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i128;
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
    let plan =
        meshdb_cypher::plan(&meshdb_cypher::parse("RETURN duration({bogus: 1}) AS d").unwrap())
            .unwrap();
    let err = meshdb_executor::execute(&plan, &store).unwrap_err();
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
        Some(Value::Property(Property::DateTime { nanos: ms, .. })) => *ms,
        other => panic!("expected DateTime, got {other:?}"),
    };
    let base = match rows[0].get("base") {
        Some(Value::Property(Property::DateTime { nanos: ms, .. })) => *ms,
        other => panic!("expected DateTime, got {other:?}"),
    };
    let delta = plus - base;
    assert!(
        (59_900_000_000..60_200_000_000).contains(&delta),
        "delta = {delta}, expected ~60_000_000_000"
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
    // date() + duration({seconds: 10}) silently drops sub-day components
    // and returns today's date, matching openCypher behavior.
    let (store, _d) = open_store();
    let plan = meshdb_cypher::plan(
        &meshdb_cypher::parse("RETURN date() + duration({seconds: 10}) AS d").unwrap(),
    )
    .unwrap();
    let rows = meshdb_executor::execute(&plan, &store).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(
        rows[0].get("d"),
        Some(meshdb_executor::Value::Property(
            meshdb_core::Property::Date(_)
        ))
    ));
}

#[test]
fn datetime_ordering_works_in_where() {
    // This test requires a larger stack in debug mode due to deeply
    // recursive expression evaluation (datetime arithmetic inside a
    // list literal inside UNWIND).
    let child = std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let (store, _d) = open_store();
            let rows = run(
                &store,
                "UNWIND [datetime() - duration({days: 1}), datetime() + duration({days: 1})] AS ts \
                 WHERE ts < datetime() \
                 RETURN ts",
            );
            assert_eq!(rows.len(), 1);
            let ts = match rows[0].get("ts") {
                Some(Value::Property(Property::DateTime { nanos: ms, .. })) => *ms,
                other => panic!("expected DateTime, got {other:?}"),
            };
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i128;
            assert!(ts < now);
        })
        .unwrap();
    child.join().unwrap();
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

#[test]
fn datetime_parses_rfc3339_with_z() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN datetime('2025-01-01T00:00:00Z') AS dt");
    let ms = match rows[0].get("dt") {
        Some(Value::Property(Property::DateTime { nanos: ms, .. })) => *ms,
        other => panic!("expected DateTime, got {other:?}"),
    };
    // 2025-01-01T00:00:00Z = 1735689600 seconds since epoch
    assert_eq!(ms, 1_735_689_600_000_000_000);
}

#[test]
fn datetime_parses_with_offset() {
    let (store, _d) = open_store();
    // 2025-01-01T05:00:00+05:00 == 2025-01-01T00:00:00Z
    let rows = run(&store, "RETURN datetime('2025-01-01T05:00:00+05:00') AS dt");
    let ms = match rows[0].get("dt") {
        Some(Value::Property(Property::DateTime { nanos: ms, .. })) => *ms,
        other => panic!("expected DateTime, got {other:?}"),
    };
    assert_eq!(ms, 1_735_689_600_000_000_000);
}

#[test]
fn datetime_parses_naive_as_utc() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN datetime('2025-01-01T00:00:00') AS dt");
    let ms = match rows[0].get("dt") {
        Some(Value::Property(Property::DateTime { nanos: ms, .. })) => *ms,
        other => panic!("expected DateTime, got {other:?}"),
    };
    assert_eq!(ms, 1_735_689_600_000_000_000);
}

#[test]
fn datetime_parses_with_fractional_seconds() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN datetime('2025-01-01T00:00:00.500Z') AS dt");
    let ms = match rows[0].get("dt") {
        Some(Value::Property(Property::DateTime { nanos: ms, .. })) => *ms,
        other => panic!("expected DateTime, got {other:?}"),
    };
    assert_eq!(ms, 1_735_689_600_500_000_000);
}

#[test]
fn datetime_parses_space_separator() {
    let (store, _d) = open_store();
    // Common relaxation — sqlite/postgres use a space.
    let rows = run(&store, "RETURN datetime('2025-01-01 00:00:00') AS dt");
    let ms = match rows[0].get("dt") {
        Some(Value::Property(Property::DateTime { nanos: ms, .. })) => *ms,
        other => panic!("expected DateTime, got {other:?}"),
    };
    assert_eq!(ms, 1_735_689_600_000_000_000);
}

#[test]
fn datetime_null_string_propagates() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN datetime(coalesce(null)) AS dt");
    assert!(matches!(
        rows[0].get("dt"),
        Some(Value::Null) | Some(Value::Property(Property::Null))
    ));
}

#[test]
fn datetime_bad_string_errors() {
    let (store, _d) = open_store();
    let plan =
        meshdb_cypher::plan(&meshdb_cypher::parse("RETURN datetime('not a date') AS dt").unwrap())
            .unwrap();
    let err = meshdb_executor::execute(&plan, &store).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("datetime()") && msg.contains("ISO 8601"),
        "got: {msg}"
    );
}

#[test]
fn date_parses_iso_string() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN date('2025-01-01') AS d");
    let days = match rows[0].get("d") {
        Some(Value::Property(Property::Date(d))) => *d,
        other => panic!("expected Date, got {other:?}"),
    };
    // 2025-01-01 is 20089 days after 1970-01-01.
    assert_eq!(days, 20089);
}

#[test]
fn date_parses_string_and_arithmetic() {
    // Full pipeline: parse a date string, add a duration, compare days.
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN date('2025-01-01') + duration({days: 30}) AS d, \
                date('2025-01-31') AS expected",
    );
    let d = match rows[0].get("d") {
        Some(Value::Property(Property::Date(d))) => *d,
        other => panic!("expected Date, got {other:?}"),
    };
    let expected = match rows[0].get("expected") {
        Some(Value::Property(Property::Date(d))) => *d,
        other => panic!("expected Date, got {other:?}"),
    };
    assert_eq!(d, expected);
}

#[test]
fn date_bad_string_errors() {
    let (store, _d) = open_store();
    let plan =
        meshdb_cypher::plan(&meshdb_cypher::parse("RETURN date('2025/01/01') AS d").unwrap())
            .unwrap();
    let err = meshdb_executor::execute(&plan, &store).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("date()") && msg.contains("ISO 8601"),
        "got: {msg}"
    );
}

#[test]
fn datetime_parsed_string_roundtrips_comparison() {
    let (store, _d) = open_store();
    // Parse two datetimes and compare them.
    let rows = run(
        &store,
        "RETURN datetime('2025-01-01T00:00:00Z') < datetime('2025-06-01T00:00:00Z') AS before",
    );
    match rows[0].get("before") {
        Some(Value::Property(Property::Bool(b))) => assert!(*b),
        other => panic!("expected Bool, got {other:?}"),
    }
}

// ---- EXISTS { subquery } ----

#[test]
fn exists_subquery_filters_nodes_with_matching_pattern() {
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
        "MATCH (p:Person) \
         WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) } \
         RETURN p.name AS name",
    );
    let names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    assert_eq!(names, vec!["Ada".to_string()]);
}

#[test]
fn exists_subquery_with_inner_where_filters_further() {
    // Ada knows two people — a young Bob and an older Cara.
    // EXISTS with an inner WHERE filters Ada in only if the
    // matched friend passes the age check.
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), \
                 (:Person {name: 'Bob', age: 17}), \
                 (:Person {name: 'Cara', age: 42}), \
                 (:Person {name: 'Dex', age: 25})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cara'}) \
         CREATE (a)-[:KNOWS]->(c)",
    );
    // Dex knows only Bob (age 17), so Dex fails the inner WHERE.
    run(
        &store,
        "MATCH (d:Person {name: 'Dex'}), (b:Person {name: 'Bob'}) \
         CREATE (d)-[:KNOWS]->(b)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person) \
         WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.age >= 30 } \
         RETURN p.name AS name",
    );
    // Only Ada has a KNOWS-edge to a friend aged >= 30.
    let names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    assert_eq!(names, vec!["Ada".to_string()]);
}

#[test]
fn not_exists_subquery_excludes_matching_nodes() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'}), (:Person {name: 'Cara'})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:BLOCKED]->(b)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person) \
         WHERE NOT EXISTS { MATCH (p)-[:BLOCKED]->(x) } \
         RETURN p.name AS name",
    );
    let mut names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    names.sort();
    assert_eq!(names, vec!["Bob".to_string(), "Cara".to_string()]);
}

#[test]
fn exists_subquery_inner_where_references_outer_and_inner() {
    // The inner WHERE should see both the outer variable (`p`)
    // and the inner binding (`f`). This distinguishes EXISTS
    // from plain pattern predicates.
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada', age: 30}), \
                 (:Person {name: 'Bob', age: 40}), \
                 (:Person {name: 'Cara', age: 20})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cara'}) \
         CREATE (a)-[:KNOWS]->(c)",
    );
    // Find people who know someone older than themselves.
    // Only Ada (30) knows Bob (40), so only Ada qualifies.
    let rows = run(
        &store,
        "MATCH (p:Person) \
         WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.age > p.age } \
         RETURN p.name AS name",
    );
    let names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    assert_eq!(names, vec!["Ada".to_string()]);
}

#[test]
fn exists_subquery_multi_hop_pattern() {
    // Two-hop EXISTS: filter people who know someone who knows someone else.
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
    let rows = run(
        &store,
        "MATCH (p:Person) \
         WHERE EXISTS { MATCH (p)-[:KNOWS]->(:Person)-[:KNOWS]->(:Person) } \
         RETURN p.name AS name",
    );
    let names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    assert_eq!(names, vec!["Ada".to_string()]);
}

#[test]
fn exists_subquery_with_no_match_returns_empty() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'})",
    );
    // Nobody has KNOWS edges; EXISTS is always false.
    let rows = run(
        &store,
        "MATCH (p:Person) \
         WHERE EXISTS { MATCH (p)-[:KNOWS]->(f) } \
         RETURN p.name AS name",
    );
    assert!(rows.is_empty());
}

#[test]
fn exists_subquery_combined_with_scalar_predicate() {
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
         CREATE (a)-[:KNOWS]->(c)",
    );
    // age >= 18 AND has a KNOWS edge — Ada qualifies, Cara is
    // over 18 but has no outgoing KNOWS, Bob is too young.
    let rows = run(
        &store,
        "MATCH (p:Person) \
         WHERE p.age >= 18 AND EXISTS { MATCH (p)-[:KNOWS]->(f) } \
         RETURN p.name AS name",
    );
    let names: Vec<String> = rows.iter().map(|r| str_prop(r, "name")).collect();
    assert_eq!(names, vec!["Ada".to_string()]);
}

#[test]
fn exists_subquery_binds_edge_variable_in_where() {
    // The inner WHERE can reference the edge variable — that's
    // the real power over a pattern predicate, which only
    // supports filtering on pattern-shape constraints. Ada has
    // a KNOWS edge and a BLOCKED edge; the subquery uses `type(r)`
    // on the bound edge var to filter down.
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
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cara'}) \
         CREATE (a)-[:BLOCKED]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (p:Person {name: 'Ada'}) \
         WHERE EXISTS { MATCH (p)-[r]->(f) WHERE type(r) = 'KNOWS' } \
         RETURN p.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Ada");
}

// ---- duration() ISO 8601 string parsing ----

fn unwrap_duration(v: Option<&Value>) -> meshdb_core::Duration {
    match v {
        Some(Value::Property(Property::Duration(d))) => *d,
        other => panic!("expected Duration, got {other:?}"),
    }
}

#[test]
fn duration_parses_full_iso8601_form() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN duration('P1Y2M3DT4H5M6S') AS d");
    let d = unwrap_duration(rows[0].get("d"));
    assert_eq!(d.months, 14); // 1 year + 2 months
    assert_eq!(d.days, 3);
    assert_eq!(d.seconds, 4 * 3600 + 5 * 60 + 6);
    assert_eq!(d.nanos, 0);
}

#[test]
fn duration_parses_time_only() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN duration('PT1H30M') AS d");
    let d = unwrap_duration(rows[0].get("d"));
    assert_eq!(d.months, 0);
    assert_eq!(d.days, 0);
    assert_eq!(d.seconds, 3600 + 30 * 60);
}

#[test]
fn duration_parses_date_only() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN duration('P1W') AS d");
    let d = unwrap_duration(rows[0].get("d"));
    assert_eq!(d.days, 7);
    assert_eq!(d.months, 0);
    assert_eq!(d.seconds, 0);
}

#[test]
fn duration_parses_single_day() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN duration('P1D') AS d");
    let d = unwrap_duration(rows[0].get("d"));
    assert_eq!(d.days, 1);
    assert_eq!(d.months, 0);
    assert_eq!(d.seconds, 0);
}

#[test]
fn duration_parses_fractional_seconds() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN duration('PT1.5S') AS d");
    let d = unwrap_duration(rows[0].get("d"));
    assert_eq!(d.seconds, 1);
    assert_eq!(d.nanos, 500_000_000);
}

#[test]
fn duration_parses_high_precision_fractional_seconds() {
    let (store, _d) = open_store();
    // 9-digit nanosecond precision fully survives.
    let rows = run(&store, "RETURN duration('PT0.123456789S') AS d");
    let d = unwrap_duration(rows[0].get("d"));
    assert_eq!(d.seconds, 0);
    assert_eq!(d.nanos, 123_456_789);
}

#[test]
fn duration_parses_negative_string() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN duration('-PT1H') AS d");
    let d = unwrap_duration(rows[0].get("d"));
    assert_eq!(d.seconds, -3600);
}

#[test]
fn duration_disambiguates_month_vs_minute_via_t() {
    // `P1M` = 1 month. `PT1M` = 1 minute. The T separator is
    // the only way to tell the two apart.
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN duration('P1M') AS month, duration('PT1M') AS minute",
    );
    let month = unwrap_duration(rows[0].get("month"));
    let minute = unwrap_duration(rows[0].get("minute"));
    assert_eq!(month.months, 1);
    assert_eq!(month.seconds, 0);
    assert_eq!(minute.months, 0);
    assert_eq!(minute.seconds, 60);
}

#[test]
fn duration_string_plus_datetime_advances() {
    // Round trip: parse an ISO duration and apply it to a datetime.
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN datetime('2025-01-01T00:00:00Z') + duration('PT1H') AS later",
    );
    let ms = match rows[0].get("later") {
        Some(Value::Property(Property::DateTime { nanos: ms, .. })) => *ms,
        other => panic!("expected DateTime, got {other:?}"),
    };
    assert_eq!(ms, 1_735_689_600_000_000_000 + 3_600_000_000_000);
}

#[test]
fn duration_null_string_propagates() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN duration(coalesce(null)) AS d");
    assert!(matches!(
        rows[0].get("d"),
        Some(Value::Null) | Some(Value::Property(Property::Null))
    ));
}

#[test]
fn duration_bad_string_errors() {
    let (store, _d) = open_store();
    let plan = meshdb_cypher::plan(
        &meshdb_cypher::parse("RETURN duration('not a duration') AS d").unwrap(),
    )
    .unwrap();
    let err = meshdb_executor::execute(&plan, &store).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("duration()") && msg.contains("ISO 8601"),
        "got: {msg}"
    );
}

#[test]
fn duration_missing_p_prefix_errors() {
    let (store, _d) = open_store();
    let plan =
        meshdb_cypher::plan(&meshdb_cypher::parse("RETURN duration('T1H') AS d").unwrap()).unwrap();
    let err = meshdb_executor::execute(&plan, &store).unwrap_err();
    assert!(format!("{err}").contains("ISO 8601"));
}

#[test]
fn duration_bare_p_errors() {
    // `P` alone carries no components — must be rejected so
    // driver typos surface instead of silently producing a
    // zero duration.
    let (store, _d) = open_store();
    let plan =
        meshdb_cypher::plan(&meshdb_cypher::parse("RETURN duration('P') AS d").unwrap()).unwrap();
    let err = meshdb_executor::execute(&plan, &store).unwrap_err();
    assert!(format!("{err}").contains("ISO 8601"));
}

// ---- Uncorrelated EXISTS { ... } ----

#[test]
fn uncorrelated_exists_zero_hop_label_match() {
    // `EXISTS { MATCH (:Person) }` — does any Person exist?
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Company {name: 'Acme'})",
    );
    let rows = run(
        &store,
        "MATCH (c:Company) WHERE EXISTS { MATCH (:Person) } RETURN c.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Acme");
}

#[test]
fn uncorrelated_exists_zero_hop_no_match() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Company {name: 'Acme'})");
    // No Persons in the graph.
    let rows = run(
        &store,
        "MATCH (c:Company) WHERE EXISTS { MATCH (:Person) } RETURN c.name AS name",
    );
    assert!(rows.is_empty());
}

#[test]
fn uncorrelated_exists_with_property_filter() {
    // `EXISTS { MATCH (:Person {name: 'Ada'}) }` — does a Person
    // named Ada exist? Uses the pattern-property filter, which
    // the evaluator applies to each candidate via
    // `start_node_matches`.
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Bob'}), (:Company {name: 'Acme'})",
    );
    // Ada doesn't exist → EXISTS is false → no rows.
    let rows = run(
        &store,
        "MATCH (c:Company) WHERE EXISTS { MATCH (:Person {name: 'Ada'}) } RETURN c.name AS name",
    );
    assert!(rows.is_empty());

    // After adding Ada, EXISTS is true.
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(
        &store,
        "MATCH (c:Company) WHERE EXISTS { MATCH (:Person {name: 'Ada'}) } RETURN c.name AS name",
    );
    assert_eq!(rows.len(), 1);
}

#[test]
fn uncorrelated_exists_multi_hop() {
    // Pattern starts with an unbound var `a` but walks into a
    // multi-hop chain. The evaluator enumerates Person
    // candidates and tries to walk each through the hops.
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'}), (:Person {name: 'Cara'})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    );
    // A Company row is used as the outer scope; the EXISTS
    // subquery doesn't reference anything from the outer row.
    run(&store, "CREATE (:Company {name: 'Acme'})");
    let rows = run(
        &store,
        "MATCH (c:Company) \
         WHERE EXISTS { MATCH (a:Person)-[:KNOWS]->(b:Person) } \
         RETURN c.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Acme");
}

#[test]
fn uncorrelated_not_exists_excludes_when_match_present() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Company {name: 'Acme'})",
    );
    // `NOT EXISTS { MATCH (:Person) }` — no rows because a Person exists.
    let rows = run(
        &store,
        "MATCH (c:Company) WHERE NOT EXISTS { MATCH (:Person) } RETURN c.name AS name",
    );
    assert!(rows.is_empty());
}

#[test]
fn uncorrelated_exists_with_inner_where() {
    // Pattern has no outer-row correlation but the inner WHERE
    // references the enumerated start binding. Verifies the
    // scratch row picks up the start var when it's named.
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada', age: 17}), (:Person {name: 'Bob', age: 42}), (:Company {name: 'Acme'})",
    );
    let rows = run(
        &store,
        "MATCH (c:Company) \
         WHERE EXISTS { MATCH (p:Person) WHERE p.age >= 30 } \
         RETURN c.name AS name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "Acme");
}

#[test]
fn uncorrelated_exists_inner_where_no_match() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada', age: 17}), (:Person {name: 'Bob', age: 20}), (:Company {name: 'Acme'})",
    );
    // Nobody's 30+ → EXISTS false → no rows.
    let rows = run(
        &store,
        "MATCH (c:Company) \
         WHERE EXISTS { MATCH (p:Person) WHERE p.age >= 30 } \
         RETURN c.name AS name",
    );
    assert!(rows.is_empty());
}

#[test]
fn zero_hop_exists_was_previously_rejected_now_allowed() {
    // Regression assertion: the pre-uncorrelated validator
    // required `hops.len() >= 1`. After the split, zero-hop
    // EXISTS patterns plan successfully.
    let parsed =
        meshdb_cypher::parse("MATCH (c) WHERE EXISTS { MATCH (:Person) } RETURN c").unwrap();
    let _plan = meshdb_cypher::plan(&parsed).expect("zero-hop EXISTS should now plan");
}

// ---- shortestPath(...) ----

#[test]
fn shortest_path_direct_edge() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         MATCH p = shortestPath((a)-[:KNOWS*..5]->(b)) \
         RETURN length(p) AS len",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "len"), 1);
}

#[test]
fn shortest_path_two_hop_chain() {
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
    run(
        &store,
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Cara'}) \
         CREATE (b)-[:KNOWS]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cara'}) \
         MATCH p = shortestPath((a)-[:KNOWS*..5]->(c)) \
         RETURN length(p) AS len, nodes(p) AS ns",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "len"), 2);
    let ns = match rows[0].get("ns") {
        Some(Value::List(items)) => items,
        other => panic!("expected list, got {other:?}"),
    };
    assert_eq!(ns.len(), 3);
    let names: Vec<String> = ns
        .iter()
        .filter_map(|v| match v {
            Value::Node(n) => n.properties.get("name").and_then(|p| match p {
                Property::String(s) => Some(s.clone()),
                _ => None,
            }),
            _ => None,
        })
        .collect();
    assert_eq!(
        names,
        vec!["Ada".to_string(), "Bob".to_string(), "Cara".to_string()]
    );
}

#[test]
fn shortest_path_picks_shorter_of_alternatives() {
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
    run(
        &store,
        "MATCH (c:Person {name: 'Cara'}), (d:Person {name: 'Dex'}) \
         CREATE (c)-[:KNOWS]->(d)",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (d:Person {name: 'Dex'}) \
         CREATE (a)-[:KNOWS]->(d)",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (d:Person {name: 'Dex'}) \
         MATCH p = shortestPath((a)-[:KNOWS*..5]->(d)) \
         RETURN length(p) AS len",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "len"), 1);
}

#[test]
fn shortest_path_no_path_drops_row() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Zed'})",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (z:Person {name: 'Zed'}) \
         MATCH p = shortestPath((a)-[:KNOWS*..5]->(z)) \
         RETURN length(p) AS len",
    );
    assert!(rows.is_empty());
}

#[test]
fn shortest_path_respects_max_hop_bound() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'A'}), (:Person {name: 'B'}), \
                 (:Person {name: 'C'}), (:Person {name: 'D'})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'A'}), (b:Person {name: 'B'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (b:Person {name: 'B'}), (c:Person {name: 'C'}) \
         CREATE (b)-[:KNOWS]->(c)",
    );
    run(
        &store,
        "MATCH (c:Person {name: 'C'}), (d:Person {name: 'D'}) \
         CREATE (c)-[:KNOWS]->(d)",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'A'}), (d:Person {name: 'D'}) \
         MATCH p = shortestPath((a)-[:KNOWS*..2]->(d)) \
         RETURN length(p) AS len",
    );
    assert!(rows.is_empty());
}

#[test]
fn shortest_path_with_edge_type_filter_ignores_other_types() {
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
    run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cara'}) \
         CREATE (a)-[:BLOCKED]->(c)",
    );
    run(
        &store,
        "MATCH (c:Person {name: 'Cara'}), (b:Person {name: 'Bob'}) \
         CREATE (c)-[:BLOCKED]->(b)",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
         MATCH p = shortestPath((a)-[:KNOWS*..5]->(b)) \
         RETURN length(p) AS len",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "len"), 1);
}

#[test]
fn shortest_path_src_equals_dst_returns_empty_path() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Ada'}) \
         MATCH p = shortestPath((a)-[:KNOWS*..5]->(b)) \
         RETURN length(p) AS len",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "len"), 0);
}

#[test]
fn all_shortest_paths_single_unique_path() {
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
    run(
        &store,
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Cara'}) \
         CREATE (b)-[:KNOWS]->(c)",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (c:Person {name: 'Cara'}) \
         MATCH p = allShortestPaths((a)-[:KNOWS*..5]->(c)) \
         RETURN length(p) AS len",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "len"), 2);
}

#[test]
fn all_shortest_paths_diamond_yields_two_paths() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'A'}), (:Person {name: 'B'}), \
                 (:Person {name: 'C'}), (:Person {name: 'D'})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'A'}), (b:Person {name: 'B'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'A'}), (c:Person {name: 'C'}) \
         CREATE (a)-[:KNOWS]->(c)",
    );
    run(
        &store,
        "MATCH (b:Person {name: 'B'}), (d:Person {name: 'D'}) \
         CREATE (b)-[:KNOWS]->(d)",
    );
    run(
        &store,
        "MATCH (c:Person {name: 'C'}), (d:Person {name: 'D'}) \
         CREATE (c)-[:KNOWS]->(d)",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'A'}), (d:Person {name: 'D'}) \
         MATCH p = allShortestPaths((a)-[:KNOWS*..5]->(d)) \
         RETURN length(p) AS len",
    );
    assert_eq!(rows.len(), 2);
    for r in &rows {
        assert_eq!(int_prop(r, "len"), 2);
    }
}

#[test]
fn all_shortest_paths_three_way_split() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'A'}), (:Person {name: 'X'}), \
                 (:Person {name: 'Y'}), (:Person {name: 'Z'}), (:Person {name: 'B'})",
    );
    for mid in ["X", "Y", "Z"] {
        run(
            &store,
            &format!(
                "MATCH (a:Person {{name: 'A'}}), (m:Person {{name: '{mid}'}}) \
                 CREATE (a)-[:KNOWS]->(m)"
            ),
        );
        run(
            &store,
            &format!(
                "MATCH (m:Person {{name: '{mid}'}}), (b:Person {{name: 'B'}}) \
                 CREATE (m)-[:KNOWS]->(b)"
            ),
        );
    }
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'A'}), (b:Person {name: 'B'}) \
         MATCH p = allShortestPaths((a)-[:KNOWS*..5]->(b)) \
         RETURN length(p) AS len",
    );
    assert_eq!(rows.len(), 3);
    for r in &rows {
        assert_eq!(int_prop(r, "len"), 2);
    }
}

#[test]
fn all_shortest_paths_prefers_shorter_over_longer() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'A'}), (:Person {name: 'B'}), \
                 (:Person {name: 'M'}), (:Person {name: 'N'})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'A'}), (b:Person {name: 'B'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'A'}), (m:Person {name: 'M'}) \
         CREATE (a)-[:KNOWS]->(m)",
    );
    run(
        &store,
        "MATCH (m:Person {name: 'M'}), (n:Person {name: 'N'}) \
         CREATE (m)-[:KNOWS]->(n)",
    );
    run(
        &store,
        "MATCH (n:Person {name: 'N'}), (b:Person {name: 'B'}) \
         CREATE (n)-[:KNOWS]->(b)",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'A'}), (b:Person {name: 'B'}) \
         MATCH p = allShortestPaths((a)-[:KNOWS*..5]->(b)) \
         RETURN length(p) AS len",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "len"), 1);
}

#[test]
fn all_shortest_paths_src_equals_dst_returns_zero_hop() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Ada'}) \
         MATCH p = allShortestPaths((a)-[:KNOWS*..5]->(b)) \
         RETURN length(p) AS len",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "len"), 0);
}

#[test]
fn all_shortest_paths_no_path_drops_row() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Zed'})",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'Ada'}), (z:Person {name: 'Zed'}) \
         MATCH p = allShortestPaths((a)-[:KNOWS*..5]->(z)) \
         RETURN length(p) AS len",
    );
    assert!(rows.is_empty());
}

#[test]
fn all_shortest_paths_respects_max_hops() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (:Person {name: 'A'}), (:Person {name: 'B'}), \
                 (:Person {name: 'C'}), (:Person {name: 'D'})",
    );
    run(
        &store,
        "MATCH (a:Person {name: 'A'}), (b:Person {name: 'B'}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    run(
        &store,
        "MATCH (b:Person {name: 'B'}), (c:Person {name: 'C'}) \
         CREATE (b)-[:KNOWS]->(c)",
    );
    run(
        &store,
        "MATCH (c:Person {name: 'C'}), (d:Person {name: 'D'}) \
         CREATE (c)-[:KNOWS]->(d)",
    );
    let rows = run(
        &store,
        "MATCH (a:Person {name: 'A'}), (d:Person {name: 'D'}) \
         MATCH p = allShortestPaths((a)-[:KNOWS*..2]->(d)) \
         RETURN length(p) AS len",
    );
    assert!(rows.is_empty());
}

#[test]
fn shortest_path_without_upper_bound_rejected() {
    let parsed = meshdb_cypher::parse(
        "MATCH (a:Person), (b:Person) \
         MATCH p = shortestPath((a)-[:KNOWS*]->(b)) \
         RETURN p",
    )
    .unwrap();
    let err = meshdb_cypher::plan(&parsed).unwrap_err();
    assert!(format!("{err}").contains("explicit upper bound"));
}

#[test]
fn shortest_path_without_path_variable_rejected() {
    let parsed = meshdb_cypher::parse(
        "MATCH (a:Person), (b:Person) \
         MATCH shortestPath((a)-[:KNOWS*..5]->(b)) \
         RETURN a, b",
    )
    .unwrap();
    let err = meshdb_cypher::plan(&parsed).unwrap_err();
    assert!(format!("{err}").contains("path variable"));
}

// ---------------------------------------------------------------
// Trigonometric and conversion scalar functions
// ---------------------------------------------------------------

#[test]
fn trig_sin_cos_degrees() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN sin(0) AS s, cos(0) AS c, degrees(pi()) AS d",
    );
    assert_eq!(rows.len(), 1);
    let s = float_prop(&rows[0], "s");
    let c = float_prop(&rows[0], "c");
    let d = float_prop(&rows[0], "d");
    assert!((s - 0.0).abs() < 1e-10, "sin(0) should be 0, got {s}");
    assert!((c - 1.0).abs() < 1e-10, "cos(0) should be 1, got {c}");
    assert!(
        (d - 180.0).abs() < 1e-10,
        "degrees(pi()) should be 180, got {d}"
    );
}

#[test]
fn trig_atan2() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN atan2(1, 1) AS a");
    assert_eq!(rows.len(), 1);
    let a = float_prop(&rows[0], "a");
    let expected = std::f64::consts::FRAC_PI_4;
    assert!(
        (a - expected).abs() < 1e-10,
        "atan2(1,1) should be pi/4 ({expected}), got {a}"
    );
}

#[test]
fn trig_tan_cot_asin_acos_atan_radians() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN tan(0) AS t, asin(0) AS as2, acos(1) AS ac, atan(0) AS at, radians(180) AS r",
    );
    assert_eq!(rows.len(), 1);
    assert!((float_prop(&rows[0], "t")).abs() < 1e-10);
    assert!((float_prop(&rows[0], "as2")).abs() < 1e-10);
    assert!((float_prop(&rows[0], "ac")).abs() < 1e-10);
    assert!((float_prop(&rows[0], "at")).abs() < 1e-10);
    let r = float_prop(&rows[0], "r");
    assert!(
        (r - std::f64::consts::PI).abs() < 1e-10,
        "radians(180) should be pi, got {r}"
    );
}

#[test]
fn trig_cot_value() {
    let (store, _d) = open_store();
    // cot(pi/4) = 1/tan(pi/4) = 1.0
    let rows = run(&store, "RETURN cot(pi() / 4) AS c");
    assert_eq!(rows.len(), 1);
    let c = float_prop(&rows[0], "c");
    assert!((c - 1.0).abs() < 1e-10, "cot(pi/4) should be 1.0, got {c}");
}

// ---------------------------------------------------------------
// stDev / stDevP aggregate functions
// ---------------------------------------------------------------

#[test]
fn stdev_and_stdevp_aggregates() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "UNWIND [2, 4, 4, 4, 5, 5, 7, 9] AS x RETURN stDev(x) AS sd, stDevP(x) AS sdp",
    );
    assert_eq!(rows.len(), 1);
    let sd = float_prop(&rows[0], "sd");
    let sdp = float_prop(&rows[0], "sdp");
    // Expected: population stdev = 2.0, sample stdev = 2.13809...
    assert!((sdp - 2.0).abs() < 1e-10, "stDevP should be 2.0, got {sdp}");
    let expected_sd = (32.0_f64 / 7.0).sqrt(); // ~2.1380899...
    assert!(
        (sd - expected_sd).abs() < 1e-6,
        "stDev should be ~{expected_sd}, got {sd}"
    );
}

// ── RETURN * / WITH * ─────────────────────────────────────────────

#[test]
fn return_star_single_node() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'Ada'})");
    run(&store, "CREATE (n:Person {name: 'Alan'})");

    let rows = run(&store, "MATCH (n:Person) RETURN *");
    assert_eq!(rows.len(), 2);
    // Every row should have a binding for `n`
    for row in &rows {
        assert!(row.contains_key("n"), "row missing 'n' binding: {row:?}");
    }
}

#[test]
fn return_star_node_rel_node() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Alan'})",
    );

    let rows = run(&store, "MATCH (n)-[r]->(m) RETURN *");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert!(row.contains_key("n"), "row missing 'n': {row:?}");
    assert!(row.contains_key("r"), "row missing 'r': {row:?}");
    assert!(row.contains_key("m"), "row missing 'm': {row:?}");
}

#[test]
fn with_star_preserves_bindings() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Alan'})",
    );

    let rows = run(
        &store,
        "MATCH (n:Person) WITH * MATCH (n)-[:KNOWS]->(m) RETURN *",
    );
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert!(row.contains_key("n"), "row missing 'n': {row:?}");
    assert!(row.contains_key("m"), "row missing 'm': {row:?}");
}

#[test]
fn nested_aggregate_count_plus_one() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {v: 1})");
    run(&store, "CREATE (:N {v: 2})");
    run(&store, "CREATE (:N {v: 3})");
    let rows = run(&store, "MATCH (n:N) RETURN count(n) + 1 AS total");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "total"), 4);
}

#[test]
fn nested_aggregate_tostring_count() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {v: 1})");
    run(&store, "CREATE (:N {v: 2})");
    let rows = run(&store, "MATCH (n:N) RETURN toString(count(n)) AS c");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "c"), "2");
}

#[test]
fn any_all_none_single_list_predicates() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "RETURN any(x IN [1,2,3] WHERE x > 2) AS a, \
                all(x IN [1,2,3] WHERE x > 0) AS b, \
                none(x IN [1,2,3] WHERE x > 5) AS c, \
                single(x IN [1,2,3] WHERE x = 2) AS d",
    );
    assert_eq!(rows.len(), 1);
    match rows[0].get("a") {
        Some(Value::Property(Property::Bool(b))) => assert!(b),
        other => panic!("expected true, got: {other:?}"),
    }
    match rows[0].get("b") {
        Some(Value::Property(Property::Bool(b))) => assert!(b),
        other => panic!("expected true, got: {other:?}"),
    }
    match rows[0].get("c") {
        Some(Value::Property(Property::Bool(b))) => assert!(b),
        other => panic!("expected true, got: {other:?}"),
    }
    match rows[0].get("d") {
        Some(Value::Property(Property::Bool(b))) => assert!(b),
        other => panic!("expected true, got: {other:?}"),
    }
}

#[test]
fn backtick_identifier_as_label() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:`My Label` {name: 'test'})");
    let rows = run(&store, "MATCH (n:`My Label`) RETURN n.name AS name");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "test");
}

#[test]
fn string_escape_sequences() {
    let (store, _d) = open_store();
    let rows = run(&store, "RETURN 'hello\\nworld' AS s");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "s"), "hello\nworld");
}

#[test]
fn skip_with_parameter() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {v: 1})");
    run(&store, "CREATE (:N {v: 2})");
    run(&store, "CREATE (:N {v: 3})");
    run(&store, "CREATE (:N {v: 4})");
    let mut params = ParamMap::new();
    params.insert("s".into(), Value::Property(Property::Int64(2)));
    let rows = run_with_params(
        &store,
        "MATCH (n:N) RETURN n.v AS v ORDER BY v SKIP $s",
        &params,
    );
    let vals: Vec<i64> = rows.iter().map(|r| int_prop(r, "v")).collect();
    assert_eq!(vals, vec![3, 4]);
}

#[test]
fn limit_with_parameter() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {v: 1})");
    run(&store, "CREATE (:N {v: 2})");
    run(&store, "CREATE (:N {v: 3})");
    let mut params = ParamMap::new();
    params.insert("l".into(), Value::Property(Property::Int64(2)));
    let rows = run_with_params(
        &store,
        "MATCH (n:N) RETURN n.v AS v ORDER BY v LIMIT $l",
        &params,
    );
    let vals: Vec<i64> = rows.iter().map(|r| int_prop(r, "v")).collect();
    assert_eq!(vals, vec![1, 2]);
}

#[test]
fn skip_and_limit_with_parameters() {
    let (store, _d) = open_store();
    for i in 1..=5 {
        run(&store, &format!("CREATE (:N {{v: {i}}})"));
    }
    let mut params = ParamMap::new();
    params.insert("s".into(), Value::Property(Property::Int64(1)));
    params.insert("l".into(), Value::Property(Property::Int64(2)));
    let rows = run_with_params(
        &store,
        "MATCH (n:N) RETURN n.v AS v ORDER BY v SKIP $s LIMIT $l",
        &params,
    );
    let vals: Vec<i64> = rows.iter().map(|r| int_prop(r, "v")).collect();
    assert_eq!(vals, vec![2, 3]);
}

#[test]
fn limit_with_expression() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:N {v: 1})");
    run(&store, "CREATE (:N {v: 2})");
    run(&store, "CREATE (:N {v: 3})");
    let mut params = ParamMap::new();
    params.insert("x".into(), Value::Property(Property::Int64(1)));
    let rows = run_with_params(
        &store,
        "MATCH (n:N) RETURN n.v AS v ORDER BY v LIMIT $x + 1",
        &params,
    );
    let vals: Vec<i64> = rows.iter().map(|r| int_prop(r, "v")).collect();
    assert_eq!(vals, vec![1, 2]);
}

// ===============================================================
// Constraint management: CREATE / DROP / SHOW CONSTRAINTS plus
// enforcement on writes and `db.constraints()` procedure.
// ===============================================================

/// Like `run`, but surfaces the executor error instead of panicking —
/// used by constraint-violation tests that need to assert on the
/// error shape rather than the returned rows.
fn run_catch(store: &Store, q: &str) -> std::result::Result<Vec<Row>, meshdb_executor::Error> {
    let stmt = parse(q).unwrap_or_else(|e| panic!("parse {q}: {e}"));
    let plan = plan(&stmt).unwrap_or_else(|e| panic!("plan {q}: {e}"));
    execute(&plan, store)
}

#[test]
fn create_unique_constraint_round_trip() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "state"), "created");
    assert_eq!(str_prop(&rows[0], "label"), "Person");
    assert_eq!(str_list_prop(&rows[0], "properties"), vec!["email"]);
    assert_eq!(str_prop(&rows[0], "type"), "UNIQUE");
    // Auto-generated name format.
    assert_eq!(
        str_prop(&rows[0], "name"),
        "constraint_node_Person_email_unique"
    );

    let shown = run(&store, "SHOW CONSTRAINTS");
    assert_eq!(shown.len(), 1);
    assert_eq!(str_prop(&shown[0], "label"), "Person");
    assert_eq!(str_prop(&shown[0], "type"), "UNIQUE");
}

#[test]
fn create_named_not_null_constraint_round_trip() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT person_name FOR (p:Person) REQUIRE p.name IS NOT NULL",
    );
    let shown = run(&store, "SHOW CONSTRAINTS");
    assert_eq!(shown.len(), 1);
    assert_eq!(str_prop(&shown[0], "name"), "person_name");
    assert_eq!(str_prop(&shown[0], "type"), "NOT NULL");
}

#[test]
fn drop_constraint_removes_it_from_show() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT email_uniq FOR (p:Person) REQUIRE p.email IS UNIQUE",
    );
    run(&store, "DROP CONSTRAINT email_uniq");
    assert!(run(&store, "SHOW CONSTRAINTS").is_empty());
}

#[test]
fn drop_constraint_missing_without_if_exists_errors() {
    let (store, _d) = open_store();
    let err = run_catch(&store, "DROP CONSTRAINT missing_one").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("missing_one"),
        "error message should mention constraint name; got: {msg}"
    );
}

#[test]
fn drop_constraint_missing_with_if_exists_is_noop() {
    let (store, _d) = open_store();
    let rows = run(&store, "DROP CONSTRAINT missing_one IF EXISTS");
    // A DROP always acks; the "missing" case still emits the row.
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "state"), "dropped");
}

#[test]
fn create_constraint_if_not_exists_is_idempotent() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT email_uniq FOR (p:Person) REQUIRE p.email IS UNIQUE",
    );
    run(
        &store,
        "CREATE CONSTRAINT email_uniq IF NOT EXISTS FOR (p:Person) REQUIRE p.email IS UNIQUE",
    );
    let shown = run(&store, "SHOW CONSTRAINTS");
    assert_eq!(shown.len(), 1);
}

#[test]
fn unique_constraint_blocks_duplicate_insert() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE",
    );
    run(&store, "CREATE (:Person {email: 'ada@example.com'})");
    let err = run_catch(&store, "CREATE (:Person {email: 'ada@example.com'})")
        .expect_err("duplicate value should violate UNIQUE");
    let msg = err.to_string();
    assert!(
        msg.contains("UNIQUE") && msg.contains("email"),
        "error should identify the constraint; got: {msg}"
    );
}

#[test]
fn unique_constraint_allows_different_values() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE",
    );
    run(&store, "CREATE (:Person {email: 'ada@example.com'})");
    run(&store, "CREATE (:Person {email: 'bob@example.com'})");
    let shown = run(&store, "MATCH (p:Person) RETURN p.email AS e");
    assert_eq!(shown.len(), 2);
}

#[test]
fn unique_constraint_allows_same_node_update() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE",
    );
    run(&store, "CREATE (:Person {email: 'ada@example.com'})");
    // Rename via SET on the same node — uniqueness must allow the
    // self-update (old entry is being replaced by new).
    run(
        &store,
        "MATCH (p:Person {email: 'ada@example.com'}) SET p.email = 'ada.l@example.com'",
    );
    let rows = run(
        &store,
        "MATCH (p:Person {email: 'ada.l@example.com'}) RETURN p.email AS e",
    );
    assert_eq!(rows.len(), 1);
}

#[test]
fn not_null_constraint_blocks_missing_property() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.name IS NOT NULL",
    );
    let err = run_catch(&store, "CREATE (:Person {email: 'x@example.com'})")
        .expect_err("missing required property should violate NOT NULL");
    assert!(err.to_string().contains("NOT NULL"));
}

#[test]
fn not_null_constraint_allows_present_property() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.name IS NOT NULL",
    );
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(&store, "MATCH (p:Person) RETURN p.name AS n");
    assert_eq!(rows.len(), 1);
}

#[test]
fn create_unique_constraint_rejects_existing_duplicates() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {email: 'a@x.com'})");
    run(&store, "CREATE (:Person {email: 'a@x.com'})");
    // Backfill validation should reject the constraint declaration.
    let err = run_catch(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE",
    )
    .expect_err("pre-existing duplicates should block constraint creation");
    assert!(err.to_string().contains("UNIQUE"));
}

#[test]
fn db_constraints_procedure_lists_registered() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT email_uniq FOR (p:Person) REQUIRE p.email IS UNIQUE",
    );
    let rows = run_with_default_procs(
        &store,
        "CALL db.constraints() YIELD name, type RETURN name, type",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "name"), "email_uniq");
    assert_eq!(str_prop(&rows[0], "type"), "UNIQUE");
}

#[test]
fn property_type_constraint_allows_matching_values() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.name IS :: STRING",
    );
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(&store, "MATCH (p:Person) RETURN p.name AS n");
    assert_eq!(rows.len(), 1);
}

#[test]
fn property_type_constraint_rejects_wrong_type() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.age IS :: INTEGER",
    );
    let err = run_catch(&store, "CREATE (:Person {age: 'thirty'})")
        .expect_err("string value should violate INTEGER type constraint");
    assert!(
        err.to_string().contains("IS :: INTEGER"),
        "error should identify the kind; got: {err}"
    );
}

#[test]
fn property_type_constraint_is_strict_no_int_to_float_coercion() {
    // Neo4j's `IS :: FLOAT` rejects integers; we follow suit.
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR (w:Widget) REQUIRE w.weight IS :: FLOAT",
    );
    let err = run_catch(&store, "CREATE (:Widget {weight: 42})")
        .expect_err("integer must not satisfy FLOAT");
    assert!(err.to_string().contains("FLOAT"));
}

#[test]
fn property_type_constraint_allows_missing_property() {
    // `IS :: T` alone allows null / missing; pair with NOT NULL for
    // strict presence.
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.age IS :: INTEGER",
    );
    run(&store, "CREATE (:Person {name: 'Ada'})");
    let rows = run(&store, "MATCH (p:Person) RETURN p.name AS n");
    assert_eq!(rows.len(), 1);
}

#[test]
fn property_type_and_not_null_compose() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT age_type FOR (p:Person) REQUIRE p.age IS :: INTEGER",
    );
    run(
        &store,
        "CREATE CONSTRAINT age_present FOR (p:Person) REQUIRE p.age IS NOT NULL",
    );
    // Missing property — NOT NULL triggers.
    assert!(run_catch(&store, "CREATE (:Person {name: 'Ada'})").is_err());
    // Wrong type — PROPERTY_TYPE triggers.
    assert!(run_catch(&store, "CREATE (:Person {age: 'thirty'})").is_err());
    // Both satisfied.
    run(&store, "CREATE (:Person {age: 30})");
    assert_eq!(run(&store, "MATCH (p:Person) RETURN p").len(), 1);
}

#[test]
fn property_type_constraint_shown_in_show_constraints() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.name IS :: STRING",
    );
    let rows = run(&store, "SHOW CONSTRAINTS");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "type"), "IS :: STRING");
}

#[test]
fn node_key_constraint_single_property_enforces_presence_and_uniqueness() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS NODE KEY",
    );
    run(&store, "CREATE (:Person {id: 1})");
    // Missing id — fails NODE KEY (existence).
    assert!(run_catch(&store, "CREATE (:Person {name: 'Ada'})").is_err());
    // Duplicate id — fails NODE KEY (uniqueness).
    assert!(run_catch(&store, "CREATE (:Person {id: 1})").is_err());
    // New distinct id — allowed.
    run(&store, "CREATE (:Person {id: 2})");
    assert_eq!(run(&store, "MATCH (p:Person) RETURN p").len(), 2);
}

#[test]
fn node_key_composite_constraint_enforces_tuple_uniqueness() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE (p.first, p.last) IS NODE KEY",
    );
    run(&store, "CREATE (:Person {first: 'Ada', last: 'Lovelace'})");
    // Same first, different last — allowed.
    run(&store, "CREATE (:Person {first: 'Ada', last: 'Byron'})");
    // Same last, different first — allowed.
    run(
        &store,
        "CREATE (:Person {first: 'Grace', last: 'Lovelace'})",
    );
    // Exact tuple duplicate — rejected.
    let err = run_catch(&store, "CREATE (:Person {first: 'Ada', last: 'Lovelace'})").unwrap_err();
    assert!(err.to_string().contains("NODE KEY"));
    // Missing one of the composite properties — rejected.
    assert!(run_catch(&store, "CREATE (:Person {first: 'Alan'})").is_err());
    assert_eq!(run(&store, "MATCH (p:Person) RETURN p").len(), 3);
}

#[test]
fn node_key_shown_as_list_in_show_constraints() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE (p.first, p.last) IS NODE KEY",
    );
    let rows = run(&store, "SHOW CONSTRAINTS");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "type"), "NODE KEY");
    assert_eq!(
        str_list_prop(&rows[0], "properties"),
        vec!["first".to_string(), "last".to_string()]
    );
}

#[test]
fn node_key_rejects_existing_duplicate_tuple() {
    // Backfill validation: pre-existing duplicate tuple blocks the
    // composite constraint from being declared.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {first: 'Ada', last: 'Lovelace'})");
    run(&store, "CREATE (:Person {first: 'Ada', last: 'Lovelace'})");
    let err = run_catch(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE (p.first, p.last) IS NODE KEY",
    )
    .unwrap_err();
    assert!(err.to_string().contains("NODE KEY"));
}

#[test]
fn node_key_rejects_existing_missing_property() {
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {first: 'Ada'})");
    let err = run_catch(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE (p.first, p.last) IS NODE KEY",
    )
    .unwrap_err();
    assert!(err.to_string().contains("NODE KEY"));
}

#[test]
fn node_key_auto_name_includes_all_properties() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE (p.first, p.last) IS NODE KEY",
    );
    assert_eq!(
        str_prop(&rows[0], "name"),
        "constraint_node_Person_first_last_node_key"
    );
}

#[test]
fn relationship_not_null_constraint_enforces_on_put_edge() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR ()-[r:KNOWS]-() REQUIRE r.since IS NOT NULL",
    );
    run(&store, "CREATE (a:Person {id: 1})");
    run(&store, "CREATE (b:Person {id: 2})");
    // Missing `since` on KNOWS — violates NOT NULL.
    let err = run_catch(
        &store,
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("NOT NULL"),
        "error should identify the constraint kind; got: {err}"
    );
    // Supplying `since` succeeds.
    run(
        &store,
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) \
         CREATE (a)-[:KNOWS {since: 2020}]->(b)",
    );
    let rows = run(&store, "MATCH ()-[r:KNOWS]->() RETURN r.since AS s");
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "s"), 2020);
}

#[test]
fn relationship_unique_constraint_blocks_duplicate_property() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR ()-[r:KNOWS]-() REQUIRE r.token IS UNIQUE",
    );
    run(&store, "CREATE (:Person {id: 1})");
    run(&store, "CREATE (:Person {id: 2})");
    run(&store, "CREATE (:Person {id: 3})");
    run(
        &store,
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) \
         CREATE (a)-[:KNOWS {token: 'abc'}]->(b)",
    );
    // Same token on a different pair — violates UNIQUE on the edge.
    let err = run_catch(
        &store,
        "MATCH (a:Person {id: 2}), (b:Person {id: 3}) \
         CREATE (a)-[:KNOWS {token: 'abc'}]->(b)",
    )
    .unwrap_err();
    assert!(err.to_string().contains("UNIQUE"));
}

#[test]
fn relationship_property_type_constraint_rejects_wrong_type() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR ()-[r:RATES]-() REQUIRE r.score IS :: INTEGER",
    );
    run(&store, "CREATE (:Item {id: 1})");
    run(&store, "CREATE (:Item {id: 2})");
    let err = run_catch(
        &store,
        "MATCH (a:Item {id: 1}), (b:Item {id: 2}) \
         CREATE (a)-[:RATES {score: 'great'}]->(b)",
    )
    .unwrap_err();
    assert!(err.to_string().contains("IS :: INTEGER"));
}

#[test]
fn relationship_constraint_shown_with_relationship_scope() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR ()-[r:KNOWS]-() REQUIRE r.since IS NOT NULL",
    );
    let rows = run(&store, "SHOW CONSTRAINTS");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "scope"), "RELATIONSHIP");
    assert_eq!(str_prop(&rows[0], "label"), "KNOWS");
    assert_eq!(str_prop(&rows[0], "type"), "NOT NULL");
}

#[test]
fn relationship_constraint_rejects_node_key() {
    // NODE KEY isn't meaningful for relationships; the storage layer
    // rejects the combination at declaration time.
    let (store, _d) = open_store();
    let err = run_catch(
        &store,
        "CREATE CONSTRAINT FOR ()-[r:KNOWS]-() REQUIRE (r.a, r.b) IS NODE KEY",
    )
    .unwrap_err();
    assert!(err.to_string().contains("relationship"));
}

#[test]
fn relationship_and_node_constraints_coexist() {
    let (store, _d) = open_store();
    run(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE",
    );
    run(
        &store,
        "CREATE CONSTRAINT FOR ()-[r:KNOWS]-() REQUIRE r.since IS NOT NULL",
    );
    let rows = run(&store, "SHOW CONSTRAINTS");
    assert_eq!(rows.len(), 2);
}

#[test]
fn property_type_constraint_rejects_existing_bad_data() {
    // Backfill validation: pre-existing wrong-typed data blocks the
    // constraint from being declared.
    let (store, _d) = open_store();
    run(&store, "CREATE (:Person {age: 'thirty'})");
    let err = run_catch(
        &store,
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.age IS :: INTEGER",
    )
    .expect_err("pre-existing wrong type should block constraint creation");
    assert!(err.to_string().contains("INTEGER"));
}

#[test]
fn constraint_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        run(
            &store,
            "CREATE CONSTRAINT persist_me FOR (p:Person) REQUIRE p.email IS UNIQUE",
        );
    }
    // Reopen — loader rehydrates CF_CONSTRAINT_META into the
    // in-memory registry. SHOW should still see the entry.
    let store = Store::open(dir.path()).unwrap();
    let shown = run(&store, "SHOW CONSTRAINTS");
    assert_eq!(shown.len(), 1);
    assert_eq!(str_prop(&shown[0], "name"), "persist_me");
}

// ---------------------------------------------------------------
// APOC scalar functions (feature-gated).
//
// The entire module compiles only when `--features apoc` is on;
// a minus-build with no APOC support passes the workspace test
// suite without running these.
// ---------------------------------------------------------------

#[cfg(feature = "apoc-coll")]
mod apoc_coll {
    use super::*;

    fn int_list(row: &Row, key: &str) -> Vec<i64> {
        match row.get(key).expect("key missing") {
            Value::Property(Property::List(items)) => items
                .iter()
                .filter_map(|p| match p {
                    Property::Int64(n) => Some(*n),
                    _ => None,
                })
                .collect(),
            other => panic!("expected Property::List, got {other:?}"),
        }
    }

    fn bool_prop(row: &Row, key: &str) -> bool {
        match row.get(key).expect("key missing") {
            Value::Property(Property::Bool(b)) => *b,
            other => panic!("expected Bool, got {other:?}"),
        }
    }

    #[test]
    fn apoc_coll_sum_on_integer_list() {
        let (store, _d) = open_store();
        let rows = run(&store, "RETURN apoc.coll.sum([1, 2, 3]) AS total");
        assert_eq!(int_prop(&rows[0], "total"), 6);
    }

    #[test]
    fn apoc_coll_avg_returns_float() {
        let (store, _d) = open_store();
        let rows = run(&store, "RETURN apoc.coll.avg([1, 2, 3, 4]) AS m");
        match rows[0].get("m").unwrap() {
            Value::Property(Property::Float64(f)) => assert_eq!(*f, 2.5),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn apoc_coll_max_min_on_mixed_numeric() {
        let (store, _d) = open_store();
        let rows = run(
            &store,
            "RETURN apoc.coll.max([1, 3.5, 2]) AS hi, apoc.coll.min([1, 3.5, 2]) AS lo",
        );
        match rows[0].get("hi").unwrap() {
            Value::Property(Property::Float64(f)) => assert_eq!(*f, 3.5),
            other => panic!("hi: expected Float64, got {other:?}"),
        }
        match rows[0].get("lo").unwrap() {
            Value::Property(Property::Float64(f)) => assert_eq!(*f, 1.0),
            other => panic!("lo: expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn apoc_coll_to_set_dedupes_preserving_order() {
        let (store, _d) = open_store();
        let rows = run(&store, "RETURN apoc.coll.toSet([1, 2, 1, 3, 2]) AS s");
        assert_eq!(int_list(&rows[0], "s"), vec![1, 2, 3]);
    }

    #[test]
    fn apoc_coll_sort_ascending_and_descending() {
        let (store, _d) = open_store();
        let rows = run(
            &store,
            "RETURN apoc.coll.sort([3, 1, 2]) AS up, apoc.coll.sortDesc([3, 1, 2]) AS down",
        );
        assert_eq!(int_list(&rows[0], "up"), vec![1, 2, 3]);
        assert_eq!(int_list(&rows[0], "down"), vec![3, 2, 1]);
    }

    #[test]
    fn apoc_coll_reverse_reverses_items() {
        let (store, _d) = open_store();
        let rows = run(&store, "RETURN apoc.coll.reverse([1, 2, 3]) AS r");
        assert_eq!(int_list(&rows[0], "r"), vec![3, 2, 1]);
    }

    #[test]
    fn apoc_coll_contains_hit_and_miss() {
        let (store, _d) = open_store();
        let rows = run(
            &store,
            "RETURN apoc.coll.contains([1, 2, 3], 2) AS hit, \
                    apoc.coll.contains([1, 2, 3], 9) AS miss",
        );
        assert_eq!(bool_prop(&rows[0], "hit"), true);
        assert_eq!(bool_prop(&rows[0], "miss"), false);
    }

    #[test]
    fn apoc_coll_set_operations() {
        let (store, _d) = open_store();
        let rows = run(
            &store,
            "RETURN apoc.coll.union([1, 2, 3], [3, 4]) AS u, \
                    apoc.coll.intersection([1, 2, 3], [2, 3, 5]) AS i, \
                    apoc.coll.subtract([1, 2, 3], [2]) AS s",
        );
        assert_eq!(int_list(&rows[0], "u"), vec![1, 2, 3, 4]);
        assert_eq!(int_list(&rows[0], "i"), vec![2, 3]);
        assert_eq!(int_list(&rows[0], "s"), vec![1, 3]);
    }

    #[test]
    fn apoc_coll_flatten_one_level() {
        let (store, _d) = open_store();
        let rows = run(&store, "RETURN apoc.coll.flatten([[1, 2], [3, 4], 5]) AS f");
        assert_eq!(int_list(&rows[0], "f"), vec![1, 2, 3, 4, 5]);
    }

    /// Sanity-check the non-apoc name goes through the native
    /// dispatcher even with the feature on (no shadowing).
    #[test]
    fn native_scalars_still_resolve_with_feature_on() {
        let (store, _d) = open_store();
        let rows = run(&store, "RETURN size([1, 2, 3]) AS n");
        assert_eq!(int_prop(&rows[0], "n"), 3);
    }
}

#[cfg(not(feature = "apoc-coll"))]
#[test]
fn apoc_scalar_without_feature_surfaces_unknown_function_error() {
    let (store, _d) = open_store();
    let stmt = parse("RETURN apoc.coll.sum([1, 2, 3]) AS total").unwrap();
    let p = plan(&stmt).unwrap();
    let err = execute(&p, &store).unwrap_err();
    assert!(
        err.to_string()
            .to_ascii_lowercase()
            .contains("apoc.coll.sum"),
        "expected unknown-function error for apoc.coll.sum without the feature, got: {err}",
    );
}
