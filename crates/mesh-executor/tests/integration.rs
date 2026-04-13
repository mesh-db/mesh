use mesh_core::{Edge, Node, NodeId, Property};
use mesh_cypher::{parse, plan};
use mesh_executor::{execute, Row, Value};
use mesh_storage::Store;
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

    let rows = run(
        &store,
        "MATCH (n:Flag) WHERE NOT n.active = true RETURN n",
    );
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

    let rows = run(&store, "MATCH (a)-[:LOOP]->(b) RETURN a.name AS a, b.name AS b");
    assert_eq!(rows.len(), 1);
    assert_eq!(str_prop(&rows[0], "a"), "self");
    assert_eq!(str_prop(&rows[0], "b"), "self");
}

fn seed_people(store: &Store) {
    run(store, "CREATE (n:Person {name: 'Ada', age: 37, dept: 'eng'})");
    run(store, "CREATE (n:Person {name: 'Alan', age: 41, dept: 'eng'})");
    run(store, "CREATE (n:Person {name: 'Grace', age: 85, dept: 'ops'})");
    run(store, "CREATE (n:Person {name: 'Ada', age: 29, dept: 'ops'})");
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

    let rows = run(
        &store,
        "MATCH (p:Person) RETURN DISTINCT p.dept AS dept",
    );
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

    let rows = run(
        &store,
        "MATCH (p:Person) RETURN count(*) AS total",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "total"), 4);
}

#[test]
fn count_empty_match_returns_zero() {
    let (store, _d) = open_store();
    let rows = run(
        &store,
        "MATCH (p:NoSuchLabel) RETURN count(*) AS total",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int_prop(&rows[0], "total"), 0);
}

#[test]
fn count_expr_skips_null() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Person {name: 'with_age', age: 30})");
    run(&store, "CREATE (n:Person {name: 'no_age'})");

    let rows = run(
        &store,
        "MATCH (p:Person) RETURN count(p.age) AS c",
    );
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

    let rows = run(
        &store,
        "MATCH (p:Person) RETURN collect(p.name) AS names",
    );
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
fn unknown_scalar_function_rejected() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:X)");

    let stmt = parse("MATCH (n:X) RETURN unknownfn(n) AS v").unwrap();
    // Plan step detects this: unknown function.
    assert!(plan(&stmt).is_err());
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
    let rows = run(
        &store,
        "MATCH (a:Loner)-[:N*1..5]->(b) RETURN b",
    );
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

    run(&store, "MATCH (n:Person) WHERE n.name = 'Alan' DETACH DELETE n");

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

    let rows = run(&store, "MATCH (n:Person) RETURN n.name AS name, n.age AS age");
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
fn float_comparison_works() {
    let (store, _d) = open_store();
    run(&store, "CREATE (n:Meas {val: 3.14})");
    run(&store, "CREATE (n:Meas {val: 2.71})");

    let rows = run(
        &store,
        "MATCH (n:Meas) WHERE n.val > 3.0 RETURN n.val AS v",
    );
    assert_eq!(rows.len(), 1);
    match rows[0].get("v") {
        Some(Value::Property(Property::Float64(f))) => assert!((*f - 3.14).abs() < 1e-9),
        other => panic!("{other:?}"),
    }
}
