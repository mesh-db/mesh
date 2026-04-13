use mesh_core::Property;
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
    let plan = plan(&stmt);
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
