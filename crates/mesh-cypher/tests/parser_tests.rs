use mesh_cypher::*;

fn unwrap_match(s: Statement) -> MatchStmt {
    match s {
        Statement::Match(m) => m,
        _ => panic!("expected match statement"),
    }
}

fn unwrap_create(s: Statement) -> CreateStmt {
    match s {
        Statement::Create(c) => c,
        _ => panic!("expected create statement"),
    }
}

#[test]
fn empty_node_creation() {
    let c = unwrap_create(parse("CREATE ()").unwrap());
    assert!(c.node.var.is_none());
    assert!(c.node.label.is_none());
    assert!(c.node.properties.is_empty());
}

#[test]
fn labeled_node_creation() {
    let c = unwrap_create(parse("CREATE (n:Person)").unwrap());
    assert_eq!(c.node.var.as_deref(), Some("n"));
    assert_eq!(c.node.label.as_deref(), Some("Person"));
}

#[test]
fn labeled_node_with_properties() {
    let c = unwrap_create(parse(r#"CREATE (n:Person {name: "Ada", age: 37})"#).unwrap());
    assert_eq!(
        c.node.properties,
        vec![
            ("name".into(), Literal::String("Ada".into())),
            ("age".into(), Literal::Integer(37)),
        ]
    );
}

#[test]
fn anonymous_labeled_node() {
    let c = unwrap_create(parse("CREATE (:Tag)").unwrap());
    assert!(c.node.var.is_none());
    assert_eq!(c.node.label.as_deref(), Some("Tag"));
}

#[test]
fn simple_match_return() {
    let m = unwrap_match(parse("MATCH (n:Person) RETURN n").unwrap());
    assert_eq!(m.node.var.as_deref(), Some("n"));
    assert_eq!(m.node.label.as_deref(), Some("Person"));
    assert_eq!(m.return_items.len(), 1);
    assert_eq!(m.return_items[0].expr, Expr::Identifier("n".into()));
    assert!(m.return_items[0].alias.is_none());
}

#[test]
fn return_property_with_alias() {
    let m = unwrap_match(parse("MATCH (n) RETURN n.name AS name").unwrap());
    assert_eq!(
        m.return_items[0].expr,
        Expr::Property {
            var: "n".into(),
            key: "name".into()
        }
    );
    assert_eq!(m.return_items[0].alias.as_deref(), Some("name"));
}

#[test]
fn return_multiple_items() {
    let m = unwrap_match(parse("MATCH (n) RETURN n, n.name, n.age AS years").unwrap());
    assert_eq!(m.return_items.len(), 3);
    assert_eq!(m.return_items[2].alias.as_deref(), Some("years"));
}

#[test]
fn where_comparison() {
    let m = unwrap_match(parse("MATCH (n:Person) WHERE n.age > 30 RETURN n").unwrap());
    let w = m.where_clause.unwrap();
    match w {
        Expr::Compare {
            op: CompareOp::Gt,
            left,
            right,
        } => {
            assert_eq!(
                *left,
                Expr::Property {
                    var: "n".into(),
                    key: "age".into()
                }
            );
            assert_eq!(*right, Expr::Literal(Literal::Integer(30)));
        }
        other => panic!("unexpected: {:?}", other),
    }
}

#[test]
fn where_and_combination() {
    let m = unwrap_match(
        parse(r#"MATCH (n) WHERE n.age > 30 AND n.name = "Ada" RETURN n"#).unwrap(),
    );
    assert!(matches!(m.where_clause.unwrap(), Expr::And(_, _)));
}

#[test]
fn where_or_combination() {
    let m = unwrap_match(parse("MATCH (n) WHERE n.a = 1 OR n.b = 2 RETURN n").unwrap());
    assert!(matches!(m.where_clause.unwrap(), Expr::Or(_, _)));
}

#[test]
fn where_not() {
    let m = unwrap_match(parse("MATCH (n) WHERE NOT n.active = true RETURN n").unwrap());
    assert!(matches!(m.where_clause.unwrap(), Expr::Not(_)));
}

#[test]
fn where_all_comparison_ops() {
    for (src, expected) in [
        ("=", CompareOp::Eq),
        ("<>", CompareOp::Ne),
        ("<", CompareOp::Lt),
        ("<=", CompareOp::Le),
        (">", CompareOp::Gt),
        (">=", CompareOp::Ge),
    ] {
        let q = format!("MATCH (n) WHERE n.x {} 1 RETURN n", src);
        let m = unwrap_match(parse(&q).unwrap());
        match m.where_clause.unwrap() {
            Expr::Compare { op, .. } => assert_eq!(op, expected, "op {}", src),
            other => panic!("expected compare for {}: {:?}", src, other),
        }
    }
}

#[test]
fn skip_and_limit() {
    let m = unwrap_match(parse("MATCH (n) RETURN n SKIP 5 LIMIT 10").unwrap());
    assert_eq!(m.skip, Some(5));
    assert_eq!(m.limit, Some(10));
}

#[test]
fn limit_only() {
    let m = unwrap_match(parse("MATCH (n) RETURN n LIMIT 3").unwrap());
    assert_eq!(m.skip, None);
    assert_eq!(m.limit, Some(3));
}

#[test]
fn negative_and_float_literals() {
    let c = unwrap_create(parse(r#"CREATE (n {x: -3.14, y: -42, z: 2.5})"#).unwrap());
    assert_eq!(
        c.node.properties,
        vec![
            ("x".into(), Literal::Float(-3.14)),
            ("y".into(), Literal::Integer(-42)),
            ("z".into(), Literal::Float(2.5)),
        ]
    );
}

#[test]
fn boolean_and_null_literals() {
    let c = unwrap_create(parse(r#"CREATE (n {a: true, b: false, c: null})"#).unwrap());
    assert_eq!(
        c.node.properties,
        vec![
            ("a".into(), Literal::Boolean(true)),
            ("b".into(), Literal::Boolean(false)),
            ("c".into(), Literal::Null),
        ]
    );
}

#[test]
fn single_quoted_string() {
    let c = unwrap_create(parse("CREATE (n {name: 'Ada'})").unwrap());
    assert_eq!(
        c.node.properties,
        vec![("name".into(), Literal::String("Ada".into()))]
    );
}

#[test]
fn case_insensitive_keywords() {
    unwrap_match(parse("match (n) return n").unwrap());
    unwrap_match(parse("Match (n) Return n").unwrap());
}

#[test]
fn reserved_word_as_variable_fails() {
    assert!(parse("MATCH (return) RETURN return").is_err());
}

#[test]
fn parenthesized_expression() {
    let m = unwrap_match(
        parse("MATCH (n) WHERE (n.a = 1 OR n.b = 2) AND n.c = 3 RETURN n").unwrap(),
    );
    match m.where_clause.unwrap() {
        Expr::And(left, _) => {
            assert!(matches!(*left, Expr::Or(_, _)));
        }
        other => panic!("expected top-level AND: {:?}", other),
    }
}
