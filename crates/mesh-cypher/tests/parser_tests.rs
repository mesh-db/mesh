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
    assert!(c.patterns[0].start.var.is_none());
    assert!(c.patterns[0].start.labels.is_empty());
    assert!(c.patterns[0].start.properties.is_empty());
}

#[test]
fn labeled_node_creation() {
    let c = unwrap_create(parse("CREATE (n:Person)").unwrap());
    assert_eq!(c.patterns[0].start.var.as_deref(), Some("n"));
    assert_eq!(
        c.patterns[0].start.labels.first().map(String::as_str),
        Some("Person")
    );
}

#[test]
fn labeled_node_with_properties() {
    let c = unwrap_create(parse(r#"CREATE (n:Person {name: "Ada", age: 37})"#).unwrap());
    assert_eq!(
        c.patterns[0].start.properties,
        vec![
            ("name".into(), Literal::String("Ada".into())),
            ("age".into(), Literal::Integer(37)),
        ]
    );
}

#[test]
fn anonymous_labeled_node() {
    let c = unwrap_create(parse("CREATE (:Tag)").unwrap());
    assert!(c.patterns[0].start.var.is_none());
    assert_eq!(
        c.patterns[0].start.labels.first().map(String::as_str),
        Some("Tag")
    );
}

#[test]
fn simple_match_return() {
    let m = unwrap_match(parse("MATCH (n:Person) RETURN n").unwrap());
    assert_eq!(m.patterns[0].start.var.as_deref(), Some("n"));
    assert_eq!(
        m.patterns[0].start.labels.first().map(String::as_str),
        Some("Person")
    );
    assert!(m.patterns[0].hops.is_empty());
    assert_eq!(m.return_items.len(), 1);
    assert_eq!(m.return_items[0].expr, Expr::Identifier("n".into()));
    assert!(m.return_items[0].alias.is_none());
}

#[test]
fn single_hop_directed() {
    let m = unwrap_match(parse("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b").unwrap());
    assert_eq!(m.patterns[0].hops.len(), 1);
    let hop = &m.patterns[0].hops[0];
    assert_eq!(hop.rel.direction, Direction::Outgoing);
    assert_eq!(hop.rel.var.as_deref(), Some("r"));
    assert_eq!(hop.rel.edge_type.as_deref(), Some("KNOWS"));
    assert_eq!(hop.target.var.as_deref(), Some("b"));
    assert_eq!(
        hop.target.labels.first().map(String::as_str),
        Some("Person")
    );
}

#[test]
fn single_hop_anonymous_rel() {
    let m = unwrap_match(parse("MATCH (a)-->(b) RETURN a, b").unwrap());
    let hop = &m.patterns[0].hops[0];
    assert_eq!(hop.rel.direction, Direction::Outgoing);
    assert!(hop.rel.var.is_none());
    assert!(hop.rel.edge_type.is_none());
}

#[test]
fn single_hop_type_only() {
    let m = unwrap_match(parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap());
    let hop = &m.patterns[0].hops[0];
    assert!(hop.rel.var.is_none());
    assert_eq!(hop.rel.edge_type.as_deref(), Some("KNOWS"));
    assert_eq!(hop.rel.direction, Direction::Outgoing);
}

#[test]
fn single_hop_incoming() {
    let m = unwrap_match(parse("MATCH (a)<-[r:KNOWS]-(b) RETURN a, b").unwrap());
    let hop = &m.patterns[0].hops[0];
    assert_eq!(hop.rel.direction, Direction::Incoming);
    assert_eq!(hop.rel.var.as_deref(), Some("r"));
    assert_eq!(hop.rel.edge_type.as_deref(), Some("KNOWS"));
}

#[test]
fn single_hop_undirected() {
    let m = unwrap_match(parse("MATCH (a)-[:KNOWS]-(b) RETURN a, b").unwrap());
    let hop = &m.patterns[0].hops[0];
    assert_eq!(hop.rel.direction, Direction::Both);
    assert_eq!(hop.rel.edge_type.as_deref(), Some("KNOWS"));
}

#[test]
fn match_set_with_return_parses() {
    let m = unwrap_match(parse("MATCH (n:Person) SET n.age = 37 RETURN n").unwrap());
    assert_eq!(m.set_items.len(), 1);
    assert_eq!(m.return_items.len(), 1);
    assert_eq!(m.return_items[0].expr, Expr::Identifier("n".into()));
}

#[test]
fn match_create_with_return_parses() {
    let m = unwrap_match(
        parse("MATCH (a:Person) CREATE (a)-[:WORKS_AT]->(c:Company) RETURN c").unwrap(),
    );
    assert_eq!(m.create_patterns.len(), 1);
    assert_eq!(m.return_items.len(), 1);
}

#[test]
fn match_delete_with_return_parses() {
    let m = unwrap_match(parse("MATCH (n:Person) DETACH DELETE n RETURN n.name").unwrap());
    assert!(m.delete.is_some());
    assert_eq!(m.return_items.len(), 1);
}

#[test]
fn pure_create_with_return_parses() {
    let c = unwrap_create(parse("CREATE (n:Person {name: 'Ada'}) RETURN n").unwrap());
    assert_eq!(c.patterns.len(), 1);
    assert_eq!(c.return_items.len(), 1);
}

#[test]
fn scalar_call_labels_parses() {
    let m = unwrap_match(parse("MATCH (n) RETURN labels(n) AS ls").unwrap());
    match &m.return_items[0].expr {
        Expr::Call {
            name,
            args: CallArgs::Exprs(es),
        } => {
            assert_eq!(name.to_lowercase(), "labels");
            assert_eq!(es.len(), 1);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn match_requires_tail() {
    assert!(parse("MATCH (n)").is_err());
}

#[test]
fn multi_pattern_match_parses() {
    let m = unwrap_match(parse("MATCH (a:Person), (b:Company) RETURN a, b").unwrap());
    assert_eq!(m.patterns.len(), 2);
    assert_eq!(
        m.patterns[0].start.labels.first().map(String::as_str),
        Some("Person")
    );
    assert_eq!(
        m.patterns[1].start.labels.first().map(String::as_str),
        Some("Company")
    );
}

#[test]
fn multi_pattern_create_parses() {
    let c = unwrap_create(parse("CREATE (a:Person), (b:Person)").unwrap());
    assert_eq!(c.patterns.len(), 2);
}

#[test]
fn match_create_tail_parses() {
    let m = unwrap_match(parse("MATCH (a:Person), (b:Person) CREATE (a)-[:KNOWS]->(b)").unwrap());
    assert_eq!(m.patterns.len(), 2);
    assert_eq!(m.create_patterns.len(), 1);
    assert_eq!(m.create_patterns[0].hops.len(), 1);
    assert_eq!(
        m.create_patterns[0].hops[0].rel.edge_type.as_deref(),
        Some("KNOWS")
    );
}

#[test]
fn order_by_single_asc_by_default() {
    let m = unwrap_match(parse("MATCH (n) RETURN n ORDER BY n.name").unwrap());
    assert_eq!(m.order_by.len(), 1);
    assert!(!m.order_by[0].descending);
}

#[test]
fn order_by_desc() {
    let m = unwrap_match(parse("MATCH (n) RETURN n ORDER BY n.age DESC").unwrap());
    assert!(m.order_by[0].descending);
}

#[test]
fn order_by_multi_key() {
    let m = unwrap_match(parse("MATCH (n) RETURN n ORDER BY n.dept ASC, n.age DESC").unwrap());
    assert_eq!(m.order_by.len(), 2);
    assert!(!m.order_by[0].descending);
    assert!(m.order_by[1].descending);
}

#[test]
fn distinct_flag_parsed() {
    let m = unwrap_match(parse("MATCH (n) RETURN DISTINCT n.name").unwrap());
    assert!(m.distinct);
}

#[test]
fn count_star_call_parsed() {
    let m = unwrap_match(parse("MATCH (n:Person) RETURN count(*)").unwrap());
    match &m.return_items[0].expr {
        Expr::Call { name, args } => {
            assert_eq!(name.to_lowercase(), "count");
            assert!(matches!(args, CallArgs::Star));
        }
        other => panic!("expected Call, got {other:?}"),
    }
}

#[test]
fn aggregate_call_with_expr_arg() {
    let m = unwrap_match(parse("MATCH (n:Person) RETURN avg(n.age) AS mean").unwrap());
    match &m.return_items[0].expr {
        Expr::Call {
            name,
            args: CallArgs::Exprs(es),
        } => {
            assert_eq!(name.to_lowercase(), "avg");
            assert_eq!(es.len(), 1);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(m.return_items[0].alias.as_deref(), Some("mean"));
}

#[test]
fn distinct_and_order_by_combined() {
    let m = unwrap_match(parse("MATCH (n) RETURN DISTINCT n.dept AS d ORDER BY d").unwrap());
    assert!(m.distinct);
    assert_eq!(m.order_by.len(), 1);
}

#[test]
fn var_length_exact_hops() {
    let m = unwrap_match(parse("MATCH (a)-[:KNOWS*3]->(b) RETURN b").unwrap());
    let vl = m.patterns[0].hops[0].rel.var_length.unwrap();
    assert_eq!(vl.min, 3);
    assert_eq!(vl.max, 3);
}

#[test]
fn var_length_bounded() {
    let m = unwrap_match(parse("MATCH (a)-[:KNOWS*1..3]->(b) RETURN b").unwrap());
    let vl = m.patterns[0].hops[0].rel.var_length.unwrap();
    assert_eq!(vl.min, 1);
    assert_eq!(vl.max, 3);
}

#[test]
fn var_length_min_only() {
    let m = unwrap_match(parse("MATCH (a)-[*2..]->(b) RETURN b").unwrap());
    let vl = m.patterns[0].hops[0].rel.var_length.unwrap();
    assert_eq!(vl.min, 2);
    assert_eq!(vl.max, u64::MAX);
}

#[test]
fn var_length_max_only() {
    let m = unwrap_match(parse("MATCH (a)-[*..4]->(b) RETURN b").unwrap());
    let vl = m.patterns[0].hops[0].rel.var_length.unwrap();
    assert_eq!(vl.min, 1);
    assert_eq!(vl.max, 4);
}

#[test]
fn var_length_unbounded_star() {
    let m = unwrap_match(parse("MATCH (a)-[*]->(b) RETURN b").unwrap());
    let vl = m.patterns[0].hops[0].rel.var_length.unwrap();
    assert_eq!(vl.min, 1);
    assert_eq!(vl.max, u64::MAX);
}

#[test]
fn var_length_with_var_and_type() {
    let m = unwrap_match(parse("MATCH (a)-[r:KNOWS*1..3]->(b) RETURN r").unwrap());
    let rel = &m.patterns[0].hops[0].rel;
    assert_eq!(rel.var.as_deref(), Some("r"));
    assert_eq!(rel.edge_type.as_deref(), Some("KNOWS"));
    let vl = rel.var_length.unwrap();
    assert_eq!(vl.min, 1);
    assert_eq!(vl.max, 3);
}

#[test]
fn single_hop_has_no_var_length() {
    let m = unwrap_match(parse("MATCH (a)-[r:KNOWS]->(b) RETURN b").unwrap());
    assert!(m.patterns[0].hops[0].rel.var_length.is_none());
}

#[test]
fn create_path_with_relationship() {
    let c = unwrap_create(parse("CREATE (a:Person)-[:KNOWS]->(b:Person)").unwrap());
    assert_eq!(c.patterns[0].hops.len(), 1);
    assert_eq!(
        c.patterns[0].start.labels.first().map(String::as_str),
        Some("Person")
    );
    assert_eq!(
        c.patterns[0].hops[0].rel.edge_type.as_deref(),
        Some("KNOWS")
    );
    assert_eq!(
        c.patterns[0].hops[0]
            .target
            .labels
            .first()
            .map(String::as_str),
        Some("Person")
    );
}

#[test]
fn match_delete() {
    let m = unwrap_match(parse("MATCH (n:Person) DELETE n").unwrap());
    let d = m.delete.unwrap();
    assert!(!d.detach);
    assert_eq!(d.vars, vec!["n".to_string()]);
    assert!(m.return_items.is_empty());
}

#[test]
fn match_detach_delete() {
    let m = unwrap_match(parse("MATCH (n:Person) DETACH DELETE n").unwrap());
    let d = m.delete.unwrap();
    assert!(d.detach);
    assert_eq!(d.vars, vec!["n".to_string()]);
}

#[test]
fn match_delete_multiple_vars() {
    let m = unwrap_match(parse("MATCH (a)-[r]->(b) DELETE r, a, b").unwrap());
    let d = m.delete.unwrap();
    assert_eq!(
        d.vars,
        vec!["r".to_string(), "a".to_string(), "b".to_string()]
    );
}

#[test]
fn match_set_single() {
    let m = unwrap_match(parse("MATCH (n:Person) SET n.name = 'Ada'").unwrap());
    assert_eq!(m.set_items.len(), 1);
    match &m.set_items[0] {
        SetItem::Property { var, key, value } => {
            assert_eq!(var, "n");
            assert_eq!(key, "name");
            assert_eq!(*value, Expr::Literal(Literal::String("Ada".into())));
        }
        other => panic!("expected Property, got {other:?}"),
    }
}

#[test]
fn match_set_multiple() {
    let m = unwrap_match(parse("MATCH (n:Person) SET n.name = 'Ada', n.age = 37").unwrap());
    assert_eq!(m.set_items.len(), 2);
    match &m.set_items[1] {
        SetItem::Property { key, value, .. } => {
            assert_eq!(key, "age");
            assert_eq!(*value, Expr::Literal(Literal::Integer(37)));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn match_set_labels() {
    let m = unwrap_match(parse("MATCH (n) SET n:Archived").unwrap());
    match &m.set_items[0] {
        SetItem::Labels { var, labels } => {
            assert_eq!(var, "n");
            assert_eq!(labels, &vec!["Archived".to_string()]);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn match_set_multi_label() {
    let m = unwrap_match(parse("MATCH (n) SET n:Archived:Tagged").unwrap());
    match &m.set_items[0] {
        SetItem::Labels { labels, .. } => {
            assert_eq!(labels, &vec!["Archived".to_string(), "Tagged".to_string()]);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn match_set_merge_map() {
    let m = unwrap_match(parse("MATCH (n) SET n += {age: 37, active: true}").unwrap());
    match &m.set_items[0] {
        SetItem::Merge { var, properties } => {
            assert_eq!(var, "n");
            assert_eq!(properties.len(), 2);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn match_set_replace_map() {
    let m = unwrap_match(parse("MATCH (n) SET n = {name: 'Ada'}").unwrap());
    match &m.set_items[0] {
        SetItem::Replace { var, properties } => {
            assert_eq!(var, "n");
            assert_eq!(properties.len(), 1);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn multi_hop_chain() {
    let m = unwrap_match(parse("MATCH (a)-[:KNOWS]->(b)-[:WORKS_AT]->(c) RETURN a, c").unwrap());
    assert_eq!(m.patterns[0].hops.len(), 2);
    assert_eq!(
        m.patterns[0].hops[0].rel.edge_type.as_deref(),
        Some("KNOWS")
    );
    assert_eq!(m.patterns[0].hops[0].target.var.as_deref(), Some("b"));
    assert_eq!(
        m.patterns[0].hops[1].rel.edge_type.as_deref(),
        Some("WORKS_AT")
    );
    assert_eq!(m.patterns[0].hops[1].target.var.as_deref(), Some("c"));
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
    let m =
        unwrap_match(parse(r#"MATCH (n) WHERE n.age > 30 AND n.name = "Ada" RETURN n"#).unwrap());
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
        c.patterns[0].start.properties,
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
        c.patterns[0].start.properties,
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
        c.patterns[0].start.properties,
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

fn unwrap_unwind(s: Statement) -> UnwindStmt {
    match s {
        Statement::Unwind(u) => u,
        _ => panic!("expected unwind statement"),
    }
}

#[test]
fn case_generic_form_parses() {
    let m = unwrap_match(
        parse("MATCH (n) RETURN CASE WHEN n.age > 30 THEN 'old' ELSE 'young' END AS bucket")
            .unwrap(),
    );
    assert_eq!(m.return_items.len(), 1);
    match &m.return_items[0].expr {
        Expr::Case {
            scrutinee,
            branches,
            else_expr,
        } => {
            assert!(scrutinee.is_none());
            assert_eq!(branches.len(), 1);
            assert!(else_expr.is_some());
        }
        other => panic!("expected Case, got {:?}", other),
    }
    assert_eq!(m.return_items[0].alias.as_deref(), Some("bucket"));
}

#[test]
fn case_simple_form_with_scrutinee() {
    let m = unwrap_match(
        parse("MATCH (n) RETURN CASE n.kind WHEN 'a' THEN 1 WHEN 'b' THEN 2 END").unwrap(),
    );
    match &m.return_items[0].expr {
        Expr::Case {
            scrutinee,
            branches,
            else_expr,
        } => {
            assert!(scrutinee.is_some());
            assert_eq!(branches.len(), 2);
            assert!(else_expr.is_none());
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

#[test]
fn list_literal_parses() {
    let m = unwrap_match(parse("MATCH (n) RETURN [1, 2, 3] AS xs").unwrap());
    match &m.return_items[0].expr {
        Expr::List(items) => assert_eq!(items.len(), 3),
        other => panic!("expected List, got {:?}", other),
    }
}

#[test]
fn empty_list_literal_parses() {
    let m = unwrap_match(parse("MATCH (n) RETURN [] AS xs").unwrap());
    match &m.return_items[0].expr {
        Expr::List(items) => assert!(items.is_empty()),
        other => panic!("expected List, got {:?}", other),
    }
}

#[test]
fn list_comprehension_full_form() {
    let m = unwrap_match(parse("MATCH (n) RETURN [x IN [1, 2, 3] WHERE x > 1 | x] AS ys").unwrap());
    match &m.return_items[0].expr {
        Expr::ListComprehension {
            var,
            predicate,
            projection,
            ..
        } => {
            assert_eq!(var, "x");
            assert!(predicate.is_some());
            assert!(projection.is_some());
        }
        other => panic!("expected ListComprehension, got {:?}", other),
    }
}

#[test]
fn list_comprehension_filter_only() {
    let m = unwrap_match(parse("MATCH (n) RETURN [x IN [1, 2, 3] WHERE x > 1] AS ys").unwrap());
    match &m.return_items[0].expr {
        Expr::ListComprehension {
            predicate,
            projection,
            ..
        } => {
            assert!(predicate.is_some());
            assert!(projection.is_none());
        }
        other => panic!("expected ListComprehension, got {:?}", other),
    }
}

#[test]
fn list_comprehension_project_only() {
    let m = unwrap_match(parse("MATCH (n) RETURN [x IN [1, 2, 3] | x] AS ys").unwrap());
    match &m.return_items[0].expr {
        Expr::ListComprehension {
            predicate,
            projection,
            ..
        } => {
            assert!(predicate.is_none());
            assert!(projection.is_some());
        }
        other => panic!("expected ListComprehension, got {:?}", other),
    }
}

#[test]
fn unwind_parses() {
    let u = unwrap_unwind(parse("UNWIND [1, 2, 3] AS x RETURN x").unwrap());
    assert_eq!(u.alias, "x");
    assert!(matches!(u.expr, Expr::List(_)));
    assert_eq!(u.return_items.len(), 1);
}

#[test]
fn unwind_with_where_and_order() {
    let u =
        unwrap_unwind(parse("UNWIND [3, 1, 2] AS x WHERE x > 1 RETURN x ORDER BY x DESC").unwrap());
    assert!(u.where_clause.is_some());
    assert_eq!(u.order_by.len(), 1);
    assert!(u.order_by[0].descending);
}

#[test]
fn parenthesized_expression() {
    let m =
        unwrap_match(parse("MATCH (n) WHERE (n.a = 1 OR n.b = 2) AND n.c = 3 RETURN n").unwrap());
    match m.where_clause.unwrap() {
        Expr::And(left, _) => {
            assert!(matches!(*left, Expr::Or(_, _)));
        }
        other => panic!("expected top-level AND: {:?}", other),
    }
}
