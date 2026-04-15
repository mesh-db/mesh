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

// --- test scaffolding for the clause-sequence MatchStmt ---
//
// These helpers pull the canonical "first MATCH clause" out
// of `MatchStmt::clauses` so the bulk of the parser tests —
// which predate chained reading clauses — stay readable after
// the AST rewrite. They are NOT AST accessors; the AST stays
// clean. New chain-aware tests walk `m.clauses` directly.

fn first_match(m: &MatchStmt) -> &MatchClause {
    match &m.clauses[0] {
        ReadingClause::Match(mc) => mc,
        other => panic!("expected first clause to be Match, got {other:?}"),
    }
}

fn first_with(m: &MatchStmt) -> Option<&WithClause> {
    m.clauses.iter().find_map(|c| match c {
        ReadingClause::With(w) => Some(w),
        _ => None,
    })
}

fn first_optional_matches(m: &MatchStmt) -> Vec<&OptionalMatchClause> {
    m.clauses
        .iter()
        .filter_map(|c| match c {
            ReadingClause::OptionalMatch(o) => Some(o),
            _ => None,
        })
        .collect()
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
            ("name".into(), Expr::Literal(Literal::String("Ada".into()))),
            ("age".into(), Expr::Literal(Literal::Integer(37))),
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
    assert_eq!(first_match(&m).patterns[0].start.var.as_deref(), Some("n"));
    assert_eq!(
        first_match(&m).patterns[0]
            .start
            .labels
            .first()
            .map(String::as_str),
        Some("Person")
    );
    assert!(first_match(&m).patterns[0].hops.is_empty());
    assert_eq!(m.terminal.return_items.len(), 1);
    assert_eq!(
        m.terminal.return_items[0].expr,
        Expr::Identifier("n".into())
    );
    assert!(m.terminal.return_items[0].alias.is_none());
}

#[test]
fn single_hop_directed() {
    let m = unwrap_match(parse("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b").unwrap());
    assert_eq!(first_match(&m).patterns[0].hops.len(), 1);
    let hop = &first_match(&m).patterns[0].hops[0];
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
    let hop = &first_match(&m).patterns[0].hops[0];
    assert_eq!(hop.rel.direction, Direction::Outgoing);
    assert!(hop.rel.var.is_none());
    assert!(hop.rel.edge_type.is_none());
}

#[test]
fn single_hop_type_only() {
    let m = unwrap_match(parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap());
    let hop = &first_match(&m).patterns[0].hops[0];
    assert!(hop.rel.var.is_none());
    assert_eq!(hop.rel.edge_type.as_deref(), Some("KNOWS"));
    assert_eq!(hop.rel.direction, Direction::Outgoing);
}

#[test]
fn single_hop_incoming() {
    let m = unwrap_match(parse("MATCH (a)<-[r:KNOWS]-(b) RETURN a, b").unwrap());
    let hop = &first_match(&m).patterns[0].hops[0];
    assert_eq!(hop.rel.direction, Direction::Incoming);
    assert_eq!(hop.rel.var.as_deref(), Some("r"));
    assert_eq!(hop.rel.edge_type.as_deref(), Some("KNOWS"));
}

#[test]
fn single_hop_undirected() {
    let m = unwrap_match(parse("MATCH (a)-[:KNOWS]-(b) RETURN a, b").unwrap());
    let hop = &first_match(&m).patterns[0].hops[0];
    assert_eq!(hop.rel.direction, Direction::Both);
    assert_eq!(hop.rel.edge_type.as_deref(), Some("KNOWS"));
}

#[test]
fn match_set_with_return_parses() {
    let m = unwrap_match(parse("MATCH (n:Person) SET n.age = 37 RETURN n").unwrap());
    assert_eq!(m.terminal.set_items.len(), 1);
    assert_eq!(m.terminal.return_items.len(), 1);
    assert_eq!(
        m.terminal.return_items[0].expr,
        Expr::Identifier("n".into())
    );
}

#[test]
fn match_create_with_return_parses() {
    let m = unwrap_match(
        parse("MATCH (a:Person) CREATE (a)-[:WORKS_AT]->(c:Company) RETURN c").unwrap(),
    );
    assert_eq!(m.terminal.create_patterns.len(), 1);
    assert_eq!(m.terminal.return_items.len(), 1);
}

#[test]
fn match_delete_with_return_parses() {
    let m = unwrap_match(parse("MATCH (n:Person) DETACH DELETE n RETURN n.name").unwrap());
    assert!(m.terminal.delete.is_some());
    assert_eq!(m.terminal.return_items.len(), 1);
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
    match &m.terminal.return_items[0].expr {
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
    // The grammar is permissive — MATCH without a terminal
    // parses fine (it's the same shape a bare MERGE needs),
    // but the planner rejects it because a read-only query
    // without a RETURN has nothing to return.
    let stmt = parse("MATCH (n)").unwrap();
    let err = plan(&stmt).unwrap_err();
    assert!(
        err.to_string().contains("must be followed by RETURN"),
        "expected missing-terminal planner error, got: {err}"
    );
}

#[test]
fn multi_pattern_match_parses() {
    let m = unwrap_match(parse("MATCH (a:Person), (b:Company) RETURN a, b").unwrap());
    assert_eq!(first_match(&m).patterns.len(), 2);
    assert_eq!(
        first_match(&m).patterns[0]
            .start
            .labels
            .first()
            .map(String::as_str),
        Some("Person")
    );
    assert_eq!(
        first_match(&m).patterns[1]
            .start
            .labels
            .first()
            .map(String::as_str),
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
    assert_eq!(first_match(&m).patterns.len(), 2);
    assert_eq!(m.terminal.create_patterns.len(), 1);
    assert_eq!(m.terminal.create_patterns[0].hops.len(), 1);
    assert_eq!(
        m.terminal.create_patterns[0].hops[0]
            .rel
            .edge_type
            .as_deref(),
        Some("KNOWS")
    );
}

#[test]
fn order_by_single_asc_by_default() {
    let m = unwrap_match(parse("MATCH (n) RETURN n ORDER BY n.name").unwrap());
    assert_eq!(m.terminal.order_by.len(), 1);
    assert!(!m.terminal.order_by[0].descending);
}

#[test]
fn order_by_desc() {
    let m = unwrap_match(parse("MATCH (n) RETURN n ORDER BY n.age DESC").unwrap());
    assert!(m.terminal.order_by[0].descending);
}

#[test]
fn order_by_multi_key() {
    let m = unwrap_match(parse("MATCH (n) RETURN n ORDER BY n.dept ASC, n.age DESC").unwrap());
    assert_eq!(m.terminal.order_by.len(), 2);
    assert!(!m.terminal.order_by[0].descending);
    assert!(m.terminal.order_by[1].descending);
}

#[test]
fn distinct_flag_parsed() {
    let m = unwrap_match(parse("MATCH (n) RETURN DISTINCT n.name").unwrap());
    assert!(m.terminal.distinct);
}

#[test]
fn count_star_call_parsed() {
    let m = unwrap_match(parse("MATCH (n:Person) RETURN count(*)").unwrap());
    match &m.terminal.return_items[0].expr {
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
    match &m.terminal.return_items[0].expr {
        Expr::Call {
            name,
            args: CallArgs::Exprs(es),
        } => {
            assert_eq!(name.to_lowercase(), "avg");
            assert_eq!(es.len(), 1);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(m.terminal.return_items[0].alias.as_deref(), Some("mean"));
}

#[test]
fn distinct_and_order_by_combined() {
    let m = unwrap_match(parse("MATCH (n) RETURN DISTINCT n.dept AS d ORDER BY d").unwrap());
    assert!(m.terminal.distinct);
    assert_eq!(m.terminal.order_by.len(), 1);
}

#[test]
fn var_length_exact_hops() {
    let m = unwrap_match(parse("MATCH (a)-[:KNOWS*3]->(b) RETURN b").unwrap());
    let vl = first_match(&m).patterns[0].hops[0].rel.var_length.unwrap();
    assert_eq!(vl.min, 3);
    assert_eq!(vl.max, 3);
}

#[test]
fn var_length_bounded() {
    let m = unwrap_match(parse("MATCH (a)-[:KNOWS*1..3]->(b) RETURN b").unwrap());
    let vl = first_match(&m).patterns[0].hops[0].rel.var_length.unwrap();
    assert_eq!(vl.min, 1);
    assert_eq!(vl.max, 3);
}

#[test]
fn var_length_min_only() {
    let m = unwrap_match(parse("MATCH (a)-[*2..]->(b) RETURN b").unwrap());
    let vl = first_match(&m).patterns[0].hops[0].rel.var_length.unwrap();
    assert_eq!(vl.min, 2);
    assert_eq!(vl.max, u64::MAX);
}

#[test]
fn var_length_max_only() {
    let m = unwrap_match(parse("MATCH (a)-[*..4]->(b) RETURN b").unwrap());
    let vl = first_match(&m).patterns[0].hops[0].rel.var_length.unwrap();
    assert_eq!(vl.min, 1);
    assert_eq!(vl.max, 4);
}

#[test]
fn var_length_unbounded_star() {
    let m = unwrap_match(parse("MATCH (a)-[*]->(b) RETURN b").unwrap());
    let vl = first_match(&m).patterns[0].hops[0].rel.var_length.unwrap();
    assert_eq!(vl.min, 1);
    assert_eq!(vl.max, u64::MAX);
}

#[test]
fn var_length_with_var_and_type() {
    let m = unwrap_match(parse("MATCH (a)-[r:KNOWS*1..3]->(b) RETURN r").unwrap());
    let rel = &first_match(&m).patterns[0].hops[0].rel;
    assert_eq!(rel.var.as_deref(), Some("r"));
    assert_eq!(rel.edge_type.as_deref(), Some("KNOWS"));
    let vl = rel.var_length.unwrap();
    assert_eq!(vl.min, 1);
    assert_eq!(vl.max, 3);
}

#[test]
fn single_hop_has_no_var_length() {
    let m = unwrap_match(parse("MATCH (a)-[r:KNOWS]->(b) RETURN b").unwrap());
    assert!(first_match(&m).patterns[0].hops[0].rel.var_length.is_none());
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
    let d = m.terminal.delete.unwrap();
    assert!(!d.detach);
    assert_eq!(d.vars, vec!["n".to_string()]);
    assert!(m.terminal.return_items.is_empty());
}

#[test]
fn match_detach_delete() {
    let m = unwrap_match(parse("MATCH (n:Person) DETACH DELETE n").unwrap());
    let d = m.terminal.delete.unwrap();
    assert!(d.detach);
    assert_eq!(d.vars, vec!["n".to_string()]);
}

#[test]
fn match_delete_multiple_vars() {
    let m = unwrap_match(parse("MATCH (a)-[r]->(b) DELETE r, a, b").unwrap());
    let d = m.terminal.delete.unwrap();
    assert_eq!(
        d.vars,
        vec!["r".to_string(), "a".to_string(), "b".to_string()]
    );
}

#[test]
fn match_set_single() {
    let m = unwrap_match(parse("MATCH (n:Person) SET n.name = 'Ada'").unwrap());
    assert_eq!(m.terminal.set_items.len(), 1);
    match &m.terminal.set_items[0] {
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
    assert_eq!(m.terminal.set_items.len(), 2);
    match &m.terminal.set_items[1] {
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
    match &m.terminal.set_items[0] {
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
    match &m.terminal.set_items[0] {
        SetItem::Labels { labels, .. } => {
            assert_eq!(labels, &vec!["Archived".to_string(), "Tagged".to_string()]);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn match_set_merge_map() {
    let m = unwrap_match(parse("MATCH (n) SET n += {age: 37, active: true}").unwrap());
    match &m.terminal.set_items[0] {
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
    match &m.terminal.set_items[0] {
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
    assert_eq!(first_match(&m).patterns[0].hops.len(), 2);
    assert_eq!(
        first_match(&m).patterns[0].hops[0].rel.edge_type.as_deref(),
        Some("KNOWS")
    );
    assert_eq!(
        first_match(&m).patterns[0].hops[0].target.var.as_deref(),
        Some("b")
    );
    assert_eq!(
        first_match(&m).patterns[0].hops[1].rel.edge_type.as_deref(),
        Some("WORKS_AT")
    );
    assert_eq!(
        first_match(&m).patterns[0].hops[1].target.var.as_deref(),
        Some("c")
    );
}

#[test]
fn return_property_with_alias() {
    let m = unwrap_match(parse("MATCH (n) RETURN n.name AS name").unwrap());
    assert_eq!(
        m.terminal.return_items[0].expr,
        Expr::Property {
            var: "n".into(),
            key: "name".into()
        }
    );
    assert_eq!(m.terminal.return_items[0].alias.as_deref(), Some("name"));
}

#[test]
fn return_multiple_items() {
    let m = unwrap_match(parse("MATCH (n) RETURN n, n.name, n.age AS years").unwrap());
    assert_eq!(m.terminal.return_items.len(), 3);
    assert_eq!(m.terminal.return_items[2].alias.as_deref(), Some("years"));
}

#[test]
fn where_comparison() {
    let m = unwrap_match(parse("MATCH (n:Person) WHERE n.age > 30 RETURN n").unwrap());
    let w = first_match(&m).where_clause.clone().unwrap();
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
    assert!(matches!(
        first_match(&m).where_clause.clone().unwrap(),
        Expr::And(_, _)
    ));
}

#[test]
fn where_or_combination() {
    let m = unwrap_match(parse("MATCH (n) WHERE n.a = 1 OR n.b = 2 RETURN n").unwrap());
    assert!(matches!(
        first_match(&m).where_clause.clone().unwrap(),
        Expr::Or(_, _)
    ));
}

#[test]
fn where_not() {
    let m = unwrap_match(parse("MATCH (n) WHERE NOT n.active = true RETURN n").unwrap());
    assert!(matches!(
        first_match(&m).where_clause.clone().unwrap(),
        Expr::Not(_)
    ));
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
        match first_match(&m).where_clause.clone().unwrap() {
            Expr::Compare { op, .. } => assert_eq!(op, expected, "op {}", src),
            other => panic!("expected compare for {}: {:?}", src, other),
        }
    }
}

#[test]
fn skip_and_limit() {
    let m = unwrap_match(parse("MATCH (n) RETURN n SKIP 5 LIMIT 10").unwrap());
    assert_eq!(m.terminal.skip, Some(5));
    assert_eq!(m.terminal.limit, Some(10));
}

#[test]
fn limit_only() {
    let m = unwrap_match(parse("MATCH (n) RETURN n LIMIT 3").unwrap());
    assert_eq!(m.terminal.skip, None);
    assert_eq!(m.terminal.limit, Some(3));
}

#[test]
fn negative_and_float_literals() {
    let c = unwrap_create(parse(r#"CREATE (n {x: -3.14, y: -42, z: 2.5})"#).unwrap());
    assert_eq!(
        c.patterns[0].start.properties,
        vec![
            ("x".into(), Expr::Literal(Literal::Float(-3.14))),
            ("y".into(), Expr::Literal(Literal::Integer(-42))),
            ("z".into(), Expr::Literal(Literal::Float(2.5))),
        ]
    );
}

#[test]
fn boolean_and_null_literals() {
    let c = unwrap_create(parse(r#"CREATE (n {a: true, b: false, c: null})"#).unwrap());
    assert_eq!(
        c.patterns[0].start.properties,
        vec![
            ("a".into(), Expr::Literal(Literal::Boolean(true))),
            ("b".into(), Expr::Literal(Literal::Boolean(false))),
            ("c".into(), Expr::Literal(Literal::Null)),
        ]
    );
}

#[test]
fn single_quoted_string() {
    let c = unwrap_create(parse("CREATE (n {name: 'Ada'})").unwrap());
    assert_eq!(
        c.patterns[0].start.properties,
        vec![("name".into(), Expr::Literal(Literal::String("Ada".into())))]
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
    assert_eq!(m.terminal.return_items.len(), 1);
    match &m.terminal.return_items[0].expr {
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
    assert_eq!(m.terminal.return_items[0].alias.as_deref(), Some("bucket"));
}

#[test]
fn case_simple_form_with_scrutinee() {
    let m = unwrap_match(
        parse("MATCH (n) RETURN CASE n.kind WHEN 'a' THEN 1 WHEN 'b' THEN 2 END").unwrap(),
    );
    match &m.terminal.return_items[0].expr {
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
    match &m.terminal.return_items[0].expr {
        Expr::List(items) => assert_eq!(items.len(), 3),
        other => panic!("expected List, got {:?}", other),
    }
}

#[test]
fn empty_list_literal_parses() {
    let m = unwrap_match(parse("MATCH (n) RETURN [] AS xs").unwrap());
    match &m.terminal.return_items[0].expr {
        Expr::List(items) => assert!(items.is_empty()),
        other => panic!("expected List, got {:?}", other),
    }
}

#[test]
fn list_comprehension_full_form() {
    let m = unwrap_match(parse("MATCH (n) RETURN [x IN [1, 2, 3] WHERE x > 1 | x] AS ys").unwrap());
    match &m.terminal.return_items[0].expr {
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
    match &m.terminal.return_items[0].expr {
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
    match &m.terminal.return_items[0].expr {
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
    match first_match(&m).where_clause.clone().unwrap() {
        Expr::And(left, _) => {
            assert!(matches!(*left, Expr::Or(_, _)));
        }
        other => panic!("expected top-level AND: {:?}", other),
    }
}

// --- Parameter parsing -------------------------------------------------

#[test]
fn parameter_in_where_clause_parses_as_compare_rhs() {
    let m = unwrap_match(parse("MATCH (n) WHERE n.name = $name RETURN n").unwrap());
    match first_match(&m).where_clause.clone().unwrap() {
        Expr::Compare { right, .. } => {
            assert_eq!(*right, Expr::Parameter("name".into()));
        }
        other => panic!("expected Compare, got {:?}", other),
    }
}

#[test]
fn parameter_in_node_pattern_property() {
    let m = unwrap_match(parse("MATCH (n:Person {name: $name}) RETURN n").unwrap());
    assert_eq!(
        first_match(&m).patterns[0].start.properties,
        vec![("name".into(), Expr::Parameter("name".into()))]
    );
}

#[test]
fn parameter_in_create_pattern_property() {
    let c = unwrap_create(parse("CREATE (n:Person {name: $name, age: $age})").unwrap());
    assert_eq!(
        c.patterns[0].start.properties,
        vec![
            ("name".into(), Expr::Parameter("name".into())),
            ("age".into(), Expr::Parameter("age".into())),
        ]
    );
}

#[test]
fn parameter_in_set_property_value() {
    let m = unwrap_match(parse("MATCH (n:Person) SET n.age = $age RETURN n").unwrap());
    match &m.terminal.set_items[0] {
        SetItem::Property { value, .. } => {
            assert_eq!(*value, Expr::Parameter("age".into()));
        }
        other => panic!("expected SetItem::Property, got {:?}", other),
    }
}

#[test]
fn parameter_in_unwind_source() {
    let u = unwrap_unwind(parse("UNWIND $items AS x RETURN x").unwrap());
    assert_eq!(u.expr, Expr::Parameter("items".into()));
    assert_eq!(u.alias, "x");
}

#[test]
fn positional_parameter_parses() {
    let m = unwrap_match(parse("MATCH (n) WHERE n.id = $0 RETURN n").unwrap());
    match first_match(&m).where_clause.clone().unwrap() {
        Expr::Compare { right, .. } => {
            assert_eq!(*right, Expr::Parameter("0".into()));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn parameter_in_return_expression() {
    let m = unwrap_match(parse("MATCH (n) RETURN $constant AS k").unwrap());
    assert_eq!(
        m.terminal.return_items[0].expr,
        Expr::Parameter("constant".into())
    );
    assert_eq!(m.terminal.return_items[0].alias.as_deref(), Some("k"));
}

#[test]
fn property_value_rejects_free_identifier() {
    // Pattern property values are restricted to literal | parameter at
    // the grammar level, so `(n {name: foo})` (foo as a free identifier)
    // is a parse error rather than an "unbound variable" runtime error.
    assert!(parse("MATCH (n {name: foo}) RETURN n").is_err());
}

#[test]
fn create_index_parses() {
    match parse("CREATE INDEX FOR (p:Person) ON (p.name)").unwrap() {
        Statement::CreateIndex(ddl) => {
            assert_eq!(ddl.label, "Person");
            assert_eq!(ddl.property, "name");
        }
        other => panic!("expected CreateIndex, got {:?}", other),
    }
}

#[test]
fn drop_index_parses() {
    match parse("DROP INDEX FOR (n:Person) ON (n.name)").unwrap() {
        Statement::DropIndex(ddl) => {
            assert_eq!(ddl.label, "Person");
            assert_eq!(ddl.property, "name");
        }
        other => panic!("expected DropIndex, got {:?}", other),
    }
}

#[test]
fn show_indexes_parses() {
    assert!(matches!(
        parse("SHOW INDEXES").unwrap(),
        Statement::ShowIndexes
    ));
}

#[test]
fn create_index_rejects_missing_label() {
    // Grammar requires `(var:Label)` — omitting the label should fail.
    assert!(parse("CREATE INDEX FOR (p) ON (p.name)").is_err());
}

// ---------------------------------------------------------------
// WHERE-clause IndexSeek rewrite: planner-level assertions.
// ---------------------------------------------------------------

fn ctx_with_index(label: &str, prop: &str) -> PlannerContext {
    PlannerContext {
        indexes: vec![(label.into(), prop.into())],
    }
}

fn plan_with(query: &str, ctx: &PlannerContext) -> LogicalPlan {
    plan_with_context(&parse(query).unwrap(), ctx).unwrap()
}

#[test]
fn where_eq_on_indexed_property_rewrites_to_index_seek() {
    let ctx = ctx_with_index("Person", "name");
    let p = plan_with("MATCH (n:Person) WHERE n.name = 'Ada' RETURN n", &ctx);
    // RETURN wraps the IndexSeek in Project; unwrap one level.
    let LogicalPlan::Project { input, .. } = p else {
        panic!("expected Project at top, got {p:?}");
    };
    let LogicalPlan::IndexSeek {
        label, property, ..
    } = *input
    else {
        panic!("expected IndexSeek under Project");
    };
    assert_eq!(label, "Person");
    assert_eq!(property, "name");
}

#[test]
fn where_eq_symmetric_form_rewrites_to_index_seek() {
    let ctx = ctx_with_index("Person", "name");
    let p = plan_with("MATCH (n:Person) WHERE 'Ada' = n.name RETURN n", &ctx);
    let LogicalPlan::Project { input, .. } = p else {
        panic!("expected Project");
    };
    assert!(matches!(*input, LogicalPlan::IndexSeek { .. }));
}

#[test]
fn where_eq_with_parameter_rewrites_to_index_seek() {
    let ctx = ctx_with_index("Person", "name");
    let p = plan_with("MATCH (n:Person) WHERE n.name = $who RETURN n", &ctx);
    let LogicalPlan::Project { input, .. } = p else {
        panic!("expected Project");
    };
    let LogicalPlan::IndexSeek { value, .. } = *input else {
        panic!("expected IndexSeek");
    };
    assert!(matches!(value, Expr::Parameter(ref s) if s == "who"));
}

#[test]
fn where_eq_with_residual_keeps_filter_wrap() {
    let ctx = ctx_with_index("Person", "name");
    let p = plan_with(
        "MATCH (n:Person) WHERE n.name = 'Ada' AND n.age > 20 RETURN n",
        &ctx,
    );
    let LogicalPlan::Project { input, .. } = p else {
        panic!("expected Project");
    };
    let LogicalPlan::Filter {
        input: seek_input,
        predicate,
    } = *input
    else {
        panic!("expected Filter wrapping IndexSeek, got {input:?}");
    };
    assert!(matches!(*seek_input, LogicalPlan::IndexSeek { .. }));
    // Residual is the age > 20 comparison, not the name = 'Ada' one.
    let Expr::Compare { left, .. } = predicate else {
        panic!("expected Compare residual");
    };
    assert!(matches!(*left, Expr::Property { ref key, .. } if key == "age"));
}

#[test]
fn where_eq_picks_indexed_conjunct_when_unindexed_first() {
    // The non-indexed `age` shouldn't block the indexed `name`
    // from being lifted into the seek.
    let ctx = ctx_with_index("Person", "name");
    let p = plan_with(
        "MATCH (n:Person) WHERE n.age = 30 AND n.name = 'Ada' RETURN n",
        &ctx,
    );
    let LogicalPlan::Project { input, .. } = p else {
        panic!("expected Project");
    };
    let LogicalPlan::Filter { input: seek, .. } = *input else {
        panic!("expected residual Filter");
    };
    assert!(matches!(*seek, LogicalPlan::IndexSeek { .. }));
}

#[test]
fn where_or_does_not_rewrite() {
    // Disjunction can't be safely converted to a seek — the rewrite
    // pass should leave the Filter intact.
    let ctx = ctx_with_index("Person", "name");
    let p = plan_with(
        "MATCH (n:Person) WHERE n.name = 'Ada' OR n.age = 30 RETURN n",
        &ctx,
    );
    let LogicalPlan::Project { input, .. } = p else {
        panic!("expected Project");
    };
    let LogicalPlan::Filter { input: scan, .. } = *input else {
        panic!("expected Filter, got {input:?}");
    };
    assert!(matches!(*scan, LogicalPlan::NodeScanByLabels { .. }));
}

#[test]
fn where_eq_against_other_var_does_not_rewrite() {
    // `n.name = m.name` is row-dependent on `m`, can't hoist.
    let ctx = ctx_with_index("Person", "name");
    let p = plan_with(
        "MATCH (n:Person), (m:Person) WHERE n.name = m.name RETURN n",
        &ctx,
    );
    // Should still be a Filter at the top (under Project), not IndexSeek.
    let LogicalPlan::Project { input, .. } = p else {
        panic!("expected Project");
    };
    assert!(matches!(*input, LogicalPlan::Filter { .. }));
}

#[test]
fn where_non_equality_does_not_rewrite() {
    let ctx = ctx_with_index("Person", "name");
    let p = plan_with("MATCH (n:Person) WHERE n.name > 'A' RETURN n", &ctx);
    let LogicalPlan::Project { input, .. } = p else {
        panic!("expected Project");
    };
    assert!(matches!(*input, LogicalPlan::Filter { .. }));
}

#[test]
fn where_eq_with_no_matching_index_does_not_rewrite() {
    // Empty planner context — no rewrite even though the query
    // pattern looks indexable.
    let p = plan_with(
        "MATCH (n:Person) WHERE n.name = 'Ada' RETURN n",
        &PlannerContext::default(),
    );
    let LogicalPlan::Project { input, .. } = p else {
        panic!("expected Project");
    };
    assert!(matches!(*input, LogicalPlan::Filter { .. }));
}

#[test]
fn bare_return_parses_to_return_statement() {
    match parse("RETURN 'hi' AS msg").unwrap() {
        Statement::Return(r) => {
            assert_eq!(r.return_items.len(), 1);
            assert_eq!(r.return_items[0].alias.as_deref(), Some("msg"));
        }
        other => panic!("expected Statement::Return, got {other:?}"),
    }
}

#[test]
fn bare_return_supports_multiple_items() {
    match parse("RETURN 1 AS a, 2 AS b, 'x' AS c").unwrap() {
        Statement::Return(r) => assert_eq!(r.return_items.len(), 3),
        other => panic!("expected Statement::Return, got {other:?}"),
    }
}

#[test]
fn with_clause_parses_with_projection() {
    let m = unwrap_match(parse("MATCH (n) WITH n.name AS name RETURN name").unwrap());
    let w = first_with(&m).expect("should have with_clause");
    assert_eq!(w.items.len(), 1);
    assert_eq!(w.items[0].alias.as_deref(), Some("name"));
    assert!(w.where_clause.is_none());
    assert!(!w.distinct);
}

#[test]
fn with_clause_parses_with_where_filter() {
    let m = unwrap_match(parse("MATCH (n) WITH n.age AS age WHERE age > 20 RETURN age").unwrap());
    let w = first_with(&m).expect("should have with_clause");
    assert!(w.where_clause.is_some());
}

#[test]
fn with_clause_parses_distinct() {
    let m = unwrap_match(parse("MATCH (n) WITH DISTINCT n.name AS name RETURN name").unwrap());
    let w = first_with(&m).expect("should have with_clause");
    assert!(w.distinct);
}

#[test]
fn with_clause_parses_order_skip_limit() {
    let m = unwrap_match(
        parse("MATCH (n) WITH n.age AS age ORDER BY age DESC SKIP 1 LIMIT 5 RETURN age").unwrap(),
    );
    let w = first_with(&m).expect("should have with_clause");
    assert_eq!(w.skip, Some(1));
    assert_eq!(w.limit, Some(5));
    assert_eq!(w.order_by.len(), 1);
}

#[test]
fn match_without_with_still_parses() {
    // Regression: existing MATCH RETURN shape stays intact.
    let m = unwrap_match(parse("MATCH (n) RETURN n").unwrap());
    assert!(first_with(&m).is_none());
}

// ---------------------------------------------------------------
// Chained reading clauses (multi-stage MATCH ... WITH ... MATCH ...)
// ---------------------------------------------------------------

#[test]
fn match_with_match_return_parses_chain() {
    // Canonical multi-stage shape: MATCH → WITH → MATCH → RETURN.
    // Produces three reading clauses in order (Match, With, Match)
    // and a terminal tail with two return items.
    let m = unwrap_match(parse("MATCH (a) WITH a WITH a MATCH (b) RETURN a, b").unwrap());
    assert_eq!(m.clauses.len(), 4);
    assert!(matches!(m.clauses[0], ReadingClause::Match(_)));
    assert!(matches!(m.clauses[1], ReadingClause::With(_)));
    assert!(matches!(m.clauses[2], ReadingClause::With(_)));
    assert!(matches!(m.clauses[3], ReadingClause::Match(_)));
    assert_eq!(m.terminal.return_items.len(), 2);
}

#[test]
fn chained_with_with_parses() {
    let m = unwrap_match(parse("MATCH (n) WITH n WITH n WHERE n.x = 1 RETURN n").unwrap());
    let with_count = m
        .clauses
        .iter()
        .filter(|c| matches!(c, ReadingClause::With(_)))
        .count();
    assert_eq!(with_count, 2);
}

#[test]
fn optional_match_after_with_parses() {
    let m =
        unwrap_match(parse("MATCH (a) WITH a OPTIONAL MATCH (a)-[:R]->(b) RETURN a, b").unwrap());
    assert_eq!(m.clauses.len(), 3);
    assert!(matches!(m.clauses[0], ReadingClause::Match(_)));
    assert!(matches!(m.clauses[1], ReadingClause::With(_)));
    assert!(matches!(m.clauses[2], ReadingClause::OptionalMatch(_)));
}

#[test]
fn three_match_stages_parse() {
    let m = unwrap_match(
        parse(
            "MATCH (a) WITH a \
             MATCH (b) WITH a, b \
             MATCH (c) RETURN a, b, c",
        )
        .unwrap(),
    );
    assert_eq!(m.clauses.len(), 5);
    let kinds: Vec<&str> = m
        .clauses
        .iter()
        .map(|c| match c {
            ReadingClause::Match(_) => "M",
            ReadingClause::With(_) => "W",
            ReadingClause::OptionalMatch(_) => "O",
            ReadingClause::Merge(_) => "G",
        })
        .collect();
    assert_eq!(kinds, vec!["M", "W", "M", "W", "M"]);
}

#[test]
fn match_without_with_still_parses_as_single_clause() {
    // Regression: plain MATCH RETURN produces exactly one
    // reading clause and populates the terminal.
    let m = unwrap_match(parse("MATCH (n) RETURN n").unwrap());
    assert_eq!(m.clauses.len(), 1);
    assert!(matches!(m.clauses[0], ReadingClause::Match(_)));
    assert_eq!(m.terminal.return_items.len(), 1);
}

#[test]
fn match_followed_by_merge_parses_as_chained_clauses() {
    // MATCH → MERGE → RETURN — the user's original query
    // shape that triggered this whole feature. Produces two
    // reading clauses (Match then Merge) plus a terminal with
    // one return item.
    let m = unwrap_match(
        parse(
            "MATCH (a:Person {id: '1'}) \
             MERGE (b:Person {id: '2'}) \
             ON CREATE SET b.name = 'Bob' \
             RETURN a, b",
        )
        .unwrap(),
    );
    assert_eq!(m.clauses.len(), 2);
    assert!(matches!(m.clauses[0], ReadingClause::Match(_)));
    let ReadingClause::Merge(mc) = &m.clauses[1] else {
        panic!("expected second clause to be Merge, got {:?}", m.clauses[1]);
    };
    assert_eq!(mc.pattern.start.var.as_deref(), Some("b"));
    assert_eq!(mc.on_create.len(), 1);
    assert_eq!(m.terminal.return_items.len(), 2);
}

#[test]
fn cross_stage_rebind_emits_edge_expand_directly_on_with_output() {
    // Structural check: MATCH (a:Person) WITH a MATCH (a)-[:KNOWS]->(b)
    // should lower to an EdgeExpand whose src_var is `a` and
    // whose input is the WITH's Project output — NOT a
    // CartesianProduct with a fresh NodeScan. This test is the
    // load-bearing one; it proves the rewrite structurally.
    let stmt = parse("MATCH (a:Person) WITH a MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
    let p = plan(&stmt).unwrap();
    // Walk down through the outer RETURN pipeline (Project)
    // until we hit the EdgeExpand we care about.
    let mut cur = &p;
    loop {
        match cur {
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::OrderBy { input, .. }
            | LogicalPlan::Skip { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Filter { input, .. } => {
                cur = input;
            }
            LogicalPlan::EdgeExpand { src_var, input, .. } => {
                assert_eq!(src_var, "a");
                // The expand's input must be the WITH's
                // projection (which sits directly on top of
                // the first MATCH's NodeScanByLabels). It must
                // NOT be a CartesianProduct — that would mean
                // the rebind didn't fire and we cross-joined a
                // fresh scan.
                assert!(
                    !matches!(**input, LogicalPlan::CartesianProduct { .. }),
                    "EdgeExpand input should be the WITH's projection, \
                     not a CartesianProduct: {input:?}"
                );
                // And it must not be a fresh scan on `a`.
                assert!(
                    !matches!(
                        **input,
                        LogicalPlan::NodeScanAll { .. } | LogicalPlan::NodeScanByLabels { .. }
                    ),
                    "EdgeExpand input should not be a fresh scan: {input:?}"
                );
                break;
            }
            other => panic!("expected EdgeExpand somewhere in the plan tree, got {other:?}"),
        }
    }
}

#[test]
fn optional_match_parses_single_clause() {
    let m = unwrap_match(
        parse("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p, f").unwrap(),
    );
    assert_eq!(first_optional_matches(&m).len(), 1);
    assert_eq!(first_optional_matches(&m)[0].patterns.len(), 1);
    assert!(first_optional_matches(&m)[0].where_clause.is_none());
}

#[test]
fn optional_match_with_where_parses() {
    let m = unwrap_match(
        parse("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) WHERE f.age > 20 RETURN p, f")
            .unwrap(),
    );
    assert_eq!(first_optional_matches(&m).len(), 1);
    assert!(first_optional_matches(&m)[0].where_clause.is_some());
}

#[test]
fn multiple_optional_match_clauses_parse() {
    let m = unwrap_match(
        parse(
            "MATCH (p:Person) \
             OPTIONAL MATCH (p)-[:KNOWS]->(f) \
             OPTIONAL MATCH (p)-[:WORKS_AT]->(c) \
             RETURN p, f, c",
        )
        .unwrap(),
    );
    assert_eq!(first_optional_matches(&m).len(), 2);
}

#[test]
fn optional_match_multi_hop_rejected_at_plan_time() {
    use mesh_cypher::plan;
    let stmt = parse("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f)-[:WORKS_AT]->(c) RETURN p")
        .unwrap();
    let err = plan(&stmt).unwrap_err();
    assert!(
        err.to_string().contains("single-hop"),
        "expected single-hop error, got: {err}"
    );
}

#[test]
fn optional_match_unbound_start_rejected_at_plan_time() {
    use mesh_cypher::plan;
    let stmt =
        parse("MATCH (p:Person) OPTIONAL MATCH (other:Person)-[:KNOWS]->(f) RETURN p").unwrap();
    let err = plan(&stmt).unwrap_err();
    assert!(
        err.to_string().contains("must be bound"),
        "expected unbound-start error, got: {err}"
    );
}

/// Extract the first MERGE clause out of a MatchStmt. Used by
/// the migrated MERGE tests below that predate the unified
/// reading-clause model and were written against the old
/// Statement::Merge variant.
fn first_merge(m: &MatchStmt) -> &MergeClause {
    m.clauses
        .iter()
        .find_map(|c| match c {
            ReadingClause::Merge(mc) => Some(mc),
            _ => None,
        })
        .expect("expected a MERGE clause in the query")
}

#[test]
fn merge_on_create_set_parses() {
    let m = unwrap_match(
        parse("MERGE (p:Person {email: 'a@b'}) ON CREATE SET p.name = 'Ada' RETURN p").unwrap(),
    );
    let mc = first_merge(&m);
    assert_eq!(mc.on_create.len(), 1);
    assert!(mc.on_match.is_empty());
}

#[test]
fn merge_on_match_set_parses() {
    let m = unwrap_match(
        parse("MERGE (p:Person {email: 'a@b'}) ON MATCH SET p.seen = true RETURN p").unwrap(),
    );
    let mc = first_merge(&m);
    assert!(mc.on_create.is_empty());
    assert_eq!(mc.on_match.len(), 1);
}

#[test]
fn multi_top_level_merge_parses_as_chained_clauses() {
    // The user's canonical upsert-and-link shape. Starts
    // with MERGE (no MATCH first), chains additional MERGEs,
    // and ends in a RETURN. Parses as a MatchStmt with a
    // Merge as its first reading clause.
    let m = unwrap_match(
        parse(
            "MERGE (a:Person {id: '1'}) ON CREATE SET a.name = 'Alice' \
             MERGE (b:Person {id: '2'}) ON CREATE SET b.name = 'Bob' \
             MERGE (a)-[r:KNOWS]->(b) \
             RETURN a, r, b",
        )
        .unwrap(),
    );
    assert_eq!(m.clauses.len(), 3);
    for (i, c) in m.clauses.iter().enumerate() {
        assert!(
            matches!(c, ReadingClause::Merge(_)),
            "clause {i} should be Merge, got {c:?}"
        );
    }
    // The third clause is the edge merge.
    let ReadingClause::Merge(edge_merge) = &m.clauses[2] else {
        unreachable!()
    };
    assert_eq!(edge_merge.pattern.hops.len(), 1);
    assert_eq!(
        edge_merge.pattern.hops[0].rel.edge_type.as_deref(),
        Some("KNOWS")
    );
    assert_eq!(m.terminal.return_items.len(), 3);
}

#[test]
fn bare_top_level_merge_parses_without_return() {
    // `MERGE (n) ON CREATE SET n.name = 'x'` with no RETURN
    // is effectful and should parse / plan cleanly.
    let m = unwrap_match(parse("MERGE (n:Thing {id: '1'}) ON CREATE SET n.name = 'x'").unwrap());
    assert_eq!(m.clauses.len(), 1);
    assert!(matches!(m.clauses[0], ReadingClause::Merge(_)));
    assert!(m.terminal.return_items.is_empty());
    // Planner should accept it — MERGE is its own mutation
    // so no RETURN is required.
    plan(&parse("MERGE (n:Thing {id: '1'}) ON CREATE SET n.name = 'x'").unwrap()).unwrap();
}

#[test]
fn edge_merge_unbound_endpoints_rejected_at_plan_time() {
    // `MERGE (a)-[:KNOWS]->(b)` without a preceding MATCH or
    // MERGE that binds `a` and `b` should error cleanly —
    // v1 requires both endpoints to be already in scope.
    let stmt = parse("MERGE (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
    let err = plan(&stmt).unwrap_err();
    assert!(
        err.to_string().contains("must be bound"),
        "expected unbound-endpoint error, got: {err}"
    );
}

#[test]
fn merge_on_create_and_on_match_both_parse() {
    let m = unwrap_match(
        parse(
            "MERGE (p:Person {email: 'a@b'}) \
             ON CREATE SET p.name = 'Ada', p.created = true \
             ON MATCH SET p.seen = true \
             RETURN p",
        )
        .unwrap(),
    );
    let mc = first_merge(&m);
    assert_eq!(mc.on_create.len(), 2);
    assert_eq!(mc.on_match.len(), 1);
}

#[test]
fn bare_return_supports_skip_and_limit() {
    match parse("RETURN 1 AS x SKIP 0 LIMIT 1").unwrap() {
        Statement::Return(r) => {
            assert_eq!(r.skip, Some(0));
            assert_eq!(r.limit, Some(1));
        }
        other => panic!("expected Statement::Return, got {other:?}"),
    }
}

#[test]
fn pattern_prop_filter_plus_where_rewrites_through_chain() {
    // MATCH (n:Person {age: 30}) WHERE n.name = 'Ada' — pattern-prop
    // emits a Filter chain over NodeScanByLabels (since `age` isn't
    // indexed), and the WHERE rewrite has to walk through that
    // inner filter to reach the scan.
    let ctx = ctx_with_index("Person", "name");
    let p = plan_with(
        "MATCH (n:Person {age: 30}) WHERE n.name = 'Ada' RETURN n",
        &ctx,
    );
    let LogicalPlan::Project { input, .. } = p else {
        panic!("expected Project");
    };
    let LogicalPlan::Filter { input: seek, .. } = *input else {
        panic!("expected residual Filter (age=30)");
    };
    assert!(matches!(*seek, LogicalPlan::IndexSeek { .. }));
}

#[test]
fn starts_with_operator_parses() {
    let m = unwrap_match(parse("MATCH (n) WHERE n.name STARTS WITH 'A' RETURN n").unwrap());
    let Expr::Compare { op, .. } = first_match(&m).where_clause.clone().unwrap() else {
        panic!("expected Compare");
    };
    assert_eq!(op, CompareOp::StartsWith);
}

#[test]
fn ends_with_operator_parses() {
    let m = unwrap_match(parse("MATCH (n) WHERE n.name ENDS WITH 'z' RETURN n").unwrap());
    let Expr::Compare { op, .. } = first_match(&m).where_clause.clone().unwrap() else {
        panic!("expected Compare");
    };
    assert_eq!(op, CompareOp::EndsWith);
}

#[test]
fn contains_operator_parses() {
    let m = unwrap_match(parse("MATCH (n) WHERE n.name CONTAINS 'ad' RETURN n").unwrap());
    let Expr::Compare { op, .. } = first_match(&m).where_clause.clone().unwrap() else {
        panic!("expected Compare");
    };
    assert_eq!(op, CompareOp::Contains);
}

#[test]
fn is_null_operator_parses() {
    let m = unwrap_match(parse("MATCH (n) WHERE n.name IS NULL RETURN n").unwrap());
    let Expr::IsNull { negated, .. } = first_match(&m).where_clause.clone().unwrap() else {
        panic!("expected IsNull");
    };
    assert!(!negated);
}

#[test]
fn is_not_null_operator_parses() {
    let m = unwrap_match(parse("MATCH (n) WHERE n.name IS NOT NULL RETURN n").unwrap());
    let Expr::IsNull { negated, .. } = first_match(&m).where_clause.clone().unwrap() else {
        panic!("expected IsNull");
    };
    assert!(negated);
}

#[test]
fn union_two_branches_parses_as_dedup() {
    let s = parse(
        "MATCH (a:Person) RETURN a.name AS name \
         UNION \
         MATCH (c:Company) RETURN c.name AS name",
    )
    .unwrap();
    let Statement::Union(u) = s else {
        panic!("expected Statement::Union");
    };
    assert_eq!(u.branches.len(), 2);
    assert!(!u.all);
    assert!(matches!(u.branches[0], Statement::Match(_)));
    assert!(matches!(u.branches[1], Statement::Match(_)));
}

#[test]
fn union_all_preserves_duplicates_flag() {
    let s = parse(
        "MATCH (a:Person) RETURN a.name AS name \
         UNION ALL \
         MATCH (c:Company) RETURN c.name AS name",
    )
    .unwrap();
    let Statement::Union(u) = s else {
        panic!("expected Statement::Union");
    };
    assert!(u.all);
}

#[test]
fn union_chain_of_three_flattens_to_single_union() {
    let s = parse(
        "RETURN 1 AS x \
         UNION \
         RETURN 2 AS x \
         UNION \
         RETURN 3 AS x",
    )
    .unwrap();
    let Statement::Union(u) = s else {
        panic!("expected Statement::Union");
    };
    assert_eq!(u.branches.len(), 3);
    assert!(!u.all);
}

#[test]
fn union_mixed_with_all_is_rejected() {
    let err = parse(
        "RETURN 1 AS x \
         UNION \
         RETURN 2 AS x \
         UNION ALL \
         RETURN 3 AS x",
    )
    .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("mix UNION and UNION ALL"),
        "expected mix-rejection message, got: {msg}"
    );
}

#[test]
fn single_read_query_without_union_is_not_wrapped() {
    // The grammar flows even a single-branch read query through
    // `union_query`; confirm the parser unwraps it back to the
    // inner statement instead of producing a trivial Union.
    let s = parse("MATCH (n) RETURN n").unwrap();
    assert!(matches!(s, Statement::Match(_)));
}

#[test]
fn union_column_mismatch_fails_at_plan_time() {
    // Parser accepts it; planner rejects because columns don't match.
    let s = parse(
        "MATCH (a:Person) RETURN a.name AS name \
         UNION \
         MATCH (c:Company) RETURN c.founded AS year",
    )
    .unwrap();
    let err = plan(&s).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("UNION branch") && msg.contains("must project the same columns"),
        "expected column-mismatch message, got: {msg}"
    );
}

#[test]
fn map_literal_empty_parses() {
    let s = parse("RETURN {} AS m").unwrap();
    let Statement::Return(r) = s else {
        panic!("expected Return");
    };
    let Expr::Map(entries) = &r.return_items[0].expr else {
        panic!("expected Expr::Map, got {:?}", r.return_items[0].expr);
    };
    assert!(entries.is_empty());
}

#[test]
fn map_literal_with_literal_values_parses() {
    let s = parse("RETURN {name: 'Ada', age: 30} AS person").unwrap();
    let Statement::Return(r) = s else {
        panic!("expected Return");
    };
    let Expr::Map(entries) = &r.return_items[0].expr else {
        panic!("expected Expr::Map");
    };
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, "name");
    assert_eq!(entries[1].0, "age");
}

#[test]
fn map_literal_preserves_source_order() {
    let s = parse("RETURN {b: 2, a: 1, c: 3} AS m").unwrap();
    let Statement::Return(r) = s else {
        panic!("expected Return");
    };
    let Expr::Map(entries) = &r.return_items[0].expr else {
        panic!("expected Expr::Map");
    };
    let keys: Vec<&str> = entries.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, vec!["b", "a", "c"]);
}

#[test]
fn map_literal_with_property_reference_parses() {
    // The value side is a full expression, so row-variable
    // references like `a.name` should be accepted.
    let s = parse("MATCH (a:Person) RETURN {name: a.name, id: a.id} AS p").unwrap();
    let Statement::Match(m) = s else {
        panic!("expected Match");
    };
    let Expr::Map(entries) = &m.terminal.return_items[0].expr else {
        panic!("expected Expr::Map");
    };
    assert_eq!(entries.len(), 2);
    assert!(matches!(
        entries[0].1,
        Expr::Property { ref var, ref key } if var == "a" && key == "name"
    ));
}
