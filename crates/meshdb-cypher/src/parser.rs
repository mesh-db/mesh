use crate::{
    ast::*,
    error::{Error, Result},
};
use pest::iterators::Pair;
use pest::Parser as _;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "cypher.pest"]
struct CypherParser;

pub fn parse(input: &str) -> Result<Statement> {
    let mut pairs =
        CypherParser::parse(Rule::query, input).map_err(|e| Error::Parse(e.to_string()))?;
    let query = pairs
        .next()
        .ok_or_else(|| Error::Parse("empty input".into()))?;
    let mut mode = None; // None, Some("explain"), Some("profile")
    let mut body_pair = None;
    for p in query.into_inner() {
        match p.as_rule() {
            Rule::kw_explain => mode = Some("explain"),
            Rule::kw_profile => mode = Some("profile"),
            Rule::query_body => body_pair = Some(p),
            _ => {}
        }
    }
    let body = body_pair.ok_or_else(|| Error::Parse("missing query_body".into()))?;
    let stmt = build_query_body(body)?;
    match mode {
        Some("explain") => Ok(Statement::Explain(Box::new(stmt))),
        Some("profile") => Ok(Statement::Profile(Box::new(stmt))),
        _ => Ok(stmt),
    }
}

fn build_query_body(pair: Pair<Rule>) -> Result<Statement> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("empty query_body".into()))?;
    match inner.as_rule() {
        Rule::create_stmt => Ok(Statement::Create(build_create(inner)?)),
        Rule::create_index_stmt => Ok(Statement::CreateIndex(build_index_ddl(inner)?)),
        Rule::drop_index_stmt => Ok(Statement::DropIndex(build_index_ddl(inner)?)),
        Rule::show_indexes_stmt => Ok(Statement::ShowIndexes),
        Rule::create_point_index_stmt => {
            Ok(Statement::CreatePointIndex(build_point_index_ddl(inner)?))
        }
        Rule::drop_point_index_stmt => Ok(Statement::DropPointIndex(build_point_index_ddl(inner)?)),
        Rule::show_point_indexes_stmt => Ok(Statement::ShowPointIndexes),
        Rule::create_constraint_stmt => {
            Ok(Statement::CreateConstraint(build_create_constraint(inner)?))
        }
        Rule::drop_constraint_stmt => Ok(Statement::DropConstraint(build_drop_constraint(inner)?)),
        Rule::show_constraints_stmt => Ok(Statement::ShowConstraints),
        Rule::union_query => build_union_query(inner),
        r => Err(Error::Parse(format!("unexpected rule: {:?}", r))),
    }
}

/// Parse a `union_query` production: one or more `read_stmt`
/// branches joined by optional `UNION` / `UNION ALL` separators.
/// Collapses to the branch's inner statement when there's only
/// one branch (no UNION keywords seen), otherwise builds a
/// `Statement::Union`. Mixing `UNION` and `UNION ALL` in the same
/// chain is rejected — Neo4j doesn't allow it and the dedup
/// semantics would be ambiguous.
fn build_union_query(pair: Pair<Rule>) -> Result<Statement> {
    let mut branches: Vec<Statement> = Vec::new();
    // Per separator: `true` when the corresponding `UNION` was
    // followed by `ALL`, `false` for plain `UNION`. Length = branches - 1
    // after the first branch is pushed.
    let mut separators: Vec<bool> = Vec::new();
    let mut pending_union = false;
    let mut pending_all = false;
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::read_stmt => {
                branches.push(build_read_stmt(child)?);
                if pending_union {
                    separators.push(pending_all);
                    pending_union = false;
                    pending_all = false;
                }
            }
            Rule::kw_union => {
                pending_union = true;
            }
            Rule::kw_all => {
                pending_all = true;
            }
            r => return Err(Error::Parse(format!("unexpected union_query child: {r:?}"))),
        }
    }
    if branches.is_empty() {
        return Err(Error::Parse("union_query produced no branches".into()));
    }
    if separators.is_empty() {
        // No UNION at all — unwrap the single branch.
        return Ok(branches.pop().unwrap());
    }
    let all = separators[0];
    if separators.iter().any(|s| *s != all) {
        return Err(Error::Parse(
            "cannot mix UNION and UNION ALL in the same query; \
             all separators must be the same kind"
                .into(),
        ));
    }
    Ok(Statement::Union(UnionStmt { branches, all }))
}

fn build_read_stmt(pair: Pair<Rule>) -> Result<Statement> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("empty read_stmt".into()))?;
    match inner.as_rule() {
        Rule::match_stmt => {
            let m = build_match(inner)?;
            // A `match_stmt` whose whole body is a single
            // `CALL ns.name[(...)] [YIELD ...]` and no terminal is a
            // *standalone* procedure call — Neo4j allows forms here
            // (implicit args, `YIELD *`) that aren't legal in
            // mid-query position. Promote to [`Statement::CallProcedure`]
            // so the planner can tell the two contexts apart.
            if m.clauses.len() == 1 && terminal_is_empty(&m.terminal) {
                if let crate::ast::ReadingClause::CallProcedure(pc) = &m.clauses[0] {
                    return Ok(Statement::CallProcedure(pc.clone()));
                }
            }
            Ok(Statement::Match(m))
        }
        Rule::unwind_stmt => Ok(Statement::Unwind(build_unwind(inner)?)),
        Rule::return_only_stmt => Ok(Statement::Return(build_return_only(inner)?)),
        r => Err(Error::Parse(format!("unexpected read_stmt child: {r:?}"))),
    }
}

/// Cheap check for "this match_stmt has no terminal tail". The
/// grammar leaves `terminal` at its default when no `terminal_tail`
/// was seen, so every field is empty / `None` / `false`.
fn terminal_is_empty(t: &crate::ast::TerminalTail) -> bool {
    t.return_items.is_empty()
        && !t.star
        && !t.distinct
        && t.order_by.is_empty()
        && t.skip.is_none()
        && t.limit.is_none()
        && t.set_items.is_empty()
        && t.delete.is_none()
        && t.create_patterns.is_empty()
        && t.remove_items.is_empty()
        && t.foreach.is_none()
}

/// Parse a bare `RETURN <items>` statement. The grammar wraps a
/// single `return_tail`, so we just walk into it and reuse
/// `parse_return_tail` to fill the projection / distinct / sort /
/// skip / limit fields.
fn build_return_only(pair: Pair<Rule>) -> Result<crate::ast::ReturnStmt> {
    let mut return_items = Vec::new();
    let mut star = false;
    let mut distinct = false;
    let mut order_by = Vec::new();
    let mut skip = None;
    let mut limit = None;
    let tail = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("return_only_stmt missing return_tail".into()))?;
    parse_return_tail(
        tail,
        &mut return_items,
        &mut star,
        &mut distinct,
        &mut order_by,
        &mut skip,
        &mut limit,
    )?;
    Ok(crate::ast::ReturnStmt {
        return_items,
        star,
        distinct,
        order_by,
        skip,
        limit,
    })
}

/// Shared builder for the two DDL statements that carry a
/// `(scope target, property)` payload (`CREATE INDEX`, `DROP INDEX`).
/// The grammar fixes the order: the scope source (node or rel) comes
/// first, then the index property. We walk positionally and dispatch
/// on which scope wrapper fired so the rel form lands as
/// `IndexScope::Relationship(edge_type)` while the node form stays
/// on `IndexScope::Node(label)`.
fn build_index_ddl(pair: Pair<Rule>) -> Result<crate::ast::IndexDdl> {
    let mut scope: Option<crate::ast::IndexScope> = None;
    let mut properties: Vec<String> = Vec::new();
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::index_node_source => {
                // Wrapping rule; descend into `index_target`. The
                // pattern variable inside is grammar-required but
                // discarded because the index is per-label.
                let target = p
                    .into_inner()
                    .find(|c| c.as_rule() == Rule::index_target)
                    .ok_or_else(|| Error::Parse("index_node_source missing target".into()))?;
                let mut inner = target.into_inner();
                let _var = inner
                    .next()
                    .ok_or_else(|| Error::Parse("index target missing var".into()))?;
                let lab = inner
                    .next()
                    .ok_or_else(|| Error::Parse("index target missing label".into()))?;
                scope = Some(crate::ast::IndexScope::Node(parse_ident(lab.as_str())));
            }
            Rule::index_rel_source => {
                // Layout: `()` `rel_edge` `[` var `:` edge_type `]`
                // `rel_edge` `()`. Identifier children arrive in
                // source order: [var, edge_type]. Direction doesn't
                // matter for the index — an edge gets indexed
                // regardless of traversal direction — so we skip the
                // rel_edge children.
                let mut idents = p.into_inner().filter(|c| c.as_rule() == Rule::identifier);
                let _var = idents
                    .next()
                    .ok_or_else(|| Error::Parse("index rel source missing var".into()))?;
                let et = idents
                    .next()
                    .ok_or_else(|| Error::Parse("index rel source missing edge type".into()))?;
                scope = Some(crate::ast::IndexScope::Relationship(parse_ident(
                    et.as_str(),
                )));
            }
            Rule::index_target => {
                // Fallback for hand-constructed pairs that skip the
                // wrapper — keeps downstream tests that build pairs
                // directly working.
                let mut inner = p.into_inner();
                let _var = inner
                    .next()
                    .ok_or_else(|| Error::Parse("index target missing var".into()))?;
                let lab = inner
                    .next()
                    .ok_or_else(|| Error::Parse("index target missing label".into()))?;
                scope = Some(crate::ast::IndexScope::Node(parse_ident(lab.as_str())));
            }
            Rule::index_property => {
                let mut inner = p.into_inner();
                let _var = inner
                    .next()
                    .ok_or_else(|| Error::Parse("index property missing var".into()))?;
                let key = inner
                    .next()
                    .ok_or_else(|| Error::Parse("index property missing key".into()))?;
                properties.push(parse_ident(key.as_str()));
            }
            _ => {}
        }
    }
    if properties.is_empty() {
        return Err(Error::Parse("index ddl missing property".into()));
    }
    Ok(crate::ast::IndexDdl {
        scope: scope.ok_or_else(|| Error::Parse("index ddl missing scope".into()))?,
        properties,
    })
}

/// Build a [`PointIndexDdl`] from a `create_point_index_stmt` or
/// `drop_point_index_stmt` pair. Grammar-enforced single property;
/// scope comes from either the node source (`(n:Label)`) or the
/// relationship source (`()-[r:TYPE]-()`). The rule that fired
/// determines the [`IndexScope`] variant.
fn build_point_index_ddl(pair: Pair<Rule>) -> Result<crate::ast::PointIndexDdl> {
    let mut scope: Option<crate::ast::IndexScope> = None;
    let mut property: Option<String> = None;
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::index_node_source => {
                let target = p
                    .into_inner()
                    .find(|c| c.as_rule() == Rule::index_target)
                    .ok_or_else(|| Error::Parse("point index source missing target".into()))?;
                let mut inner = target.into_inner();
                let _var = inner
                    .next()
                    .ok_or_else(|| Error::Parse("point index target missing var".into()))?;
                let lab = inner
                    .next()
                    .ok_or_else(|| Error::Parse("point index target missing label".into()))?;
                scope = Some(crate::ast::IndexScope::Node(parse_ident(lab.as_str())));
            }
            Rule::index_rel_source => {
                let mut idents = p.into_inner().filter(|c| c.as_rule() == Rule::identifier);
                let _var = idents
                    .next()
                    .ok_or_else(|| Error::Parse("point index rel source missing var".into()))?;
                let et = idents.next().ok_or_else(|| {
                    Error::Parse("point index rel source missing edge type".into())
                })?;
                scope = Some(crate::ast::IndexScope::Relationship(parse_ident(
                    et.as_str(),
                )));
            }
            Rule::index_property => {
                let mut inner = p.into_inner();
                let _var = inner
                    .next()
                    .ok_or_else(|| Error::Parse("point index property missing var".into()))?;
                let key = inner
                    .next()
                    .ok_or_else(|| Error::Parse("point index property missing key".into()))?;
                property = Some(parse_ident(key.as_str()));
            }
            _ => {}
        }
    }
    Ok(crate::ast::PointIndexDdl {
        scope: scope.ok_or_else(|| Error::Parse("point index ddl missing scope".into()))?,
        property: property
            .ok_or_else(|| Error::Parse("point index ddl missing property".into()))?,
    })
}

/// Build a `CreateConstraintStmt` from a `create_constraint_stmt` pair.
/// The grammar gives us (in order): an optional `constraint_name_opt`,
/// an optional `if_not_exists`, the `constraint_target`, the
/// `constraint_property`, and a `constraint_requirement`. We walk the
/// children in a single pass because the grammar guarantees the
/// relative ordering even when the optional pieces are absent.
fn build_create_constraint(pair: Pair<Rule>) -> Result<crate::ast::CreateConstraintStmt> {
    let mut name: Option<String> = None;
    let mut if_not_exists = false;
    let mut scope: Option<crate::ast::ConstraintScope> = None;
    let mut properties: Vec<String> = Vec::new();
    let mut kind: Option<crate::ast::ConstraintKind> = None;
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::constraint_name_opt => {
                let ident = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("constraint name missing identifier".into()))?;
                name = Some(parse_ident(ident.as_str()));
            }
            Rule::if_not_exists => {
                if_not_exists = true;
            }
            Rule::constraint_node_source => {
                // Wrapping rule; descend into the `constraint_target`.
                let target = p
                    .into_inner()
                    .find(|c| c.as_rule() == Rule::constraint_target)
                    .ok_or_else(|| Error::Parse("constraint_node_source missing target".into()))?;
                let mut inner = target.into_inner();
                let _var = inner
                    .next()
                    .ok_or_else(|| Error::Parse("constraint target missing var".into()))?;
                let lab = inner
                    .next()
                    .ok_or_else(|| Error::Parse("constraint target missing label".into()))?;
                scope = Some(crate::ast::ConstraintScope::Node(parse_ident(lab.as_str())));
            }
            Rule::constraint_rel_source => {
                // Layout: `()` `rel_edge` `[` var `:` edge_type `]`
                // `rel_edge` `()`. Identifier children arrive in
                // source order: [var, edge_type]. We skip the
                // rel_edge children since direction doesn't matter
                // for the constraint's scope.
                let mut idents = p.into_inner().filter(|c| c.as_rule() == Rule::identifier);
                let _var = idents
                    .next()
                    .ok_or_else(|| Error::Parse("constraint rel source missing var".into()))?;
                let et = idents.next().ok_or_else(|| {
                    Error::Parse("constraint rel source missing edge type".into())
                })?;
                scope = Some(crate::ast::ConstraintScope::Relationship(parse_ident(
                    et.as_str(),
                )));
            }
            Rule::constraint_target => {
                // Legacy pest might still hand back the old
                // `constraint_target` rule directly; keep the
                // single-rule path working so downstream tests that
                // construct pairs by hand don't have to wrap.
                let mut inner = p.into_inner();
                let _var = inner
                    .next()
                    .ok_or_else(|| Error::Parse("constraint target missing var".into()))?;
                let lab = inner
                    .next()
                    .ok_or_else(|| Error::Parse("constraint target missing label".into()))?;
                scope = Some(crate::ast::ConstraintScope::Node(parse_ident(lab.as_str())));
            }
            Rule::constraint_property => {
                let mut inner = p.into_inner();
                let _var = inner
                    .next()
                    .ok_or_else(|| Error::Parse("constraint property missing var".into()))?;
                let key = inner
                    .next()
                    .ok_or_else(|| Error::Parse("constraint property missing key".into()))?;
                properties.push(parse_ident(key.as_str()));
            }
            Rule::constraint_property_list => {
                for child in p.into_inner() {
                    if child.as_rule() == Rule::constraint_property {
                        let mut inner = child.into_inner();
                        let _var = inner.next().ok_or_else(|| {
                            Error::Parse("constraint property missing var".into())
                        })?;
                        let key = inner.next().ok_or_else(|| {
                            Error::Parse("constraint property missing key".into())
                        })?;
                        properties.push(parse_ident(key.as_str()));
                    }
                }
            }
            Rule::constraint_requirement => {
                // `constraint_requirement` is the
                // `req_unique | req_not_null | req_property_type`
                // alternation, so the single inner pair tells us
                // which shape matched. Kept as a pair (not an atomic
                // string match) so future constraint kinds drop in
                // next to it.
                let req = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty constraint_requirement".into()))?;
                kind = Some(match req.as_rule() {
                    Rule::req_unique => crate::ast::ConstraintKind::Unique,
                    Rule::req_not_null => crate::ast::ConstraintKind::NotNull,
                    Rule::req_node_key => crate::ast::ConstraintKind::NodeKey,
                    Rule::req_property_type => {
                        let tag = req
                            .into_inner()
                            .find(|inner| inner.as_rule() == Rule::property_type_tag)
                            .ok_or_else(|| {
                                Error::Parse("req_property_type missing type tag".into())
                            })?;
                        let pt = match tag.as_str().to_ascii_uppercase().as_str() {
                            "STRING" => crate::ast::PropertyType::String,
                            "INTEGER" => crate::ast::PropertyType::Integer,
                            "FLOAT" => crate::ast::PropertyType::Float,
                            "BOOLEAN" => crate::ast::PropertyType::Boolean,
                            other => {
                                return Err(Error::Parse(format!(
                                    "unknown property type `{other}`"
                                )))
                            }
                        };
                        crate::ast::ConstraintKind::PropertyType(pt)
                    }
                    r => {
                        return Err(Error::Parse(format!(
                            "unexpected constraint requirement: {r:?}"
                        )))
                    }
                });
            }
            _ => {}
        }
    }
    if properties.is_empty() {
        return Err(Error::Parse("create constraint missing property".into()));
    }
    Ok(crate::ast::CreateConstraintStmt {
        name,
        scope: scope.ok_or_else(|| Error::Parse("create constraint missing scope".into()))?,
        properties,
        kind: kind.ok_or_else(|| Error::Parse("create constraint missing requirement".into()))?,
        if_not_exists,
    })
}

/// Build a `DropConstraintStmt` from a `drop_constraint_stmt` pair.
/// The grammar pins the order as `identifier` (name) followed by an
/// optional `if_exists`, so we walk once and pick the pieces out.
fn build_drop_constraint(pair: Pair<Rule>) -> Result<crate::ast::DropConstraintStmt> {
    let mut name: Option<String> = None;
    let mut if_exists = false;
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier => {
                if name.is_none() {
                    name = Some(parse_ident(p.as_str()));
                }
            }
            Rule::if_exists => {
                if_exists = true;
            }
            _ => {}
        }
    }
    Ok(crate::ast::DropConstraintStmt {
        name: name.ok_or_else(|| Error::Parse("drop constraint missing name".into()))?,
        if_exists,
    })
}

fn build_unwind(pair: Pair<Rule>) -> Result<UnwindStmt> {
    let mut expr: Option<Expr> = None;
    let mut alias: Option<String> = None;
    let mut where_clause = None;
    let mut return_items = Vec::new();
    let mut star = false;
    let mut distinct = false;
    let mut order_by: Vec<SortItem> = Vec::new();
    let mut skip = None;
    let mut limit = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::expression if expr.is_none() => {
                expr = Some(build_expression(p)?);
            }
            Rule::identifier if alias.is_none() => {
                alias = Some(parse_ident(p.as_str()));
            }
            Rule::where_clause => {
                let ep = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty where".into()))?;
                where_clause = Some(build_expression(ep)?);
            }
            Rule::return_tail => {
                parse_return_tail(
                    p,
                    &mut return_items,
                    &mut star,
                    &mut distinct,
                    &mut order_by,
                    &mut skip,
                    &mut limit,
                )?;
            }
            Rule::kw_as | Rule::kw_unwind => {}
            r => return Err(Error::Parse(format!("unexpected rule in unwind: {:?}", r))),
        }
    }

    let expr = expr.ok_or_else(|| Error::Parse("UNWIND requires a source expression".into()))?;
    let alias = alias.ok_or_else(|| Error::Parse("UNWIND requires an AS alias".into()))?;
    if return_items.is_empty() && !star {
        return Err(Error::Parse("UNWIND requires a RETURN clause".into()));
    }
    Ok(UnwindStmt {
        expr,
        alias,
        where_clause,
        return_items,
        star,
        distinct,
        order_by,
        skip,
        limit,
    })
}

fn build_unwind_clause(pair: Pair<Rule>) -> Result<crate::ast::UnwindClause> {
    debug_assert_eq!(pair.as_rule(), Rule::unwind_clause);
    let mut expr: Option<Expr> = None;
    let mut alias: Option<String> = None;
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::expression if expr.is_none() => {
                expr = Some(build_expression(p)?);
            }
            Rule::identifier if alias.is_none() => {
                alias = Some(parse_ident(p.as_str()));
            }
            Rule::kw_unwind | Rule::kw_as => {}
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in unwind_clause: {:?}",
                    r
                )))
            }
        }
    }
    let expr = expr.ok_or_else(|| Error::Parse("UNWIND requires a source expression".into()))?;
    let alias = alias.ok_or_else(|| Error::Parse("UNWIND requires an AS alias".into()))?;
    Ok(crate::ast::UnwindClause { expr, alias })
}

/// Parse a `merge_clause` pair — the `MERGE` keyword, the
/// pattern (a node or a single-hop edge path), and any
/// `ON CREATE SET` / `ON MATCH SET` actions — into a
/// [`MergeClause`]. Used by `build_match` for every
/// `reading_clause` whose inner is a `merge_clause`; there is
/// no separate top-level MERGE statement anymore.
fn build_merge_clause(pair: Pair<Rule>) -> Result<crate::ast::MergeClause> {
    debug_assert_eq!(pair.as_rule(), Rule::merge_clause);
    let mut pattern: Option<Pattern> = None;
    let mut on_create: Vec<crate::ast::SetItem> = Vec::new();
    let mut on_match: Vec<crate::ast::SetItem> = Vec::new();
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::pattern => pattern = Some(build_pattern(p)?),
            Rule::merge_action => {
                let (which, items) = build_merge_action(p)?;
                match which {
                    MergeActionKind::OnCreate => on_create.extend(items),
                    MergeActionKind::OnMatch => on_match.extend(items),
                }
            }
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in merge_clause: {:?}",
                    r
                )))
            }
        }
    }
    let pattern = pattern.ok_or_else(|| Error::Parse("MERGE requires a pattern".into()))?;
    Ok(crate::ast::MergeClause {
        pattern,
        on_create,
        on_match,
    })
}

enum MergeActionKind {
    OnCreate,
    OnMatch,
}

/// Walk one `merge_action` pair (`ON CREATE SET ...` or
/// `ON MATCH SET ...`) and return its kind plus the list of
/// `SetItem` lowered from the embedded `set_items` rule. Reuses
/// the existing `build_set_item` so SET semantics stay identical
/// across plain SET and the MERGE-conditional flavor.
fn build_merge_action(pair: Pair<Rule>) -> Result<(MergeActionKind, Vec<crate::ast::SetItem>)> {
    let mut kind: Option<MergeActionKind> = None;
    let mut items: Vec<crate::ast::SetItem> = Vec::new();
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::merge_on_create => kind = Some(MergeActionKind::OnCreate),
            Rule::merge_on_match => kind = Some(MergeActionKind::OnMatch),
            Rule::set_items => {
                for set_item_pair in inner.into_inner() {
                    debug_assert_eq!(set_item_pair.as_rule(), Rule::set_item);
                    // build_set_item expects the inner alternative
                    // (set_prop_item, set_label_item, …), not the
                    // set_item wrapper.
                    let inner = set_item_pair
                        .into_inner()
                        .next()
                        .ok_or_else(|| Error::Parse("empty set item".into()))?;
                    items.push(build_set_item(inner)?);
                }
            }
            // Silent kw_on / kw_set don't produce pairs at all,
            // but other tokens shouldn't appear here.
            _ => {}
        }
    }
    let kind = kind.ok_or_else(|| Error::Parse("merge_action missing CREATE/MATCH".into()))?;
    Ok((kind, items))
}

fn build_create(pair: Pair<Rule>) -> Result<CreateStmt> {
    let mut patterns: Vec<Pattern> = Vec::new();
    let mut return_items = Vec::new();
    let mut star = false;
    let mut distinct = false;
    let mut order_by: Vec<SortItem> = Vec::new();
    let mut skip = None;
    let mut limit = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::pattern_list => patterns = build_pattern_list(p)?,
            Rule::return_tail => {
                parse_return_tail(
                    p,
                    &mut return_items,
                    &mut star,
                    &mut distinct,
                    &mut order_by,
                    &mut skip,
                    &mut limit,
                )?;
            }
            r => return Err(Error::Parse(format!("unexpected rule in create: {:?}", r))),
        }
    }

    if patterns.is_empty() {
        return Err(Error::Parse("CREATE requires at least one pattern".into()));
    }

    Ok(CreateStmt {
        patterns,
        return_items,
        star,
        distinct,
        order_by,
        skip,
        limit,
    })
}

fn parse_return_tail(
    pair: Pair<Rule>,
    return_items: &mut Vec<ReturnItem>,
    star: &mut bool,
    distinct: &mut bool,
    order_by: &mut Vec<SortItem>,
    skip: &mut Option<Expr>,
    limit: &mut Option<Expr>,
) -> Result<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::kw_distinct => *distinct = true,
            Rule::return_items => {
                let (items, is_star) = build_return_items(inner)?;
                *return_items = items;
                *star = is_star;
            }
            Rule::order_by_clause => *order_by = build_order_by(inner)?,
            Rule::skip_clause => {
                let expr_pair = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty skip".into()))?;
                *skip = Some(build_expression(expr_pair)?);
            }
            Rule::limit_clause => {
                let expr_pair = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty limit".into()))?;
                *limit = Some(build_expression(expr_pair)?);
            }
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in return tail: {:?}",
                    r
                )))
            }
        }
    }
    Ok(())
}

fn build_pattern_list(pair: Pair<Rule>) -> Result<Vec<Pattern>> {
    debug_assert_eq!(pair.as_rule(), Rule::pattern_list);
    let mut patterns = Vec::new();
    for p in pair.into_inner() {
        debug_assert_eq!(p.as_rule(), Rule::pattern);
        patterns.push(build_pattern(p)?);
    }
    Ok(patterns)
}

fn build_match(pair: Pair<Rule>) -> Result<MatchStmt> {
    use crate::ast::{ReadingClause, TerminalTail};

    let mut clauses: Vec<ReadingClause> = Vec::new();
    let mut terminal = TerminalTail::default();

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::reading_clause => {
                let inner = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty reading clause".into()))?;
                match inner.as_rule() {
                    Rule::match_clause => {
                        clauses.push(ReadingClause::Match(build_match_clause(inner)?));
                    }
                    Rule::optional_match_clause => {
                        clauses.push(ReadingClause::OptionalMatch(build_optional_match(inner)?));
                    }
                    Rule::with_tail => {
                        clauses.push(ReadingClause::With(build_with_tail(inner)?));
                    }
                    Rule::merge_clause => {
                        clauses.push(ReadingClause::Merge(build_merge_clause(inner)?));
                    }
                    Rule::unwind_clause => {
                        clauses.push(ReadingClause::Unwind(build_unwind_clause(inner)?));
                    }
                    Rule::load_csv_clause => {
                        let mut alias = String::new();
                        let mut path_expr = None;
                        let mut with_headers = false;
                        for child in inner.into_inner() {
                            match child.as_rule() {
                                Rule::kw_load | Rule::kw_csv | Rule::kw_from | Rule::kw_as => {}
                                Rule::kw_with => {}
                                Rule::kw_headers => with_headers = true,
                                Rule::expression if path_expr.is_none() => {
                                    path_expr = Some(build_expression(child)?);
                                }
                                Rule::identifier if alias.is_empty() => {
                                    alias = parse_ident(child.as_str());
                                }
                                _ => {}
                            }
                        }
                        clauses.push(ReadingClause::LoadCsv(crate::ast::LoadCsvClause {
                            path_expr: path_expr.ok_or_else(|| {
                                Error::Parse("LOAD CSV missing path expression".into())
                            })?,
                            alias,
                            with_headers,
                        }));
                    }
                    Rule::call_subquery => {
                        let mut body_stmt: Option<Statement> = None;
                        let mut in_tx: Option<crate::ast::InTransactionsConfig> = None;
                        for child in inner.into_inner() {
                            match child.as_rule() {
                                Rule::union_query => {
                                    body_stmt = Some(build_union_query(child)?);
                                }
                                Rule::in_transactions_suffix => {
                                    // Default batch size when `OF n ROWS` is
                                    // omitted matches Neo4j 5: 1000.
                                    let mut size = crate::ast::DEFAULT_IN_TRANSACTIONS_BATCH_SIZE;
                                    for grand in child.into_inner() {
                                        if grand.as_rule() == Rule::in_transactions_batch {
                                            for leaf in grand.into_inner() {
                                                if leaf.as_rule() == Rule::nn_integer {
                                                    size = leaf.as_str().parse().map_err(|e| {
                                                        Error::Parse(format!(
                                                            "IN TRANSACTIONS OF batch size: {e}"
                                                        ))
                                                    })?;
                                                    if size <= 0 {
                                                        return Err(Error::Parse(
                                                            "IN TRANSACTIONS OF batch size must be positive"
                                                                .into(),
                                                        ));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    in_tx =
                                        Some(crate::ast::InTransactionsConfig { batch_size: size });
                                }
                                _ => {}
                            }
                        }
                        let stmt =
                            body_stmt.ok_or_else(|| Error::Parse("CALL missing body".into()))?;
                        match in_tx {
                            Some(cfg) => {
                                clauses.push(ReadingClause::CallInTransactions(Box::new(stmt), cfg))
                            }
                            None => clauses.push(ReadingClause::Call(Box::new(stmt))),
                        }
                    }
                    Rule::call_procedure => {
                        let pc = build_procedure_call(inner)?;
                        clauses.push(ReadingClause::CallProcedure(pc));
                    }
                    r => {
                        return Err(Error::Parse(format!(
                            "unexpected rule inside reading_clause: {:?}",
                            r
                        )))
                    }
                }
            }
            Rule::mutation_clause => {
                let mc = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty mutation_clause".into()))?;
                match mc.as_rule() {
                    Rule::create_tail => {
                        let patterns = build_pattern_list_from_create(mc)?;
                        clauses.push(ReadingClause::Create(patterns));
                    }
                    Rule::set_tail => {
                        let items = build_set_items(mc)?;
                        clauses.push(ReadingClause::Set(items));
                    }
                    Rule::delete_tail => {
                        let del = build_delete_clause(mc)?;
                        clauses.push(ReadingClause::Delete(del));
                    }
                    Rule::remove_tail => {
                        let items = build_remove_items(mc)?;
                        clauses.push(ReadingClause::Remove(items));
                    }
                    Rule::foreach_tail => {
                        let fc = build_foreach_clause(mc)?;
                        clauses.push(ReadingClause::Foreach(fc));
                    }
                    r => {
                        return Err(Error::Parse(format!(
                            "unexpected rule inside mutation_clause: {:?}",
                            r
                        )))
                    }
                }
            }
            Rule::terminal_tail => {
                build_terminal_tail(p, &mut terminal)?;
            }
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in match_stmt: {:?}",
                    r
                )))
            }
        }
    }

    if clauses.is_empty() {
        return Err(Error::Parse(
            "match_stmt requires at least one reading clause".into(),
        ));
    }
    // A query must start with a producer — MATCH pulls rows
    // from the store, MERGE either matches or creates them, and
    // UNWIND expands a list (evaluated against an empty row) into
    // a row stream. OPTIONAL MATCH and WITH both depend on an
    // existing row stream, so they can't be the first clause.
    match &clauses[0] {
        ReadingClause::Match(_)
        | ReadingClause::Merge(_)
        | ReadingClause::Unwind(_)
        | ReadingClause::With(_)
        | ReadingClause::Call(_)
        | ReadingClause::CallProcedure(_)
        | ReadingClause::LoadCsv(_)
        | ReadingClause::Create(_)
        | ReadingClause::OptionalMatch(_) => {}
        _ => {
            return Err(Error::Parse(
                "query must start with MATCH, CREATE, MERGE, or UNWIND".into(),
            ));
        }
    }

    Ok(MatchStmt { clauses, terminal })
}

/// Parse a `call_procedure` pair — the `CALL ns.name[(args)] [YIELD
/// ...]` syntax — into a [`ProcedureCall`] struct. Used both by the
/// in-query reading-clause path and by the standalone-detection
/// wrapper at the top of [`build_match`]: the grammar doesn't
/// distinguish the two contexts, so the planner decides based on
/// whether the enclosing `MatchStmt` is a single-clause, no-terminal
/// envelope (standalone) or anything else (in-query).
fn build_procedure_call(pair: Pair<Rule>) -> Result<crate::ast::ProcedureCall> {
    debug_assert_eq!(pair.as_rule(), Rule::call_procedure);
    let mut qualified_name: Vec<String> = Vec::new();
    let mut args: Option<Vec<Expr>> = None;
    let mut yield_spec: Option<crate::ast::YieldSpec> = None;
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::procedure_name => {
                for id in p.into_inner() {
                    debug_assert_eq!(id.as_rule(), Rule::procedure_name_segment);
                    qualified_name.push(parse_ident(id.as_str()));
                }
            }
            Rule::procedure_args => {
                // `Some(vec![])` represents an explicit empty list
                // (`CALL ns.name()`) — the implicit-args form
                // (`CALL ns.name` with no parens) leaves `args`
                // as `None`.
                let mut list: Vec<Expr> = Vec::new();
                for child in p.into_inner() {
                    if child.as_rule() == Rule::procedure_arg_list {
                        for e in child.into_inner() {
                            if e.as_rule() == Rule::expression {
                                list.push(build_expression(e)?);
                            }
                        }
                    }
                }
                args = Some(list);
            }
            Rule::yield_clause => {
                yield_spec = Some(build_yield_clause(p)?);
            }
            Rule::kw_call => {}
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in call_procedure: {r:?}"
                )))
            }
        }
    }
    if qualified_name.is_empty() {
        return Err(Error::Parse("CALL missing procedure name".into()));
    }
    Ok(crate::ast::ProcedureCall {
        qualified_name,
        args,
        yield_spec,
    })
}

fn build_yield_clause(pair: Pair<Rule>) -> Result<crate::ast::YieldSpec> {
    debug_assert_eq!(pair.as_rule(), Rule::yield_clause);
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::yield_star => return Ok(crate::ast::YieldSpec::Star),
            Rule::yield_items => {
                let mut items: Vec<crate::ast::YieldItem> = Vec::new();
                for yi in p.into_inner() {
                    debug_assert_eq!(yi.as_rule(), Rule::yield_item);
                    let mut column = String::new();
                    let mut alias: Option<String> = None;
                    let mut saw_as = false;
                    for part in yi.into_inner() {
                        match part.as_rule() {
                            Rule::identifier => {
                                let name = parse_ident(part.as_str());
                                if column.is_empty() && !saw_as {
                                    column = name;
                                } else {
                                    alias = Some(name);
                                }
                            }
                            Rule::kw_as => saw_as = true,
                            _ => {}
                        }
                    }
                    items.push(crate::ast::YieldItem { column, alias });
                }
                return Ok(crate::ast::YieldSpec::Items(items));
            }
            Rule::kw_yield => {}
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in yield_clause: {r:?}"
                )))
            }
        }
    }
    Err(Error::Parse("empty yield_clause".into()))
}

/// Parse a bare `match_clause` (`MATCH pattern_list [WHERE]`)
/// into its AST form. Called from [`build_match`] for every
/// `reading_clause` whose inner is `match_clause`.
fn build_match_clause(pair: Pair<Rule>) -> Result<crate::ast::MatchClause> {
    debug_assert_eq!(pair.as_rule(), Rule::match_clause);
    let mut patterns: Vec<Pattern> = Vec::new();
    let mut where_clause: Option<Expr> = None;
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::pattern_list => patterns = build_pattern_list(inner)?,
            Rule::where_clause => {
                let expr_pair = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty where".into()))?;
                where_clause = Some(build_expression(expr_pair)?);
            }
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in match_clause: {:?}",
                    r
                )))
            }
        }
    }
    if patterns.is_empty() {
        return Err(Error::Parse("MATCH missing pattern list".into()));
    }
    Ok(crate::ast::MatchClause {
        patterns,
        where_clause,
    })
}

/// Parse a `terminal_tail` pair — the `RETURN` / `SET RETURN?`
/// / `DELETE RETURN?` / `CREATE RETURN?` suffix that ends a
/// match_stmt — into the fields of the shared [`TerminalTail`]
/// already owned by `build_match`.
fn build_terminal_tail(pair: Pair<Rule>, terminal: &mut crate::ast::TerminalTail) -> Result<()> {
    debug_assert_eq!(pair.as_rule(), Rule::terminal_tail);
    for p in pair.into_inner() {
        let p = if p.as_rule() == Rule::mutation_clause {
            p.into_inner()
                .next()
                .ok_or_else(|| Error::Parse("empty mutation_clause".into()))?
        } else {
            p
        };
        match p.as_rule() {
            Rule::return_tail => {
                parse_return_tail(
                    p,
                    &mut terminal.return_items,
                    &mut terminal.star,
                    &mut terminal.distinct,
                    &mut terminal.order_by,
                    &mut terminal.skip,
                    &mut terminal.limit,
                )?;
            }
            Rule::create_tail => {
                let list = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("create tail missing pattern list".into()))?;
                terminal.create_patterns = build_pattern_list(list)?;
            }
            Rule::set_tail => {
                let set_items_pair = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty set".into()))?;
                for set_item_pair in set_items_pair.into_inner() {
                    debug_assert_eq!(set_item_pair.as_rule(), Rule::set_item);
                    let inner = set_item_pair
                        .into_inner()
                        .next()
                        .ok_or_else(|| Error::Parse("empty set item".into()))?;
                    terminal.set_items.push(build_set_item(inner)?);
                }
            }
            Rule::delete_tail => {
                terminal.delete = Some(build_delete_clause(p)?);
            }
            Rule::remove_tail => {
                let items_pair = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty remove".into()))?;
                for item_pair in items_pair.into_inner() {
                    debug_assert_eq!(item_pair.as_rule(), Rule::remove_item);
                    let inner = item_pair
                        .into_inner()
                        .next()
                        .ok_or_else(|| Error::Parse("empty remove item".into()))?;
                    match inner.as_rule() {
                        Rule::remove_prop_item => {
                            let mut parts = inner.into_inner();
                            let pa = parts.next().unwrap();
                            let mut pa_inner = pa.into_inner();
                            let var = pa_inner.next().unwrap().as_str().to_string();
                            let key = pa_inner.next().unwrap().as_str().to_string();
                            terminal
                                .remove_items
                                .push(crate::ast::RemoveItem::Property { var, key });
                        }
                        Rule::remove_label_item => {
                            let mut var = String::new();
                            let mut labels = Vec::new();
                            for child in inner.into_inner() {
                                match child.as_rule() {
                                    Rule::identifier => var = parse_ident(child.as_str()),
                                    Rule::label_spec => {
                                        let label =
                                            child.into_inner().next().unwrap().as_str().to_string();
                                        labels.push(label);
                                    }
                                    _ => {}
                                }
                            }
                            terminal
                                .remove_items
                                .push(crate::ast::RemoveItem::Labels { var, labels });
                        }
                        r => {
                            return Err(Error::Parse(format!(
                                "unexpected rule in remove item: {:?}",
                                r
                            )));
                        }
                    }
                }
            }
            Rule::foreach_tail => {
                let mut var = String::new();
                let mut list_expr = None;
                let mut set_items = Vec::new();
                let mut remove_items = Vec::new();
                for inner in p.into_inner() {
                    match inner.as_rule() {
                        Rule::identifier if var.is_empty() => {
                            var = parse_ident(inner.as_str());
                        }
                        Rule::expression if list_expr.is_none() => {
                            list_expr = Some(build_expression(inner)?);
                        }
                        Rule::kw_in | Rule::kw_foreach => {}
                        Rule::foreach_body => {
                            let body_inner = inner
                                .into_inner()
                                .next()
                                .ok_or_else(|| Error::Parse("empty foreach body".into()))?;
                            match body_inner.as_rule() {
                                Rule::set_tail => {
                                    let items_pair = body_inner
                                        .into_inner()
                                        .next()
                                        .ok_or_else(|| Error::Parse("empty set".into()))?;
                                    for si in items_pair.into_inner() {
                                        let inner2 = si
                                            .into_inner()
                                            .next()
                                            .ok_or_else(|| Error::Parse("empty set item".into()))?;
                                        set_items.push(build_set_item(inner2)?);
                                    }
                                }
                                Rule::remove_tail => {
                                    let items_pair = body_inner
                                        .into_inner()
                                        .next()
                                        .ok_or_else(|| Error::Parse("empty remove".into()))?;
                                    for ri in items_pair.into_inner() {
                                        let inner2 = ri.into_inner().next().ok_or_else(|| {
                                            Error::Parse("empty remove item".into())
                                        })?;
                                        match inner2.as_rule() {
                                            Rule::remove_prop_item => {
                                                let mut parts = inner2.into_inner();
                                                let pa = parts.next().unwrap();
                                                let mut pa_inner = pa.into_inner();
                                                let v =
                                                    pa_inner.next().unwrap().as_str().to_string();
                                                let k =
                                                    pa_inner.next().unwrap().as_str().to_string();
                                                remove_items.push(
                                                    crate::ast::RemoveItem::Property {
                                                        var: v,
                                                        key: k,
                                                    },
                                                );
                                            }
                                            Rule::remove_label_item => {
                                                let mut v = String::new();
                                                let mut labels = Vec::new();
                                                for child in inner2.into_inner() {
                                                    match child.as_rule() {
                                                        Rule::identifier => {
                                                            v = parse_ident(child.as_str())
                                                        }
                                                        Rule::label_spec => {
                                                            labels.push(
                                                                child
                                                                    .into_inner()
                                                                    .next()
                                                                    .unwrap()
                                                                    .as_str()
                                                                    .to_string(),
                                                            );
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                                remove_items.push(crate::ast::RemoveItem::Labels {
                                                    var: v,
                                                    labels,
                                                });
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
                terminal.foreach = Some(crate::ast::ForeachClause {
                    var,
                    list_expr: list_expr
                        .ok_or_else(|| Error::Parse("FOREACH missing list expression".into()))?,
                    set_items,
                    remove_items,
                });
            }
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in terminal tail: {:?}",
                    r
                )))
            }
        }
    }
    Ok(())
}

/// Extract patterns from a `create_tail` pair.
fn build_pattern_list_from_create(pair: Pair<Rule>) -> Result<Vec<Pattern>> {
    let list = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("create tail missing pattern list".into()))?;
    build_pattern_list(list)
}

/// Parse the set_items from a `set_tail` pair.
fn build_set_items(pair: Pair<Rule>) -> Result<Vec<crate::ast::SetItem>> {
    let set_items_pair = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("empty set".into()))?;
    let mut items = Vec::new();
    for set_item_pair in set_items_pair.into_inner() {
        debug_assert_eq!(set_item_pair.as_rule(), Rule::set_item);
        let inner = set_item_pair
            .into_inner()
            .next()
            .ok_or_else(|| Error::Parse("empty set item".into()))?;
        items.push(build_set_item(inner)?);
    }
    Ok(items)
}

/// Parse a `delete_tail` pair into a [`DeleteClause`].
fn build_delete_clause(pair: Pair<Rule>) -> Result<DeleteClause> {
    let mut detach = false;
    let mut vars = Vec::new();
    let mut exprs = Vec::new();
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::kw_detach => detach = true,
            Rule::delete_items => {
                for item in inner.into_inner() {
                    let expr = build_expression(item)?;
                    // openCypher: DELETE targets must be graph-element
                    // expressions (Node / Edge / Path). Label predicates
                    // (`n:Label`), comparisons, and other boolean-valued
                    // expressions belong in WHERE or REMOVE, not DELETE.
                    if matches!(
                        expr,
                        Expr::HasLabels { .. }
                            | Expr::Compare { .. }
                            | Expr::And(_, _)
                            | Expr::Or(_, _)
                            | Expr::Xor(_, _)
                            | Expr::Not(_)
                            | Expr::IsNull { .. }
                            | Expr::InList { .. }
                    ) {
                        return Err(Error::Parse(
                            "DELETE targets must evaluate to a node, edge, or path (no label / boolean predicates)"
                                .into(),
                        ));
                    }
                    // Extract variable name for backwards compatibility
                    if let Expr::Identifier(ref name) = expr {
                        vars.push(name.clone());
                    }
                    exprs.push(expr);
                }
            }
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in delete tail: {:?}",
                    r
                )))
            }
        }
    }
    Ok(DeleteClause {
        detach,
        vars,
        exprs,
    })
}

/// Parse a `remove_tail` pair into [`RemoveItem`] entries.
fn build_remove_items(pair: Pair<Rule>) -> Result<Vec<crate::ast::RemoveItem>> {
    let items_pair = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("empty remove".into()))?;
    let mut items = Vec::new();
    for item_pair in items_pair.into_inner() {
        debug_assert_eq!(item_pair.as_rule(), Rule::remove_item);
        let inner = item_pair
            .into_inner()
            .next()
            .ok_or_else(|| Error::Parse("empty remove item".into()))?;
        match inner.as_rule() {
            Rule::remove_prop_item => {
                let mut parts = inner.into_inner();
                let pa = parts.next().unwrap();
                let mut pa_inner = pa.into_inner();
                let var = pa_inner.next().unwrap().as_str().to_string();
                let key = pa_inner.next().unwrap().as_str().to_string();
                items.push(crate::ast::RemoveItem::Property { var, key });
            }
            Rule::remove_label_item => {
                let mut var = String::new();
                let mut labels = Vec::new();
                for child in inner.into_inner() {
                    match child.as_rule() {
                        Rule::identifier => var = parse_ident(child.as_str()),
                        Rule::label_spec => {
                            let label = child.into_inner().next().unwrap().as_str().to_string();
                            labels.push(label);
                        }
                        _ => {}
                    }
                }
                items.push(crate::ast::RemoveItem::Labels { var, labels });
            }
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in remove item: {:?}",
                    r
                )));
            }
        }
    }
    Ok(items)
}

/// Parse a `foreach_tail` pair into a [`ForeachClause`].
fn build_foreach_clause(pair: Pair<Rule>) -> Result<crate::ast::ForeachClause> {
    let mut var = String::new();
    let mut list_expr = None;
    let mut set_items = Vec::new();
    let mut remove_items = Vec::new();
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier if var.is_empty() => {
                var = parse_ident(inner.as_str());
            }
            Rule::expression if list_expr.is_none() => {
                list_expr = Some(build_expression(inner)?);
            }
            Rule::kw_in | Rule::kw_foreach => {}
            Rule::foreach_body => {
                let body_inner = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty foreach body".into()))?;
                match body_inner.as_rule() {
                    Rule::set_tail => {
                        set_items.extend(build_set_items(body_inner)?);
                    }
                    Rule::remove_tail => {
                        remove_items.extend(build_remove_items(body_inner)?);
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    Ok(crate::ast::ForeachClause {
        var,
        list_expr: list_expr
            .ok_or_else(|| Error::Parse("FOREACH missing list expression".into()))?,
        set_items,
        remove_items,
    })
}

/// Parse one `OPTIONAL MATCH <patterns> [WHERE ...]` clause into
/// its AST form. The `kw_optional` and `kw_match` atomic keywords
/// show up as visible pairs — just skip them.
fn build_optional_match(pair: Pair<Rule>) -> Result<crate::ast::OptionalMatchClause> {
    let mut patterns: Vec<Pattern> = Vec::new();
    let mut where_clause: Option<Expr> = None;
    for inner in pair.into_inner() {
        match inner.as_rule() {
            // kw_optional is atomic so it shows up as a visible
            // pair; kw_match is silent and won't appear here.
            Rule::kw_optional => {}
            Rule::pattern_list => patterns = build_pattern_list(inner)?,
            Rule::where_clause => {
                let expr_pair = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty where in OPTIONAL MATCH".into()))?;
                where_clause = Some(build_expression(expr_pair)?);
            }
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in OPTIONAL MATCH: {:?}",
                    r
                )));
            }
        }
    }
    Ok(crate::ast::OptionalMatchClause {
        patterns,
        where_clause,
    })
}

/// Parse a `with_tail` pair (`WITH <items> [WHERE] [ORDER BY]
/// [SKIP] [LIMIT]`) into a [`WithClause`]. Walks the inner rules
/// in grammar order and fills each optional slot as it goes.
fn build_with_tail(pair: Pair<Rule>) -> Result<crate::ast::WithClause> {
    let mut items = Vec::new();
    let mut star = false;
    let mut distinct = false;
    let mut where_clause: Option<Expr> = None;
    let mut order_by: Vec<SortItem> = Vec::new();
    let mut skip: Option<Expr> = None;
    let mut limit: Option<Expr> = None;
    for inner in pair.into_inner() {
        match inner.as_rule() {
            // `kw_with` is atomic so it shows up as a visible
            // pair; other atomic keywords do the same. Just skip.
            Rule::kw_with => {}
            Rule::kw_distinct => distinct = true,
            Rule::return_items => {
                let (parsed_items, is_star) = build_return_items(inner)?;
                items = parsed_items;
                star = is_star;
            }
            Rule::where_clause => {
                let expr_pair = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty where in WITH".into()))?;
                where_clause = Some(build_expression(expr_pair)?);
            }
            Rule::order_by_clause => order_by = build_order_by(inner)?,
            Rule::skip_clause => {
                let expr_pair = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty skip in WITH".into()))?;
                skip = Some(build_expression(expr_pair)?);
            }
            Rule::limit_clause => {
                let expr_pair = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty limit in WITH".into()))?;
                limit = Some(build_expression(expr_pair)?);
            }
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in WITH tail: {:?}",
                    r
                )));
            }
        }
    }
    Ok(crate::ast::WithClause {
        items,
        star,
        distinct,
        where_clause,
        order_by,
        skip,
        limit,
    })
}

fn build_order_by(pair: Pair<Rule>) -> Result<Vec<SortItem>> {
    let mut sort_items = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() != Rule::sort_items {
            continue;
        }
        for sort_item_pair in inner.into_inner() {
            debug_assert_eq!(sort_item_pair.as_rule(), Rule::sort_item);
            let mut ii = sort_item_pair.into_inner();
            let expr_pair = ii
                .next()
                .ok_or_else(|| Error::Parse("empty sort item".into()))?;
            let expr = build_expression(expr_pair)?;
            let descending = match ii.next() {
                Some(dir_pair) if dir_pair.as_rule() == Rule::sort_dir => {
                    let inside = dir_pair
                        .into_inner()
                        .next()
                        .ok_or_else(|| Error::Parse("empty sort_dir".into()))?;
                    matches!(inside.as_rule(), Rule::kw_desc)
                }
                _ => false,
            };
            sort_items.push(SortItem { expr, descending });
        }
    }
    Ok(sort_items)
}

fn build_pattern(pair: Pair<Rule>) -> Result<Pattern> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| Error::Parse("empty pattern".into()))?;

    // Optional `identifier =` prefix that binds the whole
    // traversal as a Path. When present, the next pair in the
    // iterator is the pattern body (either a `shortest_path_form`
    // wrapper or a bare `raw_pattern`).
    let (path_var, body) = if first.as_rule() == Rule::path_var_binding {
        let name = first
            .into_inner()
            .next()
            .ok_or_else(|| Error::Parse("empty path_var_binding".into()))?
            .as_str()
            .to_string();
        let body = inner
            .next()
            .ok_or_else(|| Error::Parse("path_var_binding not followed by pattern body".into()))?;
        (Some(name), body)
    } else {
        (None, first)
    };

    match body.as_rule() {
        Rule::raw_pattern => {
            let (start, hops) = build_raw_pattern(body)?;
            Ok(Pattern {
                start,
                hops,
                path_var,
                shortest: None,
            })
        }
        Rule::shortest_path_form => {
            // `shortestPath(...)` or `allShortestPaths(...)`.
            // Inner children: the `kw_*` keyword followed by a
            // `node_pattern` and at least one `hop`. The grammar
            // already enforces `hop+` so a zero-hop wrapper is
            // impossible by construction.
            let mut kind = None;
            let mut start: Option<NodePattern> = None;
            let mut hops: Vec<Hop> = Vec::new();
            for p in body.into_inner() {
                match p.as_rule() {
                    Rule::kw_shortest_path => kind = Some(ShortestKind::Shortest),
                    Rule::kw_all_shortest_paths => kind = Some(ShortestKind::AllShortest),
                    Rule::node_pattern => start = Some(build_node_pattern(p)?),
                    Rule::hop => hops.push(build_hop(p)?),
                    r => {
                        return Err(Error::Parse(format!(
                            "unexpected rule in shortest_path_form: {r:?}"
                        )))
                    }
                }
            }
            let start = start
                .ok_or_else(|| Error::Parse("shortest_path_form missing start node".into()))?;
            let kind =
                kind.ok_or_else(|| Error::Parse("shortest_path_form missing keyword".into()))?;
            Ok(Pattern {
                start,
                hops,
                path_var,
                shortest: Some(kind),
            })
        }
        r => Err(Error::Parse(format!("unexpected pattern body rule: {r:?}"))),
    }
}

fn build_raw_pattern(pair: Pair<Rule>) -> Result<(NodePattern, Vec<Hop>)> {
    let mut inner = pair.into_inner();
    let start_pair = inner
        .next()
        .ok_or_else(|| Error::Parse("raw_pattern missing node_pattern".into()))?;
    let start = build_node_pattern(start_pair)?;
    let mut hops = Vec::new();
    for hop_pair in inner {
        debug_assert_eq!(hop_pair.as_rule(), Rule::hop);
        hops.push(build_hop(hop_pair)?);
    }
    Ok((start, hops))
}

fn build_hop(pair: Pair<Rule>) -> Result<Hop> {
    let mut inner = pair.into_inner();
    let rel_pair = inner
        .next()
        .ok_or_else(|| Error::Parse("missing rel in hop".into()))?;
    let target_pair = inner
        .next()
        .ok_or_else(|| Error::Parse("missing target in hop".into()))?;
    Ok(Hop {
        rel: build_rel_pattern(rel_pair)?,
        target: build_node_pattern(target_pair)?,
    })
}

fn build_rel_pattern(pair: Pair<Rule>) -> Result<RelPattern> {
    debug_assert_eq!(pair.as_rule(), Rule::rel_pattern);
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("empty rel pattern".into()))?;
    let direction = match inner.as_rule() {
        Rule::rel_right => Direction::Outgoing,
        Rule::rel_left => Direction::Incoming,
        Rule::rel_both | Rule::rel_both_arrows => Direction::Both,
        r => return Err(Error::Parse(format!("unexpected rel rule: {:?}", r))),
    };

    let mut var = None;
    let mut edge_types = Vec::new();
    let mut var_length = None;
    let mut properties = Vec::new();
    let mut trailing_quantifier: Option<VarLength> = None;
    for p in inner.into_inner() {
        match p.as_rule() {
            Rule::rel_detail => {
                for d in p.into_inner() {
                    match d.as_rule() {
                        Rule::identifier => var = Some(parse_ident(d.as_str())),
                        Rule::rel_type_spec => {
                            for id in d.into_inner() {
                                if id.as_rule() == Rule::identifier {
                                    edge_types.push(parse_ident(id.as_str()));
                                }
                            }
                        }
                        Rule::var_length => {
                            var_length = Some(build_var_length(d)?);
                        }
                        Rule::properties => {
                            properties = build_properties(d)?;
                        }
                        r => {
                            return Err(Error::Parse(format!(
                                "unexpected rel detail rule: {:?}",
                                r
                            )))
                        }
                    }
                }
            }
            Rule::rel_quantifier => {
                trailing_quantifier = Some(build_rel_quantifier(p)?);
            }
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rel pattern rule: {:?}",
                    r
                )))
            }
        }
    }

    if let Some(q) = trailing_quantifier {
        // Both forms present is a semantic conflict — two different
        // hop ranges on the same relationship. Reject at parse time
        // rather than silently discarding one.
        if var_length.is_some() {
            return Err(Error::Parse(
                "cannot combine inline `*min..max` with trailing `+` / `*` / `{...}` quantifier on the same relationship"
                    .into(),
            ));
        }
        var_length = Some(q);
    }

    Ok(RelPattern {
        var,
        edge_types,
        properties,
        direction,
        var_length,
    })
}

/// Lower a `rel_quantifier` pair (`+`, `*`, or a brace form) to the
/// `VarLength` range the executor consumes. Note the `{...}` forms
/// use GQL / regex semantics for absent bounds (`{,m}` == `{0,m}`);
/// this differs from the inline `*..m` form which defaults min=1,
/// matching what Neo4j 5 specifies for each surface.
fn build_rel_quantifier(pair: Pair<Rule>) -> Result<VarLength> {
    debug_assert_eq!(pair.as_rule(), Rule::rel_quantifier);
    // `+` and `*` match as literal string tokens; a brace form is
    // an inner rule.
    let text = pair.as_str();
    if text == "+" {
        return Ok(VarLength {
            min: 1,
            max: u64::MAX,
        });
    }
    if text == "*" {
        return Ok(VarLength {
            min: 0,
            max: u64::MAX,
        });
    }
    let brace = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::brace_quantifier)
        .ok_or_else(|| Error::Parse("malformed rel quantifier".into()))?;
    build_brace_quantifier(brace)
}

fn build_brace_quantifier(pair: Pair<Rule>) -> Result<VarLength> {
    debug_assert_eq!(pair.as_rule(), Rule::brace_quantifier);
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("empty brace quantifier".into()))?;
    match inner.as_rule() {
        Rule::brace_exact => {
            let n_pair = inner
                .into_inner()
                .next()
                .ok_or_else(|| Error::Parse("empty brace exact".into()))?;
            let n = parse_u64(n_pair.as_str())?;
            Ok(VarLength { min: n, max: n })
        }
        Rule::brace_range => {
            // Defaults: absent lower bound is 0 (regex-style, GQL),
            // absent upper bound is unbounded. This diverges from
            // the inline `*..m` form (which defaults min=1) — the
            // brace surface matches the Neo4j 5 spec.
            let mut min: u64 = 0;
            let mut max: u64 = u64::MAX;
            let mut saw_min = false;
            let mut saw_max = false;
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::brace_min => {
                        let n_pair = p
                            .into_inner()
                            .next()
                            .ok_or_else(|| Error::Parse("empty brace min".into()))?;
                        min = parse_u64(n_pair.as_str())?;
                        saw_min = true;
                    }
                    Rule::brace_max => {
                        let n_pair = p
                            .into_inner()
                            .next()
                            .ok_or_else(|| Error::Parse("empty brace max".into()))?;
                        max = parse_u64(n_pair.as_str())?;
                        saw_max = true;
                    }
                    r => {
                        return Err(Error::Parse(format!(
                            "unexpected rule in brace range: {:?}",
                            r
                        )))
                    }
                }
            }
            if !saw_min && !saw_max {
                // `{,}` is ambiguous — `+` / `*` already cover open
                // ranges with clearer intent, so reject this form
                // rather than silently aliasing it to `*`.
                return Err(Error::Parse(
                    "quantifier `{,}` has no bounds; use `*` for zero-or-more or `+` for one-or-more"
                        .into(),
                ));
            }
            Ok(VarLength { min, max })
        }
        r => Err(Error::Parse(format!(
            "unexpected rule in brace quantifier: {:?}",
            r
        ))),
    }
}

fn build_var_length(pair: Pair<Rule>) -> Result<VarLength> {
    debug_assert_eq!(pair.as_rule(), Rule::var_length);
    let mut range_pair_opt = None;
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::var_length_range {
            range_pair_opt = Some(inner);
        }
    }
    let Some(range_pair) = range_pair_opt else {
        return Ok(VarLength {
            min: 1,
            max: u64::MAX,
        });
    };
    let inner = range_pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("empty var length range".into()))?;
    match inner.as_rule() {
        Rule::exact_range => {
            let n_pair = inner
                .into_inner()
                .next()
                .ok_or_else(|| Error::Parse("empty exact range".into()))?;
            let n = parse_u64(n_pair.as_str())?;
            Ok(VarLength { min: n, max: n })
        }
        Rule::bounded_range => {
            let mut min = 1u64;
            let mut max = u64::MAX;
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::range_min => {
                        let n = p
                            .into_inner()
                            .next()
                            .ok_or_else(|| Error::Parse("empty range min".into()))?;
                        min = parse_u64(n.as_str())?;
                    }
                    Rule::range_max => {
                        let n = p
                            .into_inner()
                            .next()
                            .ok_or_else(|| Error::Parse("empty range max".into()))?;
                        max = parse_u64(n.as_str())?;
                    }
                    r => {
                        return Err(Error::Parse(format!(
                            "unexpected rule in bounded range: {:?}",
                            r
                        )))
                    }
                }
            }
            Ok(VarLength { min, max })
        }
        r => Err(Error::Parse(format!(
            "unexpected var length range rule: {:?}",
            r
        ))),
    }
}

fn parse_u64(s: &str) -> Result<u64> {
    s.parse().map_err(|_| Error::InvalidNumber(s.to_string()))
}

fn build_set_item(pair: Pair<Rule>) -> Result<SetItem> {
    match pair.as_rule() {
        Rule::set_label_item => {
            let mut inner = pair.into_inner();
            let var = inner
                .next()
                .ok_or_else(|| Error::Parse("set label missing var".into()))?
                .as_str()
                .to_string();
            let mut labels = Vec::new();
            for p in inner {
                debug_assert_eq!(p.as_rule(), Rule::label_spec);
                let id = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty label".into()))?;
                labels.push(parse_ident(id.as_str()));
            }
            Ok(SetItem::Labels { var, labels })
        }
        Rule::set_prop_item => {
            let mut inner = pair.into_inner();
            let target = inner
                .next()
                .ok_or_else(|| Error::Parse("set prop missing target".into()))?;
            let (var, key) = match target.as_rule() {
                Rule::property_access => {
                    let mut pa = target.into_inner();
                    let var = pa
                        .next()
                        .ok_or_else(|| Error::Parse("set target missing var".into()))?
                        .as_str()
                        .to_string();
                    let key = pa
                        .next()
                        .ok_or_else(|| Error::Parse("set target missing key".into()))?
                        .as_str()
                        .to_string();
                    (var, key)
                }
                Rule::set_paren_lhs => {
                    // `(expr).key` — pull out the inner expression's
                    // identifier + the trailing key. v1 only handles
                    // the degenerate `(var).key` case; anything more
                    // complex would need SetProperty to accept an
                    // expression for the target, which it doesn't.
                    let mut parts = target.into_inner();
                    let expr_pair = parts
                        .next()
                        .ok_or_else(|| Error::Parse("paren LHS missing inner".into()))?;
                    let key_pair = parts
                        .next()
                        .ok_or_else(|| Error::Parse("paren LHS missing key".into()))?;
                    let expr = build_expression(expr_pair)?;
                    let var = match expr {
                        Expr::Identifier(name) => name,
                        _ => {
                            return Err(Error::Parse(
                                "SET target `(expr).key` currently only supports a \
                                 bare identifier inside the parentheses"
                                    .into(),
                            ))
                        }
                    };
                    (var, key_pair.as_str().to_string())
                }
                r => return Err(Error::Parse(format!("unexpected set target: {:?}", r))),
            };
            let expr_pair = inner
                .next()
                .ok_or_else(|| Error::Parse("set prop missing value".into()))?;
            let value = build_expression(expr_pair)?;
            Ok(SetItem::Property { var, key, value })
        }
        Rule::set_merge_item => {
            let mut inner = pair.into_inner();
            let var = inner
                .next()
                .ok_or_else(|| Error::Parse("set merge missing var".into()))?
                .as_str()
                .to_string();
            let rhs = inner
                .next()
                .ok_or_else(|| Error::Parse("set merge missing rhs".into()))?;
            match rhs.as_rule() {
                Rule::properties => Ok(SetItem::Merge {
                    var,
                    properties: build_properties(rhs)?,
                }),
                Rule::identifier => Ok(SetItem::ReplaceFromExpr {
                    var,
                    source: Expr::Identifier(parse_ident(rhs.as_str())),
                    replace: false,
                }),
                Rule::parameter => Ok(SetItem::ReplaceFromExpr {
                    var,
                    source: Expr::Parameter(parameter_name(rhs)),
                    replace: false,
                }),
                r => Err(Error::Parse(format!("unexpected set merge rhs: {:?}", r))),
            }
        }
        Rule::set_replace_item => {
            let mut inner = pair.into_inner();
            let var = inner
                .next()
                .ok_or_else(|| Error::Parse("set replace missing var".into()))?
                .as_str()
                .to_string();
            let rhs = inner
                .next()
                .ok_or_else(|| Error::Parse("set replace missing rhs".into()))?;
            match rhs.as_rule() {
                Rule::properties => Ok(SetItem::Replace {
                    var,
                    properties: build_properties(rhs)?,
                }),
                Rule::identifier => Ok(SetItem::ReplaceFromExpr {
                    var,
                    source: Expr::Identifier(parse_ident(rhs.as_str())),
                    replace: true,
                }),
                Rule::parameter => Ok(SetItem::ReplaceFromExpr {
                    var,
                    source: Expr::Parameter(parameter_name(rhs)),
                    replace: true,
                }),
                r => Err(Error::Parse(format!("unexpected set replace rhs: {:?}", r))),
            }
        }
        r => Err(Error::Parse(format!("unexpected set item rule: {:?}", r))),
    }
}

fn build_node_pattern(pair: Pair<Rule>) -> Result<NodePattern> {
    let mut var = None;
    let mut labels = Vec::new();
    let mut properties = Vec::new();
    let mut has_property_clause = false;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier => var = Some(parse_ident(p.as_str())),
            Rule::label_spec => {
                let label_id = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty label".into()))?;
                labels.push(parse_ident(label_id.as_str()));
            }
            Rule::properties => {
                has_property_clause = true;
                properties = build_properties(p)?;
            }
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in node pattern: {:?}",
                    r
                )))
            }
        }
    }

    Ok(NodePattern {
        var,
        labels,
        properties,
        has_property_clause,
    })
}

fn build_properties(pair: Pair<Rule>) -> Result<Vec<(String, Expr)>> {
    let mut entries = Vec::new();
    for entry_pair in pair.into_inner() {
        debug_assert_eq!(entry_pair.as_rule(), Rule::property_entry);
        let mut inner = entry_pair.into_inner();
        let key = inner
            .next()
            .ok_or_else(|| Error::Parse("property key".into()))?
            .as_str()
            .to_string();
        let value_pair = inner
            .next()
            .ok_or_else(|| Error::Parse("property value".into()))?;
        entries.push((key, build_property_value(value_pair)?));
    }
    Ok(entries)
}

/// Lower a `property_value` parse pair (literal or parameter) into an
/// `Expr`. Mirrors the grammar's `property_value = { literal | parameter }`
/// — node-pattern property values are deliberately a strict subset of
/// `expression`.
fn build_property_value(pair: Pair<Rule>) -> Result<Expr> {
    debug_assert_eq!(pair.as_rule(), Rule::property_value);
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("empty property value".into()))?;
    build_expression(inner)
}

/// Strip the leading `$` from a `parameter` parse pair and return the
/// bare name (or positional index as a string). Shared by
/// `build_property_value` and the `Rule::parameter` arm in
/// `build_expression`.
fn parameter_name(pair: Pair<Rule>) -> String {
    debug_assert_eq!(pair.as_rule(), Rule::parameter);
    let raw = pair.as_str();
    debug_assert!(raw.starts_with('$'));
    raw[1..].to_string()
}

/// Returns `(items, is_star)`. When `return_star` is matched the
/// items vec is empty and `is_star` is `true`.
fn build_return_items(pair: Pair<Rule>) -> Result<(Vec<ReturnItem>, bool)> {
    let mut items = Vec::new();
    for item_pair in pair.into_inner() {
        if item_pair.as_rule() == Rule::return_star {
            return Ok((Vec::new(), true));
        }
        debug_assert_eq!(item_pair.as_rule(), Rule::return_item);
        let mut inner = item_pair
            .into_inner()
            .filter(|p| p.as_rule() != Rule::kw_as);
        let expr_pair = inner
            .next()
            .ok_or_else(|| Error::Parse("return expr".into()))?;
        // Capture source text verbatim so the output column is
        // named exactly as written (`count( * )` ≠ `count(*)` per
        // the TCK). Trim outer whitespace but preserve inner.
        let raw_text = Some(expr_pair.as_str().trim().to_string());
        let expr = build_expression(expr_pair)?;
        let alias = inner.next().map(|p| parse_ident(p.as_str()));
        items.push(ReturnItem {
            expr,
            alias,
            raw_text,
        });
    }
    Ok((items, false))
}

/// Strip operator-precedence wrapper rules whose only role at this
/// position is to nest into the next-higher-binding level. A wrapper
/// is "pass-through" when it has exactly one inner pair and that pair
/// isn't an operator marker — i.e. no `kw_or` / `op_add` / etc. shows
/// up at this level. See `build_expression`'s opening comment for why
/// this matters.
fn peel_passthrough(mut pair: Pair<Rule>) -> Pair<Rule> {
    loop {
        let peelable = matches!(
            pair.as_rule(),
            Rule::expression
                | Rule::base_primary
                | Rule::or_expr
                | Rule::xor_expr
                | Rule::and_expr
                | Rule::not_expr
                | Rule::comparison
                | Rule::compound
                | Rule::add_expr
                | Rule::mul_expr
                | Rule::pow_expr
                | Rule::unary_expr
                | Rule::primary
        );
        if !peelable {
            return pair;
        }
        let mut inner = pair.clone().into_inner();
        let Some(first) = inner.next() else {
            return pair;
        };
        if inner.next().is_some() {
            return pair;
        }
        if matches!(first.as_rule(), Rule::kw_not | Rule::op_neg) {
            return pair;
        }
        pair = first;
    }
}

fn build_expression(pair: Pair<Rule>) -> Result<Expr> {
    // The grammar's precedence chain (`expression → or_expr → xor_expr →
    // ... → unary_expr → primary → base_primary`) inserts ~13 wrapper
    // layers between an `expression` rule and its underlying leaf. Each
    // layer used to recurse through `build_expression`, which in debug
    // builds blew past the default 2 MB test thread stack on inputs as
    // small as `MATCH (n) RETURN size(n)`. Peel the wrappers iteratively
    // for the (very common) case where they carry no operator at this
    // level so the main `match` lands on the meaningful rule directly.
    let pair = peel_passthrough(pair);
    match pair.as_rule() {
        Rule::expression => build_expression(
            pair.into_inner()
                .next()
                .ok_or_else(|| Error::Parse("empty expression".into()))?,
        ),
        Rule::or_expr => {
            let mut inner = pair.into_inner().filter(|p| p.as_rule() != Rule::kw_or);
            let mut left = build_expression(
                inner
                    .next()
                    .ok_or_else(|| Error::Parse("empty or_expr".into()))?,
            )?;
            for right in inner {
                let right_expr = build_expression(right)?;
                left = Expr::Or(Box::new(left), Box::new(right_expr));
            }
            Ok(left)
        }
        Rule::xor_expr => {
            let mut inner = pair.into_inner().filter(|p| p.as_rule() != Rule::kw_xor);
            let mut left = build_expression(
                inner
                    .next()
                    .ok_or_else(|| Error::Parse("empty xor_expr".into()))?,
            )?;
            for right in inner {
                let right_expr = build_expression(right)?;
                left = Expr::Xor(Box::new(left), Box::new(right_expr));
            }
            Ok(left)
        }
        Rule::and_expr => {
            let mut inner = pair.into_inner().filter(|p| p.as_rule() != Rule::kw_and);
            let mut left = build_expression(
                inner
                    .next()
                    .ok_or_else(|| Error::Parse("empty and_expr".into()))?,
            )?;
            for right in inner {
                let right_expr = build_expression(right)?;
                left = Expr::And(Box::new(left), Box::new(right_expr));
            }
            Ok(left)
        }
        Rule::not_expr => {
            let mut inner = pair.into_inner();
            let first = inner
                .next()
                .ok_or_else(|| Error::Parse("empty not_expr".into()))?;
            if first.as_rule() == Rule::kw_not {
                let inner_not = inner
                    .next()
                    .ok_or_else(|| Error::Parse("NOT without operand".into()))?;
                Ok(Expr::Not(Box::new(build_expression(inner_not)?)))
            } else {
                build_expression(first)
            }
        }
        Rule::comparison => {
            // `comparison := compound (comparison_op compound)*`.
            // A single pair compiles to `Compare`. A chain like
            // `1 < x < 3` expands to `(1 < x) AND (x < 3)` — the
            // middle operand is reused for both comparisons, matching
            // Cypher's Python-style interpretation of chained
            // comparisons.
            let mut inner = pair.into_inner();
            let first_pair = inner
                .next()
                .ok_or_else(|| Error::Parse("empty comparison".into()))?;
            let first = build_expression(first_pair)?;
            let mut operands = vec![first];
            let mut ops = Vec::new();
            while let Some(op_pair) = inner.next() {
                let op = build_compare_op(op_pair)?;
                let rhs_pair = inner
                    .next()
                    .ok_or_else(|| Error::Parse("missing comparison rhs".into()))?;
                operands.push(build_expression(rhs_pair)?);
                ops.push(op);
            }
            if ops.is_empty() {
                return Ok(operands.into_iter().next().unwrap());
            }
            // Build comparisons for each adjacent pair and AND them.
            let mut terms: Vec<Expr> = Vec::with_capacity(ops.len());
            for (i, op) in ops.iter().enumerate() {
                terms.push(Expr::Compare {
                    op: op.clone(),
                    left: Box::new(operands[i].clone()),
                    right: Box::new(operands[i + 1].clone()),
                });
            }
            let mut folded = terms.remove(0);
            for t in terms {
                folded = Expr::And(Box::new(folded), Box::new(t));
            }
            Ok(folded)
        }
        Rule::compound => {
            // `compound := add_expr ~ (null_predicate | in_predicate | label_predicate)?`.
            let mut inner = pair.into_inner();
            let left = build_expression(
                inner
                    .next()
                    .ok_or_else(|| Error::Parse("empty compound".into()))?,
            )?;
            match inner.next() {
                None => Ok(left),
                Some(tail) if tail.as_rule() == Rule::label_predicate => {
                    let mut labels = Vec::new();
                    for child in tail.into_inner() {
                        if child.as_rule() == Rule::label_spec {
                            if let Some(id) = child.into_inner().next() {
                                labels.push(parse_ident(id.as_str()));
                            }
                        }
                    }
                    Ok(Expr::HasLabels {
                        expr: Box::new(left),
                        labels,
                    })
                }
                Some(tail) if tail.as_rule() == Rule::in_predicate => {
                    let rhs_pair = tail
                        .into_inner()
                        .find(|p| p.as_rule() != Rule::kw_in)
                        .ok_or_else(|| Error::Parse("IN missing list expression".into()))?;
                    let list = build_expression(rhs_pair)?;
                    Ok(Expr::InList {
                        element: Box::new(left),
                        list: Box::new(list),
                    })
                }
                Some(tail) if tail.as_rule() == Rule::null_predicate => {
                    let kind = tail
                        .into_inner()
                        .next()
                        .ok_or_else(|| Error::Parse("empty null predicate".into()))?;
                    let negated = match kind.as_rule() {
                        Rule::is_null_op => false,
                        Rule::is_not_null_op => true,
                        r => {
                            return Err(Error::Parse(format!(
                                "unexpected null predicate rule: {:?}",
                                r
                            )))
                        }
                    };
                    Ok(Expr::IsNull {
                        negated,
                        inner: Box::new(left),
                    })
                }
                Some(tail) => Err(Error::Parse(format!(
                    "unexpected compound tail rule: {:?}",
                    tail.as_rule()
                ))),
            }
        }
        Rule::add_expr => {
            // Left-associative fold over `mul_expr (op mul_expr)*`.
            // Children alternate between operands and `op_add`/
            // `op_sub` markers; drain them in pairs, building up
            // a left-leaning `BinaryOp` tree.
            let mut inner = pair.into_inner();
            let mut left = build_expression(
                inner
                    .next()
                    .ok_or_else(|| Error::Parse("empty add_expr".into()))?,
            )?;
            while let Some(op_pair) = inner.next() {
                let op = match op_pair.as_rule() {
                    Rule::op_add => BinaryOp::Add,
                    Rule::op_sub => BinaryOp::Sub,
                    r => {
                        return Err(Error::Parse(format!(
                            "unexpected rule in add_expr: {:?}",
                            r
                        )))
                    }
                };
                let right = build_expression(
                    inner
                        .next()
                        .ok_or_else(|| Error::Parse("add_expr missing rhs".into()))?,
                )?;
                left = Expr::BinaryOp {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            }
            Ok(left)
        }
        Rule::mul_expr => {
            let mut inner = pair.into_inner();
            let mut left = build_expression(
                inner
                    .next()
                    .ok_or_else(|| Error::Parse("empty mul_expr".into()))?,
            )?;
            while let Some(op_pair) = inner.next() {
                let op = match op_pair.as_rule() {
                    Rule::op_mul => BinaryOp::Mul,
                    Rule::op_div => BinaryOp::Div,
                    Rule::op_mod => BinaryOp::Mod,
                    r => {
                        return Err(Error::Parse(format!(
                            "unexpected rule in mul_expr: {:?}",
                            r
                        )))
                    }
                };
                let right = build_expression(
                    inner
                        .next()
                        .ok_or_else(|| Error::Parse("mul_expr missing rhs".into()))?,
                )?;
                left = Expr::BinaryOp {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            }
            Ok(left)
        }
        Rule::pow_expr => {
            let mut inner = pair.into_inner();
            let mut left = build_expression(
                inner
                    .next()
                    .ok_or_else(|| Error::Parse("empty pow_expr".into()))?,
            )?;
            while let Some(op_pair) = inner.next() {
                if op_pair.as_rule() != Rule::op_pow {
                    return Err(Error::Parse(format!(
                        "unexpected rule in pow_expr: {:?}",
                        op_pair.as_rule()
                    )));
                }
                let right = build_expression(
                    inner
                        .next()
                        .ok_or_else(|| Error::Parse("pow_expr missing rhs".into()))?,
                )?;
                left = Expr::BinaryOp {
                    op: BinaryOp::Pow,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            }
            Ok(left)
        }
        Rule::unary_expr => {
            let mut inner = pair.into_inner();
            let first = inner
                .next()
                .ok_or_else(|| Error::Parse("empty unary_expr".into()))?;
            if first.as_rule() == Rule::op_neg {
                let operand_pair = inner
                    .next()
                    .ok_or_else(|| Error::Parse("unary - missing operand".into()))?;
                // `-<integer>` takes the integer in negated form so
                // `i64::MIN` (`-9223372036854775808`) parses — parsing
                // the positive half alone overflows i64.
                if let Some(lit) = peel_to_integer_literal(&operand_pair) {
                    let combined = format!("-{}", lit);
                    if let Ok(n) = parse_integer(&combined) {
                        return Ok(Expr::Literal(Literal::Integer(n)));
                    }
                }
                // `-<float>` folds at parse time so callers see
                // `Float(-3.14)` instead of `UnaryOp(Neg, Float(3.14))`
                // — pattern-property values in particular compare
                // structurally and the unary wrapper would mask the
                // literal shape.
                if let Some(lit) = peel_to_float_literal(&operand_pair) {
                    if let Ok(f) = format!("-{}", lit).parse::<f64>() {
                        return Ok(Expr::Literal(Literal::Float(f)));
                    }
                }
                let operand = build_expression(operand_pair)?;
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Neg,
                    operand: Box::new(operand),
                })
            } else {
                build_expression(first)
            }
        }
        Rule::primary => {
            let mut inner = pair.into_inner();
            let base_pair = inner
                .next()
                .ok_or_else(|| Error::Parse("empty primary".into()))?;
            let mut expr = build_expression(base_pair)?;
            for chain in inner {
                if chain.as_rule() != Rule::postfix_chain {
                    continue;
                }
                let inner_rule = chain
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty postfix_chain".into()))?;
                match inner_rule.as_rule() {
                    Rule::property_chain => {
                        let key = parse_ident(
                            inner_rule
                                .into_inner()
                                .find(|p| p.as_rule() == Rule::identifier)
                                .ok_or_else(|| Error::Parse("property_chain missing key".into()))?
                                .as_str(),
                        );
                        expr = Expr::PropertyAccess {
                            base: Box::new(expr),
                            key,
                        };
                    }
                    Rule::index_access => {
                        let child = inner_rule
                            .into_inner()
                            .next()
                            .ok_or_else(|| Error::Parse("index_access missing content".into()))?;
                        match child.as_rule() {
                            Rule::slice_both => {
                                let mut exprs = child.into_inner();
                                let s = build_expression(exprs.next().unwrap())?;
                                let e = build_expression(exprs.next().unwrap())?;
                                expr = Expr::SliceAccess {
                                    base: Box::new(expr),
                                    start: Some(Box::new(s)),
                                    end: Some(Box::new(e)),
                                };
                            }
                            Rule::slice_from => {
                                let s = build_expression(child.into_inner().next().unwrap())?;
                                expr = Expr::SliceAccess {
                                    base: Box::new(expr),
                                    start: Some(Box::new(s)),
                                    end: None,
                                };
                            }
                            Rule::slice_to => {
                                let e = build_expression(child.into_inner().next().unwrap())?;
                                expr = Expr::SliceAccess {
                                    base: Box::new(expr),
                                    start: None,
                                    end: Some(Box::new(e)),
                                };
                            }
                            Rule::slice_all => {
                                expr = Expr::SliceAccess {
                                    base: Box::new(expr),
                                    start: None,
                                    end: None,
                                };
                            }
                            _ => {
                                let index = build_expression(child)?;
                                expr = Expr::IndexAccess {
                                    base: Box::new(expr),
                                    index: Box::new(index),
                                };
                            }
                        }
                    }
                    r => {
                        return Err(Error::Parse(format!(
                            "unexpected postfix_chain child: {:?}",
                            r
                        )));
                    }
                }
            }
            Ok(expr)
        }
        Rule::base_primary => build_expression(
            pair.into_inner()
                .next()
                .ok_or_else(|| Error::Parse("empty base_primary".into()))?,
        ),
        Rule::literal => Ok(Expr::Literal(build_literal(pair)?)),
        Rule::function_call => {
            let mut inner = pair.into_inner();
            // Collect identifiers to build the qualified function
            // name. Single-level (`distance`), one-level namespace
            // (`point.distance`), and multi-level APOC-style names
            // (`apoc.coll.sum`) all lower through the same loop —
            // the grammar allows zero or more `.<ident>` suffixes
            // after the first identifier.
            let first = inner
                .next()
                .ok_or_else(|| Error::Parse("function call missing name".into()))?;
            let mut name = parse_ident(first.as_str());
            let mut next = inner.next();
            while let Some(ref p) = next {
                if matches!(p.as_rule(), Rule::identifier | Rule::qualified_part) {
                    name = format!("{}.{}", name, parse_ident(p.as_str()));
                    next = inner.next();
                } else {
                    break;
                }
            }
            let args = match next {
                None => CallArgs::Exprs(Vec::new()),
                Some(fn_args_pair) => {
                    let inside = fn_args_pair
                        .into_inner()
                        .next()
                        .ok_or_else(|| Error::Parse("empty fn args".into()))?;
                    match inside.as_rule() {
                        Rule::count_star => CallArgs::Star,
                        Rule::fn_arg_list => {
                            let mut distinct = false;
                            let mut exprs: Vec<Expr> = Vec::new();
                            for p in inside.into_inner() {
                                if p.as_rule() == Rule::kw_distinct {
                                    distinct = true;
                                } else {
                                    exprs.push(build_expression(p)?);
                                }
                            }
                            if distinct {
                                CallArgs::DistinctExprs(exprs)
                            } else {
                                CallArgs::Exprs(exprs)
                            }
                        }
                        r => {
                            return Err(Error::Parse(format!("unexpected fn args inner: {:?}", r)))
                        }
                    }
                }
            };
            Ok(Expr::Call { name, args })
        }
        Rule::property_access => {
            let mut inner = pair.into_inner();
            let var = parse_ident(
                inner
                    .next()
                    .ok_or_else(|| Error::Parse("property var".into()))?
                    .as_str(),
            );
            let key = parse_ident(
                inner
                    .next()
                    .ok_or_else(|| Error::Parse("property key".into()))?
                    .as_str(),
            );
            Ok(Expr::Property { var, key })
        }
        Rule::identifier => Ok(Expr::Identifier(parse_ident(pair.as_str()))),
        Rule::parameter => Ok(Expr::Parameter(parameter_name(pair))),
        Rule::case_expr => build_case_expr(pair),
        Rule::list_literal => {
            let mut items = Vec::new();
            for p in pair.into_inner() {
                items.push(build_expression(p)?);
            }
            Ok(Expr::List(items))
        }
        Rule::map_literal => {
            let mut entries: Vec<(String, Expr)> = Vec::new();
            for entry in pair.into_inner() {
                // Each `map_entry` is `identifier ~ ":" ~ expression`.
                // The ":" is silent in the grammar so only the two
                // inner rule pairs come through.
                let mut inner = entry.into_inner();
                let key_pair = inner
                    .next()
                    .ok_or_else(|| Error::Parse("map entry missing key".into()))?;
                let value_pair = inner
                    .next()
                    .ok_or_else(|| Error::Parse("map entry missing value".into()))?;
                let key = parse_ident(key_pair.as_str());
                let value = build_expression(value_pair)?;
                entries.push((key, value));
            }
            Ok(Expr::Map(entries))
        }
        Rule::list_comp => build_list_comp(pair),
        Rule::pattern_comprehension => {
            let mut start: Option<crate::ast::NodePattern> = None;
            let mut hops: Vec<crate::ast::Hop> = Vec::new();
            let mut predicate: Option<Expr> = None;
            let mut projection: Option<Expr> = None;
            let mut path_var: Option<String> = None;
            for p in pair.into_inner() {
                match p.as_rule() {
                    Rule::identifier => {
                        // The only bare identifier inside a
                        // pattern comprehension is the optional
                        // path variable (`[p = (n)-->() | p]`).
                        path_var = Some(parse_ident(p.as_str()));
                    }
                    Rule::node_pattern => {
                        start = Some(build_node_pattern(p)?);
                    }
                    Rule::hop => {
                        hops.push(build_hop(p)?);
                    }
                    Rule::list_comp_where => {
                        let inner = p
                            .into_inner()
                            .find(|e| e.as_rule() == Rule::expression)
                            .ok_or_else(|| {
                                Error::Parse("pattern comprehension WHERE missing expr".into())
                            })?;
                        predicate = Some(build_expression(inner)?);
                    }
                    Rule::expression => {
                        projection = Some(build_expression(p)?);
                    }
                    Rule::kw_where => {}
                    _ => {}
                }
            }
            let start = start
                .ok_or_else(|| Error::Parse("pattern comprehension missing start node".into()))?;
            let projection = projection
                .ok_or_else(|| Error::Parse("pattern comprehension missing projection".into()))?;
            Ok(Expr::PatternComprehension {
                pattern: Pattern {
                    start,
                    hops,
                    path_var,
                    shortest: None,
                },
                predicate: predicate.map(Box::new),
                projection: Box::new(projection),
            })
        }
        Rule::reduce_expr => build_reduce_expr(pair),
        Rule::list_predicate => {
            let mut inner = pair.into_inner();
            let name_pair = inner
                .next()
                .ok_or_else(|| Error::Parse("list_predicate missing name".into()))?;
            let kind = match name_pair.as_str().to_ascii_lowercase().as_str() {
                "any" => crate::ast::ListPredicateKind::Any,
                "all" => crate::ast::ListPredicateKind::All,
                "none" => crate::ast::ListPredicateKind::None,
                "single" => crate::ast::ListPredicateKind::Single,
                other => return Err(Error::Parse(format!("unknown list predicate: {other}"))),
            };
            let mut var = String::new();
            let mut list = None;
            let mut predicate = None;
            for p in inner {
                match p.as_rule() {
                    Rule::identifier if var.is_empty() => var = parse_ident(p.as_str()),
                    Rule::expression if list.is_none() => list = Some(build_expression(p)?),
                    Rule::expression => predicate = Some(build_expression(p)?),
                    Rule::kw_in | Rule::kw_where => {}
                    _ => {}
                }
            }
            Ok(Expr::ListPredicate {
                kind,
                var,
                list: Box::new(
                    list.ok_or_else(|| Error::Parse("list_predicate missing list".into()))?,
                ),
                predicate: Box::new(
                    predicate
                        .ok_or_else(|| Error::Parse("list_predicate missing predicate".into()))?,
                ),
            })
        }
        Rule::count_subquery => {
            let inner = pair
                .into_inner()
                .find(|p| p.as_rule() == Rule::subquery_body)
                .ok_or_else(|| Error::Parse("count subquery missing body".into()))?;
            let stmt = build_subquery_body(inner)?;
            Ok(Expr::CountSubquery {
                body: Box::new(stmt),
            })
        }
        Rule::exists_subquery => {
            let inner = pair
                .into_inner()
                .find(|p| p.as_rule() == Rule::subquery_body)
                .ok_or_else(|| Error::Parse("exists subquery missing body".into()))?;
            let stmt = build_subquery_body(inner)?;
            Ok(Expr::ExistsSubquery {
                body: Box::new(stmt),
            })
        }
        Rule::collect_subquery => {
            let inner = pair
                .into_inner()
                .find(|p| p.as_rule() == Rule::subquery_body)
                .ok_or_else(|| Error::Parse("collect subquery missing body".into()))?;
            let stmt = build_subquery_body(inner)?;
            Ok(Expr::CollectSubquery {
                body: Box::new(stmt),
            })
        }
        Rule::pattern_predicate => {
            // `pattern_predicate` has a simpler inner structure
            // than the top-level `pattern` rule (no
            // `path_var_binding`, no shortestPath wrapper — just
            // `node_pattern ~ hop+`). Build it as a raw pattern
            // and wrap with the zero-state Pattern fields.
            let (start, hops) = build_raw_pattern(pair)?;
            if hops.is_empty() {
                return Err(Error::Parse(
                    "pattern predicates must have at least one relationship hop".into(),
                ));
            }
            Ok(Expr::PatternExists(Pattern {
                start,
                hops,
                path_var: None,
                shortest: None,
            }))
        }
        r => Err(Error::Parse(format!(
            "unexpected rule in expression: {:?}",
            r
        ))),
    }
}

fn build_case_expr(pair: Pair<Rule>) -> Result<Expr> {
    let mut scrutinee: Option<Box<Expr>> = None;
    let mut branches: Vec<(Expr, Expr)> = Vec::new();
    let mut else_expr: Option<Box<Expr>> = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::kw_case | Rule::kw_end => {}
            Rule::case_scrutinee => {
                let inner = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty case scrutinee".into()))?;
                scrutinee = Some(Box::new(build_expression(inner)?));
            }
            Rule::when_branch => {
                let mut ii = p
                    .into_inner()
                    .filter(|q| q.as_rule() != Rule::kw_when && q.as_rule() != Rule::kw_then);
                let cond = build_expression(
                    ii.next()
                        .ok_or_else(|| Error::Parse("empty when condition".into()))?,
                )?;
                let result = build_expression(
                    ii.next()
                        .ok_or_else(|| Error::Parse("empty when result".into()))?,
                )?;
                branches.push((cond, result));
            }
            Rule::else_branch => {
                let inner = p
                    .into_inner()
                    .filter(|q| q.as_rule() != Rule::kw_else)
                    .next()
                    .ok_or_else(|| Error::Parse("empty else branch".into()))?;
                else_expr = Some(Box::new(build_expression(inner)?));
            }
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in case expr: {:?}",
                    r
                )))
            }
        }
    }

    if branches.is_empty() {
        return Err(Error::Parse(
            "CASE requires at least one WHEN branch".into(),
        ));
    }
    Ok(Expr::Case {
        scrutinee,
        branches,
        else_expr,
    })
}

fn build_reduce_expr(pair: Pair<Rule>) -> Result<Expr> {
    // Grammar children after filtering out the silent `kw_reduce`
    // and `kw_in` atoms are (in order):
    //   identifier  acc_var
    //   expression  acc_init
    //   identifier  elem_var
    //   expression  source
    //   expression  body
    // Keep the 5 positional slots rather than name-matching so a
    // future grammar tweak that adds WHERE-style guards doesn't
    // silently break the builder.
    let mut ids: Vec<String> = Vec::with_capacity(2);
    let mut exprs: Vec<Expr> = Vec::with_capacity(3);
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::kw_reduce | Rule::kw_in => {}
            Rule::identifier => ids.push(parse_ident(p.as_str())),
            Rule::expression => exprs.push(build_expression(p)?),
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in reduce_expr: {:?}",
                    r
                )))
            }
        }
    }
    if ids.len() != 2 || exprs.len() != 3 {
        return Err(Error::Parse(format!(
            "reduce requires acc/elem identifiers and init/source/body exprs; \
             got {} identifiers and {} expressions",
            ids.len(),
            exprs.len()
        )));
    }
    let mut exprs_iter = exprs.into_iter();
    let acc_init = exprs_iter.next().unwrap();
    let source = exprs_iter.next().unwrap();
    let body = exprs_iter.next().unwrap();
    let mut ids_iter = ids.into_iter();
    let acc_var = ids_iter.next().unwrap();
    let elem_var = ids_iter.next().unwrap();
    Ok(Expr::Reduce {
        acc_var,
        acc_init: Box::new(acc_init),
        elem_var,
        source: Box::new(source),
        body: Box::new(body),
    })
}

fn build_list_comp(pair: Pair<Rule>) -> Result<Expr> {
    let mut var: Option<String> = None;
    let mut source: Option<Expr> = None;
    let mut predicate: Option<Box<Expr>> = None;
    let mut projection: Option<Box<Expr>> = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::kw_in => {}
            Rule::identifier if var.is_none() => {
                var = Some(parse_ident(p.as_str()));
            }
            Rule::expression if source.is_none() => {
                source = Some(build_expression(p)?);
            }
            Rule::list_comp_where => {
                let inner = p
                    .into_inner()
                    .filter(|q| q.as_rule() != Rule::kw_where)
                    .next()
                    .ok_or_else(|| Error::Parse("empty list comp where".into()))?;
                predicate = Some(Box::new(build_expression(inner)?));
            }
            Rule::list_comp_proj => {
                let inner = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty list comp projection".into()))?;
                projection = Some(Box::new(build_expression(inner)?));
            }
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in list comp: {:?}",
                    r
                )))
            }
        }
    }

    let var = var.ok_or_else(|| Error::Parse("list comprehension missing variable".into()))?;
    let source = source.ok_or_else(|| Error::Parse("list comprehension missing source".into()))?;
    Ok(Expr::ListComprehension {
        var,
        source: Box::new(source),
        predicate,
        projection,
    })
}

fn build_literal(pair: Pair<Rule>) -> Result<Literal> {
    let lit_pair = if pair.as_rule() == Rule::literal {
        pair.into_inner()
            .next()
            .ok_or_else(|| Error::Parse("empty literal".into()))?
    } else {
        pair
    };
    match lit_pair.as_rule() {
        Rule::integer => Ok(Literal::Integer(parse_integer(lit_pair.as_str())?)),
        Rule::float => {
            let s = lit_pair.as_str();
            let f: f64 = s.parse().map_err(|_| Error::InvalidNumber(s.into()))?;
            if !f.is_finite() {
                return Err(Error::Parse(format!(
                    "FloatingPointOverflow: float literal `{s}` overflows"
                )));
            }
            Ok(Literal::Float(f))
        }
        Rule::string => {
            let inner = lit_pair
                .into_inner()
                .next()
                .ok_or_else(|| Error::Parse("empty string literal".into()))?;
            Ok(Literal::String(unescape_string(inner.as_str())?))
        }
        Rule::boolean_lit => Ok(Literal::Boolean(
            lit_pair.as_str().eq_ignore_ascii_case("true"),
        )),
        Rule::null_lit => Ok(Literal::Null),
        r => Err(Error::Parse(format!("unexpected literal rule: {:?}", r))),
    }
}

fn parse_ident(s: &str) -> String {
    if s.starts_with('`') && s.ends_with('`') && s.len() >= 2 {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

fn build_subquery_body(pair: Pair<Rule>) -> Result<Statement> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("empty subquery_body".into()))?;
    match inner.as_rule() {
        Rule::read_stmt => build_read_stmt(inner),
        Rule::bare_match_body => {
            let mut patterns = Vec::new();
            let mut where_clause = None;
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::pattern_list => patterns = build_pattern_list(p)?,
                    Rule::where_clause => {
                        let ep = p
                            .into_inner()
                            .next()
                            .ok_or_else(|| Error::Parse("empty where".into()))?;
                        where_clause = Some(build_expression(ep)?);
                    }
                    _ => {}
                }
            }
            Ok(Statement::Match(crate::ast::MatchStmt {
                clauses: vec![crate::ast::ReadingClause::Match(crate::ast::MatchClause {
                    patterns,
                    where_clause,
                })],
                terminal: crate::ast::TerminalTail {
                    star: true,
                    ..Default::default()
                },
            }))
        }
        Rule::bare_pattern_body => {
            let mut patterns = Vec::new();
            let mut where_clause = None;
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::pattern_list => patterns = build_pattern_list(p)?,
                    Rule::where_clause => {
                        let ep = p
                            .into_inner()
                            .next()
                            .ok_or_else(|| Error::Parse("empty where".into()))?;
                        where_clause = Some(build_expression(ep)?);
                    }
                    _ => {}
                }
            }
            Ok(Statement::Match(crate::ast::MatchStmt {
                clauses: vec![crate::ast::ReadingClause::Match(crate::ast::MatchClause {
                    patterns,
                    where_clause,
                })],
                terminal: crate::ast::TerminalTail {
                    star: true,
                    ..Default::default()
                },
            }))
        }
        r => Err(Error::Parse(format!(
            "unexpected subquery_body child: {r:?}"
        ))),
    }
}

fn unescape_string(s: &str) -> Result<String> {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => out.push('\\'),
                Some('\'') => out.push('\''),
                Some('"') => out.push('"'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('t') => out.push('\t'),
                Some('b') => out.push('\u{0008}'),
                Some('f') => out.push('\u{000C}'),
                Some('u') => {
                    // `\uNNNN` — exactly four hex digits, decoded as a
                    // Unicode scalar. Shorter sequences or non-hex
                    // digits raise `InvalidUnicodeLiteral`.
                    let mut hex = String::with_capacity(4);
                    for _ in 0..4 {
                        match chars.next() {
                            Some(d) if d.is_ascii_hexdigit() => hex.push(d),
                            _ => {
                                return Err(Error::Parse(format!(
                                    "InvalidUnicodeLiteral: `\\u` must be followed by 4 hex digits"
                                )));
                            }
                        }
                    }
                    let code = u32::from_str_radix(&hex, 16).map_err(|_| {
                        Error::Parse(format!(
                            "InvalidUnicodeLiteral: `\\u{hex}` is not valid hex"
                        ))
                    })?;
                    let ch = char::from_u32(code).ok_or_else(|| {
                        Error::Parse(format!(
                            "InvalidUnicodeLiteral: `\\u{hex}` is not a valid code point"
                        ))
                    })?;
                    out.push(ch);
                }
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    Ok(out)
}

/// Drill through trivial wrapper rules (`add_expr`, `mul_expr`,
/// `pow_expr`, `unary_expr`, `primary`, `compound`, `comparison`,
/// `base_primary`, `literal`) looking for a bare integer literal.
/// Returns its source text if the chain is nothing but wrappers +
/// integer, or `None` otherwise.
fn peel_to_integer_literal(pair: &Pair<Rule>) -> Option<String> {
    peel_to_numeric_literal(pair, Rule::integer)
}

fn peel_to_float_literal(pair: &Pair<Rule>) -> Option<String> {
    peel_to_numeric_literal(pair, Rule::float)
}

fn peel_to_numeric_literal(pair: &Pair<Rule>, target: Rule) -> Option<String> {
    let mut current = pair.clone();
    loop {
        if current.as_rule() == target {
            return Some(current.as_str().to_string());
        }
        match current.as_rule() {
            Rule::literal
            | Rule::primary
            | Rule::base_primary
            | Rule::add_expr
            | Rule::mul_expr
            | Rule::pow_expr
            | Rule::unary_expr
            | Rule::compound
            | Rule::comparison
            | Rule::not_expr
            | Rule::and_expr
            | Rule::xor_expr
            | Rule::or_expr
            | Rule::expression => {
                let mut inner = current.into_inner();
                let single = inner.next()?;
                if inner.next().is_some() {
                    return None;
                }
                current = single;
            }
            _ => return None,
        }
    }
}

fn parse_integer(s: &str) -> Result<i64> {
    let (negative, digits) = if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else {
        (false, s)
    };
    // Parse as u64 first to handle i64::MIN correctly (-2^63)
    let uval = if let Some(hex) = digits
        .strip_prefix("0x")
        .or_else(|| digits.strip_prefix("0X"))
    {
        u64::from_str_radix(hex, 16).map_err(|_| Error::InvalidNumber(s.to_string()))?
    } else if let Some(oct) = digits
        .strip_prefix("0o")
        .or_else(|| digits.strip_prefix("0O"))
    {
        u64::from_str_radix(oct, 8).map_err(|_| Error::InvalidNumber(s.to_string()))?
    } else {
        digits
            .parse::<u64>()
            .map_err(|_| Error::InvalidNumber(s.to_string()))?
    };
    if negative {
        // -(2^63) = i64::MIN, handle without overflow
        if uval > (i64::MAX as u64) + 1 {
            return Err(Error::InvalidNumber(s.to_string()));
        }
        Ok(uval.wrapping_neg() as i64)
    } else {
        if uval > i64::MAX as u64 {
            return Err(Error::InvalidNumber(s.to_string()));
        }
        Ok(uval as i64)
    }
}

fn build_compare_op(pair: Pair<Rule>) -> Result<CompareOp> {
    debug_assert_eq!(pair.as_rule(), Rule::comparison_op);
    let op_pair = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("empty comparison op".into()))?;
    Ok(match op_pair.as_rule() {
        Rule::op_eq => CompareOp::Eq,
        Rule::op_neq => CompareOp::Ne,
        Rule::op_lt => CompareOp::Lt,
        Rule::op_leq => CompareOp::Le,
        Rule::op_gt => CompareOp::Gt,
        Rule::op_geq => CompareOp::Ge,
        Rule::op_starts_with => CompareOp::StartsWith,
        Rule::op_ends_with => CompareOp::EndsWith,
        Rule::op_contains => CompareOp::Contains,
        Rule::op_regex_match => CompareOp::RegexMatch,
        r => return Err(Error::Parse(format!("unexpected op: {:?}", r))),
    })
}
