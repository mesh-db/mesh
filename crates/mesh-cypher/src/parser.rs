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
        Rule::match_stmt => Ok(Statement::Match(build_match(inner)?)),
        Rule::unwind_stmt => Ok(Statement::Unwind(build_unwind(inner)?)),
        Rule::return_only_stmt => Ok(Statement::Return(build_return_only(inner)?)),
        r => Err(Error::Parse(format!("unexpected read_stmt child: {r:?}"))),
    }
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
/// `(label, property)` payload (`CREATE INDEX`, `DROP INDEX`). The
/// grammar puts `index_target` and `index_property` in a fixed order,
/// so we extract them positionally and discard the required-but-
/// ignored variable identifiers.
fn build_index_ddl(pair: Pair<Rule>) -> Result<crate::ast::IndexDdl> {
    let mut label = None;
    let mut property = None;
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::index_target => {
                // `ident ":" ident` — the leading identifier is a
                // pattern variable required by Cypher surface
                // syntax but has no meaning at the index level.
                let mut inner = p.into_inner();
                let _var = inner
                    .next()
                    .ok_or_else(|| Error::Parse("index target missing var".into()))?;
                let lab = inner
                    .next()
                    .ok_or_else(|| Error::Parse("index target missing label".into()))?;
                label = Some(lab.as_str().to_string());
            }
            Rule::index_property => {
                let mut inner = p.into_inner();
                let _var = inner
                    .next()
                    .ok_or_else(|| Error::Parse("index property missing var".into()))?;
                let key = inner
                    .next()
                    .ok_or_else(|| Error::Parse("index property missing key".into()))?;
                property = Some(key.as_str().to_string());
            }
            _ => {}
        }
    }
    Ok(crate::ast::IndexDdl {
        label: label.ok_or_else(|| Error::Parse("index ddl missing label".into()))?,
        property: property.ok_or_else(|| Error::Parse("index ddl missing property".into()))?,
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
                alias = Some(p.as_str().to_string());
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
                alias = Some(p.as_str().to_string());
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
    skip: &mut Option<i64>,
    limit: &mut Option<i64>,
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
                let int_pair = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty skip".into()))?;
                *skip = Some(parse_integer(int_pair.as_str())?);
            }
            Rule::limit_clause => {
                let int_pair = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty limit".into()))?;
                *limit = Some(parse_integer(int_pair.as_str())?);
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
                                    alias = child.as_str().to_string();
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
                        let body = inner
                            .into_inner()
                            .find(|p| p.as_rule() == Rule::union_query)
                            .ok_or_else(|| Error::Parse("CALL missing body".into()))?;
                        let stmt = build_union_query(body)?;
                        clauses.push(ReadingClause::Call(Box::new(stmt)));
                    }
                    r => {
                        return Err(Error::Parse(format!(
                            "unexpected rule inside reading_clause: {:?}",
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
        | ReadingClause::LoadCsv(_) => {}
        ReadingClause::OptionalMatch(_) => {
            return Err(Error::Parse(
                "OPTIONAL MATCH can only appear after an initial producer clause".into(),
            ));
        }
    }

    Ok(MatchStmt { clauses, terminal })
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
                let mut detach = false;
                let mut vars = Vec::new();
                for inner in p.into_inner() {
                    match inner.as_rule() {
                        Rule::kw_detach => detach = true,
                        Rule::delete_items => {
                            for id in inner.into_inner() {
                                vars.push(id.as_str().to_string());
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
                terminal.delete = Some(DeleteClause { detach, vars });
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
                                    Rule::identifier => var = child.as_str().to_string(),
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
                            var = inner.as_str().to_string();
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
                                                            v = child.as_str().to_string()
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
    let mut skip: Option<i64> = None;
    let mut limit: Option<i64> = None;
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
                let int_pair = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty skip in WITH".into()))?;
                skip = Some(parse_integer(int_pair.as_str())?);
            }
            Rule::limit_clause => {
                let int_pair = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty limit in WITH".into()))?;
                limit = Some(parse_integer(int_pair.as_str())?);
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
        Rule::rel_both => Direction::Both,
        r => return Err(Error::Parse(format!("unexpected rel rule: {:?}", r))),
    };

    let mut var = None;
    let mut edge_type = None;
    let mut var_length = None;
    for p in inner.into_inner() {
        if p.as_rule() == Rule::rel_detail {
            for d in p.into_inner() {
                match d.as_rule() {
                    Rule::identifier => var = Some(d.as_str().to_string()),
                    Rule::rel_type_spec => {
                        let id = d
                            .into_inner()
                            .next()
                            .ok_or_else(|| Error::Parse("empty rel type".into()))?;
                        edge_type = Some(id.as_str().to_string());
                    }
                    Rule::var_length => {
                        var_length = Some(build_var_length(d)?);
                    }
                    r => return Err(Error::Parse(format!("unexpected rel detail rule: {:?}", r))),
                }
            }
        }
    }

    Ok(RelPattern {
        var,
        edge_type,
        direction,
        var_length,
    })
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
                labels.push(id.as_str().to_string());
            }
            Ok(SetItem::Labels { var, labels })
        }
        Rule::set_prop_item => {
            let mut inner = pair.into_inner();
            let prop_access = inner
                .next()
                .ok_or_else(|| Error::Parse("set prop missing target".into()))?;
            let mut pa = prop_access.into_inner();
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
            let props_pair = inner
                .next()
                .ok_or_else(|| Error::Parse("set merge missing props".into()))?;
            let properties = build_properties(props_pair)?;
            Ok(SetItem::Merge { var, properties })
        }
        Rule::set_replace_item => {
            let mut inner = pair.into_inner();
            let var = inner
                .next()
                .ok_or_else(|| Error::Parse("set replace missing var".into()))?
                .as_str()
                .to_string();
            let props_pair = inner
                .next()
                .ok_or_else(|| Error::Parse("set replace missing props".into()))?;
            let properties = build_properties(props_pair)?;
            Ok(SetItem::Replace { var, properties })
        }
        r => Err(Error::Parse(format!("unexpected set item rule: {:?}", r))),
    }
}

fn build_node_pattern(pair: Pair<Rule>) -> Result<NodePattern> {
    let mut var = None;
    let mut labels = Vec::new();
    let mut properties = Vec::new();

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier => var = Some(p.as_str().to_string()),
            Rule::label_spec => {
                let label_id = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty label".into()))?;
                labels.push(label_id.as_str().to_string());
            }
            Rule::properties => properties = build_properties(p)?,
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
    match inner.as_rule() {
        Rule::literal => Ok(Expr::Literal(build_literal(inner)?)),
        Rule::parameter => Ok(Expr::Parameter(parameter_name(inner))),
        r => Err(Error::Parse(format!(
            "unexpected rule in property value: {:?}",
            r
        ))),
    }
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
        let expr = build_expression(
            inner
                .next()
                .ok_or_else(|| Error::Parse("return expr".into()))?,
        )?;
        let alias = inner.next().map(|p| p.as_str().to_string());
        items.push(ReturnItem { expr, alias });
    }
    Ok((items, false))
}

fn build_expression(pair: Pair<Rule>) -> Result<Expr> {
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
            let mut inner = pair.into_inner();
            let left = build_expression(
                inner
                    .next()
                    .ok_or_else(|| Error::Parse("empty comparison".into()))?,
            )?;
            // The comparison tail is either a `null_predicate`
            // (postfix IS NULL / IS NOT NULL) or a binary
            // `comparison_op primary` pair. The grammar tries
            // `null_predicate` first so it doesn't conflict with
            // the binary form.
            match inner.next() {
                None => Ok(left),
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
                Some(op_pair) => {
                    let op = build_compare_op(op_pair)?;
                    let right = build_expression(
                        inner
                            .next()
                            .ok_or_else(|| Error::Parse("missing comparison rhs".into()))?,
                    )?;
                    Ok(Expr::Compare {
                        op,
                        left: Box::new(left),
                        right: Box::new(right),
                    })
                }
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
        Rule::unary_expr => {
            let mut inner = pair.into_inner();
            let first = inner
                .next()
                .ok_or_else(|| Error::Parse("empty unary_expr".into()))?;
            if first.as_rule() == Rule::op_neg {
                let operand = build_expression(
                    inner
                        .next()
                        .ok_or_else(|| Error::Parse("unary - missing operand".into()))?,
                )?;
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
                        let key = inner_rule
                            .into_inner()
                            .find(|p| p.as_rule() == Rule::identifier)
                            .ok_or_else(|| Error::Parse("property_chain missing key".into()))?
                            .as_str()
                            .to_string();
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
            let name = inner
                .next()
                .ok_or_else(|| Error::Parse("function call missing name".into()))?
                .as_str()
                .to_string();
            let args = match inner.next() {
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
            let var = inner
                .next()
                .ok_or_else(|| Error::Parse("property var".into()))?
                .as_str()
                .to_string();
            let key = inner
                .next()
                .ok_or_else(|| Error::Parse("property key".into()))?
                .as_str()
                .to_string();
            Ok(Expr::Property { var, key })
        }
        Rule::identifier => Ok(Expr::Identifier(pair.as_str().to_string())),
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
                let key = key_pair.as_str().to_string();
                let value = build_expression(value_pair)?;
                entries.push((key, value));
            }
            Ok(Expr::Map(entries))
        }
        Rule::list_comp => build_list_comp(pair),
        Rule::reduce_expr => build_reduce_expr(pair),
        Rule::count_subquery => {
            let mut pat: Option<Pattern> = None;
            let mut where_expr: Option<Box<Expr>> = None;
            for p in pair.into_inner() {
                match p.as_rule() {
                    Rule::kw_count | Rule::kw_match => {}
                    Rule::pattern => pat = Some(build_pattern(p)?),
                    Rule::where_clause => {
                        let ep = p
                            .into_inner()
                            .next()
                            .ok_or_else(|| Error::Parse("empty count where".into()))?;
                        where_expr = Some(Box::new(build_expression(ep)?));
                    }
                    r => {
                        return Err(Error::Parse(format!(
                            "unexpected rule in count_subquery: {r:?}"
                        )))
                    }
                }
            }
            let pattern =
                pat.ok_or_else(|| Error::Parse("count_subquery missing pattern".into()))?;
            if pattern.path_var.is_some() {
                return Err(Error::Parse(
                    "path variable binding is not allowed inside count { ... }".into(),
                ));
            }
            Ok(Expr::CountSubquery {
                pattern,
                where_clause: where_expr,
            })
        }
        Rule::exists_subquery => {
            // Children (silent `kw_exists` / `kw_match` filtered
            // out): a single `pattern`, then an optional
            // `where_clause`. v1 enforces no path var on the
            // pattern — the `p =` prefix is a pattern-binding
            // form, not a predicate form.
            let mut pat: Option<Pattern> = None;
            let mut where_expr: Option<Box<Expr>> = None;
            for p in pair.into_inner() {
                match p.as_rule() {
                    Rule::kw_exists | Rule::kw_match => {}
                    Rule::pattern => pat = Some(build_pattern(p)?),
                    Rule::where_clause => {
                        let ep = p
                            .into_inner()
                            .next()
                            .ok_or_else(|| Error::Parse("empty exists where".into()))?;
                        where_expr = Some(Box::new(build_expression(ep)?));
                    }
                    r => {
                        return Err(Error::Parse(format!(
                            "unexpected rule in exists_subquery: {r:?}"
                        )))
                    }
                }
            }
            let pattern =
                pat.ok_or_else(|| Error::Parse("exists_subquery missing pattern".into()))?;
            if pattern.path_var.is_some() {
                return Err(Error::Parse(
                    "path variable binding is not allowed inside EXISTS { ... }".into(),
                ));
            }
            Ok(Expr::ExistsSubquery {
                pattern,
                where_clause: where_expr,
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
            Rule::identifier => ids.push(p.as_str().to_string()),
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
                var = Some(p.as_str().to_string());
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
            Ok(Literal::Float(lit_pair.as_str().parse().map_err(|_| {
                Error::InvalidNumber(lit_pair.as_str().into())
            })?))
        }
        Rule::string => {
            let inner = lit_pair
                .into_inner()
                .next()
                .ok_or_else(|| Error::Parse("empty string literal".into()))?;
            Ok(Literal::String(inner.as_str().to_string()))
        }
        Rule::boolean_lit => Ok(Literal::Boolean(
            lit_pair.as_str().eq_ignore_ascii_case("true"),
        )),
        Rule::null_lit => Ok(Literal::Null),
        r => Err(Error::Parse(format!("unexpected literal rule: {:?}", r))),
    }
}

fn parse_integer(s: &str) -> Result<i64> {
    s.parse().map_err(|_| Error::InvalidNumber(s.to_string()))
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
