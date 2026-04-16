use crate::{
    error::{Error, Result},
    reader::GraphReader,
    value::{ParamMap, Row, Value},
};
use mesh_core::{NodeId, Property};
use mesh_cypher::{
    BinaryOp, CallArgs, CompareOp, Direction, Expr, Literal, NodePattern, Pattern, UnaryOp,
};
use std::cmp::Ordering;

/// Per-evaluation context threaded through every `eval_expr`
/// call. Holds the current row, the query parameters, and a
/// reference to the graph reader so expression variants that
/// need to touch the graph (e.g. `Expr::PatternExists`) can
/// do so without widening the call signature. Scratch-row
/// iteration (`reduce`, list comprehensions) constructs a
/// derived ctx via [`EvalCtx::with_row`] so the params +
/// reader are inherited for free.
#[derive(Clone, Copy)]
pub(crate) struct EvalCtx<'a> {
    pub row: &'a Row,
    pub params: &'a ParamMap,
    pub reader: &'a dyn GraphReader,
}

impl<'a> EvalCtx<'a> {
    /// Build a fresh ctx that swaps only the row, preserving
    /// params and reader. Used by iteration primitives that
    /// evaluate sub-expressions against a scratch row (list
    /// comprehensions, reduce).
    pub(crate) fn with_row(&self, new_row: &'a Row) -> EvalCtx<'a> {
        EvalCtx {
            row: new_row,
            params: self.params,
            reader: self.reader,
        }
    }
}

pub(crate) fn eval_expr(expr: &Expr, ctx: &EvalCtx) -> Result<Value> {
    match expr {
        Expr::Literal(lit) => Ok(literal_to_value(lit)),
        Expr::Identifier(name) => ctx
            .row
            .get(name)
            .cloned()
            .ok_or_else(|| Error::UnboundVariable(name.clone())),
        Expr::Parameter(name) => ctx
            .params
            .get(name)
            .cloned()
            .ok_or_else(|| Error::UnboundParameter(name.clone())),
        Expr::Property { var, key } => {
            let bound = ctx
                .row
                .get(var)
                .ok_or_else(|| Error::UnboundVariable(var.clone()))?;
            match bound {
                Value::Node(n) => Ok(n
                    .properties
                    .get(key)
                    .cloned()
                    .map(Value::Property)
                    .unwrap_or(Value::Null)),
                Value::Edge(e) => Ok(e
                    .properties
                    .get(key)
                    .cloned()
                    .map(Value::Property)
                    .unwrap_or(Value::Null)),
                // Null-propagate property access so OPTIONAL MATCH
                // patterns like `f.name` return Null when `f`
                // itself is Null (i.e. the optional pattern
                // didn't match). Matches SQL / openCypher
                // left-join semantics.
                Value::Null | Value::Property(Property::Null) => Ok(Value::Null),
                Value::Property(Property::Map(m)) => Ok(m
                    .get(key)
                    .cloned()
                    .map(Value::Property)
                    .unwrap_or(Value::Null)),
                _ => Err(Error::NotNodeOrEdge),
            }
        }
        Expr::PropertyAccess { base, key } => {
            let v = eval_expr(base, ctx)?;
            match v {
                Value::Node(n) => Ok(n
                    .properties
                    .get(key)
                    .cloned()
                    .map(Value::Property)
                    .unwrap_or(Value::Null)),
                Value::Edge(e) => Ok(e
                    .properties
                    .get(key)
                    .cloned()
                    .map(Value::Property)
                    .unwrap_or(Value::Null)),
                Value::Null | Value::Property(Property::Null) => Ok(Value::Null),
                Value::Property(Property::Map(m)) => Ok(m
                    .get(key)
                    .cloned()
                    .map(Value::Property)
                    .unwrap_or(Value::Null)),
                _ => Err(Error::NotNodeOrEdge),
            }
        }
        Expr::IndexAccess { base, index } => {
            let base_val = eval_expr(base, ctx)?;
            let idx_val = eval_expr(index, ctx)?;
            if matches!(base_val, Value::Null | Value::Property(Property::Null))
                || matches!(idx_val, Value::Null | Value::Property(Property::Null))
            {
                return Ok(Value::Null);
            }
            let items = match base_val {
                Value::List(items) => items,
                Value::Property(Property::List(props)) => {
                    props.into_iter().map(Value::Property).collect()
                }
                _ => return Err(Error::TypeMismatch),
            };
            let i = match idx_val {
                Value::Property(Property::Int64(i)) => i,
                _ => return Err(Error::TypeMismatch),
            };
            let resolved = if i >= 0 {
                items.into_iter().nth(i as usize)
            } else {
                let len = items.len() as i64;
                let pos = len + i;
                if pos < 0 {
                    None
                } else {
                    items.into_iter().nth(pos as usize)
                }
            };
            Ok(resolved.unwrap_or(Value::Null))
        }
        Expr::SliceAccess { base, start, end } => {
            let base_val = eval_expr(base, ctx)?;
            if matches!(base_val, Value::Null | Value::Property(Property::Null)) {
                return Ok(Value::Null);
            }
            let items: Vec<Value> = match base_val {
                Value::List(items) => items,
                Value::Property(Property::List(props)) => {
                    props.into_iter().map(Value::Property).collect()
                }
                _ => return Err(Error::TypeMismatch),
            };
            let len = items.len() as i64;
            let resolve = |expr: &Expr| -> Result<i64> {
                match eval_expr(expr, ctx)? {
                    Value::Property(Property::Int64(i)) => Ok(i),
                    _ => Err(Error::TypeMismatch),
                }
            };
            let s = match start {
                Some(e) => {
                    let raw = resolve(e)?;
                    let abs = if raw < 0 {
                        (len + raw).max(0)
                    } else {
                        raw.min(len)
                    };
                    abs as usize
                }
                None => 0,
            };
            let e = match end {
                Some(e) => {
                    let raw = resolve(e)?;
                    let abs = if raw < 0 {
                        (len + raw).max(0)
                    } else {
                        raw.min(len)
                    };
                    abs as usize
                }
                None => len as usize,
            };
            if s >= e {
                return Ok(Value::List(Vec::new()));
            }
            Ok(Value::List(items[s..e].to_vec()))
        }
        Expr::Not(inner) => {
            let v = eval_expr(inner, ctx)?;
            Ok(Value::Property(Property::Bool(!to_bool(&v)?)))
        }
        Expr::And(a, b) => {
            let va = to_bool(&eval_expr(a, ctx)?)?;
            if !va {
                return Ok(Value::Property(Property::Bool(false)));
            }
            let vb = to_bool(&eval_expr(b, ctx)?)?;
            Ok(Value::Property(Property::Bool(vb)))
        }
        Expr::Or(a, b) => {
            let va = to_bool(&eval_expr(a, ctx)?)?;
            if va {
                return Ok(Value::Property(Property::Bool(true)));
            }
            let vb = to_bool(&eval_expr(b, ctx)?)?;
            Ok(Value::Property(Property::Bool(vb)))
        }
        Expr::Compare { op, left, right } => {
            let vl = eval_expr(left, ctx)?;
            let vr = eval_expr(right, ctx)?;
            Ok(Value::Property(Property::Bool(compare(*op, &vl, &vr)?)))
        }
        Expr::IsNull { negated, inner } => {
            let v = eval_expr(inner, ctx)?;
            let is_null = matches!(v, Value::Null | Value::Property(Property::Null));
            Ok(Value::Property(Property::Bool(if *negated {
                !is_null
            } else {
                is_null
            })))
        }
        Expr::InList { element, list } => {
            let elem = eval_expr(element, ctx)?;
            if matches!(elem, Value::Null | Value::Property(Property::Null)) {
                return Ok(Value::Property(Property::Bool(false)));
            }
            let list_val = eval_expr(list, ctx)?;
            let items = match list_val {
                Value::List(items) => items,
                Value::Property(Property::List(props)) => {
                    props.into_iter().map(Value::Property).collect()
                }
                Value::Null | Value::Property(Property::Null) => {
                    return Ok(Value::Property(Property::Bool(false)));
                }
                _ => return Err(Error::TypeMismatch),
            };
            let found = items.iter().any(|item| values_equal(&elem, item));
            Ok(Value::Property(Property::Bool(found)))
        }
        Expr::Call { name, args } => call_scalar(name, args, ctx),
        Expr::List(items) => {
            let mut out = Vec::with_capacity(items.len());
            for e in items {
                out.push(eval_expr(e, ctx)?);
            }
            Ok(Value::List(out))
        }
        Expr::Map(entries) => {
            // Evaluate each entry value, unwrap to a Property, and
            // stash in a HashMap. v1 restriction: map values must be
            // Property (string, int, float, bool, list, map, null) —
            // nested Nodes/Edges are rejected with TypeMismatch.
            // Null values flow through as Property::Null rather than
            // short-circuiting the whole map, matching how Neo4j
            // returns `{name: null}` as a 1-key map.
            let mut out = std::collections::HashMap::with_capacity(entries.len());
            for (key, expr) in entries {
                let v = eval_expr(expr, ctx)?;
                let prop = match v {
                    Value::Property(p) => p,
                    Value::Null => Property::Null,
                    Value::List(items) => {
                        let mut props = Vec::with_capacity(items.len());
                        for item in items {
                            match item {
                                Value::Property(p) => props.push(p),
                                Value::Null => props.push(Property::Null),
                                _ => return Err(Error::TypeMismatch),
                            }
                        }
                        Property::List(props)
                    }
                    // Nodes / Edges / Paths can't nest inside a
                    // Property::Map in the current type model.
                    // Cypher's real semantics allow this; revisit
                    // if driver code actually needs it.
                    Value::Node(_) | Value::Edge(_) | Value::Path { .. } => {
                        return Err(Error::TypeMismatch)
                    }
                };
                out.insert(key.clone(), prop);
            }
            Ok(Value::Property(Property::Map(out)))
        }
        Expr::Case {
            scrutinee,
            branches,
            else_expr,
        } => {
            let scrutinee_val = match scrutinee {
                Some(e) => Some(eval_expr(e, ctx)?),
                None => None,
            };
            for (cond, result) in branches {
                let cond_val = eval_expr(cond, ctx)?;
                let matched = match &scrutinee_val {
                    Some(sv) => case_equals(sv, &cond_val),
                    None => to_bool(&cond_val).unwrap_or(false),
                };
                if matched {
                    return eval_expr(result, ctx);
                }
            }
            match else_expr {
                Some(e) => eval_expr(e, ctx),
                None => Ok(Value::Null),
            }
        }
        Expr::ListComprehension {
            var,
            source,
            predicate,
            projection,
        } => {
            let source_val = eval_expr(source, ctx)?;
            let items = match source_val {
                Value::List(items) => items,
                Value::Property(Property::List(props)) => {
                    props.into_iter().map(Value::Property).collect()
                }
                Value::Null => return Ok(Value::Null),
                _ => return Err(Error::TypeMismatch),
            };
            let mut scratch = ctx.row.clone();
            let had_prev = scratch.contains_key(var);
            let prev = scratch.get(var).cloned();
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                scratch.insert(var.clone(), item);
                let sub_ctx = ctx.with_row(&scratch);
                if let Some(pred) = predicate {
                    let pv = eval_expr(pred, &sub_ctx)?;
                    if !to_bool(&pv).unwrap_or(false) {
                        continue;
                    }
                }
                let projected = match projection {
                    Some(p) => eval_expr(p, &sub_ctx)?,
                    None => scratch.get(var).cloned().unwrap_or(Value::Null),
                };
                out.push(projected);
            }
            if had_prev {
                if let Some(v) = prev {
                    scratch.insert(var.clone(), v);
                }
            }
            Ok(Value::List(out))
        }
        Expr::Reduce {
            acc_var,
            acc_init,
            elem_var,
            source,
            body,
        } => {
            // Left fold: evaluate acc_init once, then for each
            // element bind acc + elem into a scratch row and
            // evaluate body to get the next acc. Empty source
            // returns the init value unchanged. Null source
            // propagates as in every other collection primitive.
            let source_val = eval_expr(source, ctx)?;
            let items = match source_val {
                Value::List(items) => items,
                Value::Property(Property::List(props)) => {
                    props.into_iter().map(Value::Property).collect()
                }
                Value::Null => return Ok(Value::Null),
                Value::Property(Property::Null) => return Ok(Value::Null),
                _ => return Err(Error::TypeMismatch),
            };
            let mut acc = eval_expr(acc_init, ctx)?;
            let mut scratch = ctx.row.clone();
            // Save any prior bindings under the two iteration
            // variables so reduce's scope doesn't leak into
            // sibling expressions in the same row — Cypher's
            // scoping rules make the fold's acc/elem ephemeral.
            let prev_acc = scratch.get(acc_var).cloned();
            let prev_elem = scratch.get(elem_var).cloned();
            for item in items {
                scratch.insert(acc_var.clone(), acc);
                scratch.insert(elem_var.clone(), item);
                let sub_ctx = ctx.with_row(&scratch);
                acc = eval_expr(body, &sub_ctx)?;
            }
            match prev_acc {
                Some(v) => {
                    scratch.insert(acc_var.clone(), v);
                }
                None => {
                    scratch.remove(acc_var);
                }
            }
            match prev_elem {
                Some(v) => {
                    scratch.insert(elem_var.clone(), v);
                }
                None => {
                    scratch.remove(elem_var);
                }
            }
            Ok(acc)
        }
        Expr::BinaryOp { op, left, right } => {
            let l = eval_expr(left, ctx)?;
            let r = eval_expr(right, ctx)?;
            eval_binary_op(*op, l, r)
        }
        Expr::UnaryOp { op, operand } => {
            let v = eval_expr(operand, ctx)?;
            eval_unary_op(*op, v)
        }
        Expr::PatternExists(pattern) => {
            let b = pattern_exists(pattern, ctx)?;
            Ok(Value::Property(Property::Bool(b)))
        }
        Expr::ExistsSubquery {
            pattern,
            where_clause,
        } => {
            let b = exists_subquery_matches(pattern, where_clause.as_deref(), ctx)?;
            Ok(Value::Property(Property::Bool(b)))
        }
        Expr::CountSubquery {
            pattern,
            where_clause,
        } => {
            let n = count_subquery_matches(pattern, where_clause.as_deref(), ctx)?;
            Ok(Value::Property(Property::Int64(n)))
        }
    }
}

/// Check whether a fixed-hop pattern has at least one match in the
/// graph given the outer row's bindings. Walks a BFS-style frontier
/// from the (bound) start node through each hop, filtering by edge
/// type, direction, and target label/property constraints. Returns
/// `true` on the first complete walk; returns `false` if any hop
/// produces an empty frontier or if the start variable isn't bound
/// to a `Value::Node` in the outer row.
///
/// v1 invariants (the parser and planner enforce these):
/// - `pattern.path_var` is `None`
/// - `pattern.hops` is non-empty
/// - `pattern.start.var` is `Some(_)`
/// - No `hop.rel.var_length` is set
fn pattern_exists(pattern: &Pattern, ctx: &EvalCtx) -> Result<bool> {
    let start_var = pattern
        .start
        .var
        .as_ref()
        .expect("planner guarantees bound start var for pattern predicates");
    let start_node = match ctx.row.get(start_var) {
        Some(Value::Node(n)) => n.clone(),
        // If the start variable isn't a Node (unbound, Null from an
        // OPTIONAL MATCH, something else entirely), the pattern
        // can't possibly match — behave like Neo4j and return false.
        _ => return Ok(false),
    };
    // Start-node label constraint: every label on the start pattern
    // must be present on the bound node. Pattern properties on the
    // start node are also checked so `(a:Person {name: 'Ada'})` as a
    // predicate filters on the name even when `a` was bound by an
    // earlier MATCH without that constraint.
    if !start_node_matches(&start_node, &pattern.start, ctx)? {
        return Ok(false);
    }

    let mut frontier: Vec<NodeId> = vec![start_node.id];
    for hop in &pattern.hops {
        let mut next: Vec<NodeId> = Vec::new();
        if let Some(vl) = hop.rel.var_length {
            for node_id in &frontier {
                expand_var_length_predicate(*node_id, hop, vl.min, vl.max, ctx, &mut next)?;
            }
        } else {
            for node_id in &frontier {
                expand_single_hop_predicate(*node_id, hop, ctx, &mut next)?;
            }
        }
        if next.is_empty() {
            return Ok(false);
        }
        frontier = next;
    }
    Ok(!frontier.is_empty())
}

fn expand_single_hop_predicate(
    node_id: NodeId,
    hop: &mesh_cypher::Hop,
    ctx: &EvalCtx,
    out: &mut Vec<NodeId>,
) -> Result<()> {
    let neighbors = match hop.rel.direction {
        Direction::Outgoing => ctx.reader.outgoing(node_id)?,
        Direction::Incoming => ctx.reader.incoming(node_id)?,
        Direction::Both => {
            let mut all = ctx.reader.outgoing(node_id)?;
            all.extend(ctx.reader.incoming(node_id)?);
            all
        }
    };
    for (edge_id, neighbor_id) in neighbors {
        if let Some(t) = &hop.rel.edge_type {
            let edge = match ctx.reader.get_edge(edge_id)? {
                Some(e) => e,
                None => continue,
            };
            if &edge.edge_type != t {
                continue;
            }
        }
        if !hop.target.labels.is_empty() || !hop.target.properties.is_empty() {
            let neighbor = match ctx.reader.get_node(neighbor_id)? {
                Some(n) => n,
                None => continue,
            };
            if !node_pattern_matches(&neighbor, &hop.target, ctx)? {
                continue;
            }
        }
        if let Some(target_var) = &hop.target.var {
            if let Some(Value::Node(bound)) = ctx.row.get(target_var) {
                if bound.id != neighbor_id {
                    continue;
                }
            }
        }
        out.push(neighbor_id);
    }
    Ok(())
}

fn expand_var_length_predicate(
    start: NodeId,
    hop: &mesh_cypher::Hop,
    min: u64,
    max: u64,
    ctx: &EvalCtx,
    out: &mut Vec<NodeId>,
) -> Result<()> {
    let mut used = std::collections::HashSet::new();
    vl_pred_dfs(start, hop, min, max, 0, ctx, &mut used, out)
}

fn vl_pred_dfs(
    current: NodeId,
    hop: &mesh_cypher::Hop,
    min: u64,
    max: u64,
    depth: u64,
    ctx: &EvalCtx,
    used: &mut std::collections::HashSet<mesh_core::EdgeId>,
    out: &mut Vec<NodeId>,
) -> Result<()> {
    if depth >= min && depth <= max {
        let ok = if !hop.target.labels.is_empty() || !hop.target.properties.is_empty() {
            match ctx.reader.get_node(current)? {
                Some(n) => node_pattern_matches(&n, &hop.target, ctx)?,
                None => false,
            }
        } else {
            true
        };
        if ok {
            if let Some(target_var) = &hop.target.var {
                if let Some(Value::Node(bound)) = ctx.row.get(target_var) {
                    if bound.id == current {
                        out.push(current);
                    }
                } else {
                    out.push(current);
                }
            } else {
                out.push(current);
            }
        }
    }
    if depth >= max {
        return Ok(());
    }
    let neighbors = match hop.rel.direction {
        Direction::Outgoing => ctx.reader.outgoing(current)?,
        Direction::Incoming => ctx.reader.incoming(current)?,
        Direction::Both => {
            let mut all = ctx.reader.outgoing(current)?;
            all.extend(ctx.reader.incoming(current)?);
            all
        }
    };
    for (edge_id, neighbor_id) in neighbors {
        if used.contains(&edge_id) {
            continue;
        }
        if let Some(t) = &hop.rel.edge_type {
            let edge = match ctx.reader.get_edge(edge_id)? {
                Some(e) => e,
                None => continue,
            };
            if &edge.edge_type != t {
                continue;
            }
        }
        used.insert(edge_id);
        vl_pred_dfs(neighbor_id, hop, min, max, depth + 1, ctx, used, out)?;
        used.remove(&edge_id);
    }
    Ok(())
}

/// Check whether `EXISTS { MATCH pattern [WHERE expr] }` has
/// any satisfying assignment given the outer row's bindings.
///
/// Structurally a correlated-or-uncorrelated subquery:
/// 1. Resolve the set of start-node candidates. If the pattern's
///    start var is already bound in the outer row, there's
///    exactly one candidate. Otherwise enumerate the graph
///    (via a label index when available, else a full scan).
/// 2. For each candidate, walk the pattern collecting
///    `(edge, target)` pairs at each hop, materialize each
///    complete binding as a scratch row, and evaluate the
///    optional `where_clause` against it.
/// 3. Return `true` on the first candidate × binding where the
///    pattern matches and the WHERE evaluates truthy.
///
/// Uncorrelated EXISTS fires when the start var is `None` (the
/// pattern has an anonymous start node) or when the named start
/// var isn't present in the outer row. That's the
/// `EXISTS { MATCH (:Person) }` and
/// `EXISTS { MATCH (a:Person)-[:KNOWS]->(b) }` shapes — uses
/// that don't reference any outer binding.
///
/// Compared to [`pattern_exists`], this version can't early-
/// stop on the first complete walk per candidate — it needs to
/// hold each candidate's full binding state long enough to
/// evaluate WHERE. The frontier therefore carries
/// `(node, scratch_row)` pairs instead of just node ids.
fn exists_subquery_matches(
    pattern: &Pattern,
    where_clause: Option<&Expr>,
    ctx: &EvalCtx,
) -> Result<bool> {
    let start_candidates = resolve_exists_start_candidates(&pattern.start, ctx)?;
    for start_node in start_candidates {
        if !start_node_matches(&start_node, &pattern.start, ctx)? {
            continue;
        }

        // Build the seed scratch row, binding the start var if
        // the pattern names one. Uncorrelated anonymous-start
        // patterns don't leak any new binding into the row.
        let mut seed = ctx.row.clone();
        if let Some(start_var) = &pattern.start.var {
            seed.insert(start_var.clone(), Value::Node(start_node.clone()));
        }

        // Zero-hop patterns: no frontier to walk. With no WHERE
        // the mere existence of a matching start node is enough;
        // with a WHERE we evaluate it against the seed.
        if pattern.hops.is_empty() {
            match where_clause {
                None => return Ok(true),
                Some(w) => {
                    let sub_ctx = ctx.with_row(&seed);
                    let v = eval_expr(w, &sub_ctx)?;
                    if to_bool(&v).unwrap_or(false) {
                        return Ok(true);
                    }
                    continue;
                }
            }
        }

        // Multi-hop: walk the frontier from this candidate,
        // collecting every complete binding that matches the
        // pattern shape, then check each against WHERE.
        let mut frontier: Vec<(mesh_core::Node, Row)> = vec![(start_node.clone(), seed)];
        if walk_exists_hops(pattern, where_clause, &mut frontier, ctx)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn count_subquery_matches(
    pattern: &Pattern,
    where_clause: Option<&Expr>,
    ctx: &EvalCtx,
) -> Result<i64> {
    let mut total: i64 = 0;
    let start_candidates = resolve_exists_start_candidates(&pattern.start, ctx)?;
    for start_node in start_candidates {
        if !start_node_matches(&start_node, &pattern.start, ctx)? {
            continue;
        }
        let mut seed = ctx.row.clone();
        if let Some(start_var) = &pattern.start.var {
            seed.insert(start_var.clone(), Value::Node(start_node.clone()));
        }
        if pattern.hops.is_empty() {
            match where_clause {
                None => total += 1,
                Some(w) => {
                    let sub_ctx = ctx.with_row(&seed);
                    let v = eval_expr(w, &sub_ctx)?;
                    if to_bool(&v).unwrap_or(false) {
                        total += 1;
                    }
                }
            }
            continue;
        }
        let mut frontier: Vec<(mesh_core::Node, Row)> = vec![(start_node.clone(), seed)];
        total += count_walk_hops(pattern, where_clause, &mut frontier, ctx)?;
    }
    Ok(total)
}

fn count_walk_hops(
    pattern: &Pattern,
    where_clause: Option<&Expr>,
    frontier: &mut Vec<(mesh_core::Node, Row)>,
    ctx: &EvalCtx,
) -> Result<i64> {
    for hop in &pattern.hops {
        let mut next: Vec<(mesh_core::Node, Row)> = Vec::new();
        if let Some(vl) = hop.rel.var_length {
            for (node, row) in frontier.iter() {
                let mut targets = Vec::new();
                expand_var_length_predicate(node.id, hop, vl.min, vl.max, ctx, &mut targets)?;
                for tid in targets {
                    let target = match ctx.reader.get_node(tid)? {
                        Some(n) => n,
                        None => continue,
                    };
                    let mut next_row = row.clone();
                    if let Some(tv) = &hop.target.var {
                        next_row.insert(tv.clone(), Value::Node(target.clone()));
                    }
                    next.push((target, next_row));
                }
            }
            if next.is_empty() {
                return Ok(0);
            }
            *frontier = next;
            continue;
        }
        for (node, row) in frontier.iter() {
            let neighbors = match hop.rel.direction {
                Direction::Outgoing => ctx.reader.outgoing(node.id)?,
                Direction::Incoming => ctx.reader.incoming(node.id)?,
                Direction::Both => {
                    let mut out = ctx.reader.outgoing(node.id)?;
                    out.extend(ctx.reader.incoming(node.id)?);
                    out
                }
            };
            for (edge_id, neighbor_id) in neighbors {
                let edge_record = if hop.rel.edge_type.is_some() || hop.rel.var.is_some() {
                    match ctx.reader.get_edge(edge_id)? {
                        Some(e) => Some(e),
                        None => continue,
                    }
                } else {
                    None
                };
                if let Some(t) = &hop.rel.edge_type {
                    if let Some(e) = &edge_record {
                        if &e.edge_type != t {
                            continue;
                        }
                    }
                }
                let need_target = !hop.target.labels.is_empty()
                    || !hop.target.properties.is_empty()
                    || hop.target.var.is_some();
                let target_node = if need_target {
                    match ctx.reader.get_node(neighbor_id)? {
                        Some(n) => Some(n),
                        None => continue,
                    }
                } else {
                    None
                };
                if let Some(n) = &target_node {
                    if (!hop.target.labels.is_empty() || !hop.target.properties.is_empty())
                        && !node_pattern_matches(n, &hop.target, ctx)?
                    {
                        continue;
                    }
                }
                if let Some(target_var) = &hop.target.var {
                    if let Some(Value::Node(bound)) = row.get(target_var) {
                        if bound.id != neighbor_id {
                            continue;
                        }
                    }
                }
                let mut next_row = row.clone();
                if let Some(edge_var) = &hop.rel.var {
                    if let Some(e) = &edge_record {
                        next_row.insert(edge_var.clone(), Value::Edge(e.clone()));
                    }
                }
                let concrete = match target_node {
                    Some(n) => n,
                    None => match ctx.reader.get_node(neighbor_id)? {
                        Some(n) => n,
                        None => continue,
                    },
                };
                if let Some(target_var) = &hop.target.var {
                    next_row.insert(target_var.clone(), Value::Node(concrete.clone()));
                }
                next.push((concrete, next_row));
            }
        }
        if next.is_empty() {
            return Ok(0);
        }
        *frontier = next;
    }
    match where_clause {
        None => Ok(frontier.len() as i64),
        Some(w) => {
            let mut count = 0i64;
            for (_, row) in frontier.iter() {
                let sub_ctx = ctx.with_row(row);
                let v = eval_expr(w, &sub_ctx)?;
                if to_bool(&v).unwrap_or(false) {
                    count += 1;
                }
            }
            Ok(count)
        }
    }
}

/// Compute the list of candidate start nodes for an
/// `EXISTS { ... }` subquery. Correlated case (start var
/// bound in outer row) returns a singleton; uncorrelated case
/// enumerates the graph — preferring a label index when the
/// pattern carries a label, otherwise falling back to
/// `all_node_ids`.
fn resolve_exists_start_candidates(
    start: &NodePattern,
    ctx: &EvalCtx,
) -> Result<Vec<mesh_core::Node>> {
    // Correlated: the outer row already has this variable
    // bound to a Node. Return it as the sole candidate so the
    // walk downstream sees the correlation constraint.
    if let Some(var) = &start.var {
        if let Some(Value::Node(n)) = ctx.row.get(var) {
            return Ok(vec![n.clone()]);
        }
    }
    // Uncorrelated: enumerate from the graph. Prefer a label
    // index when available — most real EXISTS patterns carry a
    // label and this is much cheaper than a full scan. The
    // per-node `start_node_matches` check in the caller handles
    // pattern properties and any additional labels.
    let ids = match start.labels.first() {
        Some(label) => ctx.reader.nodes_by_label(label)?,
        None => ctx.reader.all_node_ids()?,
    };
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(n) = ctx.reader.get_node(id)? {
            out.push(n);
        }
    }
    Ok(out)
}

/// Walk the hops of a subquery pattern from a single seeded
/// frontier entry. Returns `true` on the first complete binding
/// where `where_clause` (or `None`) evaluates truthy; returns
/// `false` otherwise. Used only by `exists_subquery_matches` —
/// extracted so the outer loop can iterate start candidates
/// without duplicating the frontier-walk logic.
fn walk_exists_hops(
    pattern: &Pattern,
    where_clause: Option<&Expr>,
    frontier: &mut Vec<(mesh_core::Node, Row)>,
    ctx: &EvalCtx,
) -> Result<bool> {
    for hop in &pattern.hops {
        let mut next: Vec<(mesh_core::Node, Row)> = Vec::new();
        if let Some(vl) = hop.rel.var_length {
            for (node, row) in frontier.iter() {
                let mut targets = Vec::new();
                expand_var_length_predicate(node.id, hop, vl.min, vl.max, ctx, &mut targets)?;
                for tid in targets {
                    let target = match ctx.reader.get_node(tid)? {
                        Some(n) => n,
                        None => continue,
                    };
                    let mut next_row = row.clone();
                    if let Some(tv) = &hop.target.var {
                        next_row.insert(tv.clone(), Value::Node(target.clone()));
                    }
                    next.push((target, next_row));
                }
            }
            if next.is_empty() {
                return Ok(false);
            }
            *frontier = next;
            continue;
        }
        for (node, row) in frontier.iter() {
            let neighbors = match hop.rel.direction {
                Direction::Outgoing => ctx.reader.outgoing(node.id)?,
                Direction::Incoming => ctx.reader.incoming(node.id)?,
                Direction::Both => {
                    let mut out = ctx.reader.outgoing(node.id)?;
                    out.extend(ctx.reader.incoming(node.id)?);
                    out
                }
            };
            for (edge_id, neighbor_id) in neighbors {
                // Fetch the edge when we need its type or when
                // the WHERE clause will bind it by variable.
                let edge_record = if hop.rel.edge_type.is_some() || hop.rel.var.is_some() {
                    match ctx.reader.get_edge(edge_id)? {
                        Some(e) => Some(e),
                        None => continue,
                    }
                } else {
                    None
                };
                if let Some(t) = &hop.rel.edge_type {
                    if let Some(e) = &edge_record {
                        if &e.edge_type != t {
                            continue;
                        }
                    }
                }
                // Fetch the target node when we need its
                // labels/properties or the WHERE binds it.
                let need_target = !hop.target.labels.is_empty()
                    || !hop.target.properties.is_empty()
                    || hop.target.var.is_some();
                let target_node = if need_target {
                    match ctx.reader.get_node(neighbor_id)? {
                        Some(n) => Some(n),
                        None => continue,
                    }
                } else {
                    None
                };
                if let Some(n) = &target_node {
                    if (!hop.target.labels.is_empty() || !hop.target.properties.is_empty())
                        && !node_pattern_matches(n, &hop.target, ctx)?
                    {
                        continue;
                    }
                }
                // Correlated constraint: if the target var is
                // already bound in the scratch row (from an
                // outer row OR an earlier hop), require the
                // walked neighbor to match it.
                if let Some(target_var) = &hop.target.var {
                    if let Some(Value::Node(bound)) = row.get(target_var) {
                        if bound.id != neighbor_id {
                            continue;
                        }
                    }
                }
                // Extend the scratch row. Bind the edge var
                // (if named) and the target var (if named) so
                // the WHERE clause can touch them.
                let mut next_row = row.clone();
                if let Some(edge_var) = &hop.rel.var {
                    if let Some(e) = &edge_record {
                        next_row.insert(edge_var.clone(), Value::Edge(e.clone()));
                    }
                }
                // We need a concrete target node for the next
                // hop's frontier regardless of whether we
                // fetched it above.
                let concrete = match target_node {
                    Some(n) => n,
                    None => match ctx.reader.get_node(neighbor_id)? {
                        Some(n) => n,
                        None => continue,
                    },
                };
                if let Some(target_var) = &hop.target.var {
                    next_row.insert(target_var.clone(), Value::Node(concrete.clone()));
                }
                next.push((concrete, next_row));
            }
        }
        if next.is_empty() {
            return Ok(false);
        }
        *frontier = next;
    }

    // Every frontier entry now represents a complete walk
    // matching the pattern shape. With no WHERE we return true
    // on the first one; with a WHERE we evaluate it against
    // each candidate's scratch row until one passes.
    match where_clause {
        None => Ok(true),
        Some(w) => {
            for (_, row) in frontier.iter() {
                let sub_ctx = ctx.with_row(row);
                let v = eval_expr(w, &sub_ctx)?;
                if to_bool(&v).unwrap_or(false) {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

/// Check whether the bound start node satisfies its pattern's
/// label and property constraints. Pattern properties on a start
/// node are `Expr::Literal` / `Expr::Parameter` per the grammar's
/// `property_value` rule, so evaluation is cheap and side-effect
/// free.
fn start_node_matches(
    node: &mesh_core::Node,
    pattern: &NodePattern,
    ctx: &EvalCtx,
) -> Result<bool> {
    for required in &pattern.labels {
        if !node.labels.contains(required) {
            return Ok(false);
        }
    }
    for (key, expr) in &pattern.properties {
        let expected = eval_expr(expr, ctx)?;
        let actual = node
            .properties
            .get(key)
            .cloned()
            .map(Value::Property)
            .unwrap_or(Value::Null);
        if !case_equals(&expected, &actual) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Same as [`start_node_matches`] but for target nodes on a hop's
/// far end. Separate function for symmetry; the logic is identical
/// today but the two sides may diverge in future (e.g. if target
/// patterns gain constraints the start side can't express).
fn node_pattern_matches(
    node: &mesh_core::Node,
    pattern: &NodePattern,
    ctx: &EvalCtx,
) -> Result<bool> {
    start_node_matches(node, pattern, ctx)
}

/// Apply a binary arithmetic operator to two already-evaluated
/// operands. Null on either side short-circuits to Null
/// (three-valued logic). Numeric operands coerce via the
/// widening rule: `Int op Int → Int`, anything with a `Float`
/// widens the result to `Float`. For `+`, String operands
/// concatenate and List operands concatenate; for the other
/// operators those shapes are a type error. Division and
/// modulo with a zero RHS return [`Error::DivideByZero`] on
/// integer operands and produce `±inf` / `NaN` on float
/// operands, matching the behavior of the underlying `f64`
/// arithmetic.
pub(crate) fn eval_binary_op(op: BinaryOp, left: Value, right: Value) -> Result<Value> {
    // Null propagation: any Null operand collapses the whole
    // expression to Null, regardless of the operator.
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    if matches!(left, Value::Property(Property::Null))
        || matches!(right, Value::Property(Property::Null))
    {
        return Ok(Value::Null);
    }

    // String and list concatenation only live on `+`.
    if op == BinaryOp::Add {
        match (&left, &right) {
            (Value::Property(Property::String(a)), Value::Property(Property::String(b))) => {
                let mut out = String::with_capacity(a.len() + b.len());
                out.push_str(a);
                out.push_str(b);
                return Ok(Value::Property(Property::String(out)));
            }
            (Value::List(a), Value::List(b)) => {
                let mut out = Vec::with_capacity(a.len() + b.len());
                out.extend(a.iter().cloned());
                out.extend(b.iter().cloned());
                return Ok(Value::List(out));
            }
            _ => {}
        }
    }

    // Temporal arithmetic: DateTime/Date/Duration combinations.
    // Handled here before numeric coercion because these types
    // aren't captured by `to_number` and we don't want them
    // silently coerced to meaningless floats.
    if let Some(v) = eval_temporal_binary_op(op, &left, &right) {
        return v;
    }

    // Fall through to numeric arithmetic. Extract i64 / f64
    // from either `Value::Property(Int64/Float64)` directly,
    // erroring on anything else (Node, Edge, List, Map, etc.).
    let ln = to_number(&left)?;
    let rn = to_number(&right)?;
    let result = match (ln, rn) {
        (Num::Int(a), Num::Int(b)) => match op {
            BinaryOp::Add => Num::Int(a.wrapping_add(b)),
            BinaryOp::Sub => Num::Int(a.wrapping_sub(b)),
            BinaryOp::Mul => Num::Int(a.wrapping_mul(b)),
            BinaryOp::Div => {
                if b == 0 {
                    return Err(Error::DivideByZero);
                }
                // Cypher and Neo4j use truncated integer
                // division for int/int; `/` on f64 is floating
                // and diverges. `wrapping_div` guards against
                // `i64::MIN / -1` panicking.
                Num::Int(a.wrapping_div(b))
            }
            BinaryOp::Mod => {
                if b == 0 {
                    return Err(Error::DivideByZero);
                }
                Num::Int(a.wrapping_rem(b))
            }
        },
        (a, b) => {
            let af = a.to_f64();
            let bf = b.to_f64();
            let r = match op {
                BinaryOp::Add => af + bf,
                BinaryOp::Sub => af - bf,
                BinaryOp::Mul => af * bf,
                BinaryOp::Div => af / bf,
                BinaryOp::Mod => af % bf,
            };
            Num::Float(r)
        }
    };
    Ok(result.into_value())
}

/// Unary negation. Nulls propagate; non-numeric values are a
/// type error.
pub(crate) fn eval_unary_op(op: UnaryOp, v: Value) -> Result<Value> {
    match op {
        UnaryOp::Neg => match v {
            Value::Null => Ok(Value::Null),
            Value::Property(Property::Null) => Ok(Value::Null),
            Value::Property(Property::Int64(i)) => {
                // `i64::MIN.wrapping_neg() == i64::MIN`, which
                // preserves the type shape without panicking.
                Ok(Value::Property(Property::Int64(i.wrapping_neg())))
            }
            Value::Property(Property::Float64(f)) => Ok(Value::Property(Property::Float64(-f))),
            _ => Err(Error::TypeMismatch),
        },
    }
}

/// Internal tagged-number type used by `eval_binary_op` to
/// carry the numeric value through the coercion branches
/// without allocating an `Value` on every step.
#[derive(Debug, Clone, Copy)]
enum Num {
    Int(i64),
    Float(f64),
}

impl Num {
    fn to_f64(self) -> f64 {
        match self {
            Num::Int(i) => i as f64,
            Num::Float(f) => f,
        }
    }

    fn into_value(self) -> Value {
        match self {
            Num::Int(i) => Value::Property(Property::Int64(i)),
            Num::Float(f) => Value::Property(Property::Float64(f)),
        }
    }
}

fn to_number(v: &Value) -> Result<Num> {
    match v {
        Value::Property(Property::Int64(i)) => Ok(Num::Int(*i)),
        Value::Property(Property::Float64(f)) => Ok(Num::Float(*f)),
        _ => Err(Error::TypeMismatch),
    }
}

/// Wall-clock epoch milliseconds. Isolated in a single helper
/// so tests can eventually mock it (e.g. via a trait injected
/// into `EvalCtx`) without scattering `SystemTime::now()` calls
/// across the temporal scalar implementations.
fn now_epoch_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    // `as_millis` returns u128. Saturating cast is fine — the
    // realistic range (current year through year 292 million)
    // fits well within i64::MAX, and pre-1970 timestamps would
    // return a zero duration from the `unwrap_or_default` above.
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

/// Parse an ISO 8601 / RFC 3339 datetime string into UTC epoch
/// milliseconds. Accepts a few common forms:
///
/// - `2025-01-01T00:00:00Z` — RFC 3339 with explicit UTC
/// - `2025-01-01T00:00:00+05:00` — RFC 3339 with offset
/// - `2025-01-01T00:00:00` — ISO 8601 naive, treated as UTC
/// - `2025-01-01T00:00:00.123` — with fractional seconds
/// - `2025-01-01 00:00:00` — space instead of `T` (common relaxation)
///
/// Anything else is rejected with a clean parse error. Sub-
/// millisecond precision is truncated toward zero since our
/// on-the-wire DateTime is millis-resolution.
fn parse_datetime(s: &str) -> Result<i64> {
    use chrono::{DateTime, FixedOffset, NaiveDateTime};
    let trimmed = s.trim();
    // Try RFC 3339 first (fastest path for the canonical form
    // drivers emit). Chrono's `DateTime::parse_from_rfc3339`
    // accepts both `Z` and explicit offsets.
    if let Ok(dt) = DateTime::<FixedOffset>::parse_from_rfc3339(trimmed) {
        return Ok(dt.timestamp_millis());
    }
    // Fall back to a few naive forms. Each is interpreted as
    // UTC. Formats are tried in descending specificity so a
    // string with a sub-second component doesn't get truncated
    // by a coarser format.
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
    ] {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(trimmed, fmt) {
            return Ok(ndt.and_utc().timestamp_millis());
        }
    }
    Err(Error::UnknownScalarFunction(format!(
        "datetime() could not parse {s:?} as ISO 8601 / RFC 3339"
    )))
}

/// Parse an ISO 8601 calendar date (`YYYY-MM-DD`) into days
/// since the UNIX epoch. Any other form is rejected.
fn parse_date(s: &str) -> Result<i32> {
    use chrono::NaiveDate;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a valid calendar date");
    let parsed = NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").map_err(|_| {
        Error::UnknownScalarFunction(format!("date() could not parse {s:?} as YYYY-MM-DD"))
    })?;
    let days = parsed.signed_duration_since(epoch).num_days();
    i32::try_from(days).map_err(|_| {
        Error::UnknownScalarFunction(format!(
            "date() {s:?} is outside the representable i32 day range"
        ))
    })
}

/// Parse an ISO 8601 duration string into a
/// [`mesh_core::Duration`]. The grammar is
/// `[-]P[nY][nM][nW][nD][T[nH][nM][nS]]` where each `n` is a
/// non-negative integer and `nS` may carry a fractional part
/// (up to nanosecond precision). An optional leading `-`
/// negates every component of the parsed result.
///
/// Reuses chrono's date/time types for neither parsing nor
/// storage: chrono's `Duration` is exact-seconds-only and
/// doesn't carry months/years, which Cypher durations do. The
/// hand-rolled state machine below handles each unit in order
/// and disambiguates `M` (months in the date part, minutes in
/// the time part) via the required `T` separator.
fn parse_iso_duration(s: &str) -> Result<mesh_core::Duration> {
    let trimmed = s.trim();
    let bad = || {
        Error::UnknownScalarFunction(format!(
            "duration() could not parse {s:?} as ISO 8601 \
             (expected P[nY][nM][nW][nD][T[nH][nM][nS]])"
        ))
    };

    // Strip optional leading sign.
    let (negative, rest) = match trimmed.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, trimmed),
    };
    // Designator: must begin with `P`.
    let rest = rest.strip_prefix('P').ok_or_else(bad)?;
    // Split into date part (before `T`) and time part (after).
    let (date_part, time_part) = match rest.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (rest, None),
    };
    // A duration string has to carry at least one unit. `P`
    // alone or `PT` alone is a parse error — trying to be
    // permissive here masks driver bugs.
    if date_part.is_empty() && time_part.map(str::is_empty).unwrap_or(true) {
        return Err(bad());
    }

    let mut months = 0_i64;
    let mut days = 0_i64;
    let mut seconds = 0_i64;
    let mut nanos = 0_i32;

    // Date-part segments: Y (years), M (months), W (weeks), D
    // (days). Each is `<digits><letter>`. Order isn't enforced
    // by the grammar — the standard requires it but real-world
    // inputs are lenient, so we accept any order.
    let mut cursor = date_part;
    while !cursor.is_empty() {
        let (n, unit, rest) = consume_segment(cursor).ok_or_else(bad)?;
        // Date part can't have fractional components.
        let (whole, frac) = n;
        if frac.is_some() {
            return Err(bad());
        }
        match unit {
            'Y' => months = months.wrapping_add(whole.wrapping_mul(12)),
            'M' => months = months.wrapping_add(whole),
            'W' => days = days.wrapping_add(whole.wrapping_mul(7)),
            'D' => days = days.wrapping_add(whole),
            _ => return Err(bad()),
        }
        cursor = rest;
    }

    if let Some(mut cursor) = time_part {
        if cursor.is_empty() {
            return Err(bad());
        }
        while !cursor.is_empty() {
            let (n, unit, rest) = consume_segment(cursor).ok_or_else(bad)?;
            let (whole, frac) = n;
            match unit {
                'H' => {
                    if frac.is_some() {
                        return Err(bad());
                    }
                    seconds = seconds.wrapping_add(whole.wrapping_mul(3600));
                }
                'M' => {
                    if frac.is_some() {
                        return Err(bad());
                    }
                    seconds = seconds.wrapping_add(whole.wrapping_mul(60));
                }
                'S' => {
                    seconds = seconds.wrapping_add(whole);
                    if let Some(fr) = frac {
                        nanos = nanos.wrapping_add(fr);
                    }
                }
                _ => return Err(bad()),
            }
            cursor = rest;
        }
    }

    let mut dur = mesh_core::Duration {
        months,
        days,
        seconds,
        nanos,
    };
    if negative {
        dur = mesh_core::Duration {
            months: dur.months.wrapping_neg(),
            days: dur.days.wrapping_neg(),
            seconds: dur.seconds.wrapping_neg(),
            nanos: dur.nanos.wrapping_neg(),
        };
    }
    Ok(dur)
}

/// Consume one `<digits>[.<digits>]<unit>` segment from the
/// front of `s` and return
/// `((whole, fractional_nanos), unit, remainder)`. Returns
/// `None` on malformed input or end-of-string.
fn consume_segment(s: &str) -> Option<((i64, Option<i32>), char, &str)> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == 0 {
        return None; // no leading digits
    }
    let whole: i64 = s[..i].parse().ok()?;

    // Optional fractional part for seconds: `.` then more digits.
    let (frac_nanos, i) = if i < bytes.len() && bytes[i] == b'.' {
        let frac_start = i + 1;
        let mut j = frac_start;
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            j += 1;
        }
        if j == frac_start {
            return None; // `.` with no digits after it
        }
        // Normalize to 9 digits (nanosecond resolution). Extra
        // digits are truncated, missing digits zero-padded.
        let raw = &s[frac_start..j];
        let mut padded = String::with_capacity(9);
        padded.push_str(raw);
        if padded.len() > 9 {
            padded.truncate(9);
        } else {
            while padded.len() < 9 {
                padded.push('0');
            }
        }
        let n: i32 = padded.parse().ok()?;
        (Some(n), j)
    } else {
        (None, i)
    };

    if i >= bytes.len() {
        return None; // no unit letter
    }
    let unit = s[i..].chars().next()?;
    // Unit letter is exactly one ASCII character (Y/M/W/D/H/S)
    // so `unit.len_utf8() == 1`. Guard against multi-byte just
    // in case someone passes garbage.
    let unit_len = unit.len_utf8();
    Some(((whole, frac_nanos), unit, &s[i + unit_len..]))
}

/// Try the temporal combinations of `left op right` and return
/// `Some(result)` if a temporal rule fires, `None` otherwise (so
/// the caller falls through to plain numeric arithmetic).
///
/// Rules:
/// - `DateTime + Duration` → `DateTime` (adds months/days/seconds/nanos)
/// - `Duration + DateTime` → same, order doesn't matter for `+`
/// - `DateTime - Duration` → `DateTime` (negate the duration then add)
/// - `DateTime - DateTime` → `Duration` of exact-seconds only
///   (months/days are `0` because real calendar diff needs a
///   reference; v1 reports the exact-millisecond delta)
/// - `Date + Duration` → `Date` (months/days only; seconds ignored
///   and reported as a `TypeMismatch` if non-zero so drivers
///   don't silently drop precision)
/// - `Date - Date` → `Duration` with `days = (left - right)`
/// - `Duration + Duration` → `Duration` (component-wise add)
/// - `Duration - Duration` → `Duration` (component-wise sub)
///
/// Unsupported temporal combinations return `None` which flows
/// through to `to_number` and errors as `TypeMismatch`.
fn eval_temporal_binary_op(op: BinaryOp, left: &Value, right: &Value) -> Option<Result<Value>> {
    use Property::{Date, DateTime, Duration as Dur};
    let l = match left {
        Value::Property(p) => p,
        _ => return None,
    };
    let r = match right {
        Value::Property(p) => p,
        _ => return None,
    };
    match (op, l, r) {
        (BinaryOp::Add, DateTime(ms), Dur(d)) | (BinaryOp::Add, Dur(d), DateTime(ms)) => Some(Ok(
            Value::Property(DateTime(datetime_add_duration(*ms, *d))),
        )),
        (BinaryOp::Sub, DateTime(ms), Dur(d)) => Some(Ok(Value::Property(DateTime(
            datetime_add_duration(*ms, negate_duration(*d)),
        )))),
        (BinaryOp::Sub, DateTime(a), DateTime(b)) => {
            let diff_ms = a.wrapping_sub(*b);
            Some(Ok(Value::Property(Dur(mesh_core::Duration {
                months: 0,
                days: 0,
                seconds: diff_ms.div_euclid(1000),
                nanos: (diff_ms.rem_euclid(1000) * 1_000_000) as i32,
            }))))
        }
        (BinaryOp::Add, Date(days), Dur(d)) | (BinaryOp::Add, Dur(d), Date(days)) => {
            Some(date_add_duration(*days, *d))
        }
        (BinaryOp::Sub, Date(days), Dur(d)) => Some(date_add_duration(*days, negate_duration(*d))),
        (BinaryOp::Sub, Date(a), Date(b)) => Some(Ok(Value::Property(Dur(mesh_core::Duration {
            months: 0,
            days: (*a - *b) as i64,
            seconds: 0,
            nanos: 0,
        })))),
        (BinaryOp::Add, Dur(a), Dur(b)) => Some(Ok(Value::Property(Dur(add_durations(*a, *b))))),
        (BinaryOp::Sub, Dur(a), Dur(b)) => Some(Ok(Value::Property(Dur(add_durations(
            *a,
            negate_duration(*b),
        ))))),
        _ => None,
    }
}

/// Add a [`Duration`] to an epoch-millisecond `DateTime`. Month
/// and day components are approximated because real calendar
/// arithmetic needs timezone + leap-year state that v1 doesn't
/// track: a month is 30 days, a day is 86_400 seconds. Callers
/// who need strict calendar semantics should use the component
/// fields on `Duration` directly and apply their own math.
fn datetime_add_duration(ms: i64, d: mesh_core::Duration) -> i64 {
    let approx_day_ms: i64 = 86_400_000;
    let approx_month_ms: i64 = 30 * approx_day_ms;
    let nanos_as_ms = (d.nanos as i64) / 1_000_000;
    ms.wrapping_add(d.months.wrapping_mul(approx_month_ms))
        .wrapping_add(d.days.wrapping_mul(approx_day_ms))
        .wrapping_add(d.seconds.wrapping_mul(1_000))
        .wrapping_add(nanos_as_ms)
}

/// Add a [`Duration`] to a day-count `Date`. Only the `months`
/// and `days` components contribute — sub-day precision is lost
/// because `Date` doesn't carry it. If the duration has a
/// non-zero `seconds` or `nanos` field, we return a type error
/// so drivers can't silently drop a time-of-day component they
/// meant to apply.
fn date_add_duration(days: i32, d: mesh_core::Duration) -> Result<Value> {
    if d.seconds != 0 || d.nanos != 0 {
        return Err(Error::TypeMismatch);
    }
    let month_days: i64 = 30;
    let new_days = (days as i64)
        .wrapping_add(d.months.wrapping_mul(month_days))
        .wrapping_add(d.days);
    let clamped = i32::try_from(new_days).map_err(|_| Error::TypeMismatch)?;
    Ok(Value::Property(Property::Date(clamped)))
}

fn negate_duration(d: mesh_core::Duration) -> mesh_core::Duration {
    mesh_core::Duration {
        months: d.months.wrapping_neg(),
        days: d.days.wrapping_neg(),
        seconds: d.seconds.wrapping_neg(),
        nanos: d.nanos.wrapping_neg(),
    }
}

fn add_durations(a: mesh_core::Duration, b: mesh_core::Duration) -> mesh_core::Duration {
    mesh_core::Duration {
        months: a.months.wrapping_add(b.months),
        days: a.days.wrapping_add(b.days),
        seconds: a.seconds.wrapping_add(b.seconds),
        nanos: a.nanos.wrapping_add(b.nanos),
    }
}

/// Equality check used by the simple `CASE x WHEN v THEN ...` form. This is
/// value-level equality rather than the three-valued logic used by `=`, so a
/// WHEN branch with a NULL scrutinee or NULL value never matches.
fn case_equals(a: &Value, b: &Value) -> bool {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return false;
    }
    a == b
}

fn call_scalar(name: &str, args: &CallArgs, ctx: &EvalCtx) -> Result<Value> {
    let arg_exprs = match args {
        CallArgs::Star => return Err(Error::UnknownScalarFunction(format!("{}(*)", name))),
        CallArgs::Exprs(e) => e.as_slice(),
        CallArgs::DistinctExprs(_) => {
            return Err(Error::UnknownScalarFunction(format!(
                "{}(DISTINCT ...) is only valid for aggregates",
                name
            )))
        }
    };
    match name.to_ascii_lowercase().as_str() {
        "size" | "length" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::List(items) => Ok(Value::Property(Property::Int64(items.len() as i64))),
                Value::Property(Property::List(items)) => {
                    Ok(Value::Property(Property::Int64(items.len() as i64)))
                }
                Value::Property(Property::String(s)) => {
                    Ok(Value::Property(Property::Int64(s.chars().count() as i64)))
                }
                // `length(p)` on a Path returns the number of
                // relationships traversed, matching Cypher's
                // definition. Lists and paths converge on `size`
                // here too so `size(p)` is a synonym.
                Value::Path { edges, .. } => {
                    Ok(Value::Property(Property::Int64(edges.len() as i64)))
                }
                _ => Err(Error::TypeMismatch),
            }
        }
        "nodes" => {
            // `nodes(p)` returns the ordered list of nodes in a
            // path. Null-propagating: `nodes(null)` → null.
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Path { nodes, .. } => {
                    Ok(Value::List(nodes.into_iter().map(Value::Node).collect()))
                }
                _ => Err(Error::TypeMismatch),
            }
        }
        "relationships" => {
            // `relationships(p)` returns the ordered list of edges
            // in a path. Null-propagating.
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Path { edges, .. } => {
                    Ok(Value::List(edges.into_iter().map(Value::Edge).collect()))
                }
                _ => Err(Error::TypeMismatch),
            }
        }
        "labels" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Node(n) => Ok(Value::List(
                    n.labels
                        .into_iter()
                        .map(|l| Value::Property(Property::String(l)))
                        .collect(),
                )),
                _ => Err(Error::TypeMismatch),
            }
        }
        "keys" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Node(n) => {
                    let mut keys: Vec<String> = n.properties.keys().cloned().collect();
                    keys.sort();
                    Ok(Value::List(
                        keys.into_iter()
                            .map(|k| Value::Property(Property::String(k)))
                            .collect(),
                    ))
                }
                Value::Edge(e) => {
                    let mut keys: Vec<String> = e.properties.keys().cloned().collect();
                    keys.sort();
                    Ok(Value::List(
                        keys.into_iter()
                            .map(|k| Value::Property(Property::String(k)))
                            .collect(),
                    ))
                }
                _ => Err(Error::TypeMismatch),
            }
        }
        "type" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Edge(e) => Ok(Value::Property(Property::String(e.edge_type))),
                _ => Err(Error::TypeMismatch),
            }
        }
        "id" | "elementid" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Node(n) => Ok(Value::Property(Property::String(n.id.to_string()))),
                Value::Edge(e) => Ok(Value::Property(Property::String(e.id.to_string()))),
                _ => Err(Error::TypeMismatch),
            }
        }
        "startnode" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Edge(e) => match ctx.reader.get_node(e.source)? {
                    Some(n) => Ok(Value::Node(n)),
                    None => Ok(Value::Null),
                },
                _ => Err(Error::TypeMismatch),
            }
        }
        "endnode" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Edge(e) => match ctx.reader.get_node(e.target)? {
                    Some(n) => Ok(Value::Node(n)),
                    None => Ok(Value::Null),
                },
                _ => Err(Error::TypeMismatch),
            }
        }
        "properties" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Node(n) => Ok(Value::Property(Property::Map(n.properties))),
                Value::Edge(e) => Ok(Value::Property(Property::Map(e.properties))),
                _ => Err(Error::TypeMismatch),
            }
        }
        "exists" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            let is_present = !matches!(v, Value::Null | Value::Property(Property::Null));
            Ok(Value::Property(Property::Bool(is_present)))
        }
        "tolower" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Property(Property::String(s)) => {
                    Ok(Value::Property(Property::String(s.to_lowercase())))
                }
                _ => Err(Error::TypeMismatch),
            }
        }
        "toupper" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Property(Property::String(s)) => {
                    Ok(Value::Property(Property::String(s.to_uppercase())))
                }
                _ => Err(Error::TypeMismatch),
            }
        }
        "tostring" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            Ok(value_to_string(v))
        }
        "tointeger" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Property(Property::Int64(i)) => Ok(Value::Property(Property::Int64(i))),
                Value::Property(Property::Float64(f)) => {
                    Ok(Value::Property(Property::Int64(f as i64)))
                }
                Value::Property(Property::String(s)) => match s.trim().parse::<i64>() {
                    Ok(n) => Ok(Value::Property(Property::Int64(n))),
                    Err(_) => Ok(Value::Null),
                },
                Value::Property(Property::Bool(b)) => {
                    Ok(Value::Property(Property::Int64(if b { 1 } else { 0 })))
                }
                _ => Err(Error::TypeMismatch),
            }
        }
        "coalesce" => {
            if arg_exprs.is_empty() {
                return Err(Error::UnknownScalarFunction(
                    "coalesce requires at least one argument".into(),
                ));
            }
            for e in arg_exprs {
                let v = eval_expr(e, ctx)?;
                let is_null = matches!(v, Value::Null | Value::Property(Property::Null));
                if !is_null {
                    return Ok(v);
                }
            }
            Ok(Value::Null)
        }
        "substring" => {
            // substring(str, start [, length]) — 0-indexed,
            // clamp at string length, negative start → 0.
            // Missing length → suffix from start.
            if arg_exprs.len() != 2 && arg_exprs.len() != 3 {
                return Err(Error::UnknownScalarFunction(
                    "substring expects 2 or 3 arguments".into(),
                ));
            }
            let sv = eval_expr(&arg_exprs[0], ctx)?;
            let start_v = eval_expr(&arg_exprs[1], ctx)?;
            if matches!(sv, Value::Null) || matches!(start_v, Value::Null) {
                return Ok(Value::Null);
            }
            let s = match sv {
                Value::Property(Property::String(s)) => s,
                _ => return Err(Error::TypeMismatch),
            };
            let start = match start_v {
                Value::Property(Property::Int64(i)) => i.max(0) as usize,
                _ => return Err(Error::TypeMismatch),
            };
            // Work on Unicode chars, not bytes, so non-ASCII
            // inputs don't produce split-byte garbage.
            let chars: Vec<char> = s.chars().collect();
            let start = start.min(chars.len());
            let end = if arg_exprs.len() == 3 {
                let len_v = eval_expr(&arg_exprs[2], ctx)?;
                if matches!(len_v, Value::Null) {
                    return Ok(Value::Null);
                }
                let len = match len_v {
                    Value::Property(Property::Int64(i)) => i.max(0) as usize,
                    _ => return Err(Error::TypeMismatch),
                };
                (start + len).min(chars.len())
            } else {
                chars.len()
            };
            let result: String = chars[start..end].iter().collect();
            Ok(Value::Property(Property::String(result)))
        }
        "trim" | "ltrim" | "rtrim" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Property(Property::String(s)) => {
                    let trimmed = match name.to_ascii_lowercase().as_str() {
                        "trim" => s.trim().to_string(),
                        "ltrim" => s.trim_start().to_string(),
                        "rtrim" => s.trim_end().to_string(),
                        _ => unreachable!(),
                    };
                    Ok(Value::Property(Property::String(trimmed)))
                }
                _ => Err(Error::TypeMismatch),
            }
        }
        "replace" => {
            if arg_exprs.len() != 3 {
                return Err(Error::UnknownScalarFunction(
                    "replace expects 3 arguments (str, search, replacement)".into(),
                ));
            }
            let sv = eval_expr(&arg_exprs[0], ctx)?;
            let fv = eval_expr(&arg_exprs[1], ctx)?;
            let tv = eval_expr(&arg_exprs[2], ctx)?;
            if matches!(sv, Value::Null) || matches!(fv, Value::Null) || matches!(tv, Value::Null) {
                return Ok(Value::Null);
            }
            let (s, f, t) = match (sv, fv, tv) {
                (
                    Value::Property(Property::String(s)),
                    Value::Property(Property::String(f)),
                    Value::Property(Property::String(t)),
                ) => (s, f, t),
                _ => return Err(Error::TypeMismatch),
            };
            Ok(Value::Property(Property::String(s.replace(&f, &t))))
        }
        "split" => {
            // split(str, delim) → list of strings. Matches
            // openCypher: empty delimiter returns individual
            // characters.
            if arg_exprs.len() != 2 {
                return Err(Error::UnknownScalarFunction(
                    "split expects 2 arguments (str, delimiter)".into(),
                ));
            }
            let sv = eval_expr(&arg_exprs[0], ctx)?;
            let dv = eval_expr(&arg_exprs[1], ctx)?;
            if matches!(sv, Value::Null) || matches!(dv, Value::Null) {
                return Ok(Value::Null);
            }
            let (s, d) = match (sv, dv) {
                (Value::Property(Property::String(s)), Value::Property(Property::String(d))) => {
                    (s, d)
                }
                _ => return Err(Error::TypeMismatch),
            };
            let items: Vec<Value> = if d.is_empty() {
                s.chars()
                    .map(|c| Value::Property(Property::String(c.to_string())))
                    .collect()
            } else {
                s.split(&d)
                    .map(|p| Value::Property(Property::String(p.to_string())))
                    .collect()
            };
            Ok(Value::List(items))
        }
        "tofloat" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Property(Property::Float64(f)) => Ok(Value::Property(Property::Float64(f))),
                Value::Property(Property::Int64(i)) => {
                    Ok(Value::Property(Property::Float64(i as f64)))
                }
                Value::Property(Property::String(s)) => match s.trim().parse::<f64>() {
                    Ok(f) => Ok(Value::Property(Property::Float64(f))),
                    Err(_) => Ok(Value::Null),
                },
                _ => Err(Error::TypeMismatch),
            }
        }
        "toboolean" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Property(Property::Bool(b)) => Ok(Value::Property(Property::Bool(b))),
                Value::Property(Property::String(s)) => {
                    match s.trim().to_ascii_lowercase().as_str() {
                        "true" => Ok(Value::Property(Property::Bool(true))),
                        "false" => Ok(Value::Property(Property::Bool(false))),
                        _ => Ok(Value::Null),
                    }
                }
                _ => Err(Error::TypeMismatch),
            }
        }

        // --------------------------------------------------------
        // List scalar functions
        // --------------------------------------------------------
        "range" => {
            // `range(start, end)` → [start, start+1, …, end]
            // `range(start, end, step)` → same with custom step.
            // Both endpoints are **inclusive**, matching
            // openCypher. Step defaults to 1. A zero step is
            // rejected (would loop forever); a negative step is
            // allowed for descending ranges.
            if arg_exprs.len() != 2 && arg_exprs.len() != 3 {
                return Err(Error::UnknownScalarFunction(
                    "range expects 2 or 3 arguments".into(),
                ));
            }
            let sv = eval_expr(&arg_exprs[0], ctx)?;
            let ev = eval_expr(&arg_exprs[1], ctx)?;
            let step_v = if arg_exprs.len() == 3 {
                Some(eval_expr(&arg_exprs[2], ctx)?)
            } else {
                None
            };
            if matches!(sv, Value::Null)
                || matches!(ev, Value::Null)
                || matches!(step_v, Some(Value::Null))
            {
                return Ok(Value::Null);
            }
            let start = match sv {
                Value::Property(Property::Int64(i)) => i,
                _ => return Err(Error::TypeMismatch),
            };
            let end = match ev {
                Value::Property(Property::Int64(i)) => i,
                _ => return Err(Error::TypeMismatch),
            };
            let step = match step_v {
                Some(Value::Property(Property::Int64(i))) => i,
                None => 1,
                _ => return Err(Error::TypeMismatch),
            };
            if step == 0 {
                return Err(Error::UnknownScalarFunction(
                    "range step must not be zero".into(),
                ));
            }
            let mut out: Vec<Value> = Vec::new();
            let mut cur = start;
            if step > 0 {
                while cur <= end {
                    out.push(Value::Property(Property::Int64(cur)));
                    cur += step;
                }
            } else {
                while cur >= end {
                    out.push(Value::Property(Property::Int64(cur)));
                    cur += step;
                }
            }
            Ok(Value::List(out))
        }
        "head" => {
            // First element of a list, or Null on empty / Null.
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::List(items) => Ok(items.into_iter().next().unwrap_or(Value::Null)),
                Value::Property(Property::List(items)) => Ok(items
                    .into_iter()
                    .next()
                    .map(Value::Property)
                    .unwrap_or(Value::Null)),
                _ => Err(Error::TypeMismatch),
            }
        }
        "last" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::List(items) => Ok(items.into_iter().last().unwrap_or(Value::Null)),
                Value::Property(Property::List(items)) => Ok(items
                    .into_iter()
                    .last()
                    .map(Value::Property)
                    .unwrap_or(Value::Null)),
                _ => Err(Error::TypeMismatch),
            }
        }
        "tail" => {
            // Everything except the first element. Empty list
            // or single-element list → empty list.
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::List(mut items) => {
                    if items.is_empty() {
                        Ok(Value::List(Vec::new()))
                    } else {
                        items.remove(0);
                        Ok(Value::List(items))
                    }
                }
                Value::Property(Property::List(mut items)) => {
                    if items.is_empty() {
                        Ok(Value::List(Vec::new()))
                    } else {
                        items.remove(0);
                        Ok(Value::List(
                            items.into_iter().map(Value::Property).collect(),
                        ))
                    }
                }
                _ => Err(Error::TypeMismatch),
            }
        }
        "reverse" => {
            // Works on lists *and* strings. Length functions
            // in openCypher similarly overload on both; the
            // string form is pretty common.
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::List(items) => Ok(Value::List(items.into_iter().rev().collect())),
                Value::Property(Property::List(items)) => Ok(Value::List(
                    items.into_iter().rev().map(Value::Property).collect(),
                )),
                Value::Property(Property::String(s)) => {
                    Ok(Value::Property(Property::String(s.chars().rev().collect())))
                }
                _ => Err(Error::TypeMismatch),
            }
        }

        // --------------------------------------------------------
        // Math scalar functions
        // --------------------------------------------------------
        "abs" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Property(Property::Int64(i)) => {
                    // Use saturating_abs so `abs(i64::MIN)`
                    // doesn't panic in debug builds.
                    Ok(Value::Property(Property::Int64(i.saturating_abs())))
                }
                Value::Property(Property::Float64(f)) => {
                    Ok(Value::Property(Property::Float64(f.abs())))
                }
                _ => Err(Error::TypeMismatch),
            }
        }
        "ceil" => math_unary(name, arg_exprs, ctx, |f| f.ceil()),
        "floor" => math_unary(name, arg_exprs, ctx, |f| f.floor()),
        "round" => math_unary(name, arg_exprs, ctx, |f| f.round()),
        "sqrt" => math_unary(name, arg_exprs, ctx, |f| f.sqrt()),
        "sign" => {
            // Returns -1, 0, or 1 as Int64 regardless of
            // whether the input was int or float. NaN maps to
            // 0 to stay total; callers who care can test for
            // NaN via `n <> n` first.
            let v = single_arg(name, arg_exprs, ctx)?;
            let s: i64 = match v {
                Value::Null => return Ok(Value::Null),
                Value::Property(Property::Int64(i)) => i.signum(),
                Value::Property(Property::Float64(f)) => {
                    if f > 0.0 {
                        1
                    } else if f < 0.0 {
                        -1
                    } else {
                        0
                    }
                }
                _ => return Err(Error::TypeMismatch),
            };
            Ok(Value::Property(Property::Int64(s)))
        }
        "pi" => {
            if !arg_exprs.is_empty() {
                return Err(Error::UnknownScalarFunction(
                    "pi() takes no arguments".into(),
                ));
            }
            Ok(Value::Property(Property::Float64(std::f64::consts::PI)))
        }

        // --- Temporal constructors ---
        //
        // `datetime()`, `date()`, and `timestamp()` with no args
        // return the current wall-clock time. `datetime()` → epoch
        // millis wrapped as Property::DateTime; `date()` → today
        // as Property::Date (days since 1970-01-01 UTC);
        // `timestamp()` → epoch millis as a plain Int64 (matches
        // Neo4j's `timestamp()` signature, which returns an
        // integer, not a DateTime).
        //
        // None of these accept arguments in v1 — string parsing
        // and timezone conversions are deferred. Drivers that
        // need to build a specific DateTime from components should
        // send it as a Bolt parameter, which goes through the
        // wire-format path (Bolt LocalDateTime struct).
        "datetime" => {
            // Zero args → current UTC epoch millis.
            // One string arg → parse ISO 8601 / RFC 3339 and convert
            // to UTC epoch millis. Timezone offsets are respected
            // during conversion; naive strings (no `Z` or offset)
            // are interpreted as UTC, matching the rest of the
            // temporal surface's UTC-only v1.
            match arg_exprs.len() {
                0 => Ok(Value::Property(Property::DateTime(now_epoch_millis()))),
                1 => {
                    let v = eval_expr(&arg_exprs[0], ctx)?;
                    match v {
                        Value::Null | Value::Property(Property::Null) => Ok(Value::Null),
                        Value::Property(Property::String(s)) => {
                            Ok(Value::Property(Property::DateTime(parse_datetime(&s)?)))
                        }
                        _ => Err(Error::TypeMismatch),
                    }
                }
                _ => Err(Error::UnknownScalarFunction(
                    "datetime() takes zero or one argument".into(),
                )),
            }
        }
        "date" => {
            // Zero args → today (UTC). One string arg → parse
            // `YYYY-MM-DD` into days-since-epoch.
            match arg_exprs.len() {
                0 => {
                    let days = now_epoch_millis().div_euclid(86_400_000);
                    let clamped = i32::try_from(days).map_err(|_| {
                        Error::UnknownScalarFunction(format!("date() overflowed i32 days: {days}"))
                    })?;
                    Ok(Value::Property(Property::Date(clamped)))
                }
                1 => {
                    let v = eval_expr(&arg_exprs[0], ctx)?;
                    match v {
                        Value::Null | Value::Property(Property::Null) => Ok(Value::Null),
                        Value::Property(Property::String(s)) => {
                            Ok(Value::Property(Property::Date(parse_date(&s)?)))
                        }
                        _ => Err(Error::TypeMismatch),
                    }
                }
                _ => Err(Error::UnknownScalarFunction(
                    "date() takes zero or one argument".into(),
                )),
            }
        }
        "timestamp" => {
            if !arg_exprs.is_empty() {
                return Err(Error::UnknownScalarFunction(
                    "timestamp() takes no arguments".into(),
                ));
            }
            Ok(Value::Property(Property::Int64(now_epoch_millis())))
        }
        "duration" => {
            // `duration({months: 1, days: 2, seconds: 30, nanos: 0})`
            // or `duration("PT1H30M")`. Takes a single argument —
            // either a map (component form) or a string (ISO 8601).
            // Unknown map keys are rejected; malformed ISO strings
            // surface a parse error naming the expected shape.
            if arg_exprs.len() != 1 {
                return Err(Error::UnknownScalarFunction(
                    "duration() expects a single argument (map or ISO 8601 string)".into(),
                ));
            }
            let arg = eval_expr(&arg_exprs[0], ctx)?;
            let entries = match arg {
                Value::Property(Property::Map(m)) => m,
                Value::Property(Property::String(s)) => {
                    return Ok(Value::Property(Property::Duration(parse_iso_duration(&s)?)));
                }
                Value::Null | Value::Property(Property::Null) => return Ok(Value::Null),
                _ => return Err(Error::TypeMismatch),
            };
            let mut months = 0_i64;
            let mut days = 0_i64;
            let mut seconds = 0_i64;
            let mut nanos = 0_i32;
            for (k, v) in &entries {
                let as_i64 = match v {
                    Property::Int64(i) => *i,
                    Property::Null => continue,
                    _ => return Err(Error::TypeMismatch),
                };
                match k.as_str() {
                    "years" => {
                        months = months.wrapping_add(as_i64.wrapping_mul(12));
                    }
                    "months" => {
                        months = months.wrapping_add(as_i64);
                    }
                    "weeks" => {
                        days = days.wrapping_add(as_i64.wrapping_mul(7));
                    }
                    "days" => {
                        days = days.wrapping_add(as_i64);
                    }
                    "hours" => {
                        seconds = seconds.wrapping_add(as_i64.wrapping_mul(3600));
                    }
                    "minutes" => {
                        seconds = seconds.wrapping_add(as_i64.wrapping_mul(60));
                    }
                    "seconds" => {
                        seconds = seconds.wrapping_add(as_i64);
                    }
                    "milliseconds" => {
                        seconds = seconds.wrapping_add(as_i64 / 1000);
                        nanos = nanos.wrapping_add(((as_i64 % 1000) as i32) * 1_000_000);
                    }
                    "microseconds" => {
                        nanos = nanos.wrapping_add((as_i64 as i32).wrapping_mul(1000));
                    }
                    "nanoseconds" => {
                        nanos = nanos.wrapping_add(as_i64 as i32);
                    }
                    other => {
                        return Err(Error::UnknownScalarFunction(format!(
                            "duration() does not recognise component `{other}`"
                        )))
                    }
                }
            }
            Ok(Value::Property(Property::Duration(mesh_core::Duration {
                months,
                days,
                seconds,
                nanos,
            })))
        }

        _ => Err(Error::UnknownScalarFunction(name.to_string())),
    }
}

/// Shared single-argument math helper: evaluate the one
/// argument, null-propagate on Null, accept Int64 (implicit
/// cast to f64) or Float64, apply `f`, return a Float64. Used
/// by `ceil` / `floor` / `round` / `sqrt`.
fn math_unary(
    name: &str,
    arg_exprs: &[Expr],
    ctx: &EvalCtx,
    f: impl FnOnce(f64) -> f64,
) -> Result<Value> {
    let v = single_arg(name, arg_exprs, ctx)?;
    match v {
        Value::Null => Ok(Value::Null),
        Value::Property(Property::Int64(i)) => Ok(Value::Property(Property::Float64(f(i as f64)))),
        Value::Property(Property::Float64(x)) => Ok(Value::Property(Property::Float64(f(x)))),
        _ => Err(Error::TypeMismatch),
    }
}

fn single_arg(name: &str, args: &[Expr], ctx: &EvalCtx) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::UnknownScalarFunction(format!(
            "{} expects 1 argument, got {}",
            name,
            args.len()
        )));
    }
    eval_expr(&args[0], ctx)
}

fn value_to_string(v: Value) -> Value {
    match v {
        Value::Null => Value::Null,
        Value::Property(Property::String(s)) => Value::Property(Property::String(s)),
        Value::Property(Property::Int64(i)) => Value::Property(Property::String(i.to_string())),
        Value::Property(Property::Float64(f)) => Value::Property(Property::String(f.to_string())),
        Value::Property(Property::Bool(b)) => Value::Property(Property::String(b.to_string())),
        Value::Property(Property::Null) => Value::Null,
        other => Value::Property(Property::String(format!("{:?}", other))),
    }
}

pub(crate) fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Property(ap), Value::Property(bp)) => compare_props(ap, bp),
        _ => Ordering::Equal,
    }
}

fn compare_props(a: &Property, b: &Property) -> Ordering {
    match (a, b) {
        (Property::Int64(a), Property::Int64(b)) => a.cmp(b),
        (Property::String(a), Property::String(b)) => a.cmp(b),
        (Property::Bool(a), Property::Bool(b)) => a.cmp(b),
        (Property::Float64(a), Property::Float64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Property::Int64(a), Property::Float64(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (Property::Float64(a), Property::Int64(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }
        // Temporal ordering: DateTime by epoch millis, Date by
        // day count. Duration has no total order in general (3
        // months vs 90 days depends on the reference date) so it
        // just compares equal — queries that need a deterministic
        // order over durations should project a specific field.
        (Property::DateTime(a), Property::DateTime(b)) => a.cmp(b),
        (Property::Date(a), Property::Date(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

pub(crate) fn value_key(v: &Value) -> String {
    match v {
        Value::Null => "~null".to_string(),
        Value::Property(Property::Null) => "~null".to_string(),
        Value::Property(Property::Bool(b)) => format!("b:{}", b),
        Value::Property(Property::Int64(i)) => format!("i:{}", i),
        Value::Property(Property::Float64(f)) => format!("f:{}", f.to_bits()),
        Value::Property(Property::String(s)) => format!("s:{}", s),
        Value::Property(Property::List(items)) => {
            let mut out = String::from("pl:[");
            for it in items {
                out.push_str(&prop_key(it));
                out.push(',');
            }
            out.push(']');
            out
        }
        Value::Property(Property::Map(m)) => {
            let mut keys: Vec<_> = m.keys().collect();
            keys.sort();
            let mut out = String::from("pm:{");
            for k in keys {
                out.push_str(k);
                out.push('=');
                out.push_str(&prop_key(&m[k]));
                out.push(',');
            }
            out.push('}');
            out
        }
        Value::Property(Property::DateTime(ms)) => format!("dt:{}", ms),
        Value::Property(Property::Date(days)) => format!("d:{}", days),
        Value::Property(Property::Duration(d)) => {
            format!("dur:{},{},{},{}", d.months, d.days, d.seconds, d.nanos)
        }
        Value::Node(n) => format!("N:{}", n.id),
        Value::Edge(e) => format!("E:{}", e.id),
        Value::Path { nodes, edges } => {
            // Stable key: "P:<start>;<e0>,<n1>;<e1>,<n2>;...". Enough
            // to distinguish paths for DISTINCT/UNION dedup even
            // when two paths visit the same set of nodes in
            // different orders.
            let mut out = String::from("P:");
            if let Some(first) = nodes.first() {
                out.push_str(&first.id.to_string());
            }
            for (i, e) in edges.iter().enumerate() {
                out.push(';');
                out.push_str(&e.id.to_string());
                out.push(',');
                if let Some(n) = nodes.get(i + 1) {
                    out.push_str(&n.id.to_string());
                }
            }
            out
        }
        Value::List(items) => {
            let mut out = String::from("L:[");
            for it in items {
                out.push_str(&value_key(it));
                out.push(',');
            }
            out.push(']');
            out
        }
    }
}

fn prop_key(p: &Property) -> String {
    value_key(&Value::Property(p.clone()))
}

pub(crate) fn row_key(row: &Row) -> String {
    let mut keys: Vec<_> = row.keys().collect();
    keys.sort();
    let mut out = String::new();
    for k in keys {
        out.push_str(k);
        out.push('=');
        out.push_str(&value_key(&row[k]));
        out.push(';');
    }
    out
}

fn literal_to_value(lit: &Literal) -> Value {
    match lit {
        Literal::String(s) => Value::Property(Property::String(s.clone())),
        Literal::Integer(i) => Value::Property(Property::Int64(*i)),
        Literal::Float(f) => Value::Property(Property::Float64(*f)),
        Literal::Boolean(b) => Value::Property(Property::Bool(*b)),
        Literal::Null => Value::Null,
    }
}

pub(crate) fn to_bool(v: &Value) -> Result<bool> {
    match v {
        Value::Property(Property::Bool(b)) => Ok(*b),
        Value::Null => Ok(false),
        _ => Err(Error::NotBoolean),
    }
}

pub(crate) fn values_equal(a: &Value, b: &Value) -> bool {
    compare(CompareOp::Eq, a, b).unwrap_or(false)
}

fn compare(op: CompareOp, l: &Value, r: &Value) -> Result<bool> {
    if matches!(l, Value::Null) || matches!(r, Value::Null) {
        return Ok(false);
    }
    // String-predicate operators dispatch separately from
    // ordering comparisons because they're only defined on
    // strings and don't need a cross-type Ordering coercion.
    match op {
        CompareOp::StartsWith | CompareOp::EndsWith | CompareOp::Contains => {
            let (ls, rs) = match (l, r) {
                (Value::Property(Property::String(a)), Value::Property(Property::String(b))) => {
                    (a.as_str(), b.as_str())
                }
                _ => return Err(Error::TypeMismatch),
            };
            return Ok(match op {
                CompareOp::StartsWith => ls.starts_with(rs),
                CompareOp::EndsWith => ls.ends_with(rs),
                CompareOp::Contains => ls.contains(rs),
                _ => unreachable!(),
            });
        }
        CompareOp::RegexMatch => {
            let (ls, rs) = match (l, r) {
                (Value::Property(Property::String(a)), Value::Property(Property::String(b))) => {
                    (a, b)
                }
                _ => return Err(Error::TypeMismatch),
            };
            // Cypher's =~ is a full-match operator (the entire
            // string must match), so anchor the pattern.
            let anchored = format!("^(?:{})$", rs);
            let re =
                regex::Regex::new(&anchored).map_err(|_| Error::InvalidRegex(rs.to_string()))?;
            return Ok(re.is_match(ls));
        }
        _ => {}
    }
    let (lp, rp) = match (l, r) {
        (Value::Property(lp), Value::Property(rp)) => (lp, rp),
        _ => return Err(Error::TypeMismatch),
    };
    let ord = match (lp, rp) {
        (Property::Int64(a), Property::Int64(b)) => a.cmp(b),
        (Property::String(a), Property::String(b)) => a.cmp(b),
        (Property::Float64(a), Property::Float64(b)) => {
            a.partial_cmp(b).ok_or(Error::TypeMismatch)?
        }
        (Property::Int64(a), Property::Float64(b)) => {
            (*a as f64).partial_cmp(b).ok_or(Error::TypeMismatch)?
        }
        (Property::Float64(a), Property::Int64(b)) => {
            a.partial_cmp(&(*b as f64)).ok_or(Error::TypeMismatch)?
        }
        (Property::Bool(a), Property::Bool(b)) => {
            return match op {
                CompareOp::Eq => Ok(a == b),
                CompareOp::Ne => Ok(a != b),
                _ => Err(Error::UnsupportedComparison),
            };
        }
        // Temporal comparisons — DateTime and Date carry total
        // orderings by their epoch-offset components. Duration
        // only supports equality because component ordering
        // isn't well-defined (months-vs-days).
        (Property::DateTime(a), Property::DateTime(b)) => a.cmp(b),
        (Property::Date(a), Property::Date(b)) => a.cmp(b),
        (Property::Duration(a), Property::Duration(b)) => {
            return match op {
                CompareOp::Eq => Ok(a == b),
                CompareOp::Ne => Ok(a != b),
                _ => Err(Error::UnsupportedComparison),
            };
        }
        _ => return Err(Error::TypeMismatch),
    };
    Ok(match op {
        CompareOp::Eq => ord == Ordering::Equal,
        CompareOp::Ne => ord != Ordering::Equal,
        CompareOp::Lt => ord == Ordering::Less,
        CompareOp::Le => ord != Ordering::Greater,
        CompareOp::Gt => ord == Ordering::Greater,
        CompareOp::Ge => ord != Ordering::Less,
        // String-predicate ops handled in the early-return
        // branch above.
        CompareOp::StartsWith
        | CompareOp::EndsWith
        | CompareOp::Contains
        | CompareOp::RegexMatch => unreachable!(),
    })
}
