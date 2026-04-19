use crate::{
    error::{Error, Result},
    procedures::ProcedureRegistry,
    reader::GraphReader,
    value::{ParamMap, Row, Value},
};
use chrono::{Datelike, Timelike};
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
    pub procedures: &'a ProcedureRegistry,
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
            procedures: self.procedures,
        }
    }
}

/// Recursive check for whether a value contains a graph element
/// (Node / Edge / Path / a Value::Map / a List thereof). Used by
/// Expr::Map eval to decide between `Value::Map` (graph-aware)
/// and `Property::Map` (scalar-only) wrappers.
fn value_contains_graph_element(v: &Value) -> bool {
    match v {
        Value::Node(_) | Value::Edge(_) | Value::Path { .. } | Value::Map(_) => true,
        Value::List(items) => items.iter().any(value_contains_graph_element),
        _ => false,
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
                Value::Map(m) => Ok(m.get(key).cloned().unwrap_or(Value::Null)),
                // Temporal property access for Expr::Property
                Value::Property(Property::Date(days)) => {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let d = epoch + chrono::Duration::days(*days as i64);
                    temporal_date_prop(&d, key)
                }
                Value::Property(Property::DateTime {
                    nanos,
                    tz_offset_secs,
                    tz_name,
                }) => Ok(datetime_accessor(
                    *nanos,
                    *tz_offset_secs,
                    tz_name.as_deref(),
                    key,
                )),
                Value::Property(Property::LocalDateTime(ns)) => {
                    Ok(datetime_accessor(*ns, None, None, key))
                }
                Value::Property(Property::Time {
                    nanos,
                    tz_offset_secs,
                }) => Ok(time_accessor(*nanos, *tz_offset_secs, key)),
                Value::Property(Property::Duration(ref dur)) => {
                    Ok(duration_accessor(dur, key))
                }
                // Accessing a property on something that isn't a
                // container (map, node, edge, temporal) is a type
                // error — openCypher raises `InvalidArgumentType`
                // rather than silently returning null, so an
                // accidental `123.num` is caught instead of
                // producing phantom nulls.
                _ => Err(Error::TypeMismatch),
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
                Value::Map(m) => Ok(m.get(key).cloned().unwrap_or(Value::Null)),
                // Temporal property access
                Value::Property(Property::Date(days)) => {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let d = epoch + chrono::Duration::days(days as i64);
                    temporal_date_prop(&d, key)
                }
                Value::Property(Property::DateTime {
                    nanos,
                    tz_offset_secs,
                    tz_name,
                }) => Ok(datetime_accessor(
                    nanos,
                    tz_offset_secs,
                    tz_name.as_deref(),
                    key,
                )),
                Value::Property(Property::LocalDateTime(ns)) => {
                    Ok(datetime_accessor(ns, None, None, key))
                }
                Value::Property(Property::Time {
                    nanos,
                    tz_offset_secs,
                }) => Ok(time_accessor(nanos, tz_offset_secs, key)),
                Value::Property(Property::Duration(ref dur)) => {
                    Ok(duration_accessor(dur, key))
                }
                // Same rule as `Expr::Property` above — `.key` on a
                // non-container is a type error, not null.
                _ => Err(Error::TypeMismatch),
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
            // String index on node/edge/map = dynamic property access
            if let Value::Property(Property::String(key)) = &idx_val {
                return match base_val {
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
                    Value::Property(Property::Map(m)) => Ok(m
                        .get(key)
                        .cloned()
                        .map(Value::Property)
                        .unwrap_or(Value::Null)),
                    Value::Map(m) => Ok(m.get(key).cloned().unwrap_or(Value::Null)),
                    // String indexing is only valid on map-like
                    // values; indexing a list / scalar with a
                    // string is `InvalidArgumentType`.
                    _ => Err(Error::TypeMismatch),
                };
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
            let resolve = |expr: &Expr| -> Result<Option<i64>> {
                match eval_expr(expr, ctx)? {
                    Value::Property(Property::Int64(i)) => Ok(Some(i)),
                    Value::Null | Value::Property(Property::Null) => Ok(None),
                    _ => Err(Error::TypeMismatch),
                }
            };
            let s = match start {
                Some(e) => match resolve(e)? {
                    Some(raw) => {
                        let abs = if raw < 0 {
                            (len + raw).max(0)
                        } else {
                            raw.min(len)
                        };
                        abs as usize
                    }
                    None => return Ok(Value::Null),
                },
                None => 0,
            };
            let e = match end {
                Some(e) => match resolve(e)? {
                    Some(raw) => {
                        let abs = if raw < 0 {
                            (len + raw).max(0)
                        } else {
                            raw.min(len)
                        };
                        abs as usize
                    }
                    None => return Ok(Value::Null),
                },
                None => len as usize,
            };
            if s >= e {
                return Ok(Value::List(Vec::new()));
            }
            Ok(Value::List(items[s..e].to_vec()))
        }
        Expr::Not(inner) => {
            let v = eval_expr(inner, ctx)?;
            match to_bool_3v(&v)? {
                Some(b) => Ok(Value::Property(Property::Bool(!b))),
                None => Ok(Value::Null),
            }
        }
        Expr::And(a, b) => {
            // Three-valued AND: evaluate both sides (type-check each
            // before applying the short-circuit) so that `false AND 123`
            // still raises rather than quietly returning false.
            let va = to_bool_3v(&eval_expr(a, ctx)?)?;
            let vb = to_bool_3v(&eval_expr(b, ctx)?)?;
            if va == Some(false) || vb == Some(false) {
                return Ok(Value::Property(Property::Bool(false)));
            }
            match (va, vb) {
                (Some(true), Some(true)) => Ok(Value::Property(Property::Bool(true))),
                _ => Ok(Value::Null),
            }
        }
        Expr::Or(a, b) => {
            // Three-valued OR: same full-evaluation rule as AND so that
            // `true OR 123` raises InvalidArgumentType instead of
            // short-circuiting to true.
            let va = to_bool_3v(&eval_expr(a, ctx)?)?;
            let vb = to_bool_3v(&eval_expr(b, ctx)?)?;
            if va == Some(true) || vb == Some(true) {
                return Ok(Value::Property(Property::Bool(true)));
            }
            match (va, vb) {
                (Some(false), Some(false)) => Ok(Value::Property(Property::Bool(false))),
                _ => Ok(Value::Null),
            }
        }
        Expr::Xor(a, b) => {
            // Three-valued XOR: null XOR anything = null
            let va = to_bool_3v(&eval_expr(a, ctx)?)?;
            let vb = to_bool_3v(&eval_expr(b, ctx)?)?;
            match (va, vb) {
                (Some(x), Some(y)) => Ok(Value::Property(Property::Bool(x ^ y))),
                _ => Ok(Value::Null),
            }
        }
        Expr::ListPredicate {
            kind,
            var,
            list,
            predicate,
        } => {
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
            let mut true_count = 0usize;
            let mut has_null = false;
            for item in &items {
                let mut scratch = ctx.row.clone();
                scratch.insert(var.clone(), item.clone());
                let sub_ctx = ctx.with_row(&scratch);
                let v = eval_expr(predicate, &sub_ctx)?;
                match to_bool_3v(&v).unwrap_or(None) {
                    Some(true) => {
                        true_count += 1;
                        if *kind == mesh_cypher::ListPredicateKind::Any {
                            return Ok(Value::Property(Property::Bool(true)));
                        }
                    }
                    Some(false) => {
                        if *kind == mesh_cypher::ListPredicateKind::All {
                            return Ok(Value::Property(Property::Bool(false)));
                        }
                    }
                    None => has_null = true,
                }
            }
            // Three-valued result: if null was encountered and no
            // definitive answer was reached, return null.
            match kind {
                mesh_cypher::ListPredicateKind::Any => {
                    if true_count > 0 {
                        Ok(Value::Property(Property::Bool(true)))
                    } else if has_null {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Property(Property::Bool(false)))
                    }
                }
                mesh_cypher::ListPredicateKind::All => {
                    if true_count == items.len() {
                        Ok(Value::Property(Property::Bool(true)))
                    } else if has_null {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Property(Property::Bool(false)))
                    }
                }
                mesh_cypher::ListPredicateKind::None => {
                    if true_count > 0 {
                        Ok(Value::Property(Property::Bool(false)))
                    } else if has_null {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Property(Property::Bool(true)))
                    }
                }
                mesh_cypher::ListPredicateKind::Single => {
                    if true_count == 1 && !has_null {
                        Ok(Value::Property(Property::Bool(true)))
                    } else if true_count > 1 {
                        Ok(Value::Property(Property::Bool(false)))
                    } else if has_null {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Property(Property::Bool(true_count == 1)))
                    }
                }
            }
        }
        Expr::Compare { op, left, right } => {
            let vl = eval_expr(left, ctx)?;
            let vr = eval_expr(right, ctx)?;
            // Null propagation: any comparison with null returns null
            if matches!(vl, Value::Null | Value::Property(Property::Null))
                || matches!(vr, Value::Null | Value::Property(Property::Null))
            {
                return Ok(Value::Null);
            }
            // `=` / `<>` use three-valued equality so nested nulls
            // bubble up (`[1, 2] = [null, 2]` → null). Ordering
            // operators keep the existing compare() path.
            if matches!(op, CompareOp::Eq | CompareOp::Ne) {
                return Ok(match equal_three_valued(&vl, &vr) {
                    Some(true) => Value::Property(Property::Bool(matches!(op, CompareOp::Eq))),
                    Some(false) => Value::Property(Property::Bool(matches!(op, CompareOp::Ne))),
                    None => Value::Null,
                });
            }
            match compare(*op, &vl, &vr) {
                Ok(b) => Ok(Value::Property(Property::Bool(b))),
                Err(Error::TypeMismatch) => match op {
                    // `=` / `<>` between values of incomparable types
                    // are well-defined: they're simply `false` / `true`.
                    // Ordering operators (`<`, `<=`, ...) stay null.
                    CompareOp::Eq => Ok(Value::Property(Property::Bool(false))),
                    CompareOp::Ne => Ok(Value::Property(Property::Bool(true))),
                    _ => Ok(Value::Null),
                },
                Err(Error::UnsupportedComparison) => Ok(Value::Null),
                Err(e) => Err(e),
            }
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
        Expr::HasLabels { expr, labels } => {
            let v = eval_expr(expr, ctx)?;
            match v {
                Value::Node(n) => {
                    let has_all = labels.iter().all(|l| n.labels.contains(l));
                    Ok(Value::Property(Property::Bool(has_all)))
                }
                // `r:TYPE` on a relationship checks the edge type.
                // Cypher allows a single token here; multiple labels
                // don't apply to relationships but the matcher stays
                // permissive by requiring *all* listed labels to
                // equal the edge type (i.e. at most one distinct
                // token).
                Value::Edge(e) => {
                    let has_all = labels.iter().all(|l| l == &e.edge_type);
                    Ok(Value::Property(Property::Bool(has_all)))
                }
                Value::Null | Value::Property(Property::Null) => Ok(Value::Null),
                _ => Ok(Value::Property(Property::Bool(false))),
            }
        }
        Expr::InList { element, list } => {
            let elem = eval_expr(element, ctx)?;
            let list_val = eval_expr(list, ctx)?;
            // null IN anything = null; x IN null = null
            if matches!(list_val, Value::Null | Value::Property(Property::Null)) {
                return Ok(Value::Null);
            }
            let items = match list_val {
                Value::List(items) => items,
                Value::Property(Property::List(props)) => {
                    props.into_iter().map(Value::Property).collect()
                }
                _ => return Err(Error::TypeMismatch),
            };
            if matches!(elem, Value::Null | Value::Property(Property::Null)) {
                // null IN [anything] = null (unless list is empty → false)
                return if items.is_empty() {
                    Ok(Value::Property(Property::Bool(false)))
                } else {
                    Ok(Value::Null)
                };
            }
            // Three-valued IN: each item comparison is
            // three-valued, so nested nulls in either the
            // element or an item propagate to "indeterminate".
            // A definite `true` anywhere collapses to true;
            // all definite `false`s with no null collapses to
            // false; any null with no prior true collapses to
            // null.
            let mut any_null = false;
            for item in &items {
                match equal_three_valued(&elem, item) {
                    Some(true) => return Ok(Value::Property(Property::Bool(true))),
                    Some(false) => continue,
                    None => {
                        any_null = true;
                    }
                }
            }
            if any_null {
                Ok(Value::Null)
            } else {
                Ok(Value::Property(Property::Bool(false)))
            }
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
            // First pass: collect evaluated values. If every entry
            // lowers cleanly to a `Property`, keep the scalar-only
            // `Property::Map` wrapper so node / edge property
            // storage and wire format stay unchanged. If any entry
            // is a graph element (Node, Edge, Path) or a list
            // containing one, promote the whole map to
            // `Value::Map` which carries full `Value` leaves.
            let mut evaluated: Vec<(String, Value)> = Vec::with_capacity(entries.len());
            let mut contains_graph = false;
            for (key, expr) in entries {
                let v = eval_expr(expr, ctx)?;
                if value_contains_graph_element(&v) {
                    contains_graph = true;
                }
                evaluated.push((key.clone(), v));
            }
            if contains_graph {
                let mut out: std::collections::HashMap<String, Value> =
                    std::collections::HashMap::with_capacity(evaluated.len());
                for (k, v) in evaluated {
                    out.insert(k, v);
                }
                return Ok(Value::Map(out));
            }
            let mut out = std::collections::HashMap::with_capacity(evaluated.len());
            for (key, v) in evaluated {
                let prop = match v {
                    Value::Property(p) => p,
                    Value::Null => Property::Null,
                    Value::List(items) => {
                        let mut props = Vec::with_capacity(items.len());
                        for item in items {
                            match item {
                                Value::Property(p) => props.push(p),
                                Value::Null => props.push(Property::Null),
                                // Guarded by `contains_graph` above
                                // — if we're here the list was
                                // scalar-only. Still defensive:
                                // fall back to TypeMismatch rather
                                // than dropping a graph value.
                                _ => return Err(Error::TypeMismatch),
                            }
                        }
                        Property::List(props)
                    }
                    // Same guard — `contains_graph` is false here
                    // so these shouldn't happen.
                    Value::Node(_)
                    | Value::Edge(_)
                    | Value::Path { .. }
                    | Value::Map(_) => return Err(Error::TypeMismatch),
                };
                out.insert(key, prop);
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
        Expr::ExistsSubquery { body } => {
            if let Some((pattern, where_clause)) = extract_simple_match(body) {
                let b = exists_subquery_matches(pattern, where_clause, ctx)?;
                Ok(Value::Property(Property::Bool(b)))
            } else {
                let n = execute_subquery_body(body, ctx)?;
                Ok(Value::Property(Property::Bool(n > 0)))
            }
        }
        Expr::CountSubquery { body } => {
            if let Some((pattern, where_clause)) = extract_simple_match(body) {
                let n = count_subquery_matches(pattern, where_clause, ctx)?;
                Ok(Value::Property(Property::Int64(n)))
            } else {
                let n = execute_subquery_body(body, ctx)?;
                Ok(Value::Property(Property::Int64(n)))
            }
        }
        Expr::PatternComprehension {
            pattern,
            predicate,
            projection,
        } => pattern_comprehension_eval(
            pattern,
            predicate.as_deref(),
            projection,
            ctx,
        ),
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
fn extract_simple_match(
    body: &mesh_cypher::Statement,
) -> Option<(&mesh_cypher::Pattern, Option<&mesh_cypher::Expr>)> {
    if let mesh_cypher::Statement::Match(m) = body {
        if m.clauses.len() == 1 {
            if let mesh_cypher::ReadingClause::Match(mc) = &m.clauses[0] {
                if mc.patterns.len() == 1 {
                    return Some((&mc.patterns[0], mc.where_clause.as_ref()));
                }
            }
        }
    }
    None
}

fn exists_subquery_matches(
    pattern: &mesh_cypher::Pattern,
    where_clause: Option<&mesh_cypher::Expr>,
    ctx: &EvalCtx,
) -> Result<bool> {
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
                None => return Ok(true),
                Some(w) => {
                    let sub_ctx = ctx.with_row(&seed);
                    if to_bool(&eval_expr(w, &sub_ctx)?).unwrap_or(false) {
                        return Ok(true);
                    }
                    continue;
                }
            }
        }
        let mut frontier: Vec<(mesh_core::Node, Row)> = vec![(start_node.clone(), seed)];
        if walk_subquery_hops(pattern, where_clause, &mut frontier, ctx, true)? > 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

fn count_subquery_matches(
    pattern: &mesh_cypher::Pattern,
    where_clause: Option<&mesh_cypher::Expr>,
    ctx: &EvalCtx,
) -> Result<i64> {
    let mut total = 0i64;
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
                    if to_bool(&eval_expr(w, &sub_ctx)?).unwrap_or(false) {
                        total += 1;
                    }
                }
            }
            continue;
        }
        let mut frontier: Vec<(mesh_core::Node, Row)> = vec![(start_node.clone(), seed)];
        total += walk_subquery_hops(pattern, where_clause, &mut frontier, ctx, false)?;
    }
    Ok(total)
}

fn resolve_exists_start_candidates(
    start: &mesh_cypher::NodePattern,
    ctx: &EvalCtx,
) -> Result<Vec<mesh_core::Node>> {
    if let Some(var) = &start.var {
        if let Some(Value::Node(n)) = ctx.row.get(var) {
            return Ok(vec![n.clone()]);
        }
    }
    if !start.labels.is_empty() {
        let label = &start.labels[0];
        let ids = ctx.reader.nodes_by_label(label)?;
        let mut nodes = Vec::new();
        for id in ids {
            if let Some(n) = ctx.reader.get_node(id)? {
                nodes.push(n);
            }
        }
        return Ok(nodes);
    }
    let ids = ctx.reader.all_node_ids()?;
    let mut nodes = Vec::new();
    for id in ids {
        if let Some(n) = ctx.reader.get_node(id)? {
            nodes.push(n);
        }
    }
    Ok(nodes)
}

fn walk_subquery_hops(
    pattern: &mesh_cypher::Pattern,
    where_clause: Option<&mesh_cypher::Expr>,
    frontier: &mut Vec<(mesh_core::Node, Row)>,
    ctx: &EvalCtx,
    short_circuit: bool,
) -> Result<i64> {
    use mesh_cypher::Direction;
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
        } else {
            for (node, row) in frontier.iter() {
                let neighbors = match hop.rel.direction {
                    Direction::Outgoing => ctx.reader.outgoing(node.id)?,
                    Direction::Incoming => ctx.reader.incoming(node.id)?,
                    Direction::Both => {
                        let mut all = ctx.reader.outgoing(node.id)?;
                        all.extend(ctx.reader.incoming(node.id)?);
                        all
                    }
                };
                for (edge_id, neighbor_id) in neighbors {
                    if !hop.rel.edge_types.is_empty() {
                        let edge = match ctx.reader.get_edge(edge_id)? {
                            Some(e) => e,
                            None => continue,
                        };
                        if !hop.rel.edge_types.iter().any(|t| t == &edge.edge_type) {
                            continue;
                        }
                    }
                    let need_target = !hop.target.labels.is_empty()
                        || !hop.target.properties.is_empty()
                        || hop.target.var.is_some();
                    let target = if need_target {
                        match ctx.reader.get_node(neighbor_id)? {
                            Some(n) => n,
                            None => continue,
                        }
                    } else {
                        match ctx.reader.get_node(neighbor_id)? {
                            Some(n) => n,
                            None => continue,
                        }
                    };
                    if (!hop.target.labels.is_empty() || !hop.target.properties.is_empty())
                        && !node_pattern_matches(&target, &hop.target, ctx)?
                    {
                        continue;
                    }
                    if let Some(tv) = &hop.target.var {
                        if let Some(Value::Node(bound)) = row.get(tv) {
                            if bound.id != neighbor_id {
                                continue;
                            }
                        }
                    }
                    let mut next_row = row.clone();
                    if let Some(ev) = &hop.rel.var {
                        if let Some(edge) = ctx.reader.get_edge(edge_id)? {
                            next_row.insert(ev.clone(), Value::Edge(edge));
                        }
                    }
                    if let Some(tv) = &hop.target.var {
                        next_row.insert(tv.clone(), Value::Node(target.clone()));
                    }
                    next.push((target, next_row));
                }
            }
        }
        if next.is_empty() {
            return Ok(0);
        }
        *frontier = next;
    }
    match where_clause {
        None => Ok(if short_circuit && !frontier.is_empty() {
            1
        } else {
            frontier.len() as i64
        }),
        Some(w) => {
            let mut count = 0i64;
            for (_, row) in frontier.iter() {
                let sub_ctx = ctx.with_row(row);
                if to_bool(&eval_expr(w, &sub_ctx)?).unwrap_or(false) {
                    count += 1;
                    if short_circuit {
                        return Ok(1);
                    }
                }
            }
            Ok(count)
        }
    }
}

fn execute_subquery_body(body: &mesh_cypher::Statement, ctx: &EvalCtx) -> Result<i64> {
    // If the body is a bare MATCH with no terminal, treat it as RETURN *.
    let body = match body {
        mesh_cypher::Statement::Match(m)
            if m.terminal.return_items.is_empty()
                && !m.terminal.star
                && m.terminal.set_items.is_empty()
                && m.terminal.delete.is_none()
                && m.terminal.create_patterns.is_empty()
                && m.terminal.remove_items.is_empty()
                && m.terminal.foreach.is_none() =>
        {
            let mut patched = m.clone();
            patched.terminal.star = true;
            mesh_cypher::Statement::Match(patched)
        }
        other => other.clone(),
    };
    let plan = mesh_cypher::plan(&body).map_err(|e| Error::Unsupported(e.to_string()))?;

    // Wrap the plan so the outer row's bindings are available.
    // Insert a SeedRow → CartesianProduct with the planned body
    // so that variables from the outer row (like `p` in
    // `count { MATCH (p)-[:KNOWS]->() }`) resolve correctly.
    // The build_op_inner with seed=Some(row) makes SeedRow emit
    // the outer row; the plan itself runs as the right side of
    // a CartesianProduct-like join.
    //
    // For simple plans (NodeScan-based), the outer variables
    // need to be in the row for cross-stage rebind to work.
    // We achieve this by building the operator with the outer
    // row as seed — if the plan's leaf is SeedRow (from a WITH *
    // or bare match), it emits the outer row.
    let mut op = crate::ops::build_op_inner(&plan, Some(ctx.row));
    let noop = crate::ops::NoOpWriter;
    let exec_ctx = crate::ops::ExecCtx {
        store: ctx.reader,
        writer: &noop,
        params: ctx.params,
        procedures: ctx.procedures,
        outer_rows: &[],
    };
    let mut count = 0i64;
    while op.next(&exec_ctx)?.is_some() {
        count += 1;
    }
    Ok(count)
}

/// Evaluate a pattern comprehension `[pattern [WHERE pred] | proj]`:
/// enumerate every match of `pattern` anchored at the outer-row's
/// bound start variable, apply the optional WHERE filter against
/// the match's bindings, evaluate the projection, and collect into
/// a list. Inner bindings (new edge / target names introduced by
/// the pattern) are visible to WHERE and projection but don't
/// leak back to the outer row. Fixed-length hops only (no
/// var-length / shortestPath) — the TCK doesn't exercise those
/// and they'd need separate enumeration.
fn pattern_comprehension_eval(
    pattern: &Pattern,
    predicate: Option<&Expr>,
    projection: &Expr,
    ctx: &EvalCtx,
) -> Result<Value> {
    let start_var = match pattern.start.var.as_deref() {
        Some(v) => v,
        None => return Ok(Value::List(Vec::new())),
    };
    let start_node = match ctx.row.get(start_var) {
        Some(Value::Node(n)) => n.clone(),
        _ => return Ok(Value::List(Vec::new())),
    };
    if !start_node_matches(&start_node, &pattern.start, ctx)? {
        return Ok(Value::List(Vec::new()));
    }

    // Each entry: (current node, edges used so far, bindings added
    // by inner pattern variables, traversed nodes, traversed edges).
    // The last two track path reconstruction so `[p = (n)-->() | p]`
    // can bind `p` to a `Value::Path` and `length(p)` in WHERE /
    // projection works.
    use std::collections::{HashMap as StdHashMap, HashSet as StdHashSet};
    use mesh_core::EdgeId;
    type FrontierEntry = (
        NodeId,
        StdHashSet<EdgeId>,
        StdHashMap<String, Value>,
        Vec<mesh_core::Node>,
        Vec<mesh_core::Edge>,
    );
    let mut frontier: Vec<FrontierEntry> = vec![(
        start_node.id,
        StdHashSet::new(),
        StdHashMap::new(),
        vec![start_node.clone()],
        Vec::new(),
    )];
    for hop in &pattern.hops {
        // Var-length hops in a comprehension would need path-list
        // handling analogous to `VarLengthExpand` — out of scope
        // for this minimal implementation; collapse to empty.
        if hop.rel.var_length.is_some() {
            return Ok(Value::List(Vec::new()));
        }
        let mut next: Vec<FrontierEntry> = Vec::new();
        for (cur_id, used, bindings, path_nodes, path_edges) in &frontier {
            let neighbors = match hop.rel.direction {
                Direction::Outgoing => ctx.reader.outgoing(*cur_id)?,
                Direction::Incoming => ctx.reader.incoming(*cur_id)?,
                Direction::Both => {
                    let mut all = ctx.reader.outgoing(*cur_id)?;
                    all.extend(ctx.reader.incoming(*cur_id)?);
                    all
                }
            };
            for (edge_id, neighbor_id) in neighbors {
                if used.contains(&edge_id) {
                    continue;
                }
                let edge = match ctx.reader.get_edge(edge_id)? {
                    Some(e) => e,
                    None => continue,
                };
                if !hop.rel.edge_types.is_empty()
                    && !hop.rel.edge_types.iter().any(|t| t == &edge.edge_type)
                {
                    continue;
                }
                let neighbor = match ctx.reader.get_node(neighbor_id)? {
                    Some(n) => n,
                    None => continue,
                };
                if !node_pattern_matches(&neighbor, &hop.target, ctx)? {
                    continue;
                }
                // If the target var is already bound in the outer
                // row, only accept neighbors that match by id —
                // same rule `PatternExists` uses for the endpoint
                // constraint case.
                if let Some(target_var) = &hop.target.var {
                    if let Some(Value::Node(bound)) = ctx.row.get(target_var) {
                        if bound.id != neighbor.id {
                            continue;
                        }
                    }
                }
                let mut new_bindings = bindings.clone();
                if let Some(ev) = &hop.rel.var {
                    new_bindings.insert(ev.clone(), Value::Edge(edge.clone()));
                }
                if let Some(tv) = &hop.target.var {
                    new_bindings.insert(tv.clone(), Value::Node(neighbor.clone()));
                }
                let mut new_used = used.clone();
                new_used.insert(edge_id);
                let mut new_nodes = path_nodes.clone();
                new_nodes.push(neighbor.clone());
                let mut new_edges = path_edges.clone();
                new_edges.push(edge);
                next.push((neighbor_id, new_used, new_bindings, new_nodes, new_edges));
            }
        }
        frontier = next;
        if frontier.is_empty() {
            break;
        }
    }

    let mut out: Vec<Value> = Vec::new();
    for (_, _, bindings, path_nodes, path_edges) in frontier {
        // Layer inner bindings onto the outer row. The sub-row
        // needs to live long enough for `eval_expr` to borrow
        // through `with_row`.
        let mut sub_row = ctx.row.clone();
        for (k, v) in bindings {
            sub_row.insert(k, v);
        }
        if let Some(pv) = &pattern.path_var {
            sub_row.insert(
                pv.clone(),
                Value::Path {
                    nodes: path_nodes,
                    edges: path_edges,
                },
            );
        }
        let sub_ctx = ctx.with_row(&sub_row);
        if let Some(p) = predicate {
            let pv = eval_expr(p, &sub_ctx)?;
            if !matches!(pv, Value::Property(Property::Bool(true))) {
                continue;
            }
        }
        let val = eval_expr(projection, &sub_ctx)?;
        out.push(val);
    }
    Ok(Value::List(out))
}

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
        if !hop.rel.edge_types.is_empty() {
            let edge = match ctx.reader.get_edge(edge_id)? {
                Some(e) => e,
                None => continue,
            };
            if !hop.rel.edge_types.iter().any(|t| t == &edge.edge_type) {
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
        if !hop.rel.edge_types.is_empty() {
            let edge = match ctx.reader.get_edge(edge_id)? {
                Some(e) => e,
                None => continue,
            };
            if !hop.rel.edge_types.iter().any(|t| t == &edge.edge_type) {
                continue;
            }
        }
        used.insert(edge_id);
        vl_pred_dfs(neighbor_id, hop, min, max, depth + 1, ctx, used, out)?;
        used.remove(&edge_id);
    }
    Ok(())
}

fn start_node_matches(
    node: &mesh_core::Node,
    pattern: &NodePattern,
    ctx: &EvalCtx,
) -> Result<bool> {
    for label in &pattern.labels {
        if !node.labels.contains(label) {
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
        if !values_equal(&expected, &actual) {
            return Ok(false);
        }
    }
    Ok(true)
}

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
    // Normalize Property::List to Value::List for uniform handling
    let left = match left {
        Value::Property(Property::List(items)) => {
            Value::List(items.into_iter().map(Value::Property).collect())
        }
        other => other,
    };
    let right = match right {
        Value::Property(Property::List(items)) => {
            Value::List(items.into_iter().map(Value::Property).collect())
        }
        other => other,
    };
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
            // list + element: append to list
            (Value::List(a), rhs) => {
                let mut out = a.clone();
                out.push(rhs.clone());
                return Ok(Value::List(out));
            }
            // element + list: prepend to list
            (lhs, Value::List(b)) => {
                let mut out = Vec::with_capacity(b.len() + 1);
                out.push(lhs.clone());
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
                Num::Int(a.wrapping_div(b))
            }
            BinaryOp::Mod => {
                if b == 0 {
                    return Err(Error::DivideByZero);
                }
                Num::Int(a.wrapping_rem(b))
            }
            // Exponentiation always produces a float
            BinaryOp::Pow => Num::Float((a as f64).powf(b as f64)),
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
                BinaryOp::Pow => af.powf(bf),
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
fn map_int(m: &std::collections::HashMap<String, Property>, key: &str) -> Option<i64> {
    match m.get(key)? {
        Property::Int64(n) => Some(*n),
        Property::Float64(f) => Some(*f as i64),
        _ => None,
    }
}

std::thread_local! {
    // Cache of `now_epoch_nanos()` for the current statement. Cleared
    // by `reset_statement_time()` at the top of every query execution
    // so calls within a single statement see the same "now" — this
    // matches Neo4j, which evaluates `datetime()` etc. once per
    // statement so `duration.between(datetime(), datetime())` is
    // exactly `PT0S`.
    static STATEMENT_NANOS: std::cell::Cell<Option<i128>> = const { std::cell::Cell::new(None) };
}

pub(crate) fn reset_statement_time() {
    STATEMENT_NANOS.with(|c| c.set(None));
}

fn now_epoch_nanos() -> i128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    STATEMENT_NANOS.with(|cell| match cell.get() {
        Some(ns) => ns,
        None => {
            let duration = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            let ns = duration.as_nanos() as i128;
            cell.set(Some(ns));
            ns
        }
    })
}

/// Convenience: epoch nanos → (secs, subsec_nanos)
fn nanos_to_secs_nanos(epoch_nanos: i128) -> (i64, u32) {
    let secs = epoch_nanos.div_euclid(1_000_000_000) as i64;
    let nsec = epoch_nanos.rem_euclid(1_000_000_000) as u32;
    (secs, nsec)
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
fn parse_datetime(s: &str) -> Result<i128> {
    parse_datetime_with_tz(s).map(|(ns, _, _)| ns)
}

/// Parse a datetime string, returning `(utc_nanos, tz_offset_secs,
/// tz_name)`. The offset is `None` for naive inputs; the name is
/// set when the string carried an `[IANA/Region]` suffix.
fn parse_datetime_with_tz(s: &str) -> Result<(i128, Option<i32>, Option<String>)> {
    use chrono::{DateTime, FixedOffset, NaiveDateTime, Offset};
    let trimmed = s.trim();
    // An `[IANA/Region]` suffix overrides any explicit offset — when
    // both are present, the offset is just a display hint but the
    // canonical zone is the name. We parse the body to get the UTC
    // instant, then (if a name was supplied) re-resolve the offset
    // against the zone db so it stays correct for DST.
    let (body, tz_name) = match (trimmed.rfind('['), trimmed.rfind(']')) {
        (Some(open), Some(close)) if close > open => {
            let name = trimmed[open + 1..close].to_string();
            (&trimmed[..open], Some(name))
        }
        _ => (trimmed, None),
    };
    let trimmed = body.trim();
    // A "tz marker" in the body is either a trailing `Z` or a `+`/`-`
    // that follows the time-of-day portion. The date separators at
    // positions 4 and 7 (`YYYY-MM-DD`) must not be misread as an
    // offset, so look only past the `T` that separates date from time.
    let has_tz_marker = trimmed.ends_with('Z')
        || trimmed.find('T').and_then(|t_idx| {
            trimmed[t_idx..]
                .rfind(|c: char| c == '+' || c == '-')
                .map(|i| i > 0)
        }).unwrap_or(false);
    let finalise = |nanos: i128, tz: Option<i32>| {
        if let Some(name) = tz_name.as_deref() {
            // If the string already carried an explicit offset (e.g.
            // `...+02:00[Europe/Stockholm]`), `nanos` is already the
            // UTC instant — just look up the canonical zone name. If
            // the string only carried `[Region/City]`, the time we
            // parsed is the *local* wall-clock; re-resolve to UTC
            // against the zone db so the stored instant is correct.
            if tz.is_some() {
                match parse_tz_name(name, nanos) {
                    Some((off, canonical)) => (nanos, Some(off), Some(canonical)),
                    None => (nanos, tz, None),
                }
            } else {
                match parse_tz_name_local(name, nanos) {
                    Some((off, canonical)) => {
                        let utc = nanos - (off as i128) * 1_000_000_000;
                        (utc, Some(off), Some(canonical))
                    }
                    None => (nanos, tz, None),
                }
            }
        } else {
            (nanos, tz, None)
        }
    };
    if let Ok(dt) = DateTime::<FixedOffset>::parse_from_rfc3339(trimmed) {
        let offset = dt.offset().fix().local_minus_utc();
        return Ok(finalise(datetime_to_nanos(&dt), Some(offset)));
    }
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f%:z",
        "%Y-%m-%dT%H:%M:%S%:z",
        "%Y-%m-%dT%H:%M:%S%.f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M%z",
        "%Y-%m-%dT%H:%M%:z",
    ] {
        if let Ok(dt) = DateTime::<FixedOffset>::parse_from_str(trimmed, fmt) {
            let offset = dt.offset().fix().local_minus_utc();
            return Ok(finalise(datetime_to_nanos(&dt), Some(offset)));
        }
    }
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
    ] {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(trimmed, fmt) {
            let tz = if has_tz_marker { Some(0) } else { None };
            return Ok(finalise(datetime_to_nanos(&ndt.and_utc()), tz));
        }
    }
    if let Some((date_part, time_part)) = trimmed.split_once('T') {
        let days_opt = parse_iso_date(date_part)
            .map(|d| {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                d.signed_duration_since(epoch).num_days()
            })
            .or_else(|| parse_iso_date_big(date_part));
        if let Some(days) = days_opt {
            let (tod_nanos, tz) = parse_time_string_with_tz(time_part)?;
            let local_nanos = (days as i128) * 86_400_000_000_000 + tod_nanos as i128;
            let nanos = match tz {
                Some(off) => local_nanos - (off as i128) * 1_000_000_000,
                None => local_nanos,
            };
            return Ok(finalise(nanos, tz));
        }
    } else {
        // Pure date form without a time component also comes through
        // `datetime()` / `localdatetime()` in TCK scenarios.
        if let Some(days) = parse_iso_date_big(trimmed) {
            return Ok(finalise((days as i128) * 86_400_000_000_000, None));
        }
    }
    Err(Error::UnknownScalarFunction(format!(
        "datetime() could not parse {s:?} as ISO 8601 / RFC 3339"
    )))
}

/// Convert a `chrono::DateTime<Tz>` into `i128` epoch nanoseconds —
/// `timestamp_nanos_opt` is limited to i64 range (~±292 years from
/// 1970) whereas openCypher requires years 1..9999. We compute
/// `secs * 1e9 + nanos_of_second` directly in i128.
fn datetime_to_nanos<Tz: chrono::TimeZone>(dt: &chrono::DateTime<Tz>) -> i128 {
    (dt.timestamp() as i128) * 1_000_000_000 + (dt.timestamp_subsec_nanos() as i128)
}

/// Parse an ISO 8601 calendar date (`YYYY-MM-DD`) into days
/// since the UNIX epoch. Any other form is rejected.
fn parse_date(s: &str) -> Result<i64> {
    // Try the chrono-backed path first for the common ±292 K year
    // range; fall back to an out-of-range parser that only needs
    // the Gregorian rules (so we can round-trip the TCK's extreme
    // year values like ±999999999).
    use chrono::NaiveDate;
    let trimmed = s.trim();
    if let Some(parsed) = parse_iso_date(trimmed) {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        return Ok(parsed.signed_duration_since(epoch).num_days());
    }
    if let Some(days) = parse_iso_date_big(trimmed) {
        return Ok(days);
    }
    Err(Error::UnknownScalarFunction(format!(
        "date() could not parse {s:?} as ISO 8601 date"
    )))
}

/// Parse an ISO 8601 date in any supported form:
///   YYYY-MM-DD, YYYYMMDD, YYYY-MM, YYYYMM, YYYY,
///   YYYY-Www-D, YYYYWwwD, YYYY-Www, YYYYWww,
///   YYYY-DDD, YYYYDDD.
/// Missing components default to the first valid unit (Jan, day 1, week 1).
fn parse_iso_date(s: &str) -> Option<chrono::NaiveDate> {
    use chrono::NaiveDate;
    // Strip sign/year. ISO allows ±YYYYY for extended years; stick to 4-digit for now.
    let bytes = s.as_bytes();
    if bytes.len() < 4 || !bytes[..4].iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // ISO 8601 only permits ASCII digits, '-', and 'W' in the body
    // of a date. Reject anything else so alternative separators
    // (e.g. `YYYY/MM/DD`) fail rather than get quietly parsed.
    if s[4..]
        .chars()
        .any(|c| !(c.is_ascii_digit() || c == '-' || c == 'W'))
    {
        return None;
    }
    let year: i32 = s[..4].parse().ok()?;
    let rest = &s[4..];
    // Year only
    if rest.is_empty() {
        return NaiveDate::from_ymd_opt(year, 1, 1);
    }
    // Week date: '-Www-D' | '-Www' | 'WwwD' | 'Www'
    if let Some(w) = rest.strip_prefix("-W").or_else(|| rest.strip_prefix('W')) {
        let digits: String = w.chars().filter(|c| c.is_ascii_digit()).collect();
        let week: u32 = digits.get(..2)?.parse().ok()?;
        let dow: u32 = match digits.len() {
            2 => 1,
            3 => digits[2..3].parse().ok()?,
            _ => return None,
        };
        return chrono::NaiveDate::from_isoywd_opt(
            year,
            week,
            match dow {
                1 => chrono::Weekday::Mon,
                2 => chrono::Weekday::Tue,
                3 => chrono::Weekday::Wed,
                4 => chrono::Weekday::Thu,
                5 => chrono::Weekday::Fri,
                6 => chrono::Weekday::Sat,
                7 => chrono::Weekday::Sun,
                _ => return None,
            },
        );
    }
    // Ordinal date: YYYY-DDD or YYYYDDD (3 digits only, month would be 2)
    let digits: String = rest.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() == 3 {
        let ordinal: u32 = digits.parse().ok()?;
        return NaiveDate::from_yo_opt(year, ordinal);
    }
    // Calendar date: YYYY-MM, YYYYMM, YYYY-MM-DD, YYYYMMDD
    match digits.len() {
        2 => {
            let month: u32 = digits.parse().ok()?;
            NaiveDate::from_ymd_opt(year, month, 1)
        }
        4 => {
            let month: u32 = digits[..2].parse().ok()?;
            let day: u32 = digits[2..].parse().ok()?;
            NaiveDate::from_ymd_opt(year, month, day)
        }
        _ => None,
    }
}

/// Parse an ISO 8601 date with an *extended* year — `±NNNNNNNNN-MM-DD`
/// — into days since the 1970-01-01 epoch, without going through
/// `chrono::NaiveDate` (which tops out around ±262 K years). Only
/// handles the extended-year calendar form; other ISO 8601 shapes
/// funnel through `parse_iso_date` first.
fn parse_iso_date_big(s: &str) -> Option<i64> {
    let (sign, rest) = match s.as_bytes().first()? {
        b'+' => (1_i64, &s[1..]),
        b'-' => (-1_i64, &s[1..]),
        _ => return None,
    };
    // Extended-year calendar form: `YYYY...-MM-DD` (any number of
    // year digits >= 4).
    let mut parts = rest.split('-');
    let year_str = parts.next()?;
    let month_str = parts.next()?;
    let day_str = parts.next()?;
    if parts.next().is_some()
        || year_str.is_empty()
        || !year_str.chars().all(|c| c.is_ascii_digit())
        || month_str.len() != 2
        || day_str.len() != 2
    {
        return None;
    }
    let year: i64 = year_str.parse().ok()?;
    let year = sign * year;
    let month: u32 = month_str.parse().ok()?;
    let day: u32 = day_str.parse().ok()?;
    if !(1..=12).contains(&month) || day == 0 || day > 31 {
        return None;
    }
    big_ymd_to_days(year, month, day)
}

/// Convert a (year, month, day) into days since 1970-01-01 using
/// proleptic Gregorian rules. Works for the full i64 year range.
fn big_ymd_to_days(year: i64, month: u32, day: u32) -> Option<i64> {
    // Algorithm: shift the calendar to start on March 1st (so Feb
    // is the last month of the previous year and we don't have to
    // special-case its variable length), count eras of 400 years,
    // then offset back to Jan 1 1970.
    if !(1..=12).contains(&month) {
        return None;
    }
    let m = month as i64;
    let d = day as i64;
    // Sanity check day in month (leap-year aware).
    let days_in = {
        let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
        match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if leap => 29,
            _ => 28,
        }
    };
    if d < 1 || d > days_in as i64 {
        return None;
    }
    let y = if m <= 2 { year - 1 } else { year };
    let shifted_m = if m <= 2 { m + 9 } else { m - 3 } as i64;
    // `days_from_civil` — Howard Hinnant's algorithm.
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * shifted_m + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146_097 + doe - 719_468)
}

/// Convert days since 1970-01-01 back into (year, month, day). Uses
/// Hinnant's civil_from_days. Handles the full i64 range.
fn big_days_to_ymd(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

/// Parse a time string like "12:31:14.645876123" into nanoseconds since midnight.
/// Parse a timezone offset string like "+01:00", "+0100", "+01", "-02:30", "Z".
fn parse_tz_offset(s: &str) -> Option<i32> {
    let s = s.trim();
    if s == "Z" || s.is_empty() {
        return Some(0);
    }
    let (sign, rest) = if let Some(r) = s.strip_prefix('+') {
        (1, r)
    } else if let Some(r) = s.strip_prefix('-') {
        (-1, r)
    } else {
        // Not a simple offset — caller may retry via
        // `parse_tz_name_offset` if this is a region name like
        // `Europe/Stockholm`.
        return None;
    };
    let clean: String = rest.chars().filter(|c| c.is_ascii_digit()).collect();
    // Accept HH, HHMM, HHMMSS and their colon-separated variants.
    // The digit-only string's length pins the layout.
    let (h, m, sec) = match clean.len() {
        1 | 2 => (clean.parse::<i32>().ok()?, 0, 0),
        3 => (
            clean[..1].parse::<i32>().ok()?,
            clean[1..].parse::<i32>().ok()?,
            0,
        ),
        4 => (
            clean[..2].parse::<i32>().ok()?,
            clean[2..].parse::<i32>().ok()?,
            0,
        ),
        5 => (
            clean[..1].parse::<i32>().ok()?,
            clean[1..3].parse::<i32>().ok()?,
            clean[3..].parse::<i32>().ok()?,
        ),
        6 => (
            clean[..2].parse::<i32>().ok()?,
            clean[2..4].parse::<i32>().ok()?,
            clean[4..].parse::<i32>().ok()?,
        ),
        _ => return None,
    };
    Some(sign * (h * 3600 + m * 60 + sec))
}

/// Resolve an IANA timezone name (e.g. `"Europe/Stockholm"`) into
/// an (offset_seconds, canonical_name) pair at a given UTC instant.
/// Returns `None` if the name isn't a recognised IANA zone.
fn parse_tz_name(s: &str, utc_nanos: i128) -> Option<(i32, String)> {
    use chrono::{Offset, TimeZone};
    let tz: chrono_tz::Tz = s.trim().parse().ok()?;
    // Resolve the offset at the given instant — zones with DST
    // report different offsets at different times of year, so we
    // need the actual moment being referenced.
    let secs = utc_nanos.div_euclid(1_000_000_000) as i64;
    let nsec = utc_nanos.rem_euclid(1_000_000_000) as u32;
    let utc_dt = chrono::DateTime::from_timestamp(secs, nsec)?;
    let offset = tz
        .offset_from_utc_datetime(&utc_dt.naive_utc())
        .fix()
        .local_minus_utc();
    Some((offset, tz.name().to_string()))
}

/// Same as `parse_tz_name` but for a *local* wall-clock. Used when
/// the caller has built up local nanos from a map and needs to
/// figure out the correct UTC offset at that local time. Falls
/// back to the UTC interpretation on ambiguous local times (DST
/// fall-back), which matches Neo4j's documented behavior.
fn parse_tz_name_local(s: &str, local_nanos: i128) -> Option<(i32, String)> {
    use chrono::{Offset, TimeZone};
    let tz: chrono_tz::Tz = s.trim().parse().ok()?;
    let secs = local_nanos.div_euclid(1_000_000_000) as i64;
    let nsec = local_nanos.rem_euclid(1_000_000_000) as u32;
    let naive = chrono::DateTime::from_timestamp(secs, nsec)?.naive_utc();
    let resolved = tz.from_local_datetime(&naive).earliest()
        .or_else(|| tz.from_local_datetime(&naive).latest())?;
    let offset = resolved.offset().fix().local_minus_utc();
    Some((offset, tz.name().to_string()))
}

/// Parse a time string, returning (time_nanos, optional timezone offset).
fn parse_time_string_with_tz(s: &str) -> Result<(i64, Option<i32>)> {
    let trimmed = s.trim();
    // A tz suffix starts with the LAST '+', '-', or 'Z' in the string
    // — but only if that character sits at a legal split point. We
    // require at least two digits before the sign so plain `-0130`
    // (which is a tz attached to a compact time) still parses, while
    // `HH:MM:SS` on its own doesn't get mis-split.
    let (time_part, tz) = if trimmed.ends_with('Z') {
        (&trimmed[..trimmed.len() - 1], Some(0))
    } else {
        let tz_idx = trimmed.rfind(|c: char| c == '+' || c == '-');
        match tz_idx {
            Some(idx) if idx >= 2 => {
                let before = &trimmed[..idx];
                let suffix = &trimmed[idx..];
                let parsed = parse_tz_offset(suffix);
                if parsed.is_some() {
                    (before, parsed)
                } else {
                    (trimmed, None)
                }
            }
            _ => (trimmed, None),
        }
    };
    let time_nanos = parse_time_string(time_part)?;
    Ok((time_nanos, tz))
}

fn parse_time_string(s: &str) -> Result<i64> {
    let trimmed = s.trim();
    // Extended forms with ':' separators.
    for fmt in ["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"] {
        if let Ok(t) = chrono::NaiveTime::parse_from_str(trimmed, fmt) {
            let secs = t.num_seconds_from_midnight() as i64;
            let nanos = t.nanosecond() as i64;
            return Ok(secs * 1_000_000_000 + nanos);
        }
    }
    // Compact (basic) forms: "HH", "HHMM", "HHMMSS", "HHMMSS.fff".
    let (body, frac_ns) = match trimmed.split_once('.') {
        Some((b, frac)) => {
            // Pad/truncate fractional digits to 9.
            let digits: String = frac.chars().filter(|c| c.is_ascii_digit()).collect();
            let mut padded = digits.clone();
            while padded.len() < 9 {
                padded.push('0');
            }
            let ns: i64 = padded[..9].parse().unwrap_or(0);
            (b, ns)
        }
        None => (trimmed, 0_i64),
    };
    let all_digits = body.chars().all(|c| c.is_ascii_digit());
    if !all_digits {
        return Err(Error::UnknownScalarFunction(format!(
            "time() could not parse {s:?} as HH:MM:SS"
        )));
    }
    let (h, m, sec): (i64, i64, i64) = match body.len() {
        2 => (body.parse().unwrap_or(0), 0, 0),
        4 => (
            body[..2].parse().unwrap_or(0),
            body[2..].parse().unwrap_or(0),
            0,
        ),
        6 => (
            body[..2].parse().unwrap_or(0),
            body[2..4].parse().unwrap_or(0),
            body[4..].parse().unwrap_or(0),
        ),
        _ => {
            return Err(Error::UnknownScalarFunction(format!(
                "time() could not parse {s:?} as HH:MM:SS"
            )));
        }
    };
    Ok((h * 3600 + m * 60 + sec) * 1_000_000_000 + frac_ns)
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

    // ISO 8601 also allows an alternative "calendar date" duration
    // form: `P[YYYY-MM-DD]T[HH:MM:SS.fff]`. Detect it by the shape
    // `digits-digits-digits` — unit-based durations like
    // `P12Y5M-14D` also contain `-`, so we need the stronger check.
    let is_calendar_form = {
        let segs: Vec<&str> = date_part.split('-').collect();
        segs.len() == 3 && segs.iter().all(|s| !s.is_empty()
            && s.chars().all(|c| c.is_ascii_digit()))
    };
    if is_calendar_form {
        let mut d_iter = date_part.split('-');
        let y = d_iter.next().ok_or_else(bad)?.parse::<i64>().map_err(|_| bad())?;
        let mo = d_iter.next().ok_or_else(bad)?.parse::<i64>().map_err(|_| bad())?;
        let d = d_iter.next().ok_or_else(bad)?.parse::<i64>().map_err(|_| bad())?;
        if d_iter.next().is_some() {
            return Err(bad());
        }
        let mut months = y * 12 + mo;
        let mut days = d;
        let mut seconds = 0_i64;
        let mut nanos = 0_i32;
        if let Some(time) = time_part {
            let mut t_iter = time.split(':');
            let hh = t_iter.next().ok_or_else(bad)?.parse::<i64>().map_err(|_| bad())?;
            let mm = t_iter.next().ok_or_else(bad)?.parse::<i64>().map_err(|_| bad())?;
            let ss_raw = t_iter.next().ok_or_else(bad)?;
            if t_iter.next().is_some() {
                return Err(bad());
            }
            let (ss_whole, ss_frac) = match ss_raw.split_once('.') {
                Some((w, f)) => {
                    let whole: i64 = w.parse().map_err(|_| bad())?;
                    let mut padded = String::from(f);
                    while padded.len() < 9 {
                        padded.push('0');
                    }
                    padded.truncate(9);
                    let frac: i32 = padded.parse().map_err(|_| bad())?;
                    (whole, frac)
                }
                None => (ss_raw.parse::<i64>().map_err(|_| bad())?, 0),
            };
            seconds = hh * 3600 + mm * 60 + ss_whole;
            nanos = ss_frac;
        }
        if negative {
            months = months.wrapping_neg();
            days = days.wrapping_neg();
            seconds = seconds.wrapping_neg();
            nanos = nanos.wrapping_neg();
        }
        return Ok(mesh_core::Duration {
            months,
            days,
            seconds,
            nanos,
        });
    }

    let mut months = 0_i64;
    let mut days = 0_i64;
    let mut seconds = 0_i64;
    let mut nanos = 0_i32;

    // Fractional cascade helpers. Neo4j's fractional semantics:
    //   frac year  → 12 frac months
    //   frac month → 30.436875 frac days  (Gregorian mean)
    //   frac week  →  7 frac days
    //   frac day   → 86400 frac seconds
    //   frac hour  → 3600 frac seconds
    //   frac minute → 60 frac seconds
    // Fractional seconds go directly to nanos. Fractions in the
    // date part cascade through all smaller units.
    let secs_per_month: f64 = 30.436875 * 86400.0;

    // `frac` is encoded as nanos-scaled i32 (up to 9 decimal digits).
    let frac_to_f64 = |frac: i32| -> f64 { (frac as f64) / 1e9 };

    let add_seconds_f = |seconds: &mut i64, nanos: &mut i32, add: f64| {
        let whole_secs = add.trunc() as i64;
        let frac_secs = add - (whole_secs as f64);
        let add_nanos = (frac_secs * 1e9).round() as i32;
        *seconds = seconds.wrapping_add(whole_secs);
        *nanos = nanos.wrapping_add(add_nanos);
    };

    // Date-part segments: Y (years), M (months), W (weeks), D
    // (days). Each is `<digits>[.<digits>]<letter>`. Order isn't
    // enforced — the standard requires it but real-world inputs
    // are lenient, so we accept any order.
    let mut cursor = date_part;
    while !cursor.is_empty() {
        let (n, unit, rest) = consume_segment(cursor).ok_or_else(bad)?;
        let (whole, frac) = n;
        match unit {
            'Y' => {
                months = months.wrapping_add(whole.wrapping_mul(12));
                if let Some(f) = frac {
                    let f_months = frac_to_f64(f) * 12.0;
                    let extra_months = f_months.trunc() as i64;
                    months = months.wrapping_add(extra_months);
                    let leftover_months = f_months - (extra_months as f64);
                    add_seconds_f(&mut seconds, &mut nanos, leftover_months * secs_per_month);
                }
            }
            'M' => {
                months = months.wrapping_add(whole);
                if let Some(f) = frac {
                    add_seconds_f(&mut seconds, &mut nanos, frac_to_f64(f) * secs_per_month);
                }
            }
            'W' => {
                days = days.wrapping_add(whole.wrapping_mul(7));
                if let Some(f) = frac {
                    let f_days = frac_to_f64(f) * 7.0;
                    let extra_days = f_days.trunc() as i64;
                    days = days.wrapping_add(extra_days);
                    let leftover = f_days - (extra_days as f64);
                    add_seconds_f(&mut seconds, &mut nanos, leftover * 86400.0);
                }
            }
            'D' => {
                days = days.wrapping_add(whole);
                if let Some(f) = frac {
                    add_seconds_f(&mut seconds, &mut nanos, frac_to_f64(f) * 86400.0);
                }
            }
            _ => return Err(bad()),
        }
        cursor = rest;
    }
    // Cascade whole 24h chunks of the date-part's fractional seconds
    // into `days`. Neo4j's canonical output for a form like `P0.75M`
    // is `P22DT19H51M49.5S`, not `PT547H51M49.5S`. Time-part segments
    // below don't participate — `PT24H` stays as hours.
    {
        const SECS_PER_DAY: i64 = 86_400;
        let carry = seconds.div_euclid(SECS_PER_DAY);
        seconds = seconds.rem_euclid(SECS_PER_DAY);
        days = days.wrapping_add(carry);
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
                    seconds = seconds.wrapping_add(whole.wrapping_mul(3600));
                    if let Some(f) = frac {
                        add_seconds_f(&mut seconds, &mut nanos, frac_to_f64(f) * 3600.0);
                    }
                }
                'M' => {
                    seconds = seconds.wrapping_add(whole.wrapping_mul(60));
                    if let Some(f) = frac {
                        add_seconds_f(&mut seconds, &mut nanos, frac_to_f64(f) * 60.0);
                    }
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
    // Normalize nanos so |nanos| < 1e9 and the sign agrees with
    // `seconds`. Neo4j's canonical form keeps seconds and nanos
    // both non-negative or both non-positive so negative durations
    // print cleanly (`PT-1.999S` rather than `PT-2S` + `PT+0.001S`).
    let ns_per_sec: i32 = 1_000_000_000;
    while nanos >= ns_per_sec {
        nanos -= ns_per_sec;
        seconds = seconds.wrapping_add(1);
    }
    while nanos <= -ns_per_sec {
        nanos += ns_per_sec;
        seconds = seconds.wrapping_sub(1);
    }
    if seconds > 0 && nanos < 0 {
        seconds -= 1;
        nanos += ns_per_sec;
    } else if seconds < 0 && nanos > 0 {
        seconds += 1;
        nanos -= ns_per_sec;
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
    // Per-component sign. ISO 8601 permits forms like `P1DT-0.001S`
    // where individual fields carry their own sign.
    let negative = if i < bytes.len() && bytes[i] == b'-' {
        i += 1;
        true
    } else {
        false
    };
    let digit_start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == digit_start {
        return None; // no digits after optional sign
    }
    let whole: i64 = s[digit_start..i].parse().ok()?;
    let whole = if negative { -whole } else { whole };

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
        let signed = if negative { -n } else { n };
        (Some(signed), j)
    } else {
        (None, i)
    };

    if i >= bytes.len() {
        return None; // no unit letter
    }
    let unit = s[i..].chars().next()?;
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
    use Property::{Date, DateTime, Duration as Dur, LocalDateTime, Time};
    let l = match left {
        Value::Property(p) => p,
        _ => return None,
    };
    let r = match right {
        Value::Property(p) => p,
        _ => return None,
    };
    match (op, l, r) {
        // DateTime + Duration
        (
            BinaryOp::Add,
            DateTime {
                nanos: ns,
                tz_offset_secs: tz,
                tz_name: name,
            },
            Dur(d),
        )
        | (
            BinaryOp::Add,
            Dur(d),
            DateTime {
                nanos: ns,
                tz_offset_secs: tz,
                tz_name: name,
            },
        ) => Some(Ok(Value::Property(DateTime {
            nanos: datetime_add_duration(*ns, *d),
            tz_offset_secs: *tz,
            tz_name: name.clone(),
        }))),
        (
            BinaryOp::Sub,
            DateTime {
                nanos: ns,
                tz_offset_secs: tz,
                tz_name: name,
            },
            Dur(d),
        ) => Some(Ok(Value::Property(DateTime {
            nanos: datetime_add_duration(*ns, negate_duration(*d)),
            tz_offset_secs: *tz,
            tz_name: name.clone(),
        }))),
        (
            BinaryOp::Sub,
            DateTime {
                nanos: a, ..
            },
            DateTime { nanos: b, .. },
        )
        | (BinaryOp::Sub, LocalDateTime(a), LocalDateTime(b)) => {
            let diff_ns = a.wrapping_sub(*b);
            Some(Ok(Value::Property(Dur(mesh_core::Duration {
                months: 0,
                days: 0,
                seconds: diff_ns.div_euclid(1_000_000_000) as i64,
                nanos: diff_ns.rem_euclid(1_000_000_000) as i32,
            }))))
        }
        // LocalDateTime + Duration
        (BinaryOp::Add, LocalDateTime(ns), Dur(d))
        | (BinaryOp::Add, Dur(d), LocalDateTime(ns)) => Some(Ok(Value::Property(
            LocalDateTime(datetime_add_duration(*ns, *d)),
        ))),
        (BinaryOp::Sub, LocalDateTime(ns), Dur(d)) => Some(Ok(Value::Property(LocalDateTime(
            datetime_add_duration(*ns, negate_duration(*d)),
        )))),
        // Date + Duration
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
        // Time + Duration — mod 24h, sub-day components only.
        (
            BinaryOp::Add,
            Time {
                nanos,
                tz_offset_secs,
            },
            Dur(d),
        )
        | (
            BinaryOp::Add,
            Dur(d),
            Time {
                nanos,
                tz_offset_secs,
            },
        ) => Some(Ok(Value::Property(time_add_duration(
            *nanos,
            *tz_offset_secs,
            *d,
        )))),
        (
            BinaryOp::Sub,
            Time {
                nanos,
                tz_offset_secs,
            },
            Dur(d),
        ) => Some(Ok(Value::Property(time_add_duration(
            *nanos,
            *tz_offset_secs,
            negate_duration(*d),
        )))),
        (
            BinaryOp::Sub,
            Time {
                nanos: a,
                tz_offset_secs: _,
            },
            Time {
                nanos: b,
                tz_offset_secs: _,
            },
        ) => {
            let diff = a.wrapping_sub(*b);
            let diff_i128 = diff as i128;
            Some(Ok(Value::Property(Dur(mesh_core::Duration {
                months: 0,
                days: 0,
                seconds: diff_i128.div_euclid(1_000_000_000) as i64,
                nanos: diff_i128.rem_euclid(1_000_000_000) as i32,
            }))))
        }
        // Duration arithmetic
        (BinaryOp::Add, Dur(a), Dur(b)) => Some(Ok(Value::Property(Dur(add_durations(*a, *b))))),
        (BinaryOp::Sub, Dur(a), Dur(b)) => Some(Ok(Value::Property(Dur(add_durations(
            *a,
            negate_duration(*b),
        ))))),
        // Duration * number — multiplies every component. Fractional
        // results cascade: 0.5 years becomes 6 months, 0.5 days
        // becomes 12 hours, etc. (Matches Neo4j, which documents
        // Duration scalar arithmetic.)
        (BinaryOp::Mul, Dur(d), Property::Int64(n)) => {
            Some(Ok(Value::Property(Dur(scale_duration(*d, *n as f64)))))
        }
        (BinaryOp::Mul, Property::Int64(n), Dur(d)) => {
            Some(Ok(Value::Property(Dur(scale_duration(*d, *n as f64)))))
        }
        (BinaryOp::Mul, Dur(d), Property::Float64(n)) => {
            Some(Ok(Value::Property(Dur(scale_duration(*d, *n)))))
        }
        (BinaryOp::Mul, Property::Float64(n), Dur(d)) => {
            Some(Ok(Value::Property(Dur(scale_duration(*d, *n)))))
        }
        (BinaryOp::Div, Dur(d), Property::Int64(n)) => {
            if *n == 0 {
                return Some(Err(Error::DivideByZero));
            }
            Some(Ok(Value::Property(Dur(scale_duration(*d, 1.0 / (*n as f64))))))
        }
        (BinaryOp::Div, Dur(d), Property::Float64(n)) => {
            if *n == 0.0 {
                return Some(Err(Error::DivideByZero));
            }
            Some(Ok(Value::Property(Dur(scale_duration(*d, 1.0 / *n)))))
        }
        _ => None,
    }
}

/// Multiply every component of a Duration by `factor`. Fractional
/// date-level results (years / months / weeks / days) cascade into
/// days + seconds using Neo4j's mean month of 30.436875 days, but
/// integer sub-day components stay where they are — `32H` doesn't
/// get promoted to `1D8H`.
fn scale_duration(d: mesh_core::Duration, factor: f64) -> mesh_core::Duration {
    const DAY_SECS: i128 = 86_400;
    const SECS_PER_MONTH: f64 = 30.436875 * 86_400.0;
    // Scaled months: whole goes to `months`, fractional cascades
    // into days+sub-day seconds below.
    let scaled_months = d.months as f64 * factor;
    let whole_months = scaled_months.trunc() as i64;
    let frac_month_ns = ((scaled_months - whole_months as f64) * SECS_PER_MONTH * 1e9)
        .round() as i128;

    // Scaled days: whole goes to `days`, fractional cascades too.
    let scaled_days = d.days as f64 * factor;
    let whole_days = scaled_days.trunc() as i64;
    let frac_day_ns = ((scaled_days - whole_days as f64) * (DAY_SECS as f64) * 1e9)
        .round() as i128;

    // Sub-day: scale seconds+nanos independently so a nanosecond
    // that doesn't cleanly divide (1 * 0.5 = 0.5) gets truncated
    // rather than rounded up to 1. Floating-point at nanosecond
    // precision is lossy; Neo4j drops sub-nanosecond precision.
    let scaled_secs_f = d.seconds as f64 * factor;
    let whole_secs_f = scaled_secs_f.trunc();
    let frac_sec_ns = ((scaled_secs_f - whole_secs_f) * 1e9).round() as i128;
    let scaled_nanos = (d.nanos as f64 * factor).trunc() as i128;
    let scaled_sub_ns = (whole_secs_f as i128) * 1_000_000_000 + frac_sec_ns + scaled_nanos;

    // Cascade the fractional date-level pieces (which CAN legally
    // cross the 24h boundary) into `days`, leaving the scaled
    // sub-day portion in seconds+nanos.
    let cascaded_ns = frac_month_ns + frac_day_ns;
    let carry_days = cascaded_ns.div_euclid(DAY_SECS * 1_000_000_000);
    let cascaded_remainder_ns = cascaded_ns.rem_euclid(DAY_SECS * 1_000_000_000);
    let days_out = whole_days.wrapping_add(carry_days as i64);

    let total_ns = scaled_sub_ns + cascaded_remainder_ns;
    let mut seconds_out = (total_ns / 1_000_000_000) as i64;
    let mut nanos_out = (total_ns % 1_000_000_000) as i32;
    if seconds_out > 0 && nanos_out < 0 {
        seconds_out -= 1;
        nanos_out += 1_000_000_000;
    } else if seconds_out < 0 && nanos_out > 0 {
        seconds_out += 1;
        nanos_out -= 1_000_000_000;
    }
    mesh_core::Duration {
        months: whole_months,
        days: days_out,
        seconds: seconds_out,
        nanos: nanos_out,
    }
}

/// Add a Duration to a Time value, wrapping mod 24h. Sub-day
/// components only — month/day components are silently dropped
/// because Time has no calendar.
fn time_add_duration(
    nanos: i64,
    tz_offset_secs: Option<i32>,
    d: mesh_core::Duration,
) -> Property {
    let nanos_per_day: i64 = 86_400_000_000_000;
    let tod = (d.seconds as i128) * 1_000_000_000 + (d.nanos as i128);
    let new_nanos = ((nanos as i128 + tod).rem_euclid(nanos_per_day as i128)) as i64;
    Property::Time {
        nanos: new_nanos,
        tz_offset_secs,
    }
}

/// Add a [`Duration`] to an epoch-millisecond `DateTime`. Month
/// and day components are approximated because real calendar
/// arithmetic needs timezone + leap-year state that v1 doesn't
/// track: a month is 30 days, a day is 86_400 seconds. Callers
/// who need strict calendar semantics should use the component
/// fields on `Duration` directly and apply their own math.
fn datetime_add_duration(epoch_nanos: i128, d: mesh_core::Duration) -> i128 {
    let nanos_per_day: i128 = 86_400_000_000_000;
    // Split epoch_nanos into date + time-of-day for proper calendar month math
    let total_days = epoch_nanos.div_euclid(nanos_per_day);
    let time_of_day_ns = epoch_nanos.rem_euclid(nanos_per_day);
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let i64_days = i64::try_from(total_days).unwrap_or(0);
    let base_date = epoch + chrono::Duration::days(i64_days);
    // Apply months with calendar semantics
    let after_months = add_months_to_date(base_date, d.months);
    // Apply days, seconds, nanos
    let final_date = after_months + chrono::Duration::days(d.days);
    let new_days = final_date.signed_duration_since(epoch).num_days() as i128;
    let new_time_of_day = time_of_day_ns
        + (d.seconds as i128).saturating_mul(1_000_000_000)
        + (d.nanos as i128);
    new_days
        .saturating_mul(nanos_per_day)
        .saturating_add(new_time_of_day)
}

/// Add a [`Duration`] to a day-count `Date`. Only the `months`
/// and `days` components contribute — sub-day precision is lost
/// because `Date` doesn't carry it. If the duration has a
/// non-zero `seconds` or `nanos` field, we return a type error
/// so drivers can't silently drop a time-of-day component they
/// meant to apply.
fn date_add_duration(days: i64, d: mesh_core::Duration) -> Result<Value> {
    // Sub-day components normally don't affect a Date, but Neo4j
    // cascades whole 24h chunks of the Duration's seconds into
    // the `days` slot when applying to a Date (so a duration built
    // up from fractional years/months lands on the right calendar
    // day even if the canonical storage kept some spill in seconds).
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let base = epoch + chrono::Duration::days(days);
    // Truncate toward zero so a negative partial day (`-56950s`)
    // doesn't borrow a whole day, but a full positive day (`+86400s`
    // or more) still cascades. Matches Neo4j's date+duration rules.
    let seconds_day_carry = (d.seconds as i64) / 86_400;
    let new_date = add_months_to_date(base, d.months);
    let final_date = new_date
        + chrono::Duration::days(d.days.wrapping_add(seconds_day_carry));
    let new_days = final_date.signed_duration_since(epoch).num_days();
    Ok(Value::Property(Property::Date(new_days)))
}

/// Add a number of months to a date using calendar arithmetic.
/// Handles month overflow (e.g., Jan 31 + 1 month = Feb 28/29).
fn add_months_to_date(date: chrono::NaiveDate, months: i64) -> chrono::NaiveDate {
    let year = date.year() as i64;
    let month = date.month() as i64;
    let total_months = (year * 12 + (month - 1)) + months;
    let new_year = total_months.div_euclid(12) as i32;
    let new_month = (total_months.rem_euclid(12) + 1) as u32;
    // Day clamping: Jan 31 + 1 month = Feb 28/29
    let days_in_new_month = days_in_month(new_year, new_month);
    let day = date.day().min(days_in_new_month);
    chrono::NaiveDate::from_ymd_opt(new_year, new_month, day)
        .unwrap_or(date)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    let next_month = if month == 12 {
        chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        chrono::NaiveDate::from_ymd_opt(year, month + 1, 1)
    };
    let first = chrono::NaiveDate::from_ymd_opt(year, month, 1).unwrap();
    (next_month.unwrap() - first).num_days() as u32
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
                Value::Null | Value::Property(Property::Null) => Ok(Value::Null),
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
                Value::Property(Property::Map(m)) => {
                    let mut keys: Vec<String> = m.keys().cloned().collect();
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
                Value::Null | Value::Property(Property::Null) => Ok(Value::Null),
                Value::Node(n) => Ok(Value::Property(Property::Map(n.properties))),
                Value::Edge(e) => Ok(Value::Property(Property::Map(e.properties))),
                Value::Property(Property::Map(m)) => Ok(Value::Property(Property::Map(m))),
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
            // openCypher restricts `toString` to scalar values —
            // strings, numbers, booleans, temporals. Lists,
            // maps, graph elements, and paths raise
            // `InvalidArgumentValue` at runtime.
            match &v {
                Value::Null | Value::Property(Property::Null) => Ok(Value::Null),
                Value::Property(
                    Property::String(_)
                    | Property::Int64(_)
                    | Property::Float64(_)
                    | Property::Bool(_)
                    | Property::Date(_)
                    | Property::DateTime { .. }
                    | Property::LocalDateTime(_)
                    | Property::Time { .. }
                    | Property::Duration(_),
                ) => Ok(value_to_string(v)),
                _ => Err(Error::InvalidArgumentValue(
                    "toString() requires a scalar value".into(),
                )),
            }
        }
        "tointeger" => {
            let v = single_arg(name, arg_exprs, ctx)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Property(Property::Int64(i)) => Ok(Value::Property(Property::Int64(i))),
                Value::Property(Property::Float64(f)) => {
                    Ok(Value::Property(Property::Int64(f as i64)))
                }
                Value::Property(Property::String(s)) => {
                    let trimmed = s.trim();
                    if let Ok(n) = trimmed.parse::<i64>() {
                        Ok(Value::Property(Property::Int64(n)))
                    } else if let Ok(f) = trimmed.parse::<f64>() {
                        Ok(Value::Property(Property::Int64(f as i64)))
                    } else {
                        Ok(Value::Null)
                    }
                }
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
        "left" => {
            if arg_exprs.len() != 2 {
                return Err(Error::UnknownScalarFunction(format!(
                    "{name}() requires 2 arguments"
                )));
            }
            let s = eval_expr(&arg_exprs[0], ctx)?;
            let n = eval_expr(&arg_exprs[1], ctx)?;
            match (s, n) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Property(Property::Null), _) | (_, Value::Property(Property::Null)) => {
                    Ok(Value::Null)
                }
                (Value::Property(Property::String(s)), Value::Property(Property::Int64(n))) => {
                    let chars: Vec<char> = s.chars().collect();
                    let take = (n as usize).min(chars.len());
                    Ok(Value::Property(Property::String(
                        chars[..take].iter().collect(),
                    )))
                }
                _ => Err(Error::TypeMismatch),
            }
        }
        "right" => {
            if arg_exprs.len() != 2 {
                return Err(Error::UnknownScalarFunction(format!(
                    "{name}() requires 2 arguments"
                )));
            }
            let s = eval_expr(&arg_exprs[0], ctx)?;
            let n = eval_expr(&arg_exprs[1], ctx)?;
            match (s, n) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Property(Property::Null), _) | (_, Value::Property(Property::Null)) => {
                    Ok(Value::Null)
                }
                (Value::Property(Property::String(s)), Value::Property(Property::Int64(n))) => {
                    let chars: Vec<char> = s.chars().collect();
                    let skip = chars.len().saturating_sub(n as usize);
                    Ok(Value::Property(Property::String(
                        chars[skip..].iter().collect(),
                    )))
                }
                _ => Err(Error::TypeMismatch),
            }
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
        "e" => {
            if !arg_exprs.is_empty() {
                return Err(Error::UnknownScalarFunction(
                    "e() takes no arguments".into(),
                ));
            }
            Ok(Value::Property(Property::Float64(std::f64::consts::E)))
        }
        "exp" => math_unary(name, arg_exprs, ctx, |f| f.exp()),
        "log" | "ln" => math_unary(name, arg_exprs, ctx, |f| f.ln()),
        "log10" => math_unary(name, arg_exprs, ctx, |f| f.log10()),

        // --- Trigonometric functions ---
        "sin" => math_unary(name, arg_exprs, ctx, |f| f.sin()),
        "cos" => math_unary(name, arg_exprs, ctx, |f| f.cos()),
        "tan" => math_unary(name, arg_exprs, ctx, |f| f.tan()),
        "cot" => math_unary(name, arg_exprs, ctx, |f| 1.0 / f.tan()),
        "asin" => math_unary(name, arg_exprs, ctx, |f| f.asin()),
        "acos" => math_unary(name, arg_exprs, ctx, |f| f.acos()),
        "atan" => math_unary(name, arg_exprs, ctx, |f| f.atan()),
        "atan2" => {
            if arg_exprs.len() != 2 {
                return Err(Error::UnknownScalarFunction(
                    "atan2() expects 2 arguments".into(),
                ));
            }
            let y = eval_expr(&arg_exprs[0], ctx)?;
            let x = eval_expr(&arg_exprs[1], ctx)?;
            match (y, x) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Property(Property::Null), _) | (_, Value::Property(Property::Null)) => {
                    Ok(Value::Null)
                }
                (Value::Property(py), Value::Property(px)) => {
                    let yf = match py {
                        Property::Int64(i) => i as f64,
                        Property::Float64(f) => f,
                        _ => return Err(Error::TypeMismatch),
                    };
                    let xf = match px {
                        Property::Int64(i) => i as f64,
                        Property::Float64(f) => f,
                        _ => return Err(Error::TypeMismatch),
                    };
                    Ok(Value::Property(Property::Float64(yf.atan2(xf))))
                }
                _ => Err(Error::TypeMismatch),
            }
        }
        "degrees" => math_unary(name, arg_exprs, ctx, |f| f.to_degrees()),
        "radians" => math_unary(name, arg_exprs, ctx, |f| f.to_radians()),

        "rand" => {
            if !arg_exprs.is_empty() {
                return Err(Error::UnknownScalarFunction(
                    "rand() takes no arguments".into(),
                ));
            }
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .hash(&mut hasher);
            let bits = hasher.finish();
            let val = (bits as f64) / (u64::MAX as f64);
            Ok(Value::Property(Property::Float64(val)))
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
        "datetime" | "localdatetime" => {
            let is_local = name == "localdatetime";
            // `wrap` takes the epoch nanos (always UTC) and an
            // optional timezone offset (None → localdatetime, Some(0)
            // → UTC "Z", Some(n) → `+HH:MM`).
            let wrap = |ns: i128, tz: Option<i32>, tz_name: Option<String>| -> Value {
                if is_local {
                    Value::Property(Property::LocalDateTime(ns))
                } else {
                    Value::Property(Property::DateTime {
                        nanos: ns,
                        tz_offset_secs: tz,
                        tz_name,
                    })
                }
            };
            match arg_exprs.len() {
            0 => Ok(wrap(now_epoch_nanos(), Some(0), None)),
            1 => {
                let v = eval_expr(&arg_exprs[0], ctx)?;
                match v {
                    Value::Null | Value::Property(Property::Null) => Ok(Value::Null),
                    Value::Property(Property::String(s)) => {
                        let (ns, tz, tz_name) = parse_datetime_with_tz(&s)?;
                        Ok(wrap(ns, tz, tz_name))
                    }
                    Value::Property(Property::Map(m)) => {
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        // Extract base date and time-of-day from any temporal-typed value
                        let (base_date, base_tod_ns) = extract_base_date_tod(&m, &epoch);
                        let has_base = base_date.is_some();
                        let base = base_date.unwrap_or(epoch);

                        // Build date part
                        let target_date = build_date_from_map(&m, base, has_base);
                        let days_since_epoch = target_date.signed_duration_since(epoch).num_days();

                        // Time-of-day: use explicit fields or inherit from base
                        let base_hour = (base_tod_ns / 3_600_000_000_000) as i64;
                        let base_min = ((base_tod_ns % 3_600_000_000_000) / 60_000_000_000) as i64;
                        let base_sec = ((base_tod_ns % 60_000_000_000) / 1_000_000_000) as i64;
                        let base_ns = (base_tod_ns % 1_000_000_000) as i64;

                        // Neo4j semantics: when a time-carrying base (datetime/time)
                        // is given, missing time fields inherit from the base. When
                        // no base is given, missing time fields default to 0.
                        let has_time_base = m.contains_key("datetime")
                            || m.contains_key("localdatetime")
                            || m.contains_key("time");
                        let default_tod = |base_val: i64| {
                            if has_time_base {
                                base_val
                            } else {
                                0
                            }
                        };
                        let hour = map_int(&m, "hour").unwrap_or_else(|| default_tod(base_hour));
                        let minute = map_int(&m, "minute").unwrap_or_else(|| default_tod(base_min));
                        let second = map_int(&m, "second").unwrap_or_else(|| default_tod(base_sec));
                        // ms/us/ns combine: each is the appropriate order of
                        // magnitude's contribution to sub-second nanoseconds.
                        // Absent fields fall back to base. Present fields
                        // add up.
                        let has_any_sub = m.contains_key("millisecond")
                            || m.contains_key("microsecond")
                            || m.contains_key("nanosecond");
                        let nanos = if has_any_sub {
                            map_int(&m, "millisecond").unwrap_or(0) * 1_000_000
                                + map_int(&m, "microsecond").unwrap_or(0) * 1_000
                                + map_int(&m, "nanosecond").unwrap_or(0)
                        } else {
                            default_tod(base_ns)
                        };
                        let mut local_nanos: i128 = (days_since_epoch as i128) * 86_400_000_000_000
                            + (hour as i128) * 3_600_000_000_000
                            + (minute as i128) * 60_000_000_000
                            + (second as i128) * 1_000_000_000
                            + (nanos as i128);
                        // Pull timezone from the map (string offset or
                        // named region) or inherit from a base
                        // datetime/time. For datetime() (not localdatetime())
                        // we default to UTC when nothing else is specified.
                        let (tz_offset, tz_name_opt) = if is_local {
                            (None, None)
                        } else {
                            match extract_tz_spec(&m, Some(local_nanos)) {
                                Some((off, name)) => (Some(off), name),
                                None => (Some(0), None),
                            }
                        };
                        // If we inherited a zone *name*, re-resolve
                        // its offset against the new local wall-clock.
                        // Without this, projecting a datetime from
                        // Oct to March would keep the original Oct
                        // offset (+01:00) rather than moving to the
                        // DST-correct one (+02:00).
                        let (tz_offset, tz_name_opt) = if let Some(name) = &tz_name_opt {
                            match parse_tz_name_local(name, local_nanos) {
                                Some((off, canonical)) => (Some(off), Some(canonical)),
                                None => (tz_offset, tz_name_opt),
                            }
                        } else {
                            (tz_offset, tz_name_opt)
                        };
                        // If the map *explicitly* supplied a timezone
                        // alongside a zoned base value, we rotate the
                        // wall-clock to the new zone — the same instant
                        // gets a new local representation. (Without an
                        // explicit override we just reuse the base's
                        // tod, so nothing to rotate.)
                        if !is_local && m.contains_key("timezone") {
                            if let Some(base_off) = base_tz_offset(&m, local_nanos) {
                                if let Some(new_off) = tz_offset {
                                    let shift = (new_off - base_off) as i128 * 1_000_000_000;
                                    local_nanos += shift;
                                }
                            }
                        }
                        // Time components in the map are *local* to the
                        // given zone (Neo4j convention). Shift them back
                        // to UTC so the stored `nanos` is always UTC.
                        let epoch_nanos = match tz_offset {
                            Some(offset) => local_nanos - (offset as i128) * 1_000_000_000,
                            None => local_nanos,
                        };
                        Ok(wrap(epoch_nanos, tz_offset, tz_name_opt))
                    }
                    // datetime(other_temporal) — project from another
                    // temporal. Date: promote to midnight of that day.
                    // LocalDateTime: identity for localdatetime(),
                    // wrapped with UTC for datetime().
                    // DateTime: identity for datetime(), strip offset
                    // for localdatetime().
                    Value::Property(Property::Date(days)) => {
                        let ns = (days as i128) * 86_400_000_000_000;
                        Ok(wrap(ns, Some(0), None))
                    }
                    Value::Property(Property::LocalDateTime(ns)) => {
                        Ok(wrap(ns, Some(0), None))
                    }
                    Value::Property(Property::DateTime {
                        nanos,
                        tz_offset_secs,
                        tz_name,
                    }) => {
                        if is_local {
                            // Strip to local wall-clock nanos.
                            let local = match tz_offset_secs {
                                Some(off) => nanos + (off as i128) * 1_000_000_000,
                                None => nanos,
                            };
                            Ok(wrap(local, None, None))
                        } else {
                            Ok(wrap(nanos, tz_offset_secs.or(Some(0)), tz_name))
                        }
                    }
                    _ => Err(Error::TypeMismatch),
                }
            }
            _ => Err(Error::UnknownScalarFunction(
                "datetime() takes zero or one argument".into(),
            )),
        }},
        "date" => match arg_exprs.len() {
            0 => {
                let days = now_epoch_nanos().div_euclid(86_400_000_000_000) as i64;
                Ok(Value::Property(Property::Date(days)))
            }
            1 => {
                let v = eval_expr(&arg_exprs[0], ctx)?;
                match v {
                    Value::Null | Value::Property(Property::Null) => Ok(Value::Null),
                    Value::Property(Property::String(s)) => {
                        Ok(Value::Property(Property::Date(parse_date(&s)?)))
                    }
                    Value::Property(Property::Map(m)) => {
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        // Extract base date from any temporal-typed value under any of
                        // these keys: date, datetime, localdatetime
                        let base_date = extract_base_date(&m, &epoch);
                        let base_or_epoch = base_date.unwrap_or(epoch);
                        let has_base = base_date.is_some();
                        let target = build_date_from_map(&m, base_or_epoch, has_base);
                        let days = target.signed_duration_since(epoch).num_days();
                        Ok(Value::Property(Property::Date(days)))
                    }
                    // date(datetime_value) — extract local calendar
                    // date from a zoned DateTime. Shift by the offset
                    // so DST boundaries round to the *local* day.
                    Value::Property(Property::DateTime {
                        nanos,
                        tz_offset_secs,
                        tz_name: _,
                    }) => {
                        let local = match tz_offset_secs {
                            Some(off) => nanos + (off as i128) * 1_000_000_000,
                            None => nanos,
                        };
                        let days = local.div_euclid(86_400_000_000_000) as i64;
                        Ok(Value::Property(Property::Date(days)))
                    }
                    Value::Property(Property::LocalDateTime(ns)) => {
                        let days = ns.div_euclid(86_400_000_000_000) as i64;
                        Ok(Value::Property(Property::Date(days)))
                    }
                    // date(date_value) — identity, but keeps the
                    // existing Property::Date alive.
                    Value::Property(Property::Date(days)) => {
                        Ok(Value::Property(Property::Date(days)))
                    }
                    _ => Err(Error::TypeMismatch),
                }
            }
            _ => Err(Error::UnknownScalarFunction(
                "date() takes zero or one argument".into(),
            )),
        },
        "time" | "localtime" => {
            let is_tz = name == "time";
            match arg_exprs.len() {
            0 => {
                let time_nanos = (now_epoch_nanos() % 86_400_000_000_000) as i64;
                Ok(Value::Property(Property::Time {
                    nanos: time_nanos,
                    tz_offset_secs: if is_tz { Some(0) } else { None },
                }))
            }
            1 => {
                let v = eval_expr(&arg_exprs[0], ctx)?;
                match v {
                    Value::Null | Value::Property(Property::Null) => Ok(Value::Null),
                    Value::Property(Property::Map(m)) => {
                        // Extract base time-of-day from ANY temporal value under
                        // any of the temporal-source keys (time, datetime,
                        // localdatetime). TCK tests pass e.g. a LocalDateTime
                        // value under the "time" key to mean "use this value's
                        // time-of-day", so we can't assume the key name matches
                        // the value type.
                        let base_tod: i64 = {
                            let mut tod = 0i64;
                            for key in ["time", "datetime", "localdatetime"] {
                                match m.get(key) {
                                    Some(Property::Time { nanos, .. }) => {
                                        tod = *nanos;
                                        break;
                                    }
                                    Some(Property::DateTime {
                                        nanos: ns,
                                        tz_offset_secs,        tz_name: _,

                                    }) => {
                                        // DateTime's nanos are UTC; the
                                        // "base time-of-day" we inherit
                                        // is the *local* wall-clock tod.
                                        let local = match tz_offset_secs {
                                            Some(off) => {
                                                *ns + (*off as i128) * 1_000_000_000
                                            }
                                            None => *ns,
                                        };
                                        tod = local.rem_euclid(86_400_000_000_000) as i64;
                                        break;
                                    }
                                    Some(Property::LocalDateTime(ns)) => {
                                        tod = ns.rem_euclid(86_400_000_000_000) as i64;
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            tod
                        };
                        let base_h = base_tod / 3_600_000_000_000;
                        let base_min = (base_tod % 3_600_000_000_000) / 60_000_000_000;
                        let base_sec = (base_tod % 60_000_000_000) / 1_000_000_000;
                        let base_ns = base_tod % 1_000_000_000;

                        let has_any_base = m.contains_key("time") || m.contains_key("datetime")
                            || m.contains_key("localdatetime");
                        let default_tod = |b: i64| if has_any_base { b } else { 0 };

                        let hour = map_int(&m, "hour").unwrap_or_else(|| default_tod(base_h));
                        let minute = map_int(&m, "minute").unwrap_or_else(|| default_tod(base_min));
                        let second = map_int(&m, "second").unwrap_or_else(|| default_tod(base_sec));
                        // ms/us/ns combine: each is the appropriate order of
                        // magnitude's contribution to sub-second nanoseconds.
                        // Absent fields fall back to base. Present fields
                        // add up.
                        let has_any_sub = m.contains_key("millisecond")
                            || m.contains_key("microsecond")
                            || m.contains_key("nanosecond");
                        let nanos = if has_any_sub {
                            map_int(&m, "millisecond").unwrap_or(0) * 1_000_000
                                + map_int(&m, "microsecond").unwrap_or(0) * 1_000
                                + map_int(&m, "nanosecond").unwrap_or(0)
                        } else {
                            default_tod(base_ns)
                        };
                        let time_nanos =
                            hour * 3_600_000_000_000 + minute * 60_000_000_000
                            + second * 1_000_000_000 + nanos;
                        // Timezone: explicit `timezone:` in the map
                        // wins. If absent, inherit from the base
                        // temporal value (time() / datetime() under
                        // any key). Otherwise default to UTC for
                        // time(), or None for localtime().
                        let tz_offset = if is_tz {
                            extract_tz_offset(&m).or(Some(0))
                        } else {
                            None
                        };
                        // If the map supplies a timezone override
                        // but carries a zoned base value, Neo4j
                        // *rotates* the time to the new zone — the
                        // wall-clock shifts by (new - old) offset so
                        // the same instant is represented.
                        let time_nanos = if is_tz {
                            if let Some(Property::String(_)) = m.get("timezone") {
                                let base_tz: Option<i32> = {
                                    let mut tz = None;
                                    for key in ["time", "datetime"] {
                                        match m.get(key) {
                                            Some(Property::Time {
                                                tz_offset_secs: Some(o),
                                                ..
                                            }) => {
                                                tz = Some(*o);
                                                break;
                                            }
                                            Some(Property::DateTime {
                                                tz_offset_secs: Some(o),
                                                ..
                                            }) => {
                                                tz = Some(*o);
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }
                                    tz
                                };
                                match (base_tz, tz_offset) {
                                    (Some(old), Some(new)) => {
                                        let diff = (new - old) as i64;
                                        let mut out =
                                            time_nanos + diff * 1_000_000_000;
                                        let day = 86_400_000_000_000_i64;
                                        out = ((out % day) + day) % day;
                                        out
                                    }
                                    _ => time_nanos,
                                }
                            } else {
                                time_nanos
                            }
                        } else {
                            time_nanos
                        };
                        Ok(Value::Property(Property::Time {
                            nanos: time_nanos,
                            tz_offset_secs: tz_offset,
                        }))
                    }
                    Value::Property(Property::String(s)) => {
                        let (time_nanos, tz) = parse_time_string_with_tz(&s)?;
                        Ok(Value::Property(Property::Time {
                            nanos: time_nanos,
                            tz_offset_secs: if is_tz { Some(tz.unwrap_or(0)) } else { None },
                        }))
                    }
                    // Project the time-of-day out of another temporal
                    // value. For zoned sources (DateTime, zoned Time)
                    // the nanos are UTC; shift by the offset so the
                    // resulting local time of day matches the source's
                    // local wall-clock.
                    Value::Property(Property::Time {
                        nanos,
                        tz_offset_secs,
                    }) => Ok(Value::Property(Property::Time {
                        nanos,
                        tz_offset_secs: if is_tz {
                            Some(tz_offset_secs.unwrap_or(0))
                        } else {
                            None
                        },
                    })),
                    Value::Property(Property::LocalDateTime(ns)) => {
                        let tod = ns.rem_euclid(86_400_000_000_000) as i64;
                        Ok(Value::Property(Property::Time {
                            nanos: tod,
                            tz_offset_secs: if is_tz { Some(0) } else { None },
                        }))
                    }
                    Value::Property(Property::DateTime {
                        nanos,
                        tz_offset_secs,        tz_name: _,

                    }) => {
                        let local = match tz_offset_secs {
                            Some(off) => nanos + (off as i128) * 1_000_000_000,
                            None => nanos,
                        };
                        let tod = local.rem_euclid(86_400_000_000_000) as i64;
                        Ok(Value::Property(Property::Time {
                            nanos: tod,
                            tz_offset_secs: if is_tz {
                                Some(tz_offset_secs.unwrap_or(0))
                            } else {
                                None
                            },
                        }))
                    }
                    _ => Err(Error::TypeMismatch),
                }
            }
            _ => Err(Error::UnknownScalarFunction(
                "time() takes zero or one argument".into(),
            )),
        }},
        "timestamp" => {
            if !arg_exprs.is_empty() {
                return Err(Error::UnknownScalarFunction(
                    "timestamp() takes no arguments".into(),
                ));
            }
            // `timestamp()` returns epoch milliseconds (Neo4j convention)
            let ms = now_epoch_nanos() / 1_000_000;
            Ok(Value::Property(Property::Int64(ms as i64)))
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
            // Sub-day accumulator split in two so we know which
            // nanoseconds came from *fractional date-part components*
            // (those cascade into whole days at the end) versus
            // *time-part components* (which stay as seconds/nanos —
            // `duration({seconds: -2, ms: 1})` must canonicalise to
            // `PT-1.999S`, not `P-1DT…`).
            let mut date_frac_ns = 0_i128;
            let mut time_ns = 0_i128;
            // Mean-month size used when a fractional month or year
            // cascades into smaller units: 365.2425 / 12 days.
            const SECS_PER_MONTH: f64 = 30.436875 * 86400.0;
            const SECS_PER_DAY: i64 = 86_400;
            // Cascade a fractional number of *days* — year/month/week/
            // day fractions all boil down to this — into whole days
            // plus a sub-day nanosecond remainder that we'll
            // collapse back to days at the end.
            let mut push_days_f = |total_days: f64,
                                   days: &mut i64,
                                   sub_day_nanos: &mut i128| {
                let whole = total_days.trunc() as i64;
                *days = days.wrapping_add(whole);
                let frac = total_days - whole as f64;
                *sub_day_nanos += (frac * (SECS_PER_DAY as f64) * 1e9).round() as i128;
            };
            for (k, v) in &entries {
                let as_f64 = match v {
                    Property::Int64(i) => *i as f64,
                    Property::Float64(f) => *f,
                    Property::Null => continue,
                    _ => return Err(Error::TypeMismatch),
                };
                match k.as_str() {
                    "years" => {
                        let total_months = as_f64 * 12.0;
                        let whole = total_months.trunc() as i64;
                        months = months.wrapping_add(whole);
                        let frac_months = total_months - whole as f64;
                        push_days_f(
                            frac_months * SECS_PER_MONTH / (SECS_PER_DAY as f64),
                            &mut days,
                            &mut date_frac_ns,
                        );
                    }
                    "months" => {
                        let whole = as_f64.trunc() as i64;
                        months = months.wrapping_add(whole);
                        let frac = as_f64 - whole as f64;
                        push_days_f(
                            frac * SECS_PER_MONTH / (SECS_PER_DAY as f64),
                            &mut days,
                            &mut date_frac_ns,
                        );
                    }
                    "weeks" => {
                        push_days_f(as_f64 * 7.0, &mut days, &mut date_frac_ns);
                    }
                    "days" => {
                        push_days_f(as_f64, &mut days, &mut date_frac_ns);
                    }
                    // Sub-day components stay as seconds+nanos; Neo4j
                    // does not cascade integer seconds into days.
                    "hours" => {
                        time_ns += (as_f64 * 3600.0 * 1e9).round() as i128;
                    }
                    "minutes" => {
                        time_ns += (as_f64 * 60.0 * 1e9).round() as i128;
                    }
                    "seconds" => {
                        time_ns += (as_f64 * 1e9).round() as i128;
                    }
                    "milliseconds" => {
                        time_ns += (as_f64 * 1_000_000.0).round() as i128;
                    }
                    "microseconds" => {
                        time_ns += (as_f64 * 1000.0).round() as i128;
                    }
                    "nanoseconds" => {
                        time_ns += as_f64.round() as i128;
                    }
                    other => {
                        return Err(Error::UnknownScalarFunction(format!(
                            "duration() does not recognise component `{other}`"
                        )))
                    }
                }
            }
            // Finalise. Date-fraction nanos always cascade into
            // whole days; time-part nanos stay as seconds even if
            // they sum past 24h (so `{days:1, ms:-1}` canonicalises
            // to `P1DT-0.001S`, not `PT23H59M59.999S`). Both streams
            // then combine into the final seconds/nanos.
            let day_ns: i128 = (SECS_PER_DAY as i128) * 1_000_000_000;
            let date_day_carry = date_frac_ns.div_euclid(day_ns);
            let date_sub_day_ns = date_frac_ns.rem_euclid(day_ns);
            days = days.wrapping_add(date_day_carry as i64);
            let total_ns = date_sub_day_ns + time_ns;
            let mut seconds = (total_ns / 1_000_000_000) as i64;
            let mut nanos = (total_ns % 1_000_000_000) as i32;
            if seconds > 0 && nanos < 0 {
                seconds -= 1;
                nanos += 1_000_000_000;
            } else if seconds < 0 && nanos > 0 {
                seconds += 1;
                nanos -= 1_000_000_000;
            }
            Ok(Value::Property(Property::Duration(mesh_core::Duration {
                months,
                days,
                seconds,
                nanos,
            })))
        }

        // Namespace-qualified temporal functions (duration.between, etc.)
        "duration.between" | "duration.inmonths" | "duration.indays" | "duration.inseconds" => {
            if arg_exprs.len() != 2 {
                return Err(Error::UnknownScalarFunction(format!(
                    "{name}() expects 2 arguments, got {}",
                    arg_exprs.len()
                )));
            }
            let a = eval_expr(&arg_exprs[0], ctx)?;
            let b = eval_expr(&arg_exprs[1], ctx)?;
            if matches!(a, Value::Null | Value::Property(Property::Null))
                || matches!(b, Value::Null | Value::Property(Property::Null))
            {
                return Ok(Value::Null);
            }
            // Extract date and *local* time-of-day from each side
            // along with an optional tz offset (Some iff the value
            // was zoned). When both sides are zoned, shift the local
            // tod back to UTC so tz differences contribute to the
            // duration. Mixed zoned/local uses the local wall-clock
            // on both sides — this is what Neo4j does.
            let (mut a_date, mut a_tod, mut a_tz, mut a_kind) = temporal_to_date_tod(&a)?;
            let (mut b_date, mut b_tod, mut b_tz, mut b_kind) = temporal_to_date_tod(&b)?;
            // A TimeOnly value has no calendar date; when paired with
            // a zoned datetime, Neo4j anchors the time on that
            // datetime's local date (so `localtime(0:00)` to
            // `datetime(Oct 29 4:00 EU/STK)` on DST fall-back day
            // gives PT5H, not PT4H). Promote the time-only side so
            // the rest of the logic can treat both uniformly.
            if a_kind == TemporalKind::TimeOnly && b_kind == TemporalKind::HasDate {
                a_date = b_date;
                a_kind = TemporalKind::HasDate;
            } else if b_kind == TemporalKind::TimeOnly && a_kind == TemporalKind::HasDate {
                b_date = a_date;
                b_kind = TemporalKind::HasDate;
            }
            // If exactly one side carries a zone + named tz, borrow
            // that zone to interpret the other side's wall-clock —
            // matches Neo4j's DST-day behaviour (Oct 29 2017 in
            // Stockholm is 25h long from a zoned vs. naive midnight).
            let a_tz_name = match &a {
                Value::Property(Property::DateTime { tz_name, .. }) => tz_name.clone(),
                _ => None,
            };
            let b_tz_name = match &b {
                Value::Property(Property::DateTime { tz_name, .. }) => tz_name.clone(),
                _ => None,
            };
            match (&a_tz_name, &b_tz_name) {
                (Some(name), None) if b_tz.is_none() => {
                    let b_local_ns = (b_date as i128) * 86_400_000_000_000 + b_tod;
                    if let Some((off, _)) = parse_tz_name_local(name, b_local_ns) {
                        b_tz = Some(off);
                    }
                }
                (None, Some(name)) if a_tz.is_none() => {
                    let a_local_ns = (a_date as i128) * 86_400_000_000_000 + a_tod;
                    if let Some((off, _)) = parse_tz_name_local(name, a_local_ns) {
                        a_tz = Some(off);
                    }
                }
                _ => {}
            }
            if a_tz.is_some() && b_tz.is_some() {
                let shift_to_utc = |days: &mut i64, tod: &mut i128, off: i32| {
                    *tod -= (off as i128) * 1_000_000_000;
                    while *tod < 0 {
                        *tod += 86_400_000_000_000;
                        *days -= 1;
                    }
                    while *tod >= 86_400_000_000_000 {
                        *tod -= 86_400_000_000_000;
                        *days += 1;
                    }
                };
                shift_to_utc(&mut a_date, &mut a_tod, a_tz.unwrap());
                shift_to_utc(&mut b_date, &mut b_tod, b_tz.unwrap());
            }
            // When either side has no date (Time/LocalTime), collapse
            // to a time-only difference. Neo4j drops the calendar
            // component of the other side in this case.
            let time_only = a_kind == TemporalKind::TimeOnly
                || b_kind == TemporalKind::TimeOnly;
            let name_lc = name.to_ascii_lowercase();
            let duration = match name_lc.as_str() {
                "duration.inmonths" => {
                    let months = if time_only {
                        0
                    } else {
                        month_diff_tod(a_date, a_tod, b_date, b_tod)
                    };
                    mesh_core::Duration {
                        months,
                        days: 0,
                        seconds: 0,
                        nanos: 0,
                    }
                }
                "duration.indays" => {
                    // Floor-toward-zero. Raw day count is the
                    // calendar diff; subtract one whole day if we
                    // haven't yet crossed back through the source's
                    // time-of-day (so 21:40 on day N to 00:00 on
                    // day N+338 is 337 days, not 338).
                    let days = if time_only {
                        0
                    } else {
                        let mut d = b_date - a_date;
                        if d > 0 && b_tod < a_tod {
                            d -= 1;
                        } else if d < 0 && b_tod > a_tod {
                            d += 1;
                        }
                        d
                    };
                    mesh_core::Duration {
                        months: 0,
                        days,
                        seconds: 0,
                        nanos: 0,
                    }
                }
                "duration.inseconds" => {
                    let days = if time_only { 0 } else { b_date - a_date };
                    let total_nanos = (days as i128) * 86_400_000_000_000 + b_tod - a_tod;
                    let seconds = (total_nanos / 1_000_000_000) as i64;
                    let nanos = (total_nanos % 1_000_000_000) as i32;
                    mesh_core::Duration {
                        months: 0,
                        days: 0,
                        seconds,
                        nanos,
                    }
                }
                _ => {
                    // duration.between: calendar-aware when both have
                    // dates; pure time-diff otherwise.
                    if time_only {
                        let diff_ns = b_tod - a_tod;
                        let seconds = diff_ns.div_euclid(1_000_000_000) as i64;
                        let nanos = diff_ns.rem_euclid(1_000_000_000) as i32;
                        mesh_core::Duration {
                            months: 0,
                            days: 0,
                            seconds,
                            nanos,
                        }
                    } else {
                        duration_between_calendar(a_date, a_tod, b_date, b_tod)
                    }
                }
            };
            Ok(Value::Property(Property::Duration(duration)))
        }

        // Current-time namespace functions. They normally take no
        // args, but the TCK exercises null propagation by calling
        // `<func>(null)`, so accept a single null arg too.
        "datetime.transaction" | "datetime.statement" | "datetime.realtime" => {
            if let Some(null) = null_arg(arg_exprs, ctx)? {
                return Ok(null);
            }
            Ok(Value::Property(Property::DateTime {
                nanos: now_epoch_nanos(),
                tz_offset_secs: Some(0),
                tz_name: None,
            }))
        }
        // `datetime.fromepoch(seconds, nanos)` — pair of integers.
        // `datetime.fromepochmillis(ms)` — single integer.
        "datetime.fromepoch" => {
            if arg_exprs.len() != 2 {
                return Err(Error::UnknownScalarFunction(
                    "datetime.fromepoch() expects (seconds, nanoseconds)".into(),
                ));
            }
            let s = eval_expr(&arg_exprs[0], ctx)?;
            let n = eval_expr(&arg_exprs[1], ctx)?;
            let to_i64 = |v: Value| -> Option<i64> {
                match v {
                    Value::Property(Property::Int64(i)) => Some(i),
                    _ => None,
                }
            };
            let (Some(seconds), Some(sub)) = (to_i64(s), to_i64(n)) else {
                return Err(Error::TypeMismatch);
            };
            let nanos = (seconds as i128) * 1_000_000_000 + sub as i128;
            Ok(Value::Property(Property::DateTime {
                nanos,
                tz_offset_secs: Some(0),
                tz_name: None,
            }))
        }
        "datetime.fromepochmillis" => {
            if arg_exprs.len() != 1 {
                return Err(Error::UnknownScalarFunction(
                    "datetime.fromepochmillis() expects a single integer".into(),
                ));
            }
            let v = eval_expr(&arg_exprs[0], ctx)?;
            let ms: i64 = match v {
                Value::Property(Property::Int64(i)) => i,
                _ => return Err(Error::TypeMismatch),
            };
            Ok(Value::Property(Property::DateTime {
                nanos: (ms as i128) * 1_000_000,
                tz_offset_secs: Some(0),
                tz_name: None,
            }))
        }
        "localdatetime.transaction" | "localdatetime.statement" | "localdatetime.realtime" => {
            if let Some(null) = null_arg(arg_exprs, ctx)? {
                return Ok(null);
            }
            Ok(Value::Property(Property::LocalDateTime(now_epoch_nanos())))
        }
        "date.transaction" | "date.statement" | "date.realtime" => {
            if let Some(null) = null_arg(arg_exprs, ctx)? {
                return Ok(null);
            }
            let days = now_epoch_nanos().div_euclid(86_400_000_000_000) as i64;
            Ok(Value::Property(Property::Date(days)))
        }
        "time.transaction" | "time.statement" | "time.realtime" => {
            if let Some(null) = null_arg(arg_exprs, ctx)? {
                return Ok(null);
            }
            let time_nanos = (now_epoch_nanos() % 86_400_000_000_000) as i64;
            Ok(Value::Property(Property::Time {
                nanos: time_nanos,
                tz_offset_secs: Some(0),
            }))
        }
        "localtime.transaction" | "localtime.statement" | "localtime.realtime" => {
            if let Some(null) = null_arg(arg_exprs, ctx)? {
                return Ok(null);
            }
            let time_nanos = (now_epoch_nanos() % 86_400_000_000_000) as i64;
            Ok(Value::Property(Property::Time {
                nanos: time_nanos,
                tz_offset_secs: None,
            }))
        }

        // Temporal truncation functions
        "datetime.truncate" | "localdatetime.truncate" | "date.truncate"
        | "time.truncate" | "localtime.truncate" => {
            if arg_exprs.is_empty() || arg_exprs.len() > 3 {
                return Err(Error::UnknownScalarFunction(format!(
                    "{name}() expects 1-3 arguments, got {}",
                    arg_exprs.len()
                )));
            }
            let unit = eval_expr(&arg_exprs[0], ctx)?;
            let temporal = if arg_exprs.len() > 1 {
                eval_expr(&arg_exprs[1], ctx)?
            } else {
                // Default to current time
                Value::Property(Property::DateTime {
                    nanos: now_epoch_nanos(),
                    tz_offset_secs: Some(0),
                    tz_name: None,
                })
            };
            let overrides = if arg_exprs.len() > 2 {
                match eval_expr(&arg_exprs[2], ctx)? {
                    Value::Property(Property::Map(m)) => Some(m),
                    _ => None,
                }
            } else {
                None
            };
            if matches!(temporal, Value::Null | Value::Property(Property::Null)) {
                return Ok(Value::Null);
            }
            let unit_str = match &unit {
                Value::Property(Property::String(s)) => s.to_ascii_lowercase(),
                _ => return Err(Error::TypeMismatch),
            };
            truncate_temporal(name, &unit_str, &temporal, overrides.as_ref())
        }

        _ => Err(Error::UnknownScalarFunction(name.to_string())),
    }
}

/// Extract epoch milliseconds from a temporal value.
/// Extract a base date from a temporal-typed field in the map.
/// Looks at keys `date`, `datetime`, `localdatetime`, `time` — any
/// of which may carry a Date or DateTime value.
fn extract_base_date(
    m: &std::collections::HashMap<String, Property>,
    epoch: &chrono::NaiveDate,
) -> Option<chrono::NaiveDate> {
    for key in ["date", "datetime", "localdatetime"] {
        match m.get(key) {
            Some(Property::Date(d)) => {
                return Some(*epoch + chrono::Duration::days(*d as i64));
            }
            Some(Property::DateTime {
                nanos: ns,
                tz_offset_secs,        tz_name: _,

            }) => {
                // Use the *local* day for the base, matching the
                // Neo4j behavior where date() / datetime() work in
                // the zone the source DateTime was authored in.
                let local = match tz_offset_secs {
                    Some(off) => *ns + (*off as i128) * 1_000_000_000,
                    None => *ns,
                };
                let days = local.div_euclid(86_400_000_000_000);
                if let Ok(d) = i64::try_from(days) {
                    return Some(*epoch + chrono::Duration::days(d));
                }
            }
            Some(Property::LocalDateTime(ns)) => {
                let days = ns.div_euclid(86_400_000_000_000);
                if let Ok(d) = i64::try_from(days) {
                    return Some(*epoch + chrono::Duration::days(d));
                }
            }
            _ => {}
        }
    }
    None
}

/// Extract (base_date, time_of_day_nanos) from the map's temporal fields.
/// Extract a timezone offset (in seconds) from a datetime/time/localdatetime
/// construction map. Looks at an explicit `timezone` string first, then
/// any temporal-source value that carries an offset.
fn extract_tz_offset(
    m: &std::collections::HashMap<String, Property>,
) -> Option<i32> {
    extract_tz_spec(m, None).map(|(off, _)| off)
}

/// Offset of the *base* temporal value (under `datetime:` or `time:`
/// keys) when the caller needs to rotate the wall-clock against a
/// timezone override. Returns `None` if no base value carries an
/// offset.
///
/// `pivot` is the final local wall-clock being built. When the base
/// is a zoned DateTime with a named zone, we re-resolve the offset
/// *at the pivot instant* rather than reusing the offset captured
/// when the base was constructed — zones move across DST.
fn base_tz_offset(
    m: &std::collections::HashMap<String, Property>,
    pivot: i128,
) -> Option<i32> {
    for key in ["datetime", "time"] {
        match m.get(key) {
            Some(Property::DateTime {
                tz_offset_secs: Some(o),
                tz_name,
                ..
            }) => {
                if let Some(name) = tz_name.as_deref() {
                    if let Some((off, _)) = parse_tz_name_local(name, pivot) {
                        return Some(off);
                    }
                }
                return Some(*o);
            }
            Some(Property::Time {
                tz_offset_secs: Some(o),
                ..
            }) => return Some(*o),
            _ => {}
        }
    }
    None
}

/// Resolve `(offset_seconds, tz_name)` from a construction map.
/// Preference order:
///   1. explicit `timezone:` string — either an IANA name (resolved
///      against `local_nanos` so DST is correct), or a `+HH:MM`-style
///      offset.
///   2. `datetime:` / `time:` base value already carrying an offset
///      (and optionally a name, for datetimes).
///
/// Returns `None` if nothing in the map specifies a zone.
fn extract_tz_spec(
    m: &std::collections::HashMap<String, Property>,
    local_nanos: Option<i128>,
) -> Option<(i32, Option<String>)> {
    if let Some(Property::String(tz)) = m.get("timezone") {
        if let Some(off) = parse_tz_offset(tz) {
            return Some((off, None));
        }
        // Named zone. Resolve against the supplied local wall-clock
        // if we have it; otherwise fall back to the epoch and accept
        // the historical offset.
        let pivot = local_nanos.unwrap_or(0);
        if let Some((off, name)) = parse_tz_name_local(tz, pivot) {
            return Some((off, Some(name)));
        }
        // Unknown zone string — preserve a literal offset of 0 so
        // the rest of the query still runs, and leave the name as
        // None so we don't emit a bogus [Region/City] suffix.
        return Some((0, None));
    }
    for key in ["datetime", "time"] {
        match m.get(key) {
            Some(Property::DateTime {
                tz_offset_secs: Some(o),
                tz_name,
                ..
            }) => return Some((*o, tz_name.clone())),
            Some(Property::Time {
                tz_offset_secs: Some(o),
                ..
            }) => return Some((*o, None)),
            _ => {}
        }
    }
    None
}

fn extract_base_date_tod(
    m: &std::collections::HashMap<String, Property>,
    epoch: &chrono::NaiveDate,
) -> (Option<chrono::NaiveDate>, i128) {
    // Check any of the temporal-source keys; accept any temporal value
    // type under any key (TCK tests sometimes put a LocalDateTime
    // under "time" or a Time under "datetime"). When both a
    // date-bearing key and a time-bearing key are present, the date
    // wins for the calendar portion and the time wins for the tod —
    // the `"time"` key is checked last so its tod overrides.
    let mut base_date: Option<chrono::NaiveDate> = None;
    let mut base_tod: i128 = 0;
    let has_time_key = m.contains_key("time");
    for key in ["date", "datetime", "localdatetime", "time"] {
        match m.get(key) {
            Some(Property::Date(d)) => {
                if base_date.is_none() {
                    base_date = Some(*epoch + chrono::Duration::days(*d as i64));
                }
            }
            Some(Property::DateTime {
                nanos: ns,
                tz_offset_secs,        tz_name: _,

            }) => {
                // DateTime stores UTC nanos but the TCK feeds it into
                // datetime()/date() maps expecting the *local* date
                // and time-of-day as the "base". Shift by the offset
                // before splitting. If the map also has a "time" key
                // the dedicated time value wins for the tod — so skip
                // setting base_tod here when we know a time override
                // is coming later.
                let local = match tz_offset_secs {
                    Some(off) => *ns + (*off as i128) * 1_000_000_000,
                    None => *ns,
                };
                let days = local.div_euclid(86_400_000_000_000);
                let tod = local.rem_euclid(86_400_000_000_000);
                if let Ok(d) = i64::try_from(days) {
                    if base_date.is_none() {
                        base_date = Some(*epoch + chrono::Duration::days(d));
                    }
                    if base_tod == 0 && !(has_time_key && key != "time") {
                        base_tod = tod;
                    }
                }
            }
            Some(Property::LocalDateTime(ns)) => {
                let days = ns.div_euclid(86_400_000_000_000);
                let tod = ns.rem_euclid(86_400_000_000_000);
                if let Ok(d) = i64::try_from(days) {
                    if base_date.is_none() {
                        base_date = Some(*epoch + chrono::Duration::days(d));
                    }
                    if base_tod == 0 && !(has_time_key && key != "time") {
                        base_tod = tod;
                    }
                }
            }
            Some(Property::Time { nanos, .. }) => {
                if base_tod == 0 {
                    base_tod = *nanos as i128;
                }
            }
            _ => {}
        }
    }
    (base_date, base_tod)
}

/// Build a date from a map of components, supporting multiple ways:
/// - year+month+day (default)
/// - year+week (+ optional dayOfWeek)
/// - year+ordinalDay
/// - year+quarter+dayOfQuarter
/// Uses `base` to default missing fields. If `has_base` is false,
/// defaults to 1970-01-01.
fn build_date_from_map(
    m: &std::collections::HashMap<String, Property>,
    base: chrono::NaiveDate,
    has_base: bool,
) -> chrono::NaiveDate {
    // Week-based date construction takes priority
    if let Some(week) = map_int(m, "week") {
        let year = map_int(m, "year").unwrap_or_else(|| {
            if has_base {
                base.iso_week().year() as i64
            } else {
                1970
            }
        });
        let dow = map_int(m, "dayOfWeek")
            .or_else(|| map_int(m, "weekDay"))
            .unwrap_or_else(|| {
                if has_base {
                    base.weekday().num_days_from_monday() as i64 + 1
                } else {
                    1 // Monday default for week-based dates without base
                }
            });
        return iso_week_date(year, week, dow);
    }
    // Ordinal day construction
    if let Some(ordinal) = map_int(m, "ordinalDay").or_else(|| map_int(m, "dayOfYear")) {
        let year = map_int(m, "year").unwrap_or(base.year() as i64);
        return chrono::NaiveDate::from_yo_opt(year as i32, ordinal as u32)
            .unwrap_or(base);
    }
    // Quarter-based construction
    if let Some(quarter) = map_int(m, "quarter") {
        let year = map_int(m, "year").unwrap_or(base.year() as i64);
        let q_start_month = ((quarter - 1) * 3 + 1) as u32;
        let q_start = chrono::NaiveDate::from_ymd_opt(year as i32, q_start_month, 1);
        // Compute day-of-quarter: explicit in map, else derive from base.
        let day_of_quarter = map_int(m, "dayOfQuarter").unwrap_or_else(|| {
            if has_base {
                // Day-of-quarter from base = (base - base's quarter start) + 1
                let base_q_start_month = ((base.month() - 1) / 3) * 3 + 1;
                let base_q_start =
                    chrono::NaiveDate::from_ymd_opt(base.year(), base_q_start_month, 1).unwrap();
                base.signed_duration_since(base_q_start).num_days() + 1
            } else if let Some(day) = map_int(m, "day") {
                day
            } else {
                1
            }
        });
        if let Some(start) = q_start {
            return start + chrono::Duration::days(day_of_quarter - 1);
        }
        return base;
    }
    // Default: year + month + day
    let year = map_int(m, "year").unwrap_or(base.year() as i64);
    let month = map_int(m, "month").unwrap_or(base.month() as i64);
    let day = map_int(m, "day").unwrap_or(base.day() as i64);
    chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32).unwrap_or(base)
}

/// Compute an ISO week-based date.
fn iso_week_date(year: i64, week: i64, dow: i64) -> chrono::NaiveDate {
    // ISO 8601: week 1 of a year contains January 4th.
    let jan4 = chrono::NaiveDate::from_ymd_opt(year as i32, 1, 4).unwrap();
    let jan4_weekday = jan4.weekday().num_days_from_monday() as i64;
    let week1_monday = jan4 - chrono::Duration::days(jan4_weekday);
    week1_monday + chrono::Duration::weeks(week - 1) + chrono::Duration::days(dow - 1)
}

/// Extract date component properties from a NaiveDate.
fn temporal_date_prop(d: &chrono::NaiveDate, key: &str) -> Result<Value> {
    Ok(Value::Property(Property::Int64(match key {
        "year" => d.year() as i64,
        "month" => d.month() as i64,
        "day" => d.day() as i64,
        "week" => d.iso_week().week() as i64,
        "weekYear" => d.iso_week().year() as i64,
        "dayOfWeek" | "weekDay" => d.weekday().num_days_from_monday() as i64 + 1,
        "dayOfYear" | "ordinalDay" => d.ordinal() as i64,
        "quarter" => ((d.month() - 1) / 3 + 1) as i64,
        "dayOfQuarter" => {
            let q_start_month = ((d.month() - 1) / 3) * 3 + 1;
            let q_start = chrono::NaiveDate::from_ymd_opt(d.year(), q_start_month, 1).unwrap();
            (d.signed_duration_since(q_start).num_days() + 1) as i64
        }
        _ => return Ok(Value::Null),
    })))
}

/// Accessors on a Duration value. The field set mirrors Neo4j's:
/// `years`, `quarters`, `months`, `weeks`, `days`, `hours`,
/// `minutes`, `seconds`, `milli/micro/nanoseconds` return cumulative
/// totals; the `...OfX` variants return the component-of-parent
/// residue (e.g. `monthsOfYear = months % 12`).
fn duration_accessor(d: &mesh_core::Duration, key: &str) -> Value {
    let months = d.months;
    let days = d.days;
    let seconds = d.seconds;
    let nanos = d.nanos as i64;
    let total_ns: i128 = (seconds as i128) * 1_000_000_000 + nanos as i128;
    let int = |v: i64| Value::Property(Property::Int64(v));
    match key {
        "years" => int(months / 12),
        "quarters" => int(months / 3),
        "months" => int(months),
        "weeks" => int(days / 7),
        "days" => int(days),
        "hours" => int(seconds / 3600),
        "minutes" => int(seconds / 60),
        "seconds" => int(seconds),
        "milliseconds" => int((total_ns / 1_000_000) as i64),
        "microseconds" => int((total_ns / 1_000) as i64),
        "nanoseconds" => int(total_ns as i64),
        "quartersOfYear" => int((months % 12) / 3),
        "monthsOfQuarter" => int(months % 3),
        "monthsOfYear" => int(months % 12),
        "daysOfWeek" => int(days % 7),
        "minutesOfHour" => int((seconds % 3600) / 60),
        "secondsOfMinute" => int(seconds % 60),
        "millisecondsOfSecond" => int(nanos / 1_000_000),
        "microsecondsOfSecond" => int(nanos / 1_000),
        "nanosecondsOfSecond" => int(nanos),
        _ => Value::Null,
    }
}

/// Format a tz offset as `+HH:MM` / `-HH:MM` / `Z`.
fn format_offset_str(offset: i32) -> String {
    if offset == 0 {
        return "Z".to_string();
    }
    let sign = if offset >= 0 { '+' } else { '-' };
    let abs = offset.unsigned_abs();
    let oh = abs / 3600;
    let om = (abs % 3600) / 60;
    let os = abs % 60;
    // Include the seconds component only when non-zero, matching the
    // `+HH:MM[:SS]` form Neo4j round-trips.
    if os > 0 {
        format!("{sign}{oh:02}:{om:02}:{os:02}")
    } else {
        format!("{sign}{oh:02}:{om:02}")
    }
}

/// Shared accessor for the time-of-day components of a Time or the
/// time portion of a DateTime / LocalDateTime. `tod_nanos` is the
/// *local* time-of-day in nanoseconds since midnight.
fn temporal_tod_prop(tod_nanos: i64, key: &str) -> Option<Value> {
    let ns_of_sec = (tod_nanos % 1_000_000_000) as i64;
    let total_secs = tod_nanos / 1_000_000_000;
    let val: i64 = match key {
        "hour" => total_secs / 3600,
        "minute" => (total_secs % 3600) / 60,
        "second" => total_secs % 60,
        "millisecond" => ns_of_sec / 1_000_000,
        "microsecond" => ns_of_sec / 1_000,
        "nanosecond" => ns_of_sec,
        _ => return None,
    };
    Some(Value::Property(Property::Int64(val)))
}

/// Accessors on a Time value. `tz_offset_secs` is None for LocalTime.
fn time_accessor(nanos: i64, tz_offset_secs: Option<i32>, key: &str) -> Value {
    if let Some(v) = temporal_tod_prop(nanos, key) {
        return v;
    }
    match key {
        "timezone" | "offset" => match tz_offset_secs {
            Some(o) => Value::Property(Property::String(format_offset_str(o))),
            None => Value::Null,
        },
        "offsetMinutes" => match tz_offset_secs {
            Some(o) => Value::Property(Property::Int64((o / 60) as i64)),
            None => Value::Null,
        },
        "offsetSeconds" => match tz_offset_secs {
            Some(o) => Value::Property(Property::Int64(o as i64)),
            None => Value::Null,
        },
        _ => Value::Null,
    }
}

/// Accessors on a DateTime / LocalDateTime. `utc_nanos` is the
/// UTC-epoch nanoseconds stored on the value; `tz_offset_secs` is
/// None for LocalDateTime, Some(offset) for DateTime. The local
/// wall-clock is derived by shifting `utc_nanos` by the offset.
fn datetime_accessor(
    utc_nanos: i128,
    tz_offset_secs: Option<i32>,
    tz_name: Option<&str>,
    key: &str,
) -> Value {
    let local = match tz_offset_secs {
        Some(off) => utc_nanos + (off as i128) * 1_000_000_000,
        None => utc_nanos,
    };
    let local_secs = local.div_euclid(1_000_000_000) as i64;
    let ns_of_sec = local.rem_euclid(1_000_000_000) as i64;
    let tod = local.rem_euclid(86_400_000_000_000) as i64;
    let days = local.div_euclid(86_400_000_000_000) as i64;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date = epoch + chrono::Duration::days(days);
    // Calendar accessors land on the local date.
    match key {
        "year" | "month" | "day" | "week" | "weekYear" | "dayOfWeek"
        | "weekDay" | "dayOfYear" | "quarter" | "ordinalDay" | "dayOfQuarter" => {
            return temporal_date_prop(&date, key).unwrap_or(Value::Null);
        }
        _ => {}
    }
    if let Some(v) = temporal_tod_prop(tod, key) {
        return v;
    }
    match key {
        // epoch* stay on the UTC instant — that's the point of them.
        "epochMillis" => {
            let utc_secs = utc_nanos.div_euclid(1_000_000_000) as i64;
            let utc_nsec = utc_nanos.rem_euclid(1_000_000_000) as i64;
            Value::Property(Property::Int64(
                utc_secs * 1000 + utc_nsec / 1_000_000,
            ))
        }
        "epochSeconds" => Value::Property(Property::Int64(
            utc_nanos.div_euclid(1_000_000_000) as i64,
        )),
        // `timezone` reports the IANA name when one is on the value,
        // so `datetime({..., timezone: 'Europe/Stockholm'}).timezone`
        // round-trips. `offset` always reports the numeric `±HH:MM`
        // (or `Z`) derived from `tz_offset_secs`.
        "timezone" => match tz_name {
            Some(name) => Value::Property(Property::String(name.to_string())),
            None => match tz_offset_secs {
                Some(o) => Value::Property(Property::String(format_offset_str(o))),
                None => Value::Null,
            },
        },
        "offset" => match tz_offset_secs {
            Some(o) => Value::Property(Property::String(format_offset_str(o))),
            None => Value::Null,
        },
        "offsetMinutes" => match tz_offset_secs {
            Some(o) => Value::Property(Property::Int64((o / 60) as i64)),
            None => Value::Null,
        },
        "offsetSeconds" => match tz_offset_secs {
            Some(o) => Value::Property(Property::Int64(o as i64)),
            None => Value::Null,
        },
        _ => {
            // Suppress the unused-variable warning when no branch
            // above consumes the fine-grained pieces.
            let _ = (local_secs, ns_of_sec);
            Value::Null
        }
    }
}

/// Classification of a temporal value for duration.between:
/// - HasDate: value carries a calendar date (Date, DateTime, LocalDateTime)
/// - TimeOnly: value has only a time-of-day (Time, LocalTime)
#[derive(Debug, Clone, Copy, PartialEq)]
enum TemporalKind {
    HasDate,
    TimeOnly,
}

/// Extract (date, local time-of-day nanos, optional tz offset secs, kind)
/// from any temporal value. "Zoned" means the value carries an explicit
/// offset (DateTime always, Time only when a zone was supplied).
fn temporal_to_date_tod(
    v: &Value,
) -> Result<(i64, i128, Option<i32>, TemporalKind)> {
    match v {
        Value::Property(Property::Date(days)) => {
            Ok((*days, 0, None, TemporalKind::HasDate))
        }
        Value::Property(Property::DateTime {
            nanos: ns,
            tz_offset_secs,
            tz_name: _,
        }) => {
            let local = match tz_offset_secs {
                Some(off) => *ns + (*off as i128) * 1_000_000_000,
                None => *ns,
            };
            let days = local.div_euclid(86_400_000_000_000);
            let tod = local.rem_euclid(86_400_000_000_000);
            let days_i64 = i64::try_from(days).map_err(|_| Error::TypeMismatch)?;
            Ok((
                days_i64,
                tod,
                *tz_offset_secs,
                TemporalKind::HasDate,
            ))
        }
        Value::Property(Property::LocalDateTime(ns)) => {
            let days = ns.div_euclid(86_400_000_000_000);
            let tod = ns.rem_euclid(86_400_000_000_000);
            let days_i64 = i64::try_from(days).map_err(|_| Error::TypeMismatch)?;
            Ok((days_i64, tod, None, TemporalKind::HasDate))
        }
        Value::Property(Property::Time { nanos, tz_offset_secs }) => Ok((
            0,
            *nanos as i128,
            *tz_offset_secs,
            TemporalKind::TimeOnly,
        )),
        _ => Err(Error::TypeMismatch),
    }
}

/// Calculate the month difference between two dates (each expressed
/// as days since the 1970 epoch). Rounds toward zero: if we haven't
/// reached the target day-of-month, back off by one.
fn month_diff(a_days: i64, b_days: i64) -> i64 {
    let (ay, am, ad) = big_days_to_ymd(a_days);
    let (by, bm, bd) = big_days_to_ymd(b_days);
    let a_months = ay * 12 + (am as i64 - 1);
    let b_months = by * 12 + (bm as i64 - 1);
    let mut diff = b_months - a_months;
    if diff > 0 && bd < ad {
        diff -= 1;
    } else if diff < 0 && bd > ad {
        diff += 1;
    }
    diff
}

/// Like `month_diff`, but also considers time-of-day when the two
/// endpoints fall on the same day-of-month.
fn month_diff_tod(
    a_days: i64,
    a_tod: i128,
    b_days: i64,
    b_tod: i128,
) -> i64 {
    let mut diff = month_diff(a_days, b_days);
    if diff == 0 {
        return 0;
    }
    let (_ay, _am, ad) = big_days_to_ymd(a_days);
    let (_by, _bm, bd) = big_days_to_ymd(b_days);
    if ad == bd {
        if diff > 0 && b_tod < a_tod {
            diff -= 1;
        } else if diff < 0 && b_tod > a_tod {
            diff += 1;
        }
    }
    diff
}

/// Compute a calendar-aware duration between two temporal points
/// (date + time-of-day nanoseconds). Matches Neo4j's duration.between
/// semantics.
fn duration_between_calendar(
    a_days: i64,
    a_tod: i128,
    b_days: i64,
    b_tod: i128,
) -> mesh_core::Duration {
    // Compute years/months/days first using pure integer arithmetic
    // (so ±10^9-year ranges don't overflow chrono's TimeDelta).
    let mut months = month_diff(a_days, b_days);
    let after_months_days = add_months_to_days(a_days, months);
    let mut days = b_days - after_months_days;

    let mut tod_diff = b_tod - a_tod;
    if tod_diff < 0 && days > 0 {
        days -= 1;
        tod_diff += 86_400_000_000_000;
    } else if tod_diff > 0 && days < 0 {
        days += 1;
        tod_diff -= 86_400_000_000_000;
    }
    if months > 0 && days < 0 {
        months -= 1;
        let after_months_days = add_months_to_days(a_days, months);
        days = b_days - after_months_days;
        if tod_diff < 0 && days > 0 {
            days -= 1;
            tod_diff += 86_400_000_000_000;
        }
    } else if months < 0 && days > 0 {
        months += 1;
        let after_months_days = add_months_to_days(a_days, months);
        days = b_days - after_months_days;
        if tod_diff > 0 && days < 0 {
            days += 1;
            tod_diff -= 86_400_000_000_000;
        }
    }

    let seconds = tod_diff.div_euclid(1_000_000_000) as i64;
    let nanos = tod_diff.rem_euclid(1_000_000_000) as i32;

    mesh_core::Duration {
        months,
        days,
        seconds,
        nanos,
    }
}

/// Add `months` to the given epoch-day. Preserves day-of-month where
/// possible and clamps to the end of the target month otherwise
/// (Jan 31 + 1 month → Feb 28/29). Works for the full i64 range.
fn add_months_to_days(days: i64, months: i64) -> i64 {
    let (y, m, d) = big_days_to_ymd(days);
    let total_months = y * 12 + (m as i64 - 1) + months;
    let new_year = total_months.div_euclid(12);
    let new_month = (total_months.rem_euclid(12) + 1) as u32;
    let max_day = days_in_month_big(new_year, new_month);
    let new_day = d.min(max_day);
    big_ymd_to_days(new_year, new_month, new_day).unwrap_or(days)
}

fn days_in_month_big(year: i64, month: u32) -> u32 {
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        _ => 28,
    }
}

fn temporal_to_nanos(v: &Value) -> Result<i128> {
    match v {
        Value::Property(Property::DateTime { nanos: ns, .. })
        | Value::Property(Property::LocalDateTime(ns)) => Ok(*ns),
        Value::Property(Property::Date(days)) => Ok(*days as i128 * 86_400_000_000_000),
        _ => Err(Error::TypeMismatch),
    }
}

/// Truncate a time-of-day from a source temporal. Used by both
/// `time.truncate(...)` and `localtime.truncate(...)`. The source
/// can be any temporal type carrying a time component (Time,
/// LocalDateTime, DateTime); Date inputs are treated as 00:00.
fn truncate_time_value(
    name: &str,
    unit: &str,
    temporal: &Value,
    overrides: Option<&std::collections::HashMap<String, Property>>,
) -> Result<Value> {
    let is_zoned = name.starts_with("time.");
    let (tod_nanos, source_tz) = match temporal {
        Value::Property(Property::Time {
            nanos,
            tz_offset_secs,
        }) => (*nanos as i128, *tz_offset_secs),
        Value::Property(Property::LocalDateTime(ns)) => (
            ns.rem_euclid(86_400_000_000_000),
            None::<i32>,
        ),
        Value::Property(Property::DateTime {
            nanos,
            tz_offset_secs,        tz_name: _,

        }) => {
            let local = match tz_offset_secs {
                Some(off) => *nanos + (*off as i128) * 1_000_000_000,
                None => *nanos,
            };
            (local.rem_euclid(86_400_000_000_000), *tz_offset_secs)
        }
        Value::Property(Property::Date(_)) => (0_i128, None),
        _ => return Err(Error::TypeMismatch),
    };
    // Truncate to the requested unit.
    let truncated: i128 = match unit {
        "day" => 0,
        "hour" => tod_nanos - tod_nanos.rem_euclid(3_600_000_000_000),
        "minute" => tod_nanos - tod_nanos.rem_euclid(60_000_000_000),
        "second" => tod_nanos - tod_nanos.rem_euclid(1_000_000_000),
        "millisecond" => tod_nanos - tod_nanos.rem_euclid(1_000_000),
        "microsecond" => tod_nanos - tod_nanos.rem_euclid(1_000),
        _ => {
            return Err(Error::UnknownScalarFunction(format!(
                "unsupported truncation unit for time: {unit}"
            )));
        }
    };
    // Override fields: hour/minute/second/millisecond/microsecond/nanosecond,
    // plus an optional `timezone` that rotates the output zone (only
    // meaningful for `time.truncate`, since localtime has no zone).
    let mut final_nanos = truncated;
    let mut override_tz: Option<i32> = None;
    if let Some(ov) = overrides {
        for (k, v) in ov.iter() {
            if k == "timezone" {
                if let Property::String(tz) = v {
                    if let Some(off) = parse_tz_offset(tz) {
                        override_tz = Some(off);
                    } else if let Some((off, _)) = parse_tz_name_local(tz, 0) {
                        override_tz = Some(off);
                    }
                }
                continue;
            }
            let n = match v {
                Property::Int64(n) => *n,
                _ => continue,
            };
            match k.as_str() {
                "hour" => {
                    // Replace the hour-of-day.
                    let sub = final_nanos.rem_euclid(3_600_000_000_000);
                    final_nanos = (n as i128) * 3_600_000_000_000 + sub;
                }
                "minute" => {
                    let hours = final_nanos / 3_600_000_000_000;
                    let sub_minute = final_nanos.rem_euclid(60_000_000_000);
                    final_nanos =
                        hours * 3_600_000_000_000 + (n as i128) * 60_000_000_000 + sub_minute;
                }
                "second" => {
                    let minute_aligned = final_nanos - final_nanos.rem_euclid(60_000_000_000);
                    let sub_second = final_nanos.rem_euclid(1_000_000_000);
                    final_nanos = minute_aligned + (n as i128) * 1_000_000_000 + sub_second;
                }
                // Sub-second overrides add on top of whatever sub-second
                // precision the truncation unit preserved, so `truncate
                // ('millisecond', ..., {nanosecond: 2})` keeps the
                // millisecond portion and tacks on 2 nanoseconds.
                "millisecond" => {
                    final_nanos += (n as i128) * 1_000_000;
                }
                "microsecond" => {
                    final_nanos += (n as i128) * 1_000;
                }
                "nanosecond" => {
                    final_nanos += n as i128;
                }
                _ => {}
            }
        }
    }
    // Pick the output zone: override wins, otherwise inherit from
    // the source; `localtime.truncate` stays zone-less either way.
    let tz_offset_secs = if is_zoned {
        Some(override_tz.or(source_tz).unwrap_or(0))
    } else {
        None
    };
    Ok(Value::Property(Property::Time {
        nanos: final_nanos as i64,
        tz_offset_secs,
    }))
}

/// Truncate a temporal value to the specified unit.
fn truncate_temporal(
    name: &str,
    unit: &str,
    temporal: &Value,
    overrides: Option<&std::collections::HashMap<String, Property>>,
) -> Result<Value> {
    // time.truncate / localtime.truncate produce a Time value. Pull
    // the local time-of-day out of whatever source temporal we were
    // given, truncate that, apply overrides, and wrap.
    if name.starts_with("time.") || name.starts_with("localtime.") {
        return truncate_time_value(name, unit, temporal, overrides);
    }
    match temporal {
        Value::Property(Property::DateTime { .. })
        | Value::Property(Property::LocalDateTime(_))
        | Value::Property(Property::Date(_)) => {
            // For datetime() we truncate the *local* wall-clock, not
            // the UTC instant — Neo4j rounds within the zone where
            // the value was authored. Shift back to UTC at the end.
            let source_tz: Option<i32> = match temporal {
                Value::Property(Property::DateTime { tz_offset_secs, .. }) => *tz_offset_secs,
                _ => None,
            };
            let source_tz_name: Option<String> = match temporal {
                Value::Property(Property::DateTime { tz_name, .. }) => tz_name.clone(),
                _ => None,
            };
            let epoch_ns: i128 = match temporal {
                Value::Property(Property::DateTime { nanos: ns, .. }) => match source_tz {
                    Some(off) => *ns + (off as i128) * 1_000_000_000,
                    None => *ns,
                },
                Value::Property(Property::LocalDateTime(ns)) => *ns,
                Value::Property(Property::Date(days)) => *days as i128 * 86_400_000_000_000,
                _ => unreachable!(),
            };
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let (secs, nanos) = nanos_to_secs_nanos(epoch_ns);
            let dt = chrono::DateTime::from_timestamp(secs, nanos)
                .map(|d| d.naive_utc())
                .unwrap_or_else(|| epoch.and_hms_opt(0, 0, 0).unwrap());
            let truncated = match unit.as_ref() {
                "millennium" => {
                    // Neo4j: year / 1000 * 1000 (e.g., 2017 → 2000)
                    let y = (dt.year() / 1000) * 1000;
                    chrono::NaiveDate::from_ymd_opt(y, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                }
                "century" => {
                    // Neo4j: year / 100 * 100 (e.g., 1984 → 1900)
                    let y = (dt.year() / 100) * 100;
                    chrono::NaiveDate::from_ymd_opt(y, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                }
                "decade" => {
                    let y = (dt.year() / 10) * 10;
                    chrono::NaiveDate::from_ymd_opt(y, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                }
                "year" => chrono::NaiveDate::from_ymd_opt(dt.year(), 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                "weekyear" => {
                    // Jump to Mon, week 1 of the ISO week-year that
                    // owns `dt`. This differs from `week`: at year
                    // boundaries the iso week year can disagree with
                    // the calendar year (Jan 1 1984 is in weekYear
                    // 1983 since it falls on a Sunday).
                    use chrono::Datelike;
                    let iso = dt.iso_week();
                    let wy = iso.year();
                    chrono::NaiveDate::from_isoywd_opt(wy, 1, chrono::Weekday::Mon)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                }
                "week" => {
                    let weekday = dt.weekday().num_days_from_monday();
                    let d = dt.date() - chrono::Duration::days(weekday as i64);
                    d.and_hms_opt(0, 0, 0).unwrap()
                }
                "quarter" => {
                    let q = (dt.month() - 1) / 3;
                    chrono::NaiveDate::from_ymd_opt(dt.year(), q * 3 + 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                }
                "month" => chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                "day" => dt.date().and_hms_opt(0, 0, 0).unwrap(),
                "hour" => {
                    dt.date()
                        .and_hms_opt(dt.hour(), 0, 0)
                        .unwrap()
                }
                "minute" => {
                    dt.date()
                        .and_hms_opt(dt.hour(), dt.minute(), 0)
                        .unwrap()
                }
                "second" => {
                    dt.date()
                        .and_hms_opt(dt.hour(), dt.minute(), dt.second())
                        .unwrap()
                }
                "millisecond" => {
                    // Drop sub-millisecond precision — floor the
                    // nanoseconds to the nearest 1e6.
                    let ns = (dt.nanosecond() / 1_000_000) * 1_000_000;
                    dt.date()
                        .and_hms_nano_opt(dt.hour(), dt.minute(), dt.second(), ns)
                        .unwrap_or(dt)
                }
                "microsecond" => {
                    let ns = (dt.nanosecond() / 1_000) * 1_000;
                    dt.date()
                        .and_hms_nano_opt(dt.hour(), dt.minute(), dt.second(), ns)
                        .unwrap_or(dt)
                }
                _ => {
                    return Err(Error::UnknownScalarFunction(format!(
                        "unsupported truncation unit: {unit}"
                    )));
                }
            };
            // Apply overrides map (extra day/month/hour/etc. components).
            // An override `timezone:` key also steers the output zone —
            // for datetime.truncate this replaces whatever zone came
            // from the source temporal. Captured here, applied after
            // we've rebuilt the final naive wall-clock.
            let mut override_tz: Option<(i32, Option<String>)> = None;
            let truncated = if let Some(ov) = overrides {
                let mut year = truncated.year();
                let mut month = truncated.month();
                let mut day = truncated.day();
                let mut hour = truncated.hour();
                let mut minute = truncated.minute();
                let mut second = truncated.second();
                let mut nanosecond = truncated.nanosecond();
                for (k, v) in ov.iter() {
                    if k == "timezone" {
                        if let Property::String(tz) = v {
                            if let Some(off) = parse_tz_offset(tz) {
                                override_tz = Some((off, None));
                            } else {
                                // Defer zone-name resolution — we need
                                // the final local wall-clock (post
                                // override) to pick the right DST
                                // offset. Stash the name for now.
                                override_tz = Some((0, Some(tz.clone())));
                            }
                        }
                        continue;
                    }
                    let n = match v {
                        Property::Int64(n) => *n,
                        _ => continue,
                    };
                    match k.as_str() {
                        "year" => year = n as i32,
                        "month" => month = n as u32,
                        "day" => day = n as u32,
                        "hour" => hour = n as u32,
                        "minute" => minute = n as u32,
                        "second" => second = n as u32,
                        // Neo4j treats sub-second overrides as
                        // *additive* on top of whatever the truncation
                        // unit left behind, so `truncate('millisecond',
                        // ..., {nanosecond: 2})` yields `.645000002`
                        // rather than `.000000002`.
                        "nanosecond" => nanosecond = nanosecond.wrapping_add(n as u32),
                        "millisecond" => {
                            nanosecond = nanosecond.wrapping_add((n as u32) * 1_000_000)
                        }
                        "microsecond" => {
                            nanosecond = nanosecond.wrapping_add((n as u32) * 1_000)
                        }
                        "dayOfWeek" | "weekDay" => {
                            // Adjust day based on weekday within week
                            let current_dow =
                                truncated.weekday().num_days_from_monday() as i64 + 1;
                            let target_dow = n;
                            let delta = target_dow - current_dow;
                            let new_date = truncated.date()
                                + chrono::Duration::days(delta);
                            year = new_date.year();
                            month = new_date.month();
                            day = new_date.day();
                        }
                        _ => {}
                    }
                }
                chrono::NaiveDate::from_ymd_opt(year, month, day)
                    .and_then(|d| d.and_hms_nano_opt(hour, minute, second, nanosecond))
                    .unwrap_or(truncated)
            } else {
                truncated
            };
            let diff = truncated
                .signed_duration_since(epoch.and_hms_opt(0, 0, 0).unwrap());
            // Use seconds + nanoseconds to build epoch nanos
            let result_nanos: i128 = (diff.num_seconds() as i128) * 1_000_000_000
                + (diff.subsec_nanos() as i128);
            if name.starts_with("date.") {
                let days = diff.num_days();
                Ok(Value::Property(Property::Date(days)))
            } else if name.starts_with("localdatetime.") {
                Ok(Value::Property(Property::LocalDateTime(result_nanos)))
            } else {
                // `result_nanos` is local wall-clock. Choose which
                // zone to report: a `timezone:` override wins,
                // otherwise fall back to the source's zone. For a
                // zone name override we resolve against the final
                // local wall-clock so DST lands on the right side.
                let (out_offset, out_name) = match &override_tz {
                    Some((_, Some(name))) => match parse_tz_name_local(name, result_nanos) {
                        Some((off, canonical)) => (Some(off), Some(canonical)),
                        None => (Some(0), None),
                    },
                    Some((off, None)) => (Some(*off), None),
                    None => (source_tz, source_tz_name.clone()),
                };
                let utc_nanos = match out_offset {
                    Some(off) => result_nanos - (off as i128) * 1_000_000_000,
                    None => result_nanos,
                };
                Ok(Value::Property(Property::DateTime {
                    nanos: utc_nanos,
                    tz_offset_secs: out_offset,
                    tz_name: out_name,
                }))
            }
        }
        _ => {
            // For unsupported temporal types, pass through
            Ok(temporal.clone())
        }
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

/// Return `Some(Value::Null)` if the single-arg call was given null
/// (or Property::Null). Returns `None` for zero args or a non-null
/// single arg, so the caller can fall through to its normal path.
/// Anything else (more than one arg) is a TypeMismatch.
fn null_arg(args: &[Expr], ctx: &EvalCtx) -> Result<Option<Value>> {
    match args.len() {
        0 => Ok(None),
        1 => {
            let v = eval_expr(&args[0], ctx)?;
            if matches!(v, Value::Null | Value::Property(Property::Null)) {
                Ok(Some(Value::Null))
            } else {
                Ok(None)
            }
        }
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
        Value::Property(Property::Float64(f)) => {
            let s = if f == f.floor() && f.is_finite() {
                format!("{:.1}", f)
            } else {
                f.to_string()
            };
            Value::Property(Property::String(s))
        }
        Value::Property(Property::Bool(b)) => Value::Property(Property::String(b.to_string())),
        Value::Property(Property::Null) => Value::Null,
        Value::Property(Property::Date(days)) => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let d = epoch + chrono::Duration::days(days as i64);
            Value::Property(Property::String(d.format("%Y-%m-%d").to_string()))
        }
        Value::Property(Property::DateTime {
            nanos,
            tz_offset_secs,
            tz_name,
        }) => Value::Property(Property::String(format_datetime_with_tz(
            nanos,
            tz_offset_secs,
            tz_name.as_deref(),
        ))),
        Value::Property(Property::LocalDateTime(ns)) => {
            Value::Property(Property::String(format_datetime_string(ns)))
        }
        Value::Property(Property::Time {
            nanos,
            tz_offset_secs,
        }) => Value::Property(Property::String(format_time_string(nanos, tz_offset_secs))),
        Value::Property(Property::Duration(d)) => {
            Value::Property(Property::String(duration_to_iso_string(&d)))
        }
        other => Value::Property(Property::String(format!("{:?}", other))),
    }
}

/// Format a DateTime as ISO 8601 string.
fn format_datetime_string(epoch_nanos: i128) -> String {
    let days = epoch_nanos.div_euclid(86_400_000_000_000);
    let tod_ns = epoch_nanos.rem_euclid(86_400_000_000_000);
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date = epoch + chrono::Duration::days(days as i64);
    let h = (tod_ns / 3_600_000_000_000) as u32;
    let m = ((tod_ns % 3_600_000_000_000) / 60_000_000_000) as u32;
    let s = ((tod_ns % 60_000_000_000) / 1_000_000_000) as u32;
    let ns = (tod_ns % 1_000_000_000) as u32;
    let date_str = date.format("%Y-%m-%d").to_string();
    if ns > 0 {
        let frac = format!("{:09}", ns);
        let trimmed = frac.trim_end_matches('0');
        format!("{date_str}T{h:02}:{m:02}:{s:02}.{trimmed}")
    } else if s > 0 {
        format!("{date_str}T{h:02}:{m:02}:{s:02}")
    } else {
        format!("{date_str}T{h:02}:{m:02}")
    }
}

/// Format a DateTime (UTC epoch nanos + optional tz offset) as ISO 8601.
/// Shifts the nanos by the offset so the rendered time is local, then
/// appends the offset suffix (`Z` for 0, `+HH:MM` otherwise).
fn format_datetime_with_tz(
    epoch_nanos: i128,
    tz_offset_secs: Option<i32>,
    tz_name: Option<&str>,
) -> String {
    let shifted = match tz_offset_secs {
        Some(offset) => epoch_nanos + (offset as i128) * 1_000_000_000,
        None => epoch_nanos,
    };
    let body = format_datetime_string(shifted);
    // When a zone name is set, always render the offset explicitly
    // (even for UTC) so the form is `...+00:00[Zone]` rather than
    // `...Z[Zone]` — matches Neo4j and the TCK expectations.
    let tz_str = match tz_offset_secs {
        Some(0) if tz_name.is_none() => "Z".to_string(),
        Some(0) => "+00:00".to_string(),
        Some(offset) => format_offset_str(offset),
        None => String::new(),
    };
    let zone_str = match tz_name {
        Some(name) => format!("[{name}]"),
        None => String::new(),
    };
    format!("{body}{tz_str}{zone_str}")
}

/// Format a Time as ISO 8601 string.
fn format_time_string(nanos: i64, tz_offset_secs: Option<i32>) -> String {
    let total_secs = nanos / 1_000_000_000;
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    let subsec_nanos = (nanos % 1_000_000_000) as u32;
    let time_str = if subsec_nanos > 0 {
        let frac = format!("{:09}", subsec_nanos);
        let trimmed = frac.trim_end_matches('0');
        format!("{h:02}:{m:02}:{s:02}.{trimmed}")
    } else if s > 0 {
        format!("{h:02}:{m:02}:{s:02}")
    } else {
        format!("{h:02}:{m:02}")
    };
    let tz_str = match tz_offset_secs {
        Some(offset) => format_offset_str(offset),
        None => String::new(),
    };
    format!("{time_str}{tz_str}")
}

/// Format a Duration as ISO 8601 string.
fn duration_to_iso_string(d: &mesh_core::Duration) -> String {
    let months = d.months;
    let days = d.days;
    let seconds = d.seconds;
    let nanos = d.nanos;

    let years = months / 12;
    let rem_months = months % 12;

    let mut result = String::from("P");
    let mut any_date = false;
    if years != 0 {
        result.push_str(&format!("{}Y", years));
        any_date = true;
    }
    if rem_months != 0 {
        result.push_str(&format!("{}M", rem_months));
        any_date = true;
    }
    if days != 0 {
        result.push_str(&format!("{}D", days));
        any_date = true;
    }

    // Combine seconds and nanos into a signed total
    let total_ns_signed: i128 = (seconds as i128) * 1_000_000_000 + (nanos as i128);
    let has_time = total_ns_signed != 0;
    if has_time {
        result.push('T');
        let negative = total_ns_signed < 0;
        let abs_ns: i128 = total_ns_signed.unsigned_abs() as i128;
        let abs_secs: i128 = abs_ns / 1_000_000_000;
        let abs_nanos = (abs_ns % 1_000_000_000) as i32;
        let sign = if negative { "-" } else { "" };
        let hours = abs_secs / 3600;
        let minutes = (abs_secs % 3600) / 60;
        let secs_only = abs_secs % 60;
        if hours > 0 {
            result.push_str(&format!("{}{}H", sign, hours));
        }
        if minutes > 0 {
            result.push_str(&format!("{}{}M", sign, minutes));
        }
        if secs_only > 0 || abs_nanos > 0 || (hours == 0 && minutes == 0) {
            if abs_nanos > 0 {
                let frac_str = format!("{:09}", abs_nanos);
                let trimmed = frac_str.trim_end_matches('0');
                result.push_str(&format!("{}{}.{}S", sign, secs_only, trimmed));
            } else {
                result.push_str(&format!("{}{}S", sign, secs_only));
            }
        }
    } else if !any_date {
        result.push_str("T0S");
    }
    result
}

pub(crate) fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null)
        | (Value::Property(Property::Null), Value::Property(Property::Null)) => Ordering::Equal,
        (Value::Null | Value::Property(Property::Null), _) => Ordering::Greater,
        (_, Value::Null | Value::Property(Property::Null)) => Ordering::Less,
        (Value::List(la), Value::List(lb)) => {
            for (x, y) in la.iter().zip(lb.iter()) {
                let ord = compare_values(x, y);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            la.len().cmp(&lb.len())
        }
        (Value::Property(Property::List(la)), Value::Property(Property::List(lb))) => {
            for (x, y) in la.iter().zip(lb.iter()) {
                let ord = compare_props(x, y);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            la.len().cmp(&lb.len())
        }
        (Value::Property(ap), Value::Property(bp)) => compare_props(ap, bp),
        // Cross-type value ordering
        _ => type_order_value(a).cmp(&type_order_value(b)),
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
        (
            Property::DateTime { nanos: a, .. },
            Property::DateTime { nanos: b, .. },
        ) => a.cmp(b),
        (Property::LocalDateTime(a), Property::LocalDateTime(b)) => a.cmp(b),
        (Property::Date(a), Property::Date(b)) => a.cmp(b),
        (
            Property::Time {
                nanos: na,
                tz_offset_secs: tza,
            },
            Property::Time {
                nanos: nb,
                tz_offset_secs: tzb,
            },
        ) => {
            // Compare at the same instant: shift each local time to
            // UTC by subtracting its offset. Unzoned Time has no
            // offset — treat as UTC for ordering.
            let a_utc = *na - (tza.unwrap_or(0) as i64) * 1_000_000_000;
            let b_utc = *nb - (tzb.unwrap_or(0) as i64) * 1_000_000_000;
            a_utc.cmp(&b_utc)
        }
        (Property::Duration(a), Property::Duration(b)) => {
            // No total order in general, but TCK expects equal
            // durations to compare equal. Use component-wise cmp.
            (a.months, a.days, a.seconds, a.nanos)
                .cmp(&(b.months, b.days, b.seconds, b.nanos))
        }
        // Cross-type ordering: use type precedence
        // Neo4j order: Map > List > String > Boolean > Number
        _ => type_order_prop(a).cmp(&type_order_prop(b)),
    }
}

/// Type precedence for cross-type ordering (higher = sorts later in ASC).
/// Neo4j order: Map < Node < Relationship < List < Path < String < Boolean < Number < Null
fn type_order_prop(p: &Property) -> u8 {
    match p {
        Property::Map(_) => 1,
        Property::List(_) => 4,
        Property::String(_) => 6,
        Property::Bool(_) => 7,
        Property::Int64(_) | Property::Float64(_) => 8,
        Property::Date(_) | Property::DateTime { .. } | Property::LocalDateTime(_)
        | Property::Duration(_) | Property::Time { .. } => 9,
        Property::Null => 10,
    }
}

fn type_order_value(v: &Value) -> u8 {
    match v {
        Value::Property(Property::Map(_)) | Value::Map(_) => 1,
        Value::Node(_) => 2,
        Value::Edge(_) => 3,
        Value::List(_) | Value::Property(Property::List(_)) => 4,
        Value::Path { .. } => 5,
        Value::Property(Property::String(_)) => 6,
        Value::Property(Property::Bool(_)) => 7,
        Value::Property(Property::Int64(_) | Property::Float64(_)) => 8,
        Value::Property(
            Property::Date(_) | Property::DateTime { .. } | Property::LocalDateTime(_)
            | Property::Duration(_) | Property::Time { .. },
        ) => 9,
        Value::Null | Value::Property(Property::Null) => 10,
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
        Value::Property(Property::DateTime { nanos, tz_offset_secs, tz_name }) => {
            format!("dt:{},{:?},{:?}", nanos, tz_offset_secs, tz_name)
        }
        Value::Property(Property::LocalDateTime(ns)) => format!("ldt:{}", ns),
        Value::Property(Property::Date(days)) => format!("d:{}", days),
        Value::Property(Property::Duration(d)) => {
            format!("dur:{},{},{},{}", d.months, d.days, d.seconds, d.nanos)
        }
        Value::Property(Property::Time { nanos, tz_offset_secs }) => {
            format!("t:{},{:?}", nanos, tz_offset_secs)
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
        Value::Map(m) => {
            let mut keys: Vec<_> = m.keys().collect();
            keys.sort();
            let mut out = String::from("M:{");
            for k in keys {
                out.push_str(k);
                out.push('=');
                out.push_str(&value_key(&m[k]));
                out.push(',');
            }
            out.push('}');
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
        Value::Null | Value::Property(Property::Null) => Ok(false),
        _ => Err(Error::NotBoolean),
    }
}

/// Three-valued to_bool: returns None for NULL (unknown), Some(b) for booleans.
/// Used by logical operators (AND/OR/NOT/XOR) to implement proper NULL propagation.
fn to_bool_3v(v: &Value) -> Result<Option<bool>> {
    match v {
        Value::Property(Property::Bool(b)) => Ok(Some(*b)),
        Value::Null | Value::Property(Property::Null) => Ok(None),
        _ => Err(Error::NotBoolean),
    }
}

pub(crate) fn values_equal(a: &Value, b: &Value) -> bool {
    compare(CompareOp::Eq, a, b).unwrap_or(false)
}

/// Three-valued Cypher equality — returns `None` when either
/// input is `null` or when any nested element on either side is
/// null *and* the non-null parts haven't already proved the
/// lists unequal. Used by `=` / `<>` between lists and by
/// `IN` so `[1, 2] = [null, 2]` and `[null] IN [[null]]`
/// correctly bubble up to `Value::Null` instead of being
/// flattened to `false`.
pub(crate) fn equal_three_valued(a: &Value, b: &Value) -> Option<bool> {
    let a_null = matches!(a, Value::Null | Value::Property(Property::Null));
    let b_null = matches!(b, Value::Null | Value::Property(Property::Null));
    if a_null || b_null {
        return None;
    }
    // Normalise both sides to `Vec<Value>` if they look like lists
    // so the recursive comparison doesn't have to branch on
    // `Value::List` vs `Property::List`.
    let la = match a {
        Value::List(items) => Some(items.clone()),
        Value::Property(Property::List(items)) => {
            Some(items.iter().cloned().map(Value::Property).collect::<Vec<_>>())
        }
        _ => None,
    };
    let lb = match b {
        Value::List(items) => Some(items.clone()),
        Value::Property(Property::List(items)) => {
            Some(items.iter().cloned().map(Value::Property).collect::<Vec<_>>())
        }
        _ => None,
    };
    if let (Some(la), Some(lb)) = (la, lb) {
        // Different lengths = definitely unequal, regardless of
        // nulls — matches openCypher's "length-first" rule.
        if la.len() != lb.len() {
            return Some(false);
        }
        let mut any_null = false;
        for (x, y) in la.iter().zip(lb.iter()) {
            match equal_three_valued(x, y) {
                Some(true) => continue,
                Some(false) => return Some(false),
                None => any_null = true,
            }
        }
        return if any_null { None } else { Some(true) };
    }
    // One-side-is-list / other-isn't: no nulls to propagate, so
    // the types simply don't compare as equal.
    if matches!(a, Value::List(_) | Value::Property(Property::List(_)))
        != matches!(b, Value::List(_) | Value::Property(Property::List(_)))
    {
        return Some(false);
    }
    // Everything else (maps, scalars, graph elements) defers to
    // the existing `compare()` logic which already handles
    // three-valued map equality (via `Err(UnsupportedComparison)`
    // for "all common keys equal but some carry null"). Translate
    // that Err path back into `None` so the caller treats it as
    // null.
    match compare(CompareOp::Eq, a, b) {
        Ok(b) => Some(b),
        Err(Error::UnsupportedComparison) => None,
        Err(Error::TypeMismatch) => Some(false),
        Err(_) => Some(false),
    }
}

fn compare(op: CompareOp, l: &Value, r: &Value) -> Result<bool> {
    if matches!(l, Value::Null | Value::Property(Property::Null))
        || matches!(r, Value::Null | Value::Property(Property::Null))
    {
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
    // List comparison: element-by-element, shorter list is "less"
    if let (Value::List(la), Value::List(lb)) = (l, r) {
        for (a, b) in la.iter().zip(lb.iter()) {
            match compare(CompareOp::Eq, a, b) {
                Ok(true) => continue,
                Ok(false) => {
                    // Elements differ — compare for ordering
                    let lt = compare(CompareOp::Lt, a, b).unwrap_or(false);
                    let ord = if lt {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    };
                    return Ok(match op {
                        CompareOp::Eq => false,
                        CompareOp::Ne => true,
                        CompareOp::Lt => ord == Ordering::Less,
                        CompareOp::Le => ord != Ordering::Greater,
                        CompareOp::Gt => ord == Ordering::Greater,
                        CompareOp::Ge => ord != Ordering::Less,
                        _ => false,
                    });
                }
                Err(_) => return Err(Error::TypeMismatch),
            }
        }
        // All common elements are equal; compare lengths
        let ord = la.len().cmp(&lb.len());
        return Ok(match op {
            CompareOp::Eq => ord == Ordering::Equal,
            CompareOp::Ne => ord != Ordering::Equal,
            CompareOp::Lt => ord == Ordering::Less,
            CompareOp::Le => ord != Ordering::Greater,
            CompareOp::Gt => ord == Ordering::Greater,
            CompareOp::Ge => ord != Ordering::Less,
            _ => false,
        });
    }
    // Property::List comparison
    if let (Value::Property(Property::List(la)), Value::Property(Property::List(lb))) = (l, r) {
        let la_vals: Vec<Value> = la.iter().cloned().map(Value::Property).collect();
        let lb_vals: Vec<Value> = lb.iter().cloned().map(Value::Property).collect();
        return compare(op, &Value::List(la_vals), &Value::List(lb_vals));
    }
    // Map equality. Follow openCypher's three-valued rules:
    //   { } vs { k: null }       → false (key set differs)
    //   { k: 1 } vs { k: 1 }     → true  (all keys match by value)
    //   { k: null } vs { k: null } → null (common keys carry null)
    //   { k: 1 } vs { k: null }  → null
    //   { k: 1 } vs { k: 2 }     → false
    if let (Value::Property(Property::Map(la)), Value::Property(Property::Map(lb))) = (l, r) {
        let eq = match op {
            CompareOp::Eq => true,
            CompareOp::Ne => false,
            _ => return Err(Error::UnsupportedComparison),
        };
        // Different key sets → not equal, no nulls propagated.
        let keys_a: std::collections::HashSet<&String> = la.keys().collect();
        let keys_b: std::collections::HashSet<&String> = lb.keys().collect();
        if keys_a != keys_b {
            return Ok(!eq);
        }
        let mut saw_null = false;
        for (k, va) in la {
            let vb = lb.get(k).unwrap();
            let va_v = Value::Property(va.clone());
            let vb_v = Value::Property(vb.clone());
            if matches!(va_v, Value::Null | Value::Property(Property::Null))
                || matches!(vb_v, Value::Null | Value::Property(Property::Null))
            {
                saw_null = true;
                continue;
            }
            match compare(CompareOp::Eq, &va_v, &vb_v) {
                Ok(true) => {}
                Ok(false) => return Ok(!eq),
                Err(Error::TypeMismatch) => return Ok(!eq),
                Err(e) => return Err(e),
            }
        }
        if saw_null {
            return Err(Error::UnsupportedComparison);
        }
        return Ok(eq);
    }
    // Graph-element equality: nodes compare equal iff their ids
    // match; same for edges. Ordering is not defined, so any
    // inequality operator falls through to a type mismatch.
    if let (Value::Node(a), Value::Node(b)) = (l, r) {
        let eq = a.id == b.id;
        return Ok(match op {
            CompareOp::Eq => eq,
            CompareOp::Ne => !eq,
            _ => return Err(Error::UnsupportedComparison),
        });
    }
    if let (Value::Edge(a), Value::Edge(b)) = (l, r) {
        let eq = a.id == b.id;
        return Ok(match op {
            CompareOp::Eq => eq,
            CompareOp::Ne => !eq,
            _ => return Err(Error::UnsupportedComparison),
        });
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
        (Property::Bool(a), Property::Bool(b)) => a.cmp(b),
        // Temporal comparisons — DateTime and Date carry total
        // orderings by their epoch-offset components. Duration
        // only supports equality because component ordering
        // isn't well-defined (months-vs-days).
        (
            Property::DateTime { nanos: a, .. },
            Property::DateTime { nanos: b, .. },
        ) => a.cmp(b),
        (Property::LocalDateTime(a), Property::LocalDateTime(b)) => a.cmp(b),
        (Property::Date(a), Property::Date(b)) => a.cmp(b),
        (
            Property::Time {
                nanos: na,
                tz_offset_secs: tza,
            },
            Property::Time {
                nanos: nb,
                tz_offset_secs: tzb,
            },
        ) => {
            // Compare in UTC: shift the wall-clock back by each
            // offset. Treat an unzoned Time as UTC so ordering
            // against a zoned Time stays total.
            let a_utc = *na - (tza.unwrap_or(0) as i64) * 1_000_000_000;
            let b_utc = *nb - (tzb.unwrap_or(0) as i64) * 1_000_000_000;
            a_utc.cmp(&b_utc)
        }
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
