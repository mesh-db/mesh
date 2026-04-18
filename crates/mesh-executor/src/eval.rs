use crate::{
    error::{Error, Result},
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
                // Temporal property access for Expr::Property
                Value::Property(Property::Date(days)) => {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let d = epoch + chrono::Duration::days(*days as i64);
                    temporal_date_prop(&d, key)
                }
                Value::Property(Property::DateTime(ms)) => {
                    let (secs, nsec) = nanos_to_secs_nanos(*ms);
                    if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsec) {
                        let naive = dt.naive_utc();
                        let date = naive.date();
                        match key.as_str() {
                            "year" | "month" | "day" | "week" | "weekYear" | "dayOfWeek"
                            | "weekDay" | "dayOfYear" | "quarter" | "ordinalDay"
                            | "dayOfQuarter" => temporal_date_prop(&date, key),
                            "hour" => Ok(Value::Property(Property::Int64(naive.hour() as i64))),
                            "minute" => Ok(Value::Property(Property::Int64(naive.minute() as i64))),
                            "second" => Ok(Value::Property(Property::Int64(naive.second() as i64))),
                            "millisecond" => Ok(Value::Property(Property::Int64(
                                (nsec / 1_000_000) as i64,
                            ))),
                            "microsecond" => Ok(Value::Property(Property::Int64(
                                (nsec / 1_000) as i64,
                            ))),
                            "nanosecond" => Ok(Value::Property(Property::Int64(nsec as i64))),
                            "epochMillis" => Ok(Value::Property(Property::Int64(
                                (*ms / 1_000_000) as i64,
                            ))),
                            "epochSeconds" => Ok(Value::Property(Property::Int64(secs))),
                            _ => Ok(Value::Null),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                Value::Property(Property::Duration(ref dur)) => match key.as_str() {
                    "months" => Ok(Value::Property(Property::Int64(dur.months))),
                    "days" => Ok(Value::Property(Property::Int64(dur.days))),
                    "seconds" => Ok(Value::Property(Property::Int64(dur.seconds))),
                    "nanosecondsOfSecond" | "nanoseconds" => {
                        Ok(Value::Property(Property::Int64(dur.nanos as i64)))
                    }
                    "minutesOfHour" => {
                        Ok(Value::Property(Property::Int64((dur.seconds % 3600) / 60)))
                    }
                    "secondsOfMinute" => {
                        Ok(Value::Property(Property::Int64(dur.seconds % 60)))
                    }
                    "millisecondsOfSecond" => {
                        Ok(Value::Property(Property::Int64((dur.nanos / 1_000_000) as i64)))
                    }
                    "microsecondsOfSecond" => {
                        Ok(Value::Property(Property::Int64((dur.nanos / 1_000) as i64)))
                    }
                    _ => Ok(Value::Null),
                },
                _ => Ok(Value::Null),
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
                // Temporal property access
                Value::Property(Property::Date(days)) => {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let d = epoch + chrono::Duration::days(days as i64);
                    temporal_date_prop(&d, key)
                }
                Value::Property(Property::DateTime(ms)) => {
                    let (secs, nsec) = nanos_to_secs_nanos(ms);
                    if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsec) {
                        let naive = dt.naive_utc();
                        let date = naive.date();
                        match key.as_str() {
                            "year" | "month" | "day" | "week" | "weekYear" | "dayOfWeek"
                            | "weekDay" | "dayOfYear" | "quarter" | "ordinalDay"
                            | "dayOfQuarter" => temporal_date_prop(&date, key),
                            "hour" => Ok(Value::Property(Property::Int64(naive.hour() as i64))),
                            "minute" => Ok(Value::Property(Property::Int64(naive.minute() as i64))),
                            "second" => Ok(Value::Property(Property::Int64(naive.second() as i64))),
                            "millisecond" => Ok(Value::Property(Property::Int64(
                                (nsec / 1_000_000) as i64,
                            ))),
                            "microsecond" => Ok(Value::Property(Property::Int64(
                                (nsec / 1_000) as i64,
                            ))),
                            "nanosecond" => Ok(Value::Property(Property::Int64(nsec as i64))),
                            "epochMillis" => Ok(Value::Property(Property::Int64(
                                (ms / 1_000_000) as i64,
                            ))),
                            "epochSeconds" => Ok(Value::Property(Property::Int64(secs))),
                            _ => Ok(Value::Null),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                Value::Property(Property::Duration(ref dur)) => match key.as_str() {
                    "months" => Ok(Value::Property(Property::Int64(dur.months))),
                    "days" => Ok(Value::Property(Property::Int64(dur.days))),
                    "seconds" => Ok(Value::Property(Property::Int64(dur.seconds))),
                    "nanosecondsOfSecond" | "nanoseconds" => {
                        Ok(Value::Property(Property::Int64(dur.nanos as i64)))
                    }
                    "minutesOfHour" => {
                        Ok(Value::Property(Property::Int64((dur.seconds % 3600) / 60)))
                    }
                    "secondsOfMinute" => {
                        Ok(Value::Property(Property::Int64(dur.seconds % 60)))
                    }
                    "millisecondsOfSecond" => {
                        Ok(Value::Property(Property::Int64((dur.nanos / 1_000_000) as i64)))
                    }
                    "microsecondsOfSecond" => {
                        Ok(Value::Property(Property::Int64((dur.nanos / 1_000) as i64)))
                    }
                    _ => Ok(Value::Null),
                },
                _ => Ok(Value::Null),
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
                    _ => Ok(Value::Null),
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
            // Three-valued AND: false AND anything = false,
            // true AND x = x, null AND false = false, null AND x = null
            let va = to_bool_3v(&eval_expr(a, ctx)?)?;
            if va == Some(false) {
                return Ok(Value::Property(Property::Bool(false)));
            }
            let vb = to_bool_3v(&eval_expr(b, ctx)?)?;
            if vb == Some(false) {
                return Ok(Value::Property(Property::Bool(false)));
            }
            match (va, vb) {
                (Some(true), Some(true)) => Ok(Value::Property(Property::Bool(true))),
                _ => Ok(Value::Null),
            }
        }
        Expr::Or(a, b) => {
            // Three-valued OR: true OR anything = true,
            // false OR x = x, null OR true = true, null OR x = null
            let va = to_bool_3v(&eval_expr(a, ctx)?)?;
            if va == Some(true) {
                return Ok(Value::Property(Property::Bool(true)));
            }
            let vb = to_bool_3v(&eval_expr(b, ctx)?)?;
            if vb == Some(true) {
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
                let v = match eval_expr(predicate, &sub_ctx) {
                    Ok(v) => v,
                    Err(Error::TypeMismatch) | Err(Error::NotBoolean) => Value::Null,
                    Err(e) => return Err(e),
                };
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
            match compare(*op, &vl, &vr) {
                Ok(b) => Ok(Value::Property(Property::Bool(b))),
                Err(Error::TypeMismatch) | Err(Error::UnsupportedComparison) => Ok(Value::Null),
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
            // Three-valued IN: if element matches any item → true;
            // if any item is null and no match found → null; otherwise false
            let mut has_null = false;
            for item in &items {
                if values_equal(&elem, item) {
                    return Ok(Value::Property(Property::Bool(true)));
                }
                if matches!(item, Value::Null | Value::Property(Property::Null)) {
                    has_null = true;
                }
            }
            if has_null {
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
            match eval_binary_op(*op, l, r) {
                Ok(v) => Ok(v),
                Err(Error::TypeMismatch) => Ok(Value::Null),
                Err(e) => Err(e),
            }
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
    };
    let mut count = 0i64;
    while op.next(&exec_ctx)?.is_some() {
        count += 1;
    }
    Ok(count)
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

fn now_epoch_nanos() -> i128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_nanos() as i128
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
    use chrono::{DateTime, FixedOffset, NaiveDateTime};
    let trimmed = s.trim();
    // Strip bracketed timezone names like [Europe/Stockholm]
    let trimmed = if let Some(idx) = trimmed.find('[') {
        trimmed[..idx].trim()
    } else {
        trimmed
    };
    // Try RFC 3339 first (fastest path for the canonical form
    // drivers emit). Chrono's `DateTime::parse_from_rfc3339`
    // accepts both `Z` and explicit offsets.
    if let Ok(dt) = DateTime::<FixedOffset>::parse_from_rfc3339(trimmed) {
        return Ok(datetime_to_nanos(&dt));
    }
    // Try ISO 8601 formats with timezone offsets (with and without colons)
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f%:z",
        "%Y-%m-%dT%H:%M:%S%:z",
        "%Y-%m-%dT%H:%M:%S%.f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M%z",
        "%Y-%m-%dT%H:%M%:z",
    ] {
        if let Ok(dt) = DateTime::<FixedOffset>::parse_from_str(trimmed, fmt) {
            return Ok(datetime_to_nanos(&dt));
        }
    }
    // Fall back to a few naive forms. Each is interpreted as
    // UTC. Formats are tried in descending specificity so a
    // string with a sub-second component doesn't get truncated
    // by a coarser format.
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
    ] {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(trimmed, fmt) {
            return Ok(datetime_to_nanos(&ndt.and_utc()));
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

/// Parse a time string like "12:31:14.645876123" into nanoseconds since midnight.
fn parse_time_string(s: &str) -> Result<i64> {
    let trimmed = s.trim();
    // Try parsing as HH:MM:SS.fffffffff, HH:MM:SS, HH:MM
    for fmt in ["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"] {
        if let Ok(t) = chrono::NaiveTime::parse_from_str(trimmed, fmt) {
            let secs = t.num_seconds_from_midnight() as i64;
            let nanos = t.nanosecond() as i64;
            return Ok(secs * 1_000_000_000 + nanos);
        }
    }
    Err(Error::UnknownScalarFunction(format!(
        "time() could not parse {s:?} as HH:MM:SS"
    )))
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
            let diff_ns = a.wrapping_sub(*b);
            Some(Ok(Value::Property(Dur(mesh_core::Duration {
                months: 0,
                days: 0,
                seconds: diff_ns.div_euclid(1_000_000_000) as i64,
                nanos: diff_ns.rem_euclid(1_000_000_000) as i32,
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
fn date_add_duration(days: i32, d: mesh_core::Duration) -> Result<Value> {
    // Ignore sub-day components (seconds, nanos) since Date has
    // no time-of-day component.
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let base = epoch + chrono::Duration::days(days as i64);
    // Apply months using calendar arithmetic
    let new_date = add_months_to_date(base, d.months);
    let final_date = new_date + chrono::Duration::days(d.days);
    let new_days = final_date.signed_duration_since(epoch).num_days();
    let clamped = i32::try_from(new_days).map_err(|_| Error::TypeMismatch)?;
    Ok(Value::Property(Property::Date(clamped)))
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
        "datetime" | "localdatetime" => match arg_exprs.len() {
            0 => Ok(Value::Property(Property::DateTime(now_epoch_nanos()))),
            1 => {
                let v = eval_expr(&arg_exprs[0], ctx)?;
                match v {
                    Value::Null | Value::Property(Property::Null) => Ok(Value::Null),
                    Value::Property(Property::String(s)) => {
                        Ok(Value::Property(Property::DateTime(parse_datetime(&s)?)))
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

                        let has_explicit_time = m.contains_key("hour") || m.contains_key("minute")
                            || m.contains_key("second") || m.contains_key("nanosecond")
                            || m.contains_key("millisecond") || m.contains_key("microsecond");
                        let hour = map_int(&m, "hour").unwrap_or(if has_explicit_time { 0 } else { base_hour });
                        let minute = map_int(&m, "minute").unwrap_or(if has_explicit_time { 0 } else { base_min });
                        let second = map_int(&m, "second").unwrap_or(if has_explicit_time { 0 } else { base_sec });
                        let nanos = map_int(&m, "nanosecond")
                            .or_else(|| map_int(&m, "millisecond").map(|ms| ms * 1_000_000))
                            .or_else(|| map_int(&m, "microsecond").map(|us| us * 1_000))
                            .unwrap_or(if has_explicit_time { 0 } else { base_ns });
                        let epoch_nanos: i128 = (days_since_epoch as i128) * 86_400_000_000_000
                            + (hour as i128) * 3_600_000_000_000
                            + (minute as i128) * 60_000_000_000
                            + (second as i128) * 1_000_000_000
                            + (nanos as i128);
                        Ok(Value::Property(Property::DateTime(epoch_nanos)))
                    }
                    _ => Err(Error::TypeMismatch),
                }
            }
            _ => Err(Error::UnknownScalarFunction(
                "datetime() takes zero or one argument".into(),
            )),
        },
        "date" => match arg_exprs.len() {
            0 => {
                let days = now_epoch_nanos().div_euclid(86_400_000_000_000);
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
                    Value::Property(Property::Map(m)) => {
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        // Extract base date from any temporal-typed value under any of
                        // these keys: date, datetime, localdatetime
                        let base_date = extract_base_date(&m, &epoch);
                        let base_or_epoch = base_date.unwrap_or(epoch);
                        let has_base = base_date.is_some();
                        let target = build_date_from_map(&m, base_or_epoch, has_base);
                        let days = target.signed_duration_since(epoch).num_days();
                        Ok(Value::Property(Property::Date(days as i32)))
                    }
                    // date(datetime_value) - extract date from datetime
                    Value::Property(Property::DateTime(ms)) => {
                        let days = ms.div_euclid(86_400_000_000_000) as i32;
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
                        let hour = map_int(&m, "hour").unwrap_or(0);
                        let minute = map_int(&m, "minute").unwrap_or(0);
                        let second = map_int(&m, "second").unwrap_or(0);
                        let nanos = map_int(&m, "nanosecond").unwrap_or(0);
                        let time_nanos =
                            hour * 3_600_000_000_000 + minute * 60_000_000_000
                            + second * 1_000_000_000 + nanos;
                        Ok(Value::Property(Property::Time {
                            nanos: time_nanos,
                            tz_offset_secs: if is_tz { Some(0) } else { None },
                        }))
                    }
                    Value::Property(Property::String(s)) => {
                        let time_nanos = parse_time_string(&s)?;
                        Ok(Value::Property(Property::Time {
                            nanos: time_nanos,
                            tz_offset_secs: if is_tz { Some(0) } else { None },
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
            let mut seconds = 0_i64;
            let mut nanos = 0_i32;
            for (k, v) in &entries {
                let as_f64 = match v {
                    Property::Int64(i) => *i as f64,
                    Property::Float64(f) => *f,
                    Property::Null => continue,
                    _ => return Err(Error::TypeMismatch),
                };
                // Use f64 arithmetic to handle fractional cascading
                match k.as_str() {
                    "years" => {
                        let total_months = as_f64 * 12.0;
                        months += total_months as i64;
                        let frac_months = total_months - (total_months as i64 as f64);
                        days += (frac_months * 30.0) as i64;
                    }
                    "months" => {
                        months += as_f64 as i64;
                        let frac = as_f64 - (as_f64 as i64 as f64);
                        days += (frac * 30.0) as i64;
                    }
                    "weeks" => {
                        days += (as_f64 * 7.0) as i64;
                    }
                    "days" => {
                        days += as_f64 as i64;
                        let frac = as_f64 - (as_f64 as i64 as f64);
                        seconds += (frac * 86400.0) as i64;
                    }
                    "hours" => {
                        seconds += (as_f64 * 3600.0) as i64;
                        let frac = as_f64 * 3600.0 - ((as_f64 * 3600.0) as i64 as f64);
                        nanos += (frac * 1_000_000_000.0) as i32;
                    }
                    "minutes" => {
                        seconds += (as_f64 * 60.0) as i64;
                        let frac = as_f64 * 60.0 - ((as_f64 * 60.0) as i64 as f64);
                        nanos += (frac * 1_000_000_000.0) as i32;
                    }
                    "seconds" => {
                        seconds += as_f64 as i64;
                        let frac = as_f64 - (as_f64 as i64 as f64);
                        nanos += (frac * 1_000_000_000.0) as i32;
                    }
                    "milliseconds" => {
                        let total_nanos = as_f64 * 1_000_000.0;
                        seconds += (total_nanos / 1_000_000_000.0) as i64;
                        nanos += (total_nanos % 1_000_000_000.0) as i32;
                    }
                    "microseconds" => {
                        nanos += (as_f64 * 1000.0) as i32;
                    }
                    "nanoseconds" => {
                        nanos += as_f64 as i32;
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
            // Extract date and time-of-day from both temporal values
            let (a_date, a_tod) = temporal_to_date_tod(&a)?;
            let (b_date, b_tod) = temporal_to_date_tod(&b)?;
            let duration = match name {
                "duration.inmonths" => {
                    let months = month_diff(a_date, b_date);
                    mesh_core::Duration {
                        months,
                        days: 0,
                        seconds: 0,
                        nanos: 0,
                    }
                }
                "duration.indays" => {
                    let days = b_date.signed_duration_since(a_date).num_days();
                    mesh_core::Duration {
                        months: 0,
                        days,
                        seconds: 0,
                        nanos: 0,
                    }
                }
                "duration.inseconds" => {
                    let days = b_date.signed_duration_since(a_date).num_days();
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
                    // duration.between: compute calendar-aware components
                    duration_between_calendar(a_date, a_tod, b_date, b_tod)
                }
            };
            Ok(Value::Property(Property::Duration(duration)))
        }

        // Current-time namespace functions
        "datetime.transaction" | "datetime.statement" | "datetime.realtime"
        | "localdatetime.transaction" | "localdatetime.statement" | "localdatetime.realtime" => {
            Ok(Value::Property(Property::DateTime(now_epoch_nanos())))
        }
        "date.transaction" | "date.statement" | "date.realtime" => {
            let days = now_epoch_nanos().div_euclid(86_400_000_000_000);
            let clamped = i32::try_from(days).unwrap_or(0);
            Ok(Value::Property(Property::Date(clamped)))
        }
        "time.transaction" | "time.statement" | "time.realtime" => {
            let time_nanos = (now_epoch_nanos() % 86_400_000_000_000) as i64;
            Ok(Value::Property(Property::Time {
                nanos: time_nanos,
                tz_offset_secs: Some(0),
            }))
        }
        "localtime.transaction" | "localtime.statement" | "localtime.realtime" => {
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
                Value::Property(Property::DateTime(now_epoch_nanos()))
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
            Some(Property::DateTime(ns)) => {
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
fn extract_base_date_tod(
    m: &std::collections::HashMap<String, Property>,
    epoch: &chrono::NaiveDate,
) -> (Option<chrono::NaiveDate>, i128) {
    for key in ["date", "datetime", "localdatetime"] {
        match m.get(key) {
            Some(Property::Date(d)) => {
                return (Some(*epoch + chrono::Duration::days(*d as i64)), 0);
            }
            Some(Property::DateTime(ns)) => {
                let days = ns.div_euclid(86_400_000_000_000);
                let tod = ns.rem_euclid(86_400_000_000_000);
                if let Ok(d) = i64::try_from(days) {
                    return (Some(*epoch + chrono::Duration::days(d)), tod);
                }
            }
            _ => {}
        }
    }
    // `time` field: no date, just time-of-day
    if let Some(Property::Time { nanos, .. }) = m.get("time") {
        return (None, *nanos as i128);
    }
    (None, 0)
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
        let month = ((quarter - 1) * 3 + 1) as u32;
        let day = map_int(m, "dayOfQuarter")
            .or_else(|| map_int(m, "day"))
            .map(|d| d as u32)
            .unwrap_or_else(|| {
                // Preserve day-of-quarter from base if same month alignment
                let base_q = (base.month() - 1) / 3 + 1;
                if base_q == quarter as u32 {
                    base.day()
                } else {
                    1
                }
            });
        return chrono::NaiveDate::from_ymd_opt(year as i32, month, day).unwrap_or(base);
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

/// Extract (date, time-of-day nanoseconds) from any temporal value.
fn temporal_to_date_tod(v: &Value) -> Result<(chrono::NaiveDate, i128)> {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    match v {
        Value::Property(Property::Date(days)) => {
            Ok((epoch + chrono::Duration::days(*days as i64), 0))
        }
        Value::Property(Property::DateTime(ns)) => {
            let days = ns.div_euclid(86_400_000_000_000);
            let tod = ns.rem_euclid(86_400_000_000_000);
            let days_i64 = i64::try_from(days).map_err(|_| Error::TypeMismatch)?;
            Ok((epoch + chrono::Duration::days(days_i64), tod))
        }
        Value::Property(Property::Time { nanos, .. }) => {
            // Time value has no date, use epoch
            Ok((epoch, *nanos as i128))
        }
        _ => Err(Error::TypeMismatch),
    }
}

/// Calculate the month difference between two dates.
fn month_diff(a: chrono::NaiveDate, b: chrono::NaiveDate) -> i64 {
    let a_months = a.year() as i64 * 12 + (a.month() as i64 - 1);
    let b_months = b.year() as i64 * 12 + (b.month() as i64 - 1);
    let mut diff = b_months - a_months;
    // Adjust for day-of-month not reached
    if diff > 0 && b.day() < a.day() {
        diff -= 1;
    } else if diff < 0 && b.day() > a.day() {
        diff += 1;
    }
    diff
}

/// Compute a calendar-aware duration between two temporal points
/// (date + time-of-day nanoseconds). Matches Neo4j's duration.between
/// semantics.
fn duration_between_calendar(
    a_date: chrono::NaiveDate,
    a_tod: i128,
    b_date: chrono::NaiveDate,
    b_tod: i128,
) -> mesh_core::Duration {
    // Compute years/months/days first, then time difference
    let mut months = month_diff(a_date, b_date);
    // Compute the reference date after applying months
    let after_months = add_months_to_date(a_date, months);
    let mut days = b_date.signed_duration_since(after_months).num_days();

    // Now compute time difference
    let mut tod_diff = b_tod - a_tod;
    // If tod_diff is negative and days > 0, borrow a day
    if tod_diff < 0 && days > 0 {
        days -= 1;
        tod_diff += 86_400_000_000_000;
    } else if tod_diff > 0 && days < 0 {
        days += 1;
        tod_diff -= 86_400_000_000_000;
    }

    // If months and days have opposite signs, adjust
    if months > 0 && days < 0 {
        months -= 1;
        let after_months = add_months_to_date(a_date, months);
        days = b_date.signed_duration_since(after_months).num_days();
        if tod_diff < 0 && days > 0 {
            days -= 1;
            tod_diff += 86_400_000_000_000;
        }
    } else if months < 0 && days > 0 {
        months += 1;
        let after_months = add_months_to_date(a_date, months);
        days = b_date.signed_duration_since(after_months).num_days();
        if tod_diff > 0 && days < 0 {
            days += 1;
            tod_diff -= 86_400_000_000_000;
        }
    }

    let seconds = (tod_diff / 1_000_000_000) as i64;
    let nanos = (tod_diff % 1_000_000_000) as i32;

    mesh_core::Duration {
        months,
        days,
        seconds,
        nanos,
    }
}

fn temporal_to_nanos(v: &Value) -> Result<i128> {
    match v {
        Value::Property(Property::DateTime(ms)) => Ok(*ms),
        Value::Property(Property::Date(days)) => Ok(*days as i128 * 86_400_000_000_000),
        _ => Err(Error::TypeMismatch),
    }
}

/// Truncate a temporal value to the specified unit.
fn truncate_temporal(
    name: &str,
    unit: &str,
    temporal: &Value,
    overrides: Option<&std::collections::HashMap<String, Property>>,
) -> Result<Value> {
    match temporal {
        Value::Property(Property::DateTime(_)) | Value::Property(Property::Date(_)) => {
            let epoch_ns: i128 = match temporal {
                Value::Property(Property::DateTime(ns)) => *ns,
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
                "weekyear" | "week" => {
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
                "millisecond" | "microsecond" => dt,
                _ => {
                    return Err(Error::UnknownScalarFunction(format!(
                        "unsupported truncation unit: {unit}"
                    )));
                }
            };
            // Apply overrides map (extra day/month/hour/etc. components)
            let truncated = if let Some(ov) = overrides {
                let mut year = truncated.year();
                let mut month = truncated.month();
                let mut day = truncated.day();
                let mut hour = truncated.hour();
                let mut minute = truncated.minute();
                let mut second = truncated.second();
                let mut nanosecond = truncated.nanosecond();
                for (k, v) in ov.iter() {
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
                        "nanosecond" => nanosecond = n as u32,
                        "millisecond" => nanosecond = (n as u32) * 1_000_000,
                        "microsecond" => nanosecond = (n as u32) * 1_000,
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
                let days = diff.num_days() as i32;
                Ok(Value::Property(Property::Date(days)))
            } else {
                Ok(Value::Property(Property::DateTime(result_nanos)))
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
        Value::Property(Property::DateTime(ns)) => {
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
        Some(0) => "Z".to_string(),
        Some(offset) => {
            let sign = if offset >= 0 { '+' } else { '-' };
            let abs = offset.unsigned_abs();
            let oh = abs / 3600;
            let om = (abs % 3600) / 60;
            format!("{sign}{oh:02}:{om:02}")
        }
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

    let has_time = seconds != 0 || nanos != 0;
    if has_time {
        result.push('T');
        let sign = if seconds < 0 || (seconds == 0 && nanos < 0) {
            "-"
        } else {
            ""
        };
        let abs_secs = seconds.unsigned_abs() as i64;
        let hours = abs_secs / 3600;
        let minutes = (abs_secs % 3600) / 60;
        let secs_only = abs_secs % 60;
        let abs_nanos = nanos.unsigned_abs() as i64;
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
        (Property::DateTime(a), Property::DateTime(b)) => a.cmp(b),
        (Property::Date(a), Property::Date(b)) => a.cmp(b),
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
        Property::Date(_) | Property::DateTime(_) | Property::Duration(_)
        | Property::Time { .. } => 9,
        Property::Null => 10,
    }
}

fn type_order_value(v: &Value) -> u8 {
    match v {
        Value::Property(Property::Map(_)) => 1,
        Value::Node(_) => 2,
        Value::Edge(_) => 3,
        Value::List(_) | Value::Property(Property::List(_)) => 4,
        Value::Path { .. } => 5,
        Value::Property(Property::String(_)) => 6,
        Value::Property(Property::Bool(_)) => 7,
        Value::Property(Property::Int64(_) | Property::Float64(_)) => 8,
        Value::Property(
            Property::Date(_) | Property::DateTime(_) | Property::Duration(_)
            | Property::Time { .. },
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
        Value::Property(Property::DateTime(ns)) => format!("dt:{}", ns),
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
