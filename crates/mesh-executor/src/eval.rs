use crate::{
    error::{Error, Result},
    value::{ParamMap, Row, Value},
};
use mesh_core::Property;
use mesh_cypher::{CallArgs, CompareOp, Expr, Literal};
use std::cmp::Ordering;

pub(crate) fn eval_expr(expr: &Expr, row: &Row, params: &ParamMap) -> Result<Value> {
    match expr {
        Expr::Literal(lit) => Ok(literal_to_value(lit)),
        Expr::Identifier(name) => row
            .get(name)
            .cloned()
            .ok_or_else(|| Error::UnboundVariable(name.clone())),
        Expr::Parameter(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| Error::UnboundParameter(name.clone())),
        Expr::Property { var, key } => {
            let bound = row
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
                _ => Err(Error::NotNodeOrEdge),
            }
        }
        Expr::Not(inner) => {
            let v = eval_expr(inner, row, params)?;
            Ok(Value::Property(Property::Bool(!to_bool(&v)?)))
        }
        Expr::And(a, b) => {
            let va = to_bool(&eval_expr(a, row, params)?)?;
            if !va {
                return Ok(Value::Property(Property::Bool(false)));
            }
            let vb = to_bool(&eval_expr(b, row, params)?)?;
            Ok(Value::Property(Property::Bool(vb)))
        }
        Expr::Or(a, b) => {
            let va = to_bool(&eval_expr(a, row, params)?)?;
            if va {
                return Ok(Value::Property(Property::Bool(true)));
            }
            let vb = to_bool(&eval_expr(b, row, params)?)?;
            Ok(Value::Property(Property::Bool(vb)))
        }
        Expr::Compare { op, left, right } => {
            let vl = eval_expr(left, row, params)?;
            let vr = eval_expr(right, row, params)?;
            Ok(Value::Property(Property::Bool(compare(*op, &vl, &vr)?)))
        }
        Expr::IsNull { negated, inner } => {
            let v = eval_expr(inner, row, params)?;
            let is_null = matches!(v, Value::Null | Value::Property(Property::Null));
            Ok(Value::Property(Property::Bool(if *negated {
                !is_null
            } else {
                is_null
            })))
        }
        Expr::Call { name, args } => call_scalar(name, args, row, params),
        Expr::List(items) => {
            let mut out = Vec::with_capacity(items.len());
            for e in items {
                out.push(eval_expr(e, row, params)?);
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
                let v = eval_expr(expr, row, params)?;
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
                Some(e) => Some(eval_expr(e, row, params)?),
                None => None,
            };
            for (cond, result) in branches {
                let cond_val = eval_expr(cond, row, params)?;
                let matched = match &scrutinee_val {
                    Some(sv) => case_equals(sv, &cond_val),
                    None => to_bool(&cond_val).unwrap_or(false),
                };
                if matched {
                    return eval_expr(result, row, params);
                }
            }
            match else_expr {
                Some(e) => eval_expr(e, row, params),
                None => Ok(Value::Null),
            }
        }
        Expr::ListComprehension {
            var,
            source,
            predicate,
            projection,
        } => {
            let source_val = eval_expr(source, row, params)?;
            let items = match source_val {
                Value::List(items) => items,
                Value::Property(Property::List(props)) => {
                    props.into_iter().map(Value::Property).collect()
                }
                Value::Null => return Ok(Value::Null),
                _ => return Err(Error::TypeMismatch),
            };
            let mut scratch = row.clone();
            let had_prev = scratch.contains_key(var);
            let prev = scratch.get(var).cloned();
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                scratch.insert(var.clone(), item);
                if let Some(pred) = predicate {
                    let pv = eval_expr(pred, &scratch, params)?;
                    if !to_bool(&pv).unwrap_or(false) {
                        continue;
                    }
                }
                let projected = match projection {
                    Some(p) => eval_expr(p, &scratch, params)?,
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

fn call_scalar(name: &str, args: &CallArgs, row: &Row, params: &ParamMap) -> Result<Value> {
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
            let v = single_arg(name, arg_exprs, row, params)?;
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
            let v = single_arg(name, arg_exprs, row, params)?;
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
            let v = single_arg(name, arg_exprs, row, params)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Path { edges, .. } => {
                    Ok(Value::List(edges.into_iter().map(Value::Edge).collect()))
                }
                _ => Err(Error::TypeMismatch),
            }
        }
        "labels" => {
            let v = single_arg(name, arg_exprs, row, params)?;
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
            let v = single_arg(name, arg_exprs, row, params)?;
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
            let v = single_arg(name, arg_exprs, row, params)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Edge(e) => Ok(Value::Property(Property::String(e.edge_type))),
                _ => Err(Error::TypeMismatch),
            }
        }
        "tolower" => {
            let v = single_arg(name, arg_exprs, row, params)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Property(Property::String(s)) => {
                    Ok(Value::Property(Property::String(s.to_lowercase())))
                }
                _ => Err(Error::TypeMismatch),
            }
        }
        "toupper" => {
            let v = single_arg(name, arg_exprs, row, params)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Property(Property::String(s)) => {
                    Ok(Value::Property(Property::String(s.to_uppercase())))
                }
                _ => Err(Error::TypeMismatch),
            }
        }
        "tostring" => {
            let v = single_arg(name, arg_exprs, row, params)?;
            Ok(value_to_string(v))
        }
        "tointeger" => {
            let v = single_arg(name, arg_exprs, row, params)?;
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
                let v = eval_expr(e, row, params)?;
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
            let sv = eval_expr(&arg_exprs[0], row, params)?;
            let start_v = eval_expr(&arg_exprs[1], row, params)?;
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
                let len_v = eval_expr(&arg_exprs[2], row, params)?;
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
            let v = single_arg(name, arg_exprs, row, params)?;
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
            let sv = eval_expr(&arg_exprs[0], row, params)?;
            let fv = eval_expr(&arg_exprs[1], row, params)?;
            let tv = eval_expr(&arg_exprs[2], row, params)?;
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
            let sv = eval_expr(&arg_exprs[0], row, params)?;
            let dv = eval_expr(&arg_exprs[1], row, params)?;
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
            let v = single_arg(name, arg_exprs, row, params)?;
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
            let v = single_arg(name, arg_exprs, row, params)?;
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
            let sv = eval_expr(&arg_exprs[0], row, params)?;
            let ev = eval_expr(&arg_exprs[1], row, params)?;
            let step_v = if arg_exprs.len() == 3 {
                Some(eval_expr(&arg_exprs[2], row, params)?)
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
            let v = single_arg(name, arg_exprs, row, params)?;
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
            let v = single_arg(name, arg_exprs, row, params)?;
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
            let v = single_arg(name, arg_exprs, row, params)?;
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
            let v = single_arg(name, arg_exprs, row, params)?;
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
            let v = single_arg(name, arg_exprs, row, params)?;
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
        "ceil" => math_unary(name, arg_exprs, row, params, |f| f.ceil()),
        "floor" => math_unary(name, arg_exprs, row, params, |f| f.floor()),
        "round" => math_unary(name, arg_exprs, row, params, |f| f.round()),
        "sqrt" => math_unary(name, arg_exprs, row, params, |f| f.sqrt()),
        "sign" => {
            // Returns -1, 0, or 1 as Int64 regardless of
            // whether the input was int or float. NaN maps to
            // 0 to stay total; callers who care can test for
            // NaN via `n <> n` first.
            let v = single_arg(name, arg_exprs, row, params)?;
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
    row: &Row,
    params: &ParamMap,
    f: impl FnOnce(f64) -> f64,
) -> Result<Value> {
    let v = single_arg(name, arg_exprs, row, params)?;
    match v {
        Value::Null => Ok(Value::Null),
        Value::Property(Property::Int64(i)) => Ok(Value::Property(Property::Float64(f(i as f64)))),
        Value::Property(Property::Float64(x)) => Ok(Value::Property(Property::Float64(f(x)))),
        _ => Err(Error::TypeMismatch),
    }
}

fn single_arg(name: &str, args: &[Expr], row: &Row, params: &ParamMap) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::UnknownScalarFunction(format!(
            "{} expects 1 argument, got {}",
            name,
            args.len()
        )));
    }
    eval_expr(&args[0], row, params)
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
        CompareOp::StartsWith | CompareOp::EndsWith | CompareOp::Contains => unreachable!(),
    })
}
