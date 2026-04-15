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
        Expr::Call { name, args } => call_scalar(name, args, row, params),
        Expr::List(items) => {
            let mut out = Vec::with_capacity(items.len());
            for e in items {
                out.push(eval_expr(e, row, params)?);
            }
            Ok(Value::List(out))
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
        _ => Err(Error::UnknownScalarFunction(name.to_string())),
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
    })
}
