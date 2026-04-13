use crate::{
    error::{Error, Result},
    value::{Row, Value},
};
use mesh_core::Property;
use mesh_cypher::{CompareOp, Expr, Literal};
use std::cmp::Ordering;

pub(crate) fn eval_expr(expr: &Expr, row: &Row) -> Result<Value> {
    match expr {
        Expr::Literal(lit) => Ok(literal_to_value(lit)),
        Expr::Identifier(name) => row
            .get(name)
            .cloned()
            .ok_or_else(|| Error::UnboundVariable(name.clone())),
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
                _ => Err(Error::NotNodeOrEdge),
            }
        }
        Expr::Not(inner) => {
            let v = eval_expr(inner, row)?;
            Ok(Value::Property(Property::Bool(!to_bool(&v)?)))
        }
        Expr::And(a, b) => {
            let va = to_bool(&eval_expr(a, row)?)?;
            if !va {
                return Ok(Value::Property(Property::Bool(false)));
            }
            let vb = to_bool(&eval_expr(b, row)?)?;
            Ok(Value::Property(Property::Bool(vb)))
        }
        Expr::Or(a, b) => {
            let va = to_bool(&eval_expr(a, row)?)?;
            if va {
                return Ok(Value::Property(Property::Bool(true)));
            }
            let vb = to_bool(&eval_expr(b, row)?)?;
            Ok(Value::Property(Property::Bool(vb)))
        }
        Expr::Compare { op, left, right } => {
            let vl = eval_expr(left, row)?;
            let vr = eval_expr(right, row)?;
            Ok(Value::Property(Property::Bool(compare(*op, &vl, &vr)?)))
        }
        Expr::Call { name, .. } => Err(Error::UnknownScalarFunction(name.clone())),
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
        (Property::Float64(a), Property::Float64(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (Property::Int64(a), Property::Float64(b)) => (*a as f64)
            .partial_cmp(b)
            .unwrap_or(Ordering::Equal),
        (Property::Float64(a), Property::Int64(b)) => a
            .partial_cmp(&(*b as f64))
            .unwrap_or(Ordering::Equal),
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

pub(crate) fn literal_to_property(lit: &Literal) -> Property {
    match lit {
        Literal::String(s) => Property::String(s.clone()),
        Literal::Integer(i) => Property::Int64(*i),
        Literal::Float(f) => Property::Float64(*f),
        Literal::Boolean(b) => Property::Bool(*b),
        Literal::Null => Property::Null,
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
