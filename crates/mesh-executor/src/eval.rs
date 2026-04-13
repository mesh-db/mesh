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
    }
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
