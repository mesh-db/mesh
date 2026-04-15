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
                _ => Err(Error::NotNodeOrEdge),
            }
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
        for node_id in &frontier {
            let neighbors = match hop.rel.direction {
                Direction::Outgoing => ctx.reader.outgoing(*node_id)?,
                Direction::Incoming => ctx.reader.incoming(*node_id)?,
                Direction::Both => {
                    let mut out = ctx.reader.outgoing(*node_id)?;
                    out.extend(ctx.reader.incoming(*node_id)?);
                    out
                }
            };
            for (edge_id, neighbor_id) in neighbors {
                // Edge-type filter. Only pull the edge record when
                // a type constraint is present; adjacency already
                // encodes the neighbor, so unconstrained hops skip
                // the extra lookup.
                if let Some(t) = &hop.rel.edge_type {
                    let edge = match ctx.reader.get_edge(edge_id)? {
                        Some(e) => e,
                        None => continue,
                    };
                    if &edge.edge_type != t {
                        continue;
                    }
                }
                // Target-side label + pattern-property filter. When
                // the target pattern carries no constraints we skip
                // the node lookup entirely and accept any neighbor.
                if !hop.target.labels.is_empty() || !hop.target.properties.is_empty() {
                    let neighbor = match ctx.reader.get_node(neighbor_id)? {
                        Some(n) => n,
                        None => continue,
                    };
                    if !node_pattern_matches(&neighbor, &hop.target, ctx)? {
                        continue;
                    }
                }
                // Correlated lookup: if the target has a variable
                // that's already bound in the outer row, require
                // the walked neighbor to BE that bound node. This
                // lets `WHERE (a)-[:KNOWS]->(b)` check a specific
                // edge between two outer-row nodes rather than
                // wandering into unrelated neighbors.
                if let Some(target_var) = &hop.target.var {
                    if let Some(Value::Node(bound)) = ctx.row.get(target_var) {
                        if bound.id != neighbor_id {
                            continue;
                        }
                    }
                }
                next.push(neighbor_id);
            }
        }
        if next.is_empty() {
            return Ok(false);
        }
        frontier = next;
    }
    Ok(!frontier.is_empty())
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
