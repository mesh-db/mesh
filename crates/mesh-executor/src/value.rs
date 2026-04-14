use mesh_core::{Edge, Node, Property};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Node(Node),
    Edge(Edge),
    Property(Property),
    List(Vec<Value>),
    Null,
}

pub type Row = HashMap<String, Value>;

/// Per-query parameter bindings, e.g. `$name → "Ada"`. Built once per
/// `execute_with_reader` call and threaded through every `eval_expr`
/// invocation so `Expr::Parameter(name)` resolves to a concrete value.
pub type ParamMap = HashMap<String, Value>;
