use mesh_core::{Edge, Node, Property};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Node(Node),
    Edge(Edge),
    Property(Property),
    List(Vec<Value>),
    /// A Cypher path — a materialized traversal produced by
    /// `MATCH p = (...)-[...]->(...)`. The invariant is
    /// `nodes.len() == edges.len() + 1`; a zero-hop path
    /// (`MATCH p = (n)`) has one node and zero edges. Stored
    /// alongside the other Value variants (rather than inside
    /// `Property`) because paths carry full Node/Edge values
    /// which the backend-neutral `Property` type can't hold.
    Path {
        nodes: Vec<Node>,
        edges: Vec<Edge>,
    },
    Null,
}

pub type Row = HashMap<String, Value>;

/// Per-query parameter bindings, e.g. `$name → "Ada"`. Built once per
/// `execute_with_reader` call and threaded through every `eval_expr`
/// invocation so `Expr::Parameter(name)` resolves to a concrete value.
pub type ParamMap = HashMap<String, Value>;
