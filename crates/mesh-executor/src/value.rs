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
