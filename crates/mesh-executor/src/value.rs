use mesh_core::{Edge, Node, Property};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Node(Node),
    Edge(Edge),
    Property(Property),
    Null,
}

pub type Row = HashMap<String, Value>;
