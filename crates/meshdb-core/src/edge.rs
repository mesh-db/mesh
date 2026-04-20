use crate::{EdgeId, NodeId, Property};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub id: EdgeId,
    pub edge_type: String,
    pub source: NodeId,
    pub target: NodeId,
    pub properties: HashMap<String, Property>,
}

impl Edge {
    pub fn new(edge_type: impl Into<String>, source: NodeId, target: NodeId) -> Self {
        Self {
            id: EdgeId::new(),
            edge_type: edge_type.into(),
            source,
            target,
            properties: HashMap::new(),
        }
    }

    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<Property>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edge_connects_source_to_target() {
        let a = NodeId::new();
        let b = NodeId::new();
        let e = Edge::new("KNOWS", a, b).with_property("since", 2020_i64);

        assert_eq!(e.edge_type, "KNOWS");
        assert_eq!(e.source, a);
        assert_eq!(e.target, b);
        assert_eq!(e.properties.get("since"), Some(&Property::Int64(2020)));
    }
}
