use crate::{NodeId, Property};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub labels: Vec<String>,
    pub properties: HashMap<String, Property>,
}

impl Node {
    pub fn new() -> Self {
        Self {
            id: NodeId::new(),
            labels: Vec::new(),
            properties: HashMap::new(),
        }
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.labels.push(label.into());
        self
    }

    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<Property>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }
}

impl Default for Node {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_populates_labels_and_properties() {
        let n = Node::new()
            .with_label("Person")
            .with_property("name", "Ada")
            .with_property("age", 37_i64);

        assert_eq!(n.labels, vec!["Person"]);
        assert_eq!(
            n.properties.get("name"),
            Some(&Property::String("Ada".into()))
        );
        assert_eq!(n.properties.get("age"), Some(&Property::Int64(37)));
    }
}
