pub mod bolt;

use anyhow::Result;
use async_trait::async_trait;
use std::collections::BTreeMap;

use crate::config::{BackendKind, Profile};

#[async_trait]
pub trait GraphBackend: Send {
    async fn query(&mut self, cypher: &str) -> Result<QueryResult>;
    async fn schema(&mut self) -> Result<Schema>;
    fn label(&self) -> &str;
}

pub async fn connect(profile: &Profile) -> Result<Box<dyn GraphBackend>> {
    match profile.kind {
        BackendKind::Mesh | BackendKind::Neo4j => {
            let b = bolt::BoltBackend::connect(profile).await?;
            Ok(Box::new(b))
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
    pub summary: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct Schema {
    pub labels: Vec<String>,
    pub relationship_types: Vec<String>,
    pub property_keys: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<Value>),
    Map(BTreeMap<String, Value>),
    Node(GraphNode),
    Edge(GraphEdge),
    Path(Vec<Value>),
}

impl Value {
    pub fn render(&self) -> String {
        match self {
            Value::Null => "null".into(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => format!("{f}"),
            Value::String(s) => s.clone(),
            Value::List(items) => {
                let inner = items
                    .iter()
                    .map(|v| v.render())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{inner}]")
            }
            Value::Map(m) => {
                let inner = m
                    .iter()
                    .map(|(k, v)| format!("{k}: {}", v.render()))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{inner}}}")
            }
            Value::Node(n) => {
                let labels = if n.labels.is_empty() {
                    String::new()
                } else {
                    format!(":{}", n.labels.join(":"))
                };
                format!("({}{labels})", n.id)
            }
            Value::Edge(e) => format!("-[:{} {}]->", e.edge_type, e.id),
            Value::Path(nodes) => nodes
                .iter()
                .map(|v| v.render())
                .collect::<Vec<_>>()
                .join(" "),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GraphNode {
    pub id: String,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct GraphEdge {
    pub id: String,
    pub edge_type: String,
    pub source: String,
    pub target: String,
    #[allow(dead_code)]
    pub properties: BTreeMap<String, Value>,
}
