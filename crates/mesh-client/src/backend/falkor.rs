use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use redis::{Client, Value as RValue};
use std::collections::BTreeMap;

use super::{GraphBackend, GraphEdge, GraphNode, QueryResult, Schema, Value};
use crate::config::Profile;

pub struct FalkorBackend {
    conn: MultiplexedConnection,
    graph: String,
}

impl FalkorBackend {
    pub async fn connect(profile: &Profile) -> Result<Self> {
        let client = Client::open(profile.uri.as_str()).context("redis client")?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .context("redis connection")?;
        let graph = profile
            .graph
            .clone()
            .unwrap_or_else(|| "default".to_string());
        Ok(Self { conn, graph })
    }

    async fn raw_query(&mut self, cypher: &str) -> Result<RValue> {
        let v: RValue = redis::cmd("GRAPH.QUERY")
            .arg(&self.graph)
            .arg(cypher)
            .query_async(&mut self.conn)
            .await
            .context("GRAPH.QUERY")?;
        Ok(v)
    }
}

#[async_trait]
impl GraphBackend for FalkorBackend {
    fn label(&self) -> &str {
        "falkordb"
    }

    async fn query(&mut self, cypher: &str) -> Result<QueryResult> {
        let raw = self.raw_query(cypher).await?;
        parse_graph_reply(raw)
    }

    async fn schema(&mut self) -> Result<Schema> {
        let labels = single_column(self, "CALL db.labels()").await.unwrap_or_default();
        let rels = single_column(self, "CALL db.relationshipTypes()")
            .await
            .unwrap_or_default();
        let keys = single_column(self, "CALL db.propertyKeys()")
            .await
            .unwrap_or_default();
        Ok(Schema {
            labels,
            relationship_types: rels,
            property_keys: keys,
        })
    }
}

async fn single_column(be: &mut FalkorBackend, cypher: &str) -> Result<Vec<String>> {
    let raw = be.raw_query(cypher).await?;
    let qr = parse_graph_reply(raw)?;
    Ok(qr
        .rows
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .map(|v| v.render())
        .collect())
}

fn parse_graph_reply(v: RValue) -> Result<QueryResult> {
    let parts = match v {
        RValue::Array(items) => items,
        RValue::SimpleString(s) => {
            return Ok(QueryResult {
                summary: Some(s),
                ..Default::default()
            });
        }
        other => return Err(anyhow!("unexpected GRAPH.QUERY reply: {other:?}")),
    };

    // A write-only query returns just the stats array.
    if parts.len() == 1 {
        return Ok(QueryResult {
            summary: Some(stringify_stats(&parts[0])),
            ..Default::default()
        });
    }
    if parts.len() < 2 {
        return Ok(QueryResult::default());
    }

    let header = &parts[0];
    let data = &parts[1];
    let stats = parts.get(2);

    let columns = parse_header(header);

    let rows_raw = match data {
        RValue::Array(rs) => rs,
        _ => return Err(anyhow!("rows is not an array")),
    };

    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut nodes: Vec<GraphNode> = Vec::new();
    let mut edges: Vec<GraphEdge> = Vec::new();

    for row in rows_raw {
        let cells = match row {
            RValue::Array(cs) => cs,
            _ => continue,
        };
        let mut out = Vec::with_capacity(cells.len());
        for cell in cells {
            let v = parse_cell(cell, &mut nodes, &mut edges);
            out.push(v);
        }
        rows.push(out);
    }

    Ok(QueryResult {
        columns,
        rows,
        nodes,
        edges,
        summary: stats.map(stringify_stats),
    })
}

fn parse_header(v: &RValue) -> Vec<String> {
    let items = match v {
        RValue::Array(xs) => xs,
        _ => return Vec::new(),
    };
    items
        .iter()
        .map(|item| match item {
            RValue::Array(pair) if pair.len() >= 2 => redis_to_string(&pair[1]),
            other => redis_to_string(other),
        })
        .collect()
}

fn parse_cell(v: &RValue, nodes: &mut Vec<GraphNode>, edges: &mut Vec<GraphEdge>) -> Value {
    // FalkorDB verbose mode: nodes and relations come back as arrays of
    // [key, value] pairs. We try to recognize them by shape; anything
    // we can't classify falls through to a generic list/map.
    if let RValue::Array(items) = v {
        if let Some(node) = try_parse_node(items) {
            let gn = node.clone();
            nodes.push(gn);
            return Value::Node(node);
        }
        if let Some(edge) = try_parse_edge(items) {
            let ge = edge.clone();
            edges.push(ge);
            return Value::Edge(edge);
        }
        return Value::List(items.iter().map(|x| parse_cell(x, nodes, edges)).collect());
    }
    redis_to_value(v)
}

fn try_parse_node(items: &[RValue]) -> Option<GraphNode> {
    let pairs = as_kv_pairs(items)?;
    let mut id = None;
    let mut labels: Vec<String> = Vec::new();
    let mut props: BTreeMap<String, Value> = BTreeMap::new();
    for (k, v) in pairs {
        match k.as_str() {
            "id" => id = Some(redis_to_string(v)),
            "labels" => {
                if let RValue::Array(xs) = v {
                    labels = xs.iter().map(redis_to_string).collect();
                }
            }
            "properties" => {
                props = parse_props(v);
            }
            _ => {}
        }
    }
    Some(GraphNode {
        id: id?,
        labels,
        properties: props,
    })
}

fn try_parse_edge(items: &[RValue]) -> Option<GraphEdge> {
    let pairs = as_kv_pairs(items)?;
    let mut id = None;
    let mut typ = String::new();
    let mut src = String::new();
    let mut dst = String::new();
    let mut props: BTreeMap<String, Value> = BTreeMap::new();
    let mut saw_endpoints = false;
    for (k, v) in pairs {
        match k.as_str() {
            "id" => id = Some(redis_to_string(v)),
            "type" => typ = redis_to_string(v),
            "src_node" | "source" => {
                src = redis_to_string(v);
                saw_endpoints = true;
            }
            "dest_node" | "destination" | "target" => {
                dst = redis_to_string(v);
                saw_endpoints = true;
            }
            "properties" => props = parse_props(v),
            _ => {}
        }
    }
    if !saw_endpoints {
        return None;
    }
    Some(GraphEdge {
        id: id?,
        edge_type: typ,
        source: src,
        target: dst,
        properties: props,
    })
}

fn parse_props(v: &RValue) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    let items = match v {
        RValue::Array(xs) => xs,
        _ => return out,
    };
    for item in items {
        if let RValue::Array(pair) = item {
            if pair.len() >= 2 {
                let k = redis_to_string(&pair[0]);
                out.insert(k, redis_to_value(&pair[1]));
            }
        }
    }
    out
}

/// View a flat array as `[k, v, k, v, ...]` of string keys — returns
/// `None` if any odd slot isn't coercible to a string.
fn as_kv_pairs(items: &[RValue]) -> Option<Vec<(String, &RValue)>> {
    if items.is_empty() || items.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(items.len() / 2);
    let mut it = items.iter();
    while let (Some(k), Some(v)) = (it.next(), it.next()) {
        let key = match k {
            RValue::BulkString(b) => String::from_utf8_lossy(b).to_string(),
            RValue::SimpleString(s) => s.clone(),
            _ => return None,
        };
        out.push((key, v));
    }
    Some(out)
}

fn redis_to_value(v: &RValue) -> Value {
    match v {
        RValue::Nil => Value::Null,
        RValue::Int(i) => Value::Int(*i),
        RValue::BulkString(b) => Value::String(String::from_utf8_lossy(b).to_string()),
        RValue::SimpleString(s) => Value::String(s.clone()),
        RValue::Double(d) => Value::Float(*d),
        RValue::Boolean(b) => Value::Bool(*b),
        RValue::Array(items) => Value::List(items.iter().map(redis_to_value).collect()),
        RValue::Map(entries) => {
            let mut m = BTreeMap::new();
            for (k, v) in entries {
                m.insert(redis_to_string(k), redis_to_value(v));
            }
            Value::Map(m)
        }
        RValue::Okay => Value::String("OK".into()),
        other => Value::String(format!("{other:?}")),
    }
}

fn redis_to_string(v: &RValue) -> String {
    match v {
        RValue::BulkString(b) => String::from_utf8_lossy(b).to_string(),
        RValue::SimpleString(s) => s.clone(),
        RValue::Int(i) => i.to_string(),
        RValue::Double(d) => d.to_string(),
        RValue::Nil => String::new(),
        other => format!("{other:?}"),
    }
}

fn stringify_stats(v: &RValue) -> String {
    match v {
        RValue::Array(items) => items
            .iter()
            .map(redis_to_string)
            .collect::<Vec<_>>()
            .join(" | "),
        other => redis_to_string(other),
    }
}
