//! Conversion from mesh-executor row/value types onto `BoltValue` for
//! transmission over the Bolt protocol.
//!
//! Reference for the graph struct layouts (Bolt 4.4):
//! - Node (tag 0x4E): `{id: Int, labels: List<String>, properties: Map,
//!   element_id: String}`
//! - Relationship (tag 0x52): `{id, start_id, end_id, type: String,
//!   properties: Map, element_id, start_element_id, end_element_id}`
//!
//! Our `NodeId` / `EdgeId` are UUID v7 (128 bits). Bolt's numeric `id`
//! fields are i64, so we fold the low 8 bytes of the UUID into an
//! i64 — stable and deterministic per id. Drivers that care about
//! long-term stability (Bolt 5+) should use `element_id`, which we
//! set to the full UUID string.

use mesh_bolt::{BoltValue, TAG_NODE, TAG_RELATIONSHIP};
use mesh_core::{Edge, Node, NodeId, Property};
use mesh_executor::{Row, Value};

/// Convert a single executor row into the `Vec<BoltValue>` that a
/// Bolt `RECORD` message carries. Values are emitted in `fields`
/// order — the same field list that was advertised in the `RUN`
/// response's `SUCCESS` metadata.
pub fn row_to_bolt_fields(row: &Row, fields: &[String]) -> Vec<BoltValue> {
    fields
        .iter()
        .map(|name| match row.get(name) {
            Some(v) => value_to_bolt(v),
            None => BoltValue::Null,
        })
        .collect()
}

/// Derive a deterministic field-name list for the record stream. We
/// take the sorted union of keys across all rows so a query that
/// returns rows with consistent columns (the common case — all
/// executor Project outputs) yields the expected ordering, and a
/// hypothetical result with row-dependent keys still produces a
/// stable superset.
pub fn field_names_from_rows(rows: &[Row]) -> Vec<String> {
    use std::collections::BTreeSet;
    let mut seen = BTreeSet::new();
    for row in rows {
        for key in row.keys() {
            seen.insert(key.clone());
        }
    }
    seen.into_iter().collect()
}

fn value_to_bolt(value: &Value) -> BoltValue {
    match value {
        Value::Null => BoltValue::Null,
        Value::Property(p) => property_to_bolt(p),
        Value::List(items) => {
            BoltValue::List(items.iter().map(value_to_bolt).collect())
        }
        Value::Node(n) => node_to_bolt(n),
        Value::Edge(e) => edge_to_bolt(e),
    }
}

fn property_to_bolt(p: &Property) -> BoltValue {
    match p {
        Property::Null => BoltValue::Null,
        Property::Bool(b) => BoltValue::Bool(*b),
        Property::Int64(i) => BoltValue::Int(*i),
        Property::Float64(f) => BoltValue::Float(*f),
        Property::String(s) => BoltValue::String(s.clone()),
        Property::List(items) => {
            BoltValue::List(items.iter().map(property_to_bolt).collect())
        }
        Property::Map(entries) => {
            // Maps in Property use a BTreeMap-like structure, sorted
            // to keep the Bolt representation deterministic.
            let mut pairs: Vec<(String, BoltValue)> = entries
                .iter()
                .map(|(k, v)| (k.clone(), property_to_bolt(v)))
                .collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            BoltValue::Map(pairs)
        }
    }
}

fn node_to_bolt(node: &Node) -> BoltValue {
    let mut props: Vec<(String, BoltValue)> = node
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), property_to_bolt(v)))
        .collect();
    // Node.properties is a HashMap; iteration order is non-deterministic.
    // Sort by key so the Bolt representation is stable across runs.
    props.sort_by(|a, b| a.0.cmp(&b.0));
    BoltValue::Struct {
        tag: TAG_NODE,
        fields: vec![
            BoltValue::Int(uuid_to_bolt_id(node.id)),
            BoltValue::List(
                node.labels
                    .iter()
                    .map(|l| BoltValue::String(l.clone()))
                    .collect(),
            ),
            BoltValue::Map(props),
            BoltValue::String(node.id.as_uuid().to_string()),
        ],
    }
}

fn edge_to_bolt(edge: &Edge) -> BoltValue {
    let mut props: Vec<(String, BoltValue)> = edge
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), property_to_bolt(v)))
        .collect();
    props.sort_by(|a, b| a.0.cmp(&b.0));
    BoltValue::Struct {
        tag: TAG_RELATIONSHIP,
        fields: vec![
            BoltValue::Int(edge_uuid_to_bolt_id(edge.id)),
            BoltValue::Int(uuid_to_bolt_id(edge.source)),
            BoltValue::Int(uuid_to_bolt_id(edge.target)),
            BoltValue::String(edge.edge_type.clone()),
            BoltValue::Map(props),
            BoltValue::String(edge.id.as_uuid().to_string()),
            BoltValue::String(edge.source.as_uuid().to_string()),
            BoltValue::String(edge.target.as_uuid().to_string()),
        ],
    }
}

/// Fold the low 8 bytes of a NodeId UUID into an i64 so drivers still
/// get a non-zero numeric id for pre-Bolt-5 APIs. Collision-free in
/// practice for UUID v7 at reasonable scales; drivers that need strong
/// stability use `element_id` instead.
fn uuid_to_bolt_id(id: NodeId) -> i64 {
    fold_bytes(id.as_bytes())
}

fn edge_uuid_to_bolt_id(id: mesh_core::EdgeId) -> i64 {
    fold_bytes(id.as_bytes())
}

fn fold_bytes(bytes: &[u8; 16]) -> i64 {
    i64::from_be_bytes([
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
        bytes[15],
    ])
}
