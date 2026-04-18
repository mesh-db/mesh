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

use mesh_bolt::{
    BoltValue, TAG_DATE, TAG_DURATION, TAG_LOCAL_DATE_TIME, TAG_NODE, TAG_PATH, TAG_RELATIONSHIP,
    TAG_UNBOUND_RELATIONSHIP,
};
use mesh_core::{Duration, Edge, Node, NodeId, Property};
use mesh_executor::{ParamMap, Row, Value};
use std::collections::HashMap;

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
        Value::List(items) => BoltValue::List(items.iter().map(value_to_bolt).collect()),
        Value::Node(n) => node_to_bolt(n),
        Value::Edge(e) => edge_to_bolt(e),
        Value::Path { nodes, edges } => path_to_bolt(nodes, edges),
    }
}

/// Convert a `Value::Path` into a Bolt 4.4 Path struct (tag
/// `TAG_PATH`). The wire format uses three parallel lists:
/// `nodes` and `rels` are deduplicated, and `sequence`
/// alternates `(rel_index_1based_signed, node_index)` pairs so
/// the client can walk the traversal in order. Sign of the
/// rel index encodes direction: positive means source→target,
/// negative means target→source — for the v1 path binding we
/// always emit positive indices because `BindPath` assembles
/// the sequence in forward traversal order regardless of the
/// query's arrow direction.
///
/// Rel deduplication is keyed off the RocksDB `EdgeId`, which
/// is unique within a path even when the same edge type is
/// traversed twice. Node deduplication uses the `NodeId`
/// similarly — a cyclic path that returns to its start still
/// produces a single entry in `nodes` and a sequence that
/// references it twice, matching Neo4j's shape.
fn path_to_bolt(nodes: &[Node], edges: &[Edge]) -> BoltValue {
    use std::collections::HashMap as StdHashMap;

    let mut unique_nodes: Vec<BoltValue> = Vec::new();
    let mut node_index: StdHashMap<NodeId, i64> = StdHashMap::new();
    for n in nodes {
        if !node_index.contains_key(&n.id) {
            node_index.insert(n.id, unique_nodes.len() as i64);
            unique_nodes.push(node_to_bolt(n));
        }
    }

    let mut unique_rels: Vec<BoltValue> = Vec::new();
    let mut rel_index: StdHashMap<mesh_core::EdgeId, i64> = StdHashMap::new();
    for e in edges {
        if !rel_index.contains_key(&e.id) {
            // Bolt's path rel indices are 1-based (0 would be
            // ambiguous with the sign bit), so we stage the
            // 1-based slot here and push the unbound rel into
            // the deduped list.
            rel_index.insert(e.id, (unique_rels.len() as i64) + 1);
            unique_rels.push(unbound_relationship_to_bolt(e));
        }
    }

    // Sequence alternates rel_index, node_index for each hop.
    // Path with N nodes has N-1 hops; sequence length = 2*(N-1).
    let mut sequence: Vec<BoltValue> = Vec::with_capacity(2 * edges.len());
    for (i, e) in edges.iter().enumerate() {
        sequence.push(BoltValue::Int(rel_index[&e.id]));
        // Target node for this hop = nodes[i + 1].
        sequence.push(BoltValue::Int(node_index[&nodes[i + 1].id]));
    }

    BoltValue::Struct {
        tag: TAG_PATH,
        fields: vec![
            BoltValue::List(unique_nodes),
            BoltValue::List(unique_rels),
            BoltValue::List(sequence),
        ],
    }
}

/// Bolt 4.4 UnboundRelationship — a relationship without its
/// source/target node ids, used inside Path structs where the
/// endpoints are reconstructed from the `sequence` indices into
/// the accompanying `nodes` list. Fields:
/// `{id, type, properties, element_id}`.
fn unbound_relationship_to_bolt(edge: &Edge) -> BoltValue {
    let mut props: Vec<(String, BoltValue)> = edge
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), property_to_bolt(v)))
        .collect();
    props.sort_by(|a, b| a.0.cmp(&b.0));
    BoltValue::Struct {
        tag: TAG_UNBOUND_RELATIONSHIP,
        fields: vec![
            BoltValue::Int(edge_uuid_to_bolt_id(edge.id)),
            BoltValue::String(edge.edge_type.clone()),
            BoltValue::Map(props),
            BoltValue::String(edge.id.as_uuid().to_string()),
        ],
    }
}

fn property_to_bolt(p: &Property) -> BoltValue {
    match p {
        Property::Null => BoltValue::Null,
        Property::Bool(b) => BoltValue::Bool(*b),
        Property::Int64(i) => BoltValue::Int(*i),
        Property::Float64(f) => BoltValue::Float(*f),
        Property::String(s) => BoltValue::String(s.clone()),
        Property::List(items) => BoltValue::List(items.iter().map(property_to_bolt).collect()),
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
        Property::DateTime(nanos) => {
            // DateTime = UTC-aware: Bolt 5 DateTime (tag 0x49) with
            // offset 0 ("Z"). Drivers that negotiated Bolt 4.4 will
            // also accept this tag — the encoding is wire-compatible
            // at the PackStream layer.
            let seconds = nanos.div_euclid(1_000_000_000) as i64;
            let subsec = nanos.rem_euclid(1_000_000_000) as i64;
            BoltValue::Struct {
                tag: mesh_bolt::TAG_DATE_TIME,
                fields: vec![
                    BoltValue::Int(seconds),
                    BoltValue::Int(subsec),
                    BoltValue::Int(0),
                ],
            }
        }
        Property::LocalDateTime(nanos) => datetime_to_bolt(*nanos),
        Property::Date(days) => BoltValue::Struct {
            tag: TAG_DATE,
            fields: vec![BoltValue::Int(*days as i64)],
        },
        Property::Duration(d) => duration_to_bolt(*d),
        Property::Time { nanos, tz_offset_secs } => {
            // Bolt LocalTime (tag 0x74): nanoseconds since midnight
            let tag = if tz_offset_secs.is_some() { 0x54 } else { 0x74 };
            let mut fields = vec![BoltValue::Int(*nanos)];
            if let Some(offset) = tz_offset_secs {
                fields.push(BoltValue::Int(*offset as i64));
            }
            BoltValue::Struct { tag, fields }
        }
    }
}

/// Encode a UTC epoch-nanos `DateTime` as a Bolt 4.4
/// `LocalDateTime` struct. Splits the nanos into `(seconds,
/// nanos)` — Bolt's `LocalDateTime` field layout.
fn datetime_to_bolt(epoch_nanos: i128) -> BoltValue {
    // Bolt LocalDateTime struct carries seconds as i64; nanos as i32.
    // Split via div/rem, then downcast — years 1..9999 fit in i64 seconds.
    let seconds = (epoch_nanos.div_euclid(1_000_000_000)) as i64;
    let nanos = epoch_nanos.rem_euclid(1_000_000_000) as i32;
    BoltValue::Struct {
        tag: TAG_LOCAL_DATE_TIME,
        fields: vec![BoltValue::Int(seconds), BoltValue::Int(nanos as i64)],
    }
}

fn duration_to_bolt(d: Duration) -> BoltValue {
    BoltValue::Struct {
        tag: TAG_DURATION,
        fields: vec![
            BoltValue::Int(d.months),
            BoltValue::Int(d.days),
            BoltValue::Int(d.seconds),
            BoltValue::Int(d.nanos as i64),
        ],
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
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    ])
}

/// Errors produced by the Bolt → executor value converters. Always
/// surfaced to clients as `Mesh.ClientError.InvalidArgument` Bolt
/// FAILUREs (or the equivalent gRPC `Status::invalid_argument`) since
/// every variant indicates a bug in the driver-supplied params blob.
#[derive(Debug, thiserror::Error)]
pub enum ParamConversionError {
    #[error("Bolt RUN params must be a Map, got {0}")]
    NotAMap(&'static str),

    #[error("parameter values cannot carry graph types ({0})")]
    GraphValue(&'static str),

    #[error("parameter values cannot carry raw bytes")]
    Bytes,

    #[error("nested map values must be primitives or lists, got node/edge")]
    NestedGraphInMap,

    #[error("malformed Bolt temporal struct (tag {tag:#x}): {reason}")]
    MalformedTemporal { tag: u8, reason: &'static str },
}

/// Decode the `params: BoltValue` field from a Bolt `RUN` message into
/// the executor's `ParamMap`. Rejects anything that isn't a top-level
/// Map and surfaces conversion errors with a clear cause so driver
/// authors can debug their bindings.
pub fn bolt_params_to_param_map(params: &BoltValue) -> Result<ParamMap, ParamConversionError> {
    let entries = match params {
        BoltValue::Map(entries) => entries,
        BoltValue::Null => return Ok(HashMap::new()),
        BoltValue::Bool(_) => return Err(ParamConversionError::NotAMap("Bool")),
        BoltValue::Int(_) => return Err(ParamConversionError::NotAMap("Int")),
        BoltValue::Float(_) => return Err(ParamConversionError::NotAMap("Float")),
        BoltValue::String(_) => return Err(ParamConversionError::NotAMap("String")),
        BoltValue::Bytes(_) => return Err(ParamConversionError::NotAMap("Bytes")),
        BoltValue::List(_) => return Err(ParamConversionError::NotAMap("List")),
        BoltValue::Struct { .. } => return Err(ParamConversionError::NotAMap("Struct")),
    };
    let mut map = HashMap::with_capacity(entries.len());
    for (k, v) in entries {
        map.insert(k.clone(), bolt_value_to_value(v)?);
    }
    Ok(map)
}

/// Convert a single `BoltValue` into an executor `Value`. Lists become
/// `Value::List`, primitive scalars become `Value::Property(...)`,
/// nested maps become `Value::Property(Property::Map(...))` (because
/// `Property::Map` is the only graph-shaped Property value). Bytes and
/// graph structs are rejected — drivers should never send them as
/// params.
pub fn bolt_value_to_value(bv: &BoltValue) -> Result<Value, ParamConversionError> {
    match bv {
        BoltValue::Null => Ok(Value::Null),
        BoltValue::Bool(b) => Ok(Value::Property(Property::Bool(*b))),
        BoltValue::Int(i) => Ok(Value::Property(Property::Int64(*i))),
        BoltValue::Float(f) => Ok(Value::Property(Property::Float64(*f))),
        BoltValue::String(s) => Ok(Value::Property(Property::String(s.clone()))),
        BoltValue::List(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(bolt_value_to_value(item)?);
            }
            Ok(Value::List(out))
        }
        BoltValue::Map(entries) => {
            // Nested maps must collapse onto Property::Map, which only
            // accepts Property values. Convert each entry through the
            // Property-only converter to surface graph-shaped nesting.
            let mut nested = HashMap::with_capacity(entries.len());
            for (k, v) in entries {
                nested.insert(k.clone(), bolt_value_to_property(v)?);
            }
            Ok(Value::Property(Property::Map(nested)))
        }
        BoltValue::Bytes(_) => Err(ParamConversionError::Bytes),
        BoltValue::Struct { tag, fields } => {
            // Bolt 4.4 temporal structs are the only Struct variants
            // we accept as RUN params today. Graph structs (Node,
            // Relationship, Path) never appear as parameter inputs —
            // drivers send them only as response RECORDs.
            bolt_temporal_struct(*tag, fields).map(Value::Property)
        }
    }
}

/// Decode a Bolt temporal struct (`LocalDateTime`, `Date`, or
/// `Duration`) into a [`Property`]. Any other struct tag is
/// rejected with `GraphValue` since we don't accept Node /
/// Relationship / Path as parameters.
fn bolt_temporal_struct(tag: u8, fields: &[BoltValue]) -> Result<Property, ParamConversionError> {
    match tag {
        TAG_LOCAL_DATE_TIME => {
            if fields.len() != 2 {
                return Err(ParamConversionError::MalformedTemporal {
                    tag,
                    reason: "expected 2 fields",
                });
            }
            let seconds = temporal_int(fields, 0, tag, "LocalDateTime seconds")?;
            let nanos = temporal_int(fields, 1, tag, "LocalDateTime nanos")?;
            let epoch_nanos: i128 =
                (seconds as i128) * 1_000_000_000 + (nanos as i128);
            Ok(Property::LocalDateTime(epoch_nanos))
        }
        mesh_bolt::TAG_DATE_TIME | mesh_bolt::TAG_DATE_TIME_LEGACY => {
            // Bolt 5 DateTime (0x49) or Bolt 4.4 legacy (0x46):
            // [seconds, nanos, tz_offset_secs]. In Bolt 5 these are
            // UTC; in 4.4 legacy they're local wall-clock with offset
            // applied. We approximate by treating them all as UTC
            // since our DateTime storage has no offset field.
            if fields.len() != 3 {
                return Err(ParamConversionError::MalformedTemporal {
                    tag,
                    reason: "expected 3 fields",
                });
            }
            let seconds = temporal_int(fields, 0, tag, "DateTime seconds")?;
            let nanos = temporal_int(fields, 1, tag, "DateTime nanos")?;
            let _tz_offset = temporal_int(fields, 2, tag, "DateTime tz_offset")?;
            let epoch_nanos: i128 =
                (seconds as i128) * 1_000_000_000 + (nanos as i128);
            Ok(Property::DateTime(epoch_nanos))
        }
        mesh_bolt::TAG_DATE_TIME_ZONE_ID | mesh_bolt::TAG_DATE_TIME_ZONE_ID_LEGACY => {
            // Zoned DateTime with named timezone (e.g. "Europe/Stockholm").
            // Approximate by ignoring the tz name and storing as UTC.
            if fields.len() != 3 {
                return Err(ParamConversionError::MalformedTemporal {
                    tag,
                    reason: "expected 3 fields",
                });
            }
            let seconds = temporal_int(fields, 0, tag, "DateTimeZoneId seconds")?;
            let nanos = temporal_int(fields, 1, tag, "DateTimeZoneId nanos")?;
            let epoch_nanos: i128 =
                (seconds as i128) * 1_000_000_000 + (nanos as i128);
            Ok(Property::DateTime(epoch_nanos))
        }
        mesh_bolt::TAG_LOCAL_TIME => {
            if fields.len() != 1 {
                return Err(ParamConversionError::MalformedTemporal {
                    tag,
                    reason: "expected 1 field",
                });
            }
            let nanos = temporal_int(fields, 0, tag, "LocalTime nanos")?;
            Ok(Property::Time {
                nanos,
                tz_offset_secs: None,
            })
        }
        mesh_bolt::TAG_TIME => {
            if fields.len() != 2 {
                return Err(ParamConversionError::MalformedTemporal {
                    tag,
                    reason: "expected 2 fields",
                });
            }
            let nanos = temporal_int(fields, 0, tag, "Time nanos")?;
            let tz = temporal_int(fields, 1, tag, "Time tz_offset")?;
            Ok(Property::Time {
                nanos,
                tz_offset_secs: Some(tz as i32),
            })
        }
        TAG_DATE => {
            if fields.len() != 1 {
                return Err(ParamConversionError::MalformedTemporal {
                    tag,
                    reason: "expected 1 field",
                });
            }
            let days = temporal_int(fields, 0, tag, "Date days")?;
            let days_i32 =
                i32::try_from(days).map_err(|_| ParamConversionError::MalformedTemporal {
                    tag,
                    reason: "date days out of i32 range",
                })?;
            Ok(Property::Date(days_i32))
        }
        TAG_DURATION => {
            if fields.len() != 4 {
                return Err(ParamConversionError::MalformedTemporal {
                    tag,
                    reason: "expected 4 fields",
                });
            }
            let months = temporal_int(fields, 0, tag, "Duration months")?;
            let days = temporal_int(fields, 1, tag, "Duration days")?;
            let seconds = temporal_int(fields, 2, tag, "Duration seconds")?;
            let nanos = temporal_int(fields, 3, tag, "Duration nanos")?;
            let nanos_i32 =
                i32::try_from(nanos).map_err(|_| ParamConversionError::MalformedTemporal {
                    tag,
                    reason: "duration nanos out of i32 range",
                })?;
            Ok(Property::Duration(Duration {
                months,
                days,
                seconds,
                nanos: nanos_i32,
            }))
        }
        _ => Err(ParamConversionError::GraphValue("Struct")),
    }
}

fn temporal_int(
    fields: &[BoltValue],
    idx: usize,
    tag: u8,
    reason: &'static str,
) -> Result<i64, ParamConversionError> {
    match fields.get(idx) {
        Some(BoltValue::Int(i)) => Ok(*i),
        _ => Err(ParamConversionError::MalformedTemporal { tag, reason }),
    }
}

/// Convert a `BoltValue` directly to a `Property`. Used for nested
/// values inside `Property::Map`, where the map's value type is
/// `Property` not `Value`. Rejects nested graph types.
pub fn bolt_value_to_property(bv: &BoltValue) -> Result<Property, ParamConversionError> {
    match bv {
        BoltValue::Null => Ok(Property::Null),
        BoltValue::Bool(b) => Ok(Property::Bool(*b)),
        BoltValue::Int(i) => Ok(Property::Int64(*i)),
        BoltValue::Float(f) => Ok(Property::Float64(*f)),
        BoltValue::String(s) => Ok(Property::String(s.clone())),
        BoltValue::List(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(bolt_value_to_property(item)?);
            }
            Ok(Property::List(out))
        }
        BoltValue::Map(entries) => {
            let mut nested = HashMap::with_capacity(entries.len());
            for (k, v) in entries {
                nested.insert(k.clone(), bolt_value_to_property(v)?);
            }
            Ok(Property::Map(nested))
        }
        BoltValue::Bytes(_) => Err(ParamConversionError::Bytes),
        BoltValue::Struct { tag, fields } => bolt_temporal_struct(*tag, fields),
    }
}
