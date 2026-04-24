//! Conversion from meshdb-executor row/value types onto `BoltValue` for
//! transmission over the Bolt protocol.
//!
//! Graph struct layouts vary by negotiated Bolt version:
//!
//!   | Struct              | Bolt 4.4                           | Bolt 5.0+                                                             |
//!   | Node (0x4E)         | {id, labels, properties}           | {id, labels, properties, element_id}                                  |
//!   | Relationship (0x52) | {id, start, end, type, properties} | {id, start, end, type, properties, element_id, start_el, end_el}      |
//!   | UnboundRel (0x72)   | {id, type, properties}             | {id, type, properties, element_id}                                    |
//!
//! Bolt 5.0 added the `element_id` / `start_element_id` /
//! `end_element_id` trailer; a 4.4-speaking driver strict-checks
//! field count and rejects 5.x-shaped structs as protocol errors
//! (only the stricter drivers — Python's is lenient about trailing
//! fields — but JS, Go, and .NET all reject the shape).
//!
//! Our `NodeId` / `EdgeId` are UUID v7 (128 bits). Bolt's numeric `id`
//! fields are i64, so we fold the low 8 bytes of the UUID into an
//! i64 — stable and deterministic per id. Drivers that care about
//! long-term stability (Bolt 5+) use `element_id`, which we set to
//! the full UUID string.

use meshdb_bolt::{
    is_bolt_4_4, BoltValue, TAG_DATE, TAG_DATE_TIME, TAG_DATE_TIME_LEGACY, TAG_DATE_TIME_ZONE_ID,
    TAG_DATE_TIME_ZONE_ID_LEGACY, TAG_DURATION, TAG_LOCAL_DATE_TIME, TAG_NODE, TAG_PATH,
    TAG_RELATIONSHIP, TAG_UNBOUND_RELATIONSHIP,
};
use meshdb_core::{Duration, Edge, Node, NodeId, Property};
use meshdb_executor::{ParamMap, Row, Value};
use std::collections::HashMap;

/// Convert a single executor row into the `Vec<BoltValue>` that a
/// Bolt `RECORD` message carries. Values are emitted in `fields`
/// order — the same field list that was advertised in the `RUN`
/// response's `SUCCESS` metadata.
///
/// `bolt_version` is the negotiated handshake tuple. Encoding for
/// most types is version-agnostic; the only branch today is the
/// timezone-aware DateTime, which uses tag 0x46 + local wall-clock
/// seconds under Bolt 4.4 and tag 0x49 + UTC seconds under 5.0+.
pub fn row_to_bolt_fields(row: &Row, fields: &[String], bolt_version: [u8; 4]) -> Vec<BoltValue> {
    fields
        .iter()
        .map(|name| match row.get(name) {
            Some(v) => value_to_bolt(v, bolt_version),
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

fn value_to_bolt(value: &Value, bolt_version: [u8; 4]) -> BoltValue {
    match value {
        Value::Null => BoltValue::Null,
        Value::Property(p) => property_to_bolt(p, bolt_version),
        Value::List(items) => BoltValue::List(
            items
                .iter()
                .map(|v| value_to_bolt(v, bolt_version))
                .collect(),
        ),
        Value::Map(m) => {
            // Bolt map wire format: keys as strings + each value
            // converted independently. `Value::Map` (graph-aware)
            // and `Property::Map` (scalar-only) both produce the
            // same `BoltValue::Map`; the wire protocol doesn't
            // need to distinguish them.
            let mut pairs: Vec<(String, BoltValue)> = m
                .iter()
                .map(|(k, v)| (k.clone(), value_to_bolt(v, bolt_version)))
                .collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            BoltValue::Map(pairs)
        }
        Value::Node(n) => node_to_bolt(n, bolt_version),
        Value::Edge(e) => edge_to_bolt(e, bolt_version),
        Value::Path { nodes, edges } => path_to_bolt(nodes, edges, bolt_version),
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
fn path_to_bolt(nodes: &[Node], edges: &[Edge], bolt_version: [u8; 4]) -> BoltValue {
    use std::collections::HashMap as StdHashMap;

    let mut unique_nodes: Vec<BoltValue> = Vec::new();
    let mut node_index: StdHashMap<NodeId, i64> = StdHashMap::new();
    for n in nodes {
        if !node_index.contains_key(&n.id) {
            node_index.insert(n.id, unique_nodes.len() as i64);
            unique_nodes.push(node_to_bolt(n, bolt_version));
        }
    }

    let mut unique_rels: Vec<BoltValue> = Vec::new();
    let mut rel_index: StdHashMap<meshdb_core::EdgeId, i64> = StdHashMap::new();
    for e in edges {
        if !rel_index.contains_key(&e.id) {
            // Bolt's path rel indices are 1-based (0 would be
            // ambiguous with the sign bit), so we stage the
            // 1-based slot here and push the unbound rel into
            // the deduped list.
            rel_index.insert(e.id, (unique_rels.len() as i64) + 1);
            unique_rels.push(unbound_relationship_to_bolt(e, bolt_version));
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
fn unbound_relationship_to_bolt(edge: &Edge, bolt_version: [u8; 4]) -> BoltValue {
    let mut props: Vec<(String, BoltValue)> = edge
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), property_to_bolt(v, bolt_version)))
        .collect();
    props.sort_by(|a, b| a.0.cmp(&b.0));
    let mut fields = vec![
        BoltValue::Int(edge_uuid_to_bolt_id(edge.id)),
        BoltValue::String(edge.edge_type.clone()),
        BoltValue::Map(props),
    ];
    if !is_bolt_4_4(bolt_version) {
        fields.push(BoltValue::String(edge.id.as_uuid().to_string()));
    }
    BoltValue::Struct {
        tag: TAG_UNBOUND_RELATIONSHIP,
        fields,
    }
}

fn property_to_bolt(p: &Property, bolt_version: [u8; 4]) -> BoltValue {
    match p {
        Property::Null => BoltValue::Null,
        Property::Bool(b) => BoltValue::Bool(*b),
        Property::Int64(i) => BoltValue::Int(*i),
        Property::Float64(f) => BoltValue::Float(*f),
        Property::String(s) => BoltValue::String(s.clone()),
        Property::List(items) => BoltValue::List(
            items
                .iter()
                .map(|p| property_to_bolt(p, bolt_version))
                .collect(),
        ),
        Property::Map(entries) => {
            // Maps in Property use a BTreeMap-like structure, sorted
            // to keep the Bolt representation deterministic.
            let mut pairs: Vec<(String, BoltValue)> = entries
                .iter()
                .map(|(k, v)| (k.clone(), property_to_bolt(v, bolt_version)))
                .collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            BoltValue::Map(pairs)
        }
        Property::DateTime {
            nanos,
            tz_offset_secs,
            tz_name,
        } => {
            // Two distinct wire representations depending on whether
            // the zone is named:
            //
            //   * Named zone (tz_name is Some, e.g. "Europe/Stockholm"):
            //     DateTimeZoneId struct — tag 0x69 (5.0+) or 0x66
            //     (4.4 legacy). Fields `[seconds, nanos, tz_id: String]`.
            //   * Offset-only (tz_name is None): DateTime struct —
            //     tag 0x49 (5.0+) or 0x46 (4.4 legacy). Fields
            //     `[seconds, nanos, tz_offset_seconds: Int]`.
            //
            // Seconds semantics follow the same split we documented
            // on the offset-only branch: 5.0+ carries UTC, 4.4
            // carries local wall-clock with the offset already
            // applied. For named zones under 4.4 we resolve the
            // offset from `chrono_tz` at the stored UTC instant so
            // a round-trip through an old driver still rebuilds the
            // same wall-clock time.
            let utc_seconds = nanos.div_euclid(1_000_000_000) as i64;
            let subsec = nanos.rem_euclid(1_000_000_000) as i64;

            // Only emit DateTimeZoneId for *valid IANA* zone names.
            // A driver-supplied non-IANA label like Go's
            // `FixedZone("UTC+2", 7200).Location().String()` can land
            // in `tz_name` but isn't parseable back out of the zone
            // database — emitting it as a tz_id struct would make
            // the peer driver hydrate the result as InvalidValue.
            // Fall through to the offset-only DateTime branch when
            // that's the case.
            if let Some(zone) = tz_name.as_deref().filter(|z| is_known_iana_zone(z)) {
                let effective_offset = tz_offset_secs
                    .map(|s| s as i64)
                    .or_else(|| resolve_zone_offset(zone, *nanos))
                    .unwrap_or(0);
                let (tag, seconds) = if is_bolt_4_4(bolt_version) {
                    (TAG_DATE_TIME_ZONE_ID_LEGACY, utc_seconds + effective_offset)
                } else {
                    (TAG_DATE_TIME_ZONE_ID, utc_seconds)
                };
                BoltValue::Struct {
                    tag,
                    fields: vec![
                        BoltValue::Int(seconds),
                        BoltValue::Int(subsec),
                        BoltValue::String(zone.to_string()),
                    ],
                }
            } else {
                let offset = tz_offset_secs.unwrap_or(0) as i64;
                let (tag, seconds) = if is_bolt_4_4(bolt_version) {
                    (TAG_DATE_TIME_LEGACY, utc_seconds + offset)
                } else {
                    (TAG_DATE_TIME, utc_seconds)
                };
                BoltValue::Struct {
                    tag,
                    fields: vec![
                        BoltValue::Int(seconds),
                        BoltValue::Int(subsec),
                        BoltValue::Int(offset),
                    ],
                }
            }
        }
        Property::LocalDateTime(nanos) => datetime_to_bolt(*nanos),
        Property::Date(days) => BoltValue::Struct {
            tag: TAG_DATE,
            fields: vec![BoltValue::Int(*days as i64)],
        },
        Property::Duration(d) => duration_to_bolt(*d),
        Property::Time {
            nanos,
            tz_offset_secs,
        } => {
            // Bolt LocalTime (tag 0x74): nanoseconds since midnight
            let tag = if tz_offset_secs.is_some() { 0x54 } else { 0x74 };
            let mut fields = vec![BoltValue::Int(*nanos)];
            if let Some(offset) = tz_offset_secs {
                fields.push(BoltValue::Int(*offset as i64));
            }
            BoltValue::Struct { tag, fields }
        }
        Property::Point(p) => match p.z {
            Some(z) => BoltValue::Struct {
                tag: meshdb_bolt::TAG_POINT_3D,
                fields: vec![
                    BoltValue::Int(p.srid as i64),
                    BoltValue::Float(p.x),
                    BoltValue::Float(p.y),
                    BoltValue::Float(z),
                ],
            },
            None => BoltValue::Struct {
                tag: meshdb_bolt::TAG_POINT_2D,
                fields: vec![
                    BoltValue::Int(p.srid as i64),
                    BoltValue::Float(p.x),
                    BoltValue::Float(p.y),
                ],
            },
        },
    }
}

/// Resolve the UTC offset (in seconds east of UTC) for a named IANA
/// zone at a specific UTC instant. Returns None when the zone name
/// isn't recognised by chrono-tz — the caller falls back to 0 (i.e.
/// "Z") so an unknown zone doesn't produce nonsensical wire values.
///
/// Why this lookup exists: Bolt 4.4 legacy `DateTimeZoneId` carries
/// local-wall-clock seconds, which requires knowing the zone's
/// offset at the source instant. Storage only remembers the zone
/// *name* once decoded from a previous driver round-trip (and may
/// or may not have a stashed `tz_offset_secs` depending on how the
/// Property landed). This function bridges back to an offset for
/// encoding.
/// True iff `tz_name` parses as a known IANA region in the chrono-tz
/// zone database. Non-IANA labels (fixed-offset aliases like Go's
/// `FixedZone("UTC+2", 7200)`, abbreviations like `"CEST"`, user
/// typos) return false so the encoder falls back to the offset-only
/// DateTime struct rather than handing the driver a tz_id it can't
/// resolve.
fn is_known_iana_zone(tz_name: &str) -> bool {
    tz_name.parse::<chrono_tz::Tz>().is_ok()
}

fn resolve_zone_offset(tz_name: &str, utc_nanos: i128) -> Option<i64> {
    use chrono::{DateTime, Offset, Utc};
    use chrono_tz::Tz;
    let tz: Tz = tz_name.parse().ok()?;
    let utc_secs = utc_nanos.div_euclid(1_000_000_000);
    let utc_nsec = utc_nanos.rem_euclid(1_000_000_000);
    // i128 → i64 can fail for extremes well outside the year
    // 1..9999 range the TCK exercises; out-of-range falls back to
    // zero offset rather than panicking.
    let utc_secs_i64 = i64::try_from(utc_secs).ok()?;
    let dt = DateTime::<Utc>::from_timestamp(utc_secs_i64, utc_nsec as u32)?;
    Some(dt.with_timezone(&tz).offset().fix().local_minus_utc() as i64)
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

fn node_to_bolt(node: &Node, bolt_version: [u8; 4]) -> BoltValue {
    let mut props: Vec<(String, BoltValue)> = node
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), property_to_bolt(v, bolt_version)))
        .collect();
    // Node.properties is a HashMap; iteration order is non-deterministic.
    // Sort by key so the Bolt representation is stable across runs.
    props.sort_by(|a, b| a.0.cmp(&b.0));
    let mut fields = vec![
        BoltValue::Int(uuid_to_bolt_id(node.id)),
        BoltValue::List(
            node.labels
                .iter()
                .map(|l| BoltValue::String(l.clone()))
                .collect(),
        ),
        BoltValue::Map(props),
    ];
    if !is_bolt_4_4(bolt_version) {
        fields.push(BoltValue::String(node.id.as_uuid().to_string()));
    }
    BoltValue::Struct {
        tag: TAG_NODE,
        fields,
    }
}

fn edge_to_bolt(edge: &Edge, bolt_version: [u8; 4]) -> BoltValue {
    let mut props: Vec<(String, BoltValue)> = edge
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), property_to_bolt(v, bolt_version)))
        .collect();
    props.sort_by(|a, b| a.0.cmp(&b.0));
    let mut fields = vec![
        BoltValue::Int(edge_uuid_to_bolt_id(edge.id)),
        BoltValue::Int(uuid_to_bolt_id(edge.source)),
        BoltValue::Int(uuid_to_bolt_id(edge.target)),
        BoltValue::String(edge.edge_type.clone()),
        BoltValue::Map(props),
    ];
    if !is_bolt_4_4(bolt_version) {
        fields.push(BoltValue::String(edge.id.as_uuid().to_string()));
        fields.push(BoltValue::String(edge.source.as_uuid().to_string()));
        fields.push(BoltValue::String(edge.target.as_uuid().to_string()));
    }
    BoltValue::Struct {
        tag: TAG_RELATIONSHIP,
        fields,
    }
}

/// Fold the low 8 bytes of a NodeId UUID into an i64 so drivers still
/// get a non-zero numeric id for pre-Bolt-5 APIs. Collision-free in
/// practice for UUID v7 at reasonable scales; drivers that need strong
/// stability use `element_id` instead.
fn uuid_to_bolt_id(id: NodeId) -> i64 {
    fold_bytes(id.as_bytes())
}

fn edge_uuid_to_bolt_id(id: meshdb_core::EdgeId) -> i64 {
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
            let epoch_nanos: i128 = (seconds as i128) * 1_000_000_000 + (nanos as i128);
            Ok(Property::LocalDateTime(epoch_nanos))
        }
        meshdb_bolt::TAG_DATE_TIME | meshdb_bolt::TAG_DATE_TIME_LEGACY => {
            // [seconds, nanos, tz_offset_secs].
            //   * Bolt 5 tag 0x49: `seconds` is the UTC instant.
            //   * Bolt 4.4 tag 0x46 (legacy): `seconds` is the local
            //     wall-clock instant (UTC + offset). We subtract the
            //     offset to normalize to UTC for `Property::DateTime`.
            if fields.len() != 3 {
                return Err(ParamConversionError::MalformedTemporal {
                    tag,
                    reason: "expected 3 fields",
                });
            }
            let seconds = temporal_int(fields, 0, tag, "DateTime seconds")?;
            let nanos = temporal_int(fields, 1, tag, "DateTime nanos")?;
            let tz_offset = temporal_int(fields, 2, tag, "DateTime tz_offset")?;
            let utc_seconds = if tag == meshdb_bolt::TAG_DATE_TIME_LEGACY {
                seconds - tz_offset
            } else {
                seconds
            };
            let epoch_nanos: i128 = (utc_seconds as i128) * 1_000_000_000 + (nanos as i128);
            Ok(Property::DateTime {
                nanos: epoch_nanos,
                tz_offset_secs: Some(tz_offset as i32),
                tz_name: None,
            })
        }
        meshdb_bolt::TAG_DATE_TIME_ZONE_ID | meshdb_bolt::TAG_DATE_TIME_ZONE_ID_LEGACY => {
            // [seconds, nanos, tz_id: String].
            //   * Bolt 5 tag 0x69: `seconds` is the UTC instant.
            //   * Bolt 4.4 tag 0x66 (legacy): `seconds` is the local
            //     wall-clock instant in the named zone. Rebuild UTC
            //     by resolving the zone's offset via chrono-tz.
            //     Unknown zone names fall back to treating seconds
            //     as UTC directly (a best-effort compromise — loses
            //     precision but doesn't drop the datetime).
            if fields.len() != 3 {
                return Err(ParamConversionError::MalformedTemporal {
                    tag,
                    reason: "expected 3 fields",
                });
            }
            let seconds = temporal_int(fields, 0, tag, "DateTimeZoneId seconds")?;
            let nanos = temporal_int(fields, 1, tag, "DateTimeZoneId nanos")?;
            let tz_id = match &fields[2] {
                BoltValue::String(s) => s.clone(),
                other => {
                    return Err(ParamConversionError::MalformedTemporal {
                        tag,
                        reason: match other {
                            BoltValue::Int(_) => "tz_id field was Int, expected String",
                            _ => "tz_id field was not a String",
                        },
                    });
                }
            };
            let utc_nanos_initial: i128 = (seconds as i128) * 1_000_000_000 + (nanos as i128);
            // Drivers occasionally park non-IANA labels in this slot
            // (Go's `FixedZone("UTC+2", 7200).String()` is one real
            // example). Only keep the name if chrono-tz recognises
            // it; otherwise store as offset-only so a round-trip
            // doesn't re-emit an unrecognised tz_id the peer hydrates
            // as InvalidValue.
            if !is_known_iana_zone(&tz_id) {
                return Ok(Property::DateTime {
                    nanos: utc_nanos_initial,
                    tz_offset_secs: None,
                    tz_name: None,
                });
            }
            let utc_nanos = if tag == meshdb_bolt::TAG_DATE_TIME_ZONE_ID_LEGACY {
                // Shift local-wall-clock → UTC using the zone's
                // offset at that instant. Note: we resolve the
                // offset against the local-time value itself; the
                // lookup is correct for unambiguous instants and
                // deterministic (first occurrence) at DST fall-back.
                match resolve_zone_offset(&tz_id, utc_nanos_initial) {
                    Some(offset_secs) => utc_nanos_initial - (offset_secs as i128) * 1_000_000_000,
                    None => utc_nanos_initial,
                }
            } else {
                utc_nanos_initial
            };
            // Stash the offset too — `tz_offset_secs` is useful for
            // downstream Cypher expressions that don't want to
            // re-resolve the zone database.
            let tz_offset_secs = resolve_zone_offset(&tz_id, utc_nanos).map(|o| o as i32);
            Ok(Property::DateTime {
                nanos: utc_nanos,
                tz_offset_secs,
                tz_name: Some(tz_id),
            })
        }
        meshdb_bolt::TAG_LOCAL_TIME => {
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
        meshdb_bolt::TAG_TIME => {
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
            Ok(Property::Date(days))
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
        meshdb_bolt::TAG_POINT_2D => {
            if fields.len() != 3 {
                return Err(ParamConversionError::MalformedTemporal {
                    tag,
                    reason: "expected 3 fields",
                });
            }
            let srid = temporal_int(fields, 0, tag, "Point2D srid")? as i32;
            let x = temporal_float(fields, 1, tag, "Point2D x")?;
            let y = temporal_float(fields, 2, tag, "Point2D y")?;
            Ok(Property::Point(meshdb_core::Point {
                srid,
                x,
                y,
                z: None,
            }))
        }
        meshdb_bolt::TAG_POINT_3D => {
            if fields.len() != 4 {
                return Err(ParamConversionError::MalformedTemporal {
                    tag,
                    reason: "expected 4 fields",
                });
            }
            let srid = temporal_int(fields, 0, tag, "Point3D srid")? as i32;
            let x = temporal_float(fields, 1, tag, "Point3D x")?;
            let y = temporal_float(fields, 2, tag, "Point3D y")?;
            let z = temporal_float(fields, 3, tag, "Point3D z")?;
            Ok(Property::Point(meshdb_core::Point {
                srid,
                x,
                y,
                z: Some(z),
            }))
        }
        _ => Err(ParamConversionError::GraphValue("Struct")),
    }
}

fn temporal_float(
    fields: &[BoltValue],
    idx: usize,
    tag: u8,
    reason: &'static str,
) -> Result<f64, ParamConversionError> {
    match fields.get(idx) {
        Some(BoltValue::Float(f)) => Ok(*f),
        Some(BoltValue::Int(i)) => Ok(*i as f64),
        _ => Err(ParamConversionError::MalformedTemporal { tag, reason }),
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
