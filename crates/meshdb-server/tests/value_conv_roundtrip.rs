//! Wire-format roundtrip tests for the `meshdb-server::value_conv`
//! encoder/decoder pair.
//!
//! Every value type that flows between the executor and the Bolt
//! client needs to survive the full path: `Property` →
//! `row_to_bolt_fields` → `BoltValue` → PackStream bytes → decode
//! → `BoltValue` → `bolt_value_to_property` → `Property`.
//!
//! These tests exercise the temporal types end-to-end (they go
//! both directions — the server emits them in RECORDs and the
//! client can send them as RUN params) plus the Path type (server
//! → client only; the roundtrip asserts the `BoltValue` struct
//! shape survives packstream encode/decode since we don't decode
//! Paths back into `Value::Path` in production).

use meshdb_bolt::{
    decode, encode, BoltValue, TAG_DATE, TAG_DURATION, TAG_LOCAL_DATE_TIME, TAG_NODE, TAG_PATH,
    TAG_RELATIONSHIP, TAG_UNBOUND_RELATIONSHIP,
};
use meshdb_core::{
    Duration, Edge, EdgeId, Node, NodeId, Point, Property, SRID_CARTESIAN_2D, SRID_CARTESIAN_3D,
    SRID_WGS84_2D, SRID_WGS84_3D,
};
use meshdb_executor::{Row, Value};
use meshdb_server::value_conv::{
    bolt_value_to_property, field_names_from_rows, row_to_bolt_fields,
};

/// Push a property into a one-key row, run it through the Bolt
/// encode path, round-trip the bytes through packstream, and
/// decode back to a `Property`. Asserts the value survives the
/// full wire cycle.
fn roundtrip_property(p: Property) -> Property {
    let mut row = Row::new();
    row.insert("v".to_string(), Value::Property(p));
    let fields = field_names_from_rows(&[row.clone()]);
    let bolt = row_to_bolt_fields(&row, &fields, meshdb_bolt::BOLT_5_4);
    assert_eq!(bolt.len(), 1, "exactly one field");
    let bytes = encode(&bolt[0]);
    let (decoded, consumed) = decode(&bytes).expect("packstream decode");
    assert_eq!(
        consumed,
        bytes.len(),
        "decoder left trailing bytes in {bytes:?}"
    );
    bolt_value_to_property(&decoded).expect("bolt → property")
}

#[test]
fn datetime_roundtrip_preserves_epoch_millis() {
    // 2025-01-01T00:00:00Z = 1_735_689_600_000 ms since epoch
    let input = Property::DateTime {
        nanos: 1_735_689_600_000,
        tz_offset_secs: Some(0),
        tz_name: None,
    };
    let out = roundtrip_property(input.clone());
    assert_eq!(out, input);
}

#[test]
fn datetime_roundtrip_with_sub_second_fraction() {
    // Ensure the millisecond granularity survives the
    // seconds/nanos split the Bolt LocalDateTime struct uses.
    let input = Property::DateTime {
        nanos: 1_735_689_600_123,
        tz_offset_secs: Some(0),
        tz_name: None,
    };
    let out = roundtrip_property(input.clone());
    assert_eq!(out, input);
}

#[test]
fn datetime_encoder_uses_legacy_tag_under_bolt_4_4() {
    // Under Bolt 4.4 the DateTime struct tag is 0x46 (LEGACY) with
    // local-wall-clock seconds — `seconds = utc_seconds + offset`.
    // Under 5.0+ the tag is 0x49 with UTC seconds and the offset as
    // a separate field. Both versions share the same three-field
    // layout; only the tag + seconds-semantics differ.
    let input = Property::DateTime {
        nanos: 2_000_000_000_000_000_000, // 2_000_000_000 seconds UTC
        tz_offset_secs: Some(7200),       // +02:00
        tz_name: None,
    };
    let mut row = Row::new();
    row.insert("v".to_string(), Value::Property(input.clone()));
    let fields = vec!["v".to_string()];

    let bolt_44 = &row_to_bolt_fields(&row, &fields, meshdb_bolt::BOLT_4_4)[0];
    match bolt_44 {
        BoltValue::Struct { tag, fields } => {
            assert_eq!(
                *tag,
                meshdb_bolt::TAG_DATE_TIME_LEGACY,
                "4.4 should emit 0x46"
            );
            // Seconds field is local wall-clock = 2_000_000_000 + 7200.
            match &fields[0] {
                BoltValue::Int(s) => assert_eq!(*s, 2_000_000_000 + 7200),
                _ => panic!("seconds field wrong type"),
            }
            match &fields[2] {
                BoltValue::Int(o) => assert_eq!(*o, 7200),
                _ => panic!("offset field wrong type"),
            }
        }
        _ => panic!("expected Struct, got {bolt_44:?}"),
    }

    let bolt_54 = &row_to_bolt_fields(&row, &fields, meshdb_bolt::BOLT_5_4)[0];
    match bolt_54 {
        BoltValue::Struct { tag, fields } => {
            assert_eq!(*tag, meshdb_bolt::TAG_DATE_TIME, "5.x should emit 0x49");
            // Seconds field is UTC instant.
            match &fields[0] {
                BoltValue::Int(s) => assert_eq!(*s, 2_000_000_000),
                _ => panic!("seconds field wrong type"),
            }
        }
        _ => panic!("expected Struct, got {bolt_54:?}"),
    }
}

#[test]
fn datetime_with_tz_name_emits_zone_id_struct() {
    // `Property::DateTime { tz_name: Some(...) }` should produce
    // the DateTimeZoneId struct (tag 0x69 under 5.0+, 0x66 under
    // 4.4) with fields `[seconds, nanos, tz_id: String]`, not the
    // offset-only DateTime struct. Previously the encoder dropped
    // tz_name entirely and emitted tag 0x49 / 0x46.
    //
    // UTC instant: 2024-06-15T12:30:45Z. Offset in Europe/Stockholm
    // that moment is +02:00 (CEST, summer time), so 4.4 legacy
    // seconds should be UTC + 7200.
    let input = Property::DateTime {
        nanos: 1_718_454_645_000_000_000, // 2024-06-15T12:30:45Z
        tz_offset_secs: Some(7200),
        tz_name: Some("Europe/Stockholm".to_string()),
    };
    let mut row = Row::new();
    row.insert("v".to_string(), Value::Property(input));
    let fields = vec!["v".to_string()];

    let bolt_54 = &row_to_bolt_fields(&row, &fields, meshdb_bolt::BOLT_5_4)[0];
    match bolt_54 {
        BoltValue::Struct { tag, fields } => {
            assert_eq!(
                *tag,
                meshdb_bolt::TAG_DATE_TIME_ZONE_ID,
                "5.x zoned should emit 0x69"
            );
            assert_eq!(fields.len(), 3);
            match &fields[0] {
                BoltValue::Int(s) => assert_eq!(*s, 1_718_454_645),
                _ => panic!("seconds wrong type"),
            }
            match &fields[2] {
                BoltValue::String(z) => assert_eq!(z, "Europe/Stockholm"),
                _ => panic!("tz_id wrong type"),
            }
        }
        other => panic!("expected Struct, got {other:?}"),
    }

    let bolt_44 = &row_to_bolt_fields(&row, &fields, meshdb_bolt::BOLT_4_4)[0];
    match bolt_44 {
        BoltValue::Struct { tag, fields } => {
            assert_eq!(
                *tag,
                meshdb_bolt::TAG_DATE_TIME_ZONE_ID_LEGACY,
                "4.4 zoned should emit 0x66"
            );
            // 4.4 legacy: seconds = UTC + zone_offset = 1_718_454_645 + 7200.
            match &fields[0] {
                BoltValue::Int(s) => assert_eq!(*s, 1_718_454_645 + 7200),
                _ => panic!("seconds wrong type"),
            }
        }
        other => panic!("expected Struct, got {other:?}"),
    }
}

#[test]
fn datetime_zone_id_decode_preserves_name_and_shifts_legacy_to_utc() {
    // Bolt 5 zoned (tag 0x69): seconds already UTC, tz_id preserved.
    let zone5 = BoltValue::Struct {
        tag: meshdb_bolt::TAG_DATE_TIME_ZONE_ID,
        fields: vec![
            BoltValue::Int(1_718_454_645), // UTC
            BoltValue::Int(0),
            BoltValue::String("Europe/Stockholm".to_string()),
        ],
    };
    match bolt_value_to_property(&zone5).expect("zone5 decode") {
        Property::DateTime {
            nanos,
            tz_offset_secs,
            tz_name,
        } => {
            assert_eq!(nanos, 1_718_454_645_000_000_000);
            assert_eq!(tz_name.as_deref(), Some("Europe/Stockholm"));
            // June in Stockholm = CEST = UTC+2.
            assert_eq!(tz_offset_secs, Some(7200));
        }
        other => panic!("expected DateTime, got {other:?}"),
    }

    // Bolt 4.4 zoned (tag 0x66): seconds are local wall-clock;
    // decoder must subtract the zone's offset to recover UTC.
    let zone44 = BoltValue::Struct {
        tag: meshdb_bolt::TAG_DATE_TIME_ZONE_ID_LEGACY,
        fields: vec![
            BoltValue::Int(1_718_454_645 + 7200), // local wall-clock (CEST)
            BoltValue::Int(0),
            BoltValue::String("Europe/Stockholm".to_string()),
        ],
    };
    match bolt_value_to_property(&zone44).expect("zone44 decode") {
        Property::DateTime {
            nanos,
            tz_offset_secs,
            tz_name,
        } => {
            assert_eq!(nanos, 1_718_454_645_000_000_000, "must normalize to UTC");
            assert_eq!(tz_name.as_deref(), Some("Europe/Stockholm"));
            assert_eq!(tz_offset_secs, Some(7200));
        }
        other => panic!("expected DateTime, got {other:?}"),
    }
}

#[test]
fn datetime_legacy_tag_roundtrips_through_decoder_back_to_utc() {
    // Symmetric decode: a driver sending a Bolt 4.4 legacy DateTime
    // should land in storage as UTC. Build the legacy struct by hand
    // so this test covers the ingress path (not the encoder's shape).
    let legacy = BoltValue::Struct {
        tag: meshdb_bolt::TAG_DATE_TIME_LEGACY,
        fields: vec![
            BoltValue::Int(2_000_000_000 + 7200), // local wall-clock
            BoltValue::Int(0),                    // sub-second nanos
            BoltValue::Int(7200),                 // +02:00 offset
        ],
    };
    let property = bolt_value_to_property(&legacy).expect("legacy decode");
    match property {
        Property::DateTime {
            nanos,
            tz_offset_secs,
            ..
        } => {
            assert_eq!(nanos, 2_000_000_000_000_000_000, "seconds should be UTC");
            assert_eq!(tz_offset_secs, Some(7200));
        }
        other => panic!("expected DateTime, got {other:?}"),
    }
}

#[test]
fn datetime_roundtrip_pre_epoch() {
    // Negative epoch millis (1960s) must encode and decode
    // correctly — div_euclid/rem_euclid on negative `ms` should
    // land at the right seconds+nanos pair rather than drifting
    // by one whole second.
    let input = Property::DateTime {
        nanos: -315_619_200_000,
        tz_offset_secs: Some(0),
        tz_name: None,
    }; // 1960-01-01
    let out = roundtrip_property(input.clone());
    assert_eq!(out, input);
}

#[test]
fn date_roundtrip_preserves_days() {
    let input = Property::Date(20089); // 2025-01-01
    let out = roundtrip_property(input.clone());
    assert_eq!(out, input);
}

#[test]
fn date_roundtrip_epoch_day_zero() {
    let input = Property::Date(0);
    let out = roundtrip_property(input.clone());
    assert_eq!(out, input);
}

#[test]
fn date_roundtrip_negative_days() {
    let input = Property::Date(-365);
    let out = roundtrip_property(input.clone());
    assert_eq!(out, input);
}

#[test]
fn duration_roundtrip_preserves_all_components() {
    let input = Property::Duration(Duration {
        months: 5,
        days: 17,
        seconds: 3_723,
        nanos: 123_456_789,
    });
    let out = roundtrip_property(input.clone());
    assert_eq!(out, input);
}

#[test]
fn duration_roundtrip_zero_components() {
    let input = Property::Duration(Duration {
        months: 0,
        days: 0,
        seconds: 0,
        nanos: 0,
    });
    let out = roundtrip_property(input.clone());
    assert_eq!(out, input);
}

#[test]
fn duration_roundtrip_negative_components() {
    let input = Property::Duration(Duration {
        months: -1,
        days: -2,
        seconds: -3,
        nanos: -4,
    });
    let out = roundtrip_property(input.clone());
    assert_eq!(out, input);
}

#[test]
fn point_2d_cartesian_roundtrip() {
    let input = Property::Point(Point {
        srid: SRID_CARTESIAN_2D,
        x: 12.5,
        y: -3.25,
        z: None,
    });
    let out = roundtrip_property(input.clone());
    assert_eq!(out, input);
}

#[test]
fn point_3d_cartesian_roundtrip() {
    let input = Property::Point(Point {
        srid: SRID_CARTESIAN_3D,
        x: 1.0,
        y: 2.0,
        z: Some(3.5),
    });
    let out = roundtrip_property(input.clone());
    assert_eq!(out, input);
}

#[test]
fn point_2d_wgs84_roundtrip() {
    let input = Property::Point(Point {
        srid: SRID_WGS84_2D,
        x: -122.4194,
        y: 37.7749,
        z: None,
    });
    let out = roundtrip_property(input.clone());
    assert_eq!(out, input);
}

#[test]
fn point_3d_wgs84_roundtrip() {
    let input = Property::Point(Point {
        srid: SRID_WGS84_3D,
        x: 18.0686,
        y: 59.3293,
        z: Some(28.0),
    });
    let out = roundtrip_property(input.clone());
    assert_eq!(out, input);
}

// ---- Graph struct version-conditional field counts ----

#[test]
fn node_struct_drops_element_id_under_bolt_4_4() {
    // Bolt 4.4 Node is 3 fields; 5.0+ added element_id as the 4th.
    let node = sample_node("Alice");
    let mut row = Row::new();
    row.insert("n".to_string(), Value::Node(node.clone()));

    let bolt_44 = &row_to_bolt_fields(&row, &["n".to_string()], meshdb_bolt::BOLT_4_4)[0];
    match bolt_44 {
        BoltValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_NODE);
            assert_eq!(fields.len(), 3, "4.4 Node must be 3 fields");
        }
        other => panic!("expected Struct, got {other:?}"),
    }

    let bolt_54 = &row_to_bolt_fields(&row, &["n".to_string()], meshdb_bolt::BOLT_5_4)[0];
    match bolt_54 {
        BoltValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_NODE);
            assert_eq!(fields.len(), 4, "5.x Node must be 4 fields");
            // 4th field is element_id as the UUID string.
            match &fields[3] {
                BoltValue::String(s) => assert_eq!(s, &node.id.as_uuid().to_string()),
                _ => panic!("element_id field wrong type"),
            }
        }
        other => panic!("expected Struct, got {other:?}"),
    }
}

#[test]
fn relationship_struct_drops_element_ids_under_bolt_4_4() {
    // Bolt 4.4 Relationship is 5 fields; 5.0+ adds element_id,
    // start_element_id, end_element_id for a total of 8.
    let a = sample_node("A");
    let b = sample_node("B");
    let edge = sample_edge(a.id, b.id);
    let mut row = Row::new();
    row.insert("r".to_string(), Value::Edge(edge.clone()));

    let bolt_44 = &row_to_bolt_fields(&row, &["r".to_string()], meshdb_bolt::BOLT_4_4)[0];
    match bolt_44 {
        BoltValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_RELATIONSHIP);
            assert_eq!(fields.len(), 5, "4.4 Relationship must be 5 fields");
        }
        other => panic!("expected Struct, got {other:?}"),
    }

    let bolt_54 = &row_to_bolt_fields(&row, &["r".to_string()], meshdb_bolt::BOLT_5_4)[0];
    match bolt_54 {
        BoltValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_RELATIONSHIP);
            assert_eq!(fields.len(), 8, "5.x Relationship must be 8 fields");
        }
        other => panic!("expected Struct, got {other:?}"),
    }
}

// ---- Path (server → client only) ----
//
// We don't have a `BoltValue → Value::Path` decoder because
// drivers never send paths as params. The roundtrip test here
// verifies that `row_to_bolt_fields` on a `Value::Path` emits a
// well-formed Bolt Path struct whose bytes survive packstream
// encode/decode unchanged — that's the contract that matters
// for wire stability.

fn sample_node(name: &str) -> Node {
    let id = NodeId::new();
    let mut n = Node::new();
    n.id = id;
    n.labels.push("Person".to_string());
    n.properties
        .insert("name".to_string(), Property::String(name.to_string()));
    n
}

fn sample_edge(src: NodeId, dst: NodeId) -> Edge {
    let mut e = Edge::new("KNOWS", src, dst);
    e.id = EdgeId::new();
    e
}

#[test]
fn path_roundtrip_shape_survives_packstream() {
    let a = sample_node("Ada");
    let b = sample_node("Bob");
    let c = sample_node("Cara");
    let e_ab = sample_edge(a.id, b.id);
    let e_bc = sample_edge(b.id, c.id);

    let path = Value::Path {
        nodes: vec![a.clone(), b.clone(), c.clone()],
        edges: vec![e_ab.clone(), e_bc.clone()],
    };
    let mut row = Row::new();
    row.insert("p".to_string(), path);
    let fields = field_names_from_rows(&[row.clone()]);
    let bolt = row_to_bolt_fields(&row, &fields, meshdb_bolt::BOLT_5_4);
    let bytes = encode(&bolt[0]);
    let (decoded, consumed) = decode(&bytes).expect("packstream decode");
    assert_eq!(consumed, bytes.len());

    // Path struct: {nodes: List<Node>, rels: List<UnboundRel>, seq: List<Int>}
    let (tag, fields) = match &decoded {
        BoltValue::Struct { tag, fields } => (*tag, fields),
        other => panic!("expected Struct, got {other:?}"),
    };
    assert_eq!(tag, TAG_PATH);
    assert_eq!(fields.len(), 3);

    // Field 0: deduped nodes list, each a Node struct.
    let nodes_list = fields[0].as_list().expect("nodes list");
    assert_eq!(nodes_list.len(), 3);
    for n in nodes_list {
        match n {
            BoltValue::Struct { tag, .. } => assert_eq!(*tag, TAG_NODE),
            other => panic!("expected Node struct, got {other:?}"),
        }
    }

    // Field 1: deduped rels list, each an UnboundRelationship.
    let rels_list = fields[1].as_list().expect("rels list");
    assert_eq!(rels_list.len(), 2);
    for r in rels_list {
        match r {
            BoltValue::Struct { tag, .. } => assert_eq!(*tag, TAG_UNBOUND_RELATIONSHIP),
            other => panic!("expected UnboundRelationship struct, got {other:?}"),
        }
    }

    // Field 2: sequence — alternating (rel_index, node_index) pairs.
    // 2 hops → 4 entries. Rel indices are 1-based and positive
    // for forward traversal; node indices are 0-based and reference
    // the deduped nodes list.
    let seq = fields[2].as_list().expect("sequence list");
    assert_eq!(seq.len(), 4);
    let seq_ints: Vec<i64> = seq.iter().filter_map(|v| v.as_int()).collect();
    assert_eq!(seq_ints, vec![1, 1, 2, 2]);
}

#[test]
fn path_with_repeated_node_dedupes_in_wire() {
    // Cyclic path a → b → a. The deduped `nodes` list has
    // length 2; the sequence references node 0 twice (once at
    // the start, once as the final target).
    let a = sample_node("Ada");
    let b = sample_node("Bob");
    let e_ab = sample_edge(a.id, b.id);
    let e_ba = sample_edge(b.id, a.id);

    let path = Value::Path {
        nodes: vec![a.clone(), b.clone(), a.clone()],
        edges: vec![e_ab.clone(), e_ba.clone()],
    };
    let mut row = Row::new();
    row.insert("p".to_string(), path);
    let fields = field_names_from_rows(&[row.clone()]);
    let bolt = row_to_bolt_fields(&row, &fields, meshdb_bolt::BOLT_5_4);
    let bytes = encode(&bolt[0]);
    let (decoded, _) = decode(&bytes).unwrap();

    let fields = match decoded {
        BoltValue::Struct { fields, .. } => fields,
        _ => panic!("expected Struct"),
    };
    assert_eq!(fields[0].as_list().unwrap().len(), 2, "nodes deduped");
    assert_eq!(fields[1].as_list().unwrap().len(), 2, "rels not deduped");
    let seq: Vec<i64> = fields[2]
        .as_list()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_int())
        .collect();
    // (rel_index=1, node_index=1), (rel_index=2, node_index=0)
    assert_eq!(seq, vec![1, 1, 2, 0]);
}

// ---- Wire-format tag sanity ----
//
// These check the expected struct tag bytes surface at the
// right positions in the encoded stream. If the Bolt protocol
// ever bumps a tag we'd catch it here at the unit level rather
// than watching an end-to-end test fail with an obscure error.

#[test]
fn local_datetime_emits_local_date_time_tag() {
    let input = Property::LocalDateTime(1_735_689_600_000);
    let mut row = Row::new();
    row.insert("v".into(), Value::Property(input));
    let bolt = row_to_bolt_fields(&row, &["v".to_string()], meshdb_bolt::BOLT_5_4);
    match &bolt[0] {
        BoltValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_LOCAL_DATE_TIME);
            assert_eq!(fields.len(), 2);
        }
        other => panic!("expected Struct, got {other:?}"),
    }
}

#[test]
fn date_emits_date_tag() {
    let input = Property::Date(20089);
    let mut row = Row::new();
    row.insert("v".into(), Value::Property(input));
    let bolt = row_to_bolt_fields(&row, &["v".to_string()], meshdb_bolt::BOLT_5_4);
    match &bolt[0] {
        BoltValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_DATE);
            assert_eq!(fields.len(), 1);
        }
        other => panic!("expected Struct, got {other:?}"),
    }
}

#[test]
fn duration_emits_duration_tag_with_four_fields() {
    let input = Property::Duration(Duration {
        months: 1,
        days: 2,
        seconds: 3,
        nanos: 4,
    });
    let mut row = Row::new();
    row.insert("v".into(), Value::Property(input));
    let bolt = row_to_bolt_fields(&row, &["v".to_string()], meshdb_bolt::BOLT_5_4);
    match &bolt[0] {
        BoltValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_DURATION);
            assert_eq!(fields.len(), 4);
        }
        other => panic!("expected Struct, got {other:?}"),
    }
}

#[test]
fn unbound_relationship_tag_surfaces_inside_path() {
    // Sanity check that the Path's rels list uses
    // TAG_UNBOUND_RELATIONSHIP (0x72), not the full
    // TAG_RELATIONSHIP (0x52) — the wire format requires the
    // unbound variant inside a Path since the endpoints come
    // from the sequence indices.
    let a = sample_node("Ada");
    let b = sample_node("Bob");
    let e = sample_edge(a.id, b.id);
    let path = Value::Path {
        nodes: vec![a, b],
        edges: vec![e],
    };
    let mut row = Row::new();
    row.insert("p".to_string(), path);
    let bolt = row_to_bolt_fields(&row, &["p".to_string()], meshdb_bolt::BOLT_5_4);
    let rels_list = match &bolt[0] {
        BoltValue::Struct { fields, .. } => fields[1].as_list().unwrap(),
        _ => panic!("expected Struct"),
    };
    match &rels_list[0] {
        BoltValue::Struct { tag, .. } => {
            assert_eq!(*tag, TAG_UNBOUND_RELATIONSHIP);
            assert_ne!(*tag, TAG_RELATIONSHIP);
        }
        _ => panic!("expected Struct"),
    }
}
