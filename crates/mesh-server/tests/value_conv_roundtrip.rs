//! Wire-format roundtrip tests for the `mesh-server::value_conv`
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

use mesh_bolt::{
    decode, encode, BoltValue, TAG_DATE, TAG_DURATION, TAG_LOCAL_DATE_TIME, TAG_NODE, TAG_PATH,
    TAG_RELATIONSHIP, TAG_UNBOUND_RELATIONSHIP,
};
use mesh_core::{Duration, Edge, EdgeId, Node, NodeId, Property};
use mesh_executor::{Row, Value};
use mesh_server::value_conv::{bolt_value_to_property, field_names_from_rows, row_to_bolt_fields};

/// Push a property into a one-key row, run it through the Bolt
/// encode path, round-trip the bytes through packstream, and
/// decode back to a `Property`. Asserts the value survives the
/// full wire cycle.
fn roundtrip_property(p: Property) -> Property {
    let mut row = Row::new();
    row.insert("v".to_string(), Value::Property(p));
    let fields = field_names_from_rows(&[row.clone()]);
    let bolt = row_to_bolt_fields(&row, &fields);
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
    let input = Property::DateTime(1_735_689_600_000);
    let out = roundtrip_property(input.clone());
    assert_eq!(out, input);
}

#[test]
fn datetime_roundtrip_with_sub_second_fraction() {
    // Ensure the millisecond granularity survives the
    // seconds/nanos split the Bolt LocalDateTime struct uses.
    let input = Property::DateTime(1_735_689_600_123);
    let out = roundtrip_property(input.clone());
    assert_eq!(out, input);
}

#[test]
fn datetime_roundtrip_pre_epoch() {
    // Negative epoch millis (1960s) must encode and decode
    // correctly — div_euclid/rem_euclid on negative `ms` should
    // land at the right seconds+nanos pair rather than drifting
    // by one whole second.
    let input = Property::DateTime(-315_619_200_000); // 1960-01-01
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
    let bolt = row_to_bolt_fields(&row, &fields);
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
    let bolt = row_to_bolt_fields(&row, &fields);
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
fn datetime_emits_local_date_time_tag() {
    let input = Property::DateTime(1_735_689_600_000);
    let mut row = Row::new();
    row.insert("v".into(), Value::Property(input));
    let bolt = row_to_bolt_fields(&row, &["v".to_string()]);
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
    let bolt = row_to_bolt_fields(&row, &["v".to_string()]);
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
    let bolt = row_to_bolt_fields(&row, &["v".to_string()]);
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
    let bolt = row_to_bolt_fields(&row, &["p".to_string()]);
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
