use crate::error::ConvertError;
use crate::proto;
use meshdb_core::{Duration, Edge, EdgeId, Node, NodeId, Property};
use std::collections::HashMap;
use uuid::Uuid;

pub fn uuid_to_proto(uuid: Uuid) -> proto::UuidBytes {
    proto::UuidBytes {
        value: uuid.as_bytes().to_vec(),
    }
}

pub fn uuid_from_proto(b: &proto::UuidBytes) -> Result<Uuid, ConvertError> {
    if b.value.len() != 16 {
        return Err(ConvertError::InvalidUuidLength(b.value.len()));
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&b.value);
    Ok(Uuid::from_bytes(bytes))
}

pub fn node_id_from_proto(b: &proto::UuidBytes) -> Result<NodeId, ConvertError> {
    Ok(NodeId::from_uuid(uuid_from_proto(b)?))
}

pub fn edge_id_from_proto(b: &proto::UuidBytes) -> Result<EdgeId, ConvertError> {
    Ok(EdgeId::from_uuid(uuid_from_proto(b)?))
}

pub fn property_to_proto(p: &Property) -> Result<proto::Property, ConvertError> {
    use proto::property::Kind;
    let kind = match p {
        Property::Null => Kind::NullVal(proto::NullValue {}),
        Property::String(s) => Kind::StringVal(s.clone()),
        Property::Int64(i) => Kind::IntVal(*i),
        Property::Float64(f) => Kind::FloatVal(*f),
        Property::Bool(b) => Kind::BoolVal(*b),
        Property::Point(p) => Kind::PointVal(proto::PointValue {
            srid: p.srid,
            x: p.x,
            y: p.y,
            z: p.z,
        }),
        Property::DateTime {
            nanos,
            tz_offset_secs,
            tz_name,
        } => {
            // Same (seconds, subsec_nanos) split the Bolt encoder
            // uses — Property::DateTime carries nanos as i128 to
            // cover the full TCK year range, but protobuf has no
            // i128 so we split via div/rem and let the decoder
            // reassemble.
            let seconds = nanos.div_euclid(1_000_000_000) as i64;
            let subsec_nanos = nanos.rem_euclid(1_000_000_000) as i32;
            Kind::DatetimeVal(proto::DateTimeValue {
                seconds,
                subsec_nanos,
                tz_offset_secs: *tz_offset_secs,
                tz_name: tz_name.clone(),
            })
        }
        Property::LocalDateTime(nanos) => {
            let seconds = nanos.div_euclid(1_000_000_000) as i64;
            let subsec_nanos = nanos.rem_euclid(1_000_000_000) as i32;
            Kind::LocalDatetimeVal(proto::LocalDateTimeValue {
                seconds,
                subsec_nanos,
            })
        }
        Property::Date(days) => Kind::DateVal(proto::DateValue { days: *days }),
        Property::Time {
            nanos,
            tz_offset_secs,
        } => Kind::TimeVal(proto::TimeValue {
            nanos: *nanos,
            tz_offset_secs: *tz_offset_secs,
        }),
        Property::Duration(d) => Kind::DurationVal(proto::DurationValue {
            months: d.months,
            days: d.days,
            seconds: d.seconds,
            nanos: d.nanos,
        }),
        // List and Map are recursive — representing them requires a
        // self-referential protobuf schema. Left for a follow-up so
        // this change stays focused on the temporal + spatial
        // scalars that cross peers in the routing-mode write path.
        Property::List(_) | Property::Map(_) => return Err(ConvertError::UnsupportedProperty),
    };
    Ok(proto::Property { kind: Some(kind) })
}

pub fn property_from_proto(p: &proto::Property) -> Property {
    use proto::property::Kind;
    match &p.kind {
        None | Some(Kind::NullVal(_)) => Property::Null,
        Some(Kind::StringVal(s)) => Property::String(s.clone()),
        Some(Kind::IntVal(i)) => Property::Int64(*i),
        Some(Kind::FloatVal(f)) => Property::Float64(*f),
        Some(Kind::BoolVal(b)) => Property::Bool(*b),
        Some(Kind::PointVal(pv)) => Property::Point(meshdb_core::Point {
            srid: pv.srid,
            x: pv.x,
            y: pv.y,
            z: pv.z,
        }),
        Some(Kind::DatetimeVal(v)) => Property::DateTime {
            nanos: (v.seconds as i128) * 1_000_000_000 + (v.subsec_nanos as i128),
            tz_offset_secs: v.tz_offset_secs,
            tz_name: v.tz_name.clone(),
        },
        Some(Kind::LocalDatetimeVal(v)) => {
            Property::LocalDateTime((v.seconds as i128) * 1_000_000_000 + (v.subsec_nanos as i128))
        }
        Some(Kind::DateVal(v)) => Property::Date(v.days),
        Some(Kind::TimeVal(v)) => Property::Time {
            nanos: v.nanos,
            tz_offset_secs: v.tz_offset_secs,
        },
        Some(Kind::DurationVal(v)) => Property::Duration(Duration {
            months: v.months,
            days: v.days,
            seconds: v.seconds,
            nanos: v.nanos,
        }),
    }
}

pub fn node_to_proto(n: &Node) -> Result<proto::Node, ConvertError> {
    let mut properties = HashMap::with_capacity(n.properties.len());
    for (k, v) in &n.properties {
        properties.insert(k.clone(), property_to_proto(v)?);
    }
    Ok(proto::Node {
        id: Some(uuid_to_proto(n.id.as_uuid())),
        labels: n.labels.clone(),
        properties,
    })
}

pub fn node_from_proto(n: proto::Node) -> Result<Node, ConvertError> {
    let id_proto = n.id.ok_or(ConvertError::MissingId)?;
    let id = node_id_from_proto(&id_proto)?;
    let properties = n
        .properties
        .into_iter()
        .map(|(k, v)| (k, property_from_proto(&v)))
        .collect();
    Ok(Node {
        id,
        labels: n.labels,
        properties,
    })
}

pub fn edge_to_proto(e: &Edge) -> Result<proto::Edge, ConvertError> {
    let mut properties = HashMap::with_capacity(e.properties.len());
    for (k, v) in &e.properties {
        properties.insert(k.clone(), property_to_proto(v)?);
    }
    Ok(proto::Edge {
        id: Some(uuid_to_proto(e.id.as_uuid())),
        edge_type: e.edge_type.clone(),
        source: Some(uuid_to_proto(e.source.as_uuid())),
        target: Some(uuid_to_proto(e.target.as_uuid())),
        properties,
    })
}

pub fn edge_from_proto(e: proto::Edge) -> Result<Edge, ConvertError> {
    let id_proto = e.id.ok_or(ConvertError::MissingId)?;
    let id = edge_id_from_proto(&id_proto)?;
    let source = node_id_from_proto(&e.source.ok_or(ConvertError::MissingId)?)?;
    let target = node_id_from_proto(&e.target.ok_or(ConvertError::MissingId)?)?;
    let properties = e
        .properties
        .into_iter()
        .map(|(k, v)| (k, property_from_proto(&v)))
        .collect();
    Ok(Edge {
        id,
        edge_type: e.edge_type,
        source,
        target,
        properties,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use meshdb_core::{Point, SRID_CARTESIAN_2D, SRID_CARTESIAN_3D, SRID_WGS84_2D, SRID_WGS84_3D};

    fn roundtrip(p: Property) -> Property {
        let proto = property_to_proto(&p).expect("to_proto");
        property_from_proto(&proto)
    }

    #[test]
    fn point_2d_cartesian_roundtrips_through_proto() {
        let input = Property::Point(Point {
            srid: SRID_CARTESIAN_2D,
            x: 12.5,
            y: -3.25,
            z: None,
        });
        assert_eq!(roundtrip(input.clone()), input);
    }

    #[test]
    fn point_3d_cartesian_roundtrips_through_proto() {
        let input = Property::Point(Point {
            srid: SRID_CARTESIAN_3D,
            x: 1.0,
            y: 2.0,
            z: Some(3.5),
        });
        assert_eq!(roundtrip(input.clone()), input);
    }

    #[test]
    fn point_2d_wgs84_roundtrips_through_proto() {
        let input = Property::Point(Point {
            srid: SRID_WGS84_2D,
            x: -122.4194,
            y: 37.7749,
            z: None,
        });
        assert_eq!(roundtrip(input.clone()), input);
    }

    #[test]
    fn point_3d_wgs84_roundtrips_through_proto() {
        let input = Property::Point(Point {
            srid: SRID_WGS84_3D,
            x: 18.0686,
            y: 59.3293,
            z: Some(28.0),
        });
        assert_eq!(roundtrip(input.clone()), input);
    }

    #[test]
    fn datetime_with_offset_roundtrips() {
        // 2025-01-01T00:00:00.123456789Z
        let input = Property::DateTime {
            nanos: 1_735_689_600_123_456_789_i128,
            tz_offset_secs: Some(0),
            tz_name: None,
        };
        assert_eq!(roundtrip(input.clone()), input);
    }

    #[test]
    fn datetime_pre_epoch_roundtrips() {
        // 1960-01-01 — exercises the div_euclid / rem_euclid split
        // on negative nanos so the seconds + subsec pair decodes
        // back to the exact i128 we started with.
        let input = Property::DateTime {
            nanos: -315_619_200_000_000_000_i128,
            tz_offset_secs: Some(0),
            tz_name: None,
        };
        assert_eq!(roundtrip(input.clone()), input);
    }

    #[test]
    fn datetime_with_tz_name_roundtrips() {
        let input = Property::DateTime {
            nanos: 1_735_689_600_000_000_000_i128,
            tz_offset_secs: Some(3600),
            tz_name: Some("Europe/Stockholm".to_string()),
        };
        assert_eq!(roundtrip(input.clone()), input);
    }

    #[test]
    fn local_datetime_roundtrips() {
        let input = Property::LocalDateTime(1_735_689_600_123_456_789_i128);
        assert_eq!(roundtrip(input.clone()), input);
    }

    #[test]
    fn date_roundtrips() {
        let input = Property::Date(20_089); // 2025-01-01
        assert_eq!(roundtrip(input.clone()), input);
    }

    #[test]
    fn date_negative_roundtrips() {
        let input = Property::Date(-365);
        assert_eq!(roundtrip(input.clone()), input);
    }

    #[test]
    fn time_naive_roundtrips() {
        let input = Property::Time {
            nanos: 45_123_456_789,
            tz_offset_secs: None,
        };
        assert_eq!(roundtrip(input.clone()), input);
    }

    #[test]
    fn time_with_offset_roundtrips() {
        let input = Property::Time {
            nanos: 45_123_456_789,
            tz_offset_secs: Some(-7200),
        };
        assert_eq!(roundtrip(input.clone()), input);
    }

    #[test]
    fn duration_all_components_roundtrip() {
        let input = Property::Duration(Duration {
            months: 5,
            days: 17,
            seconds: 3_723,
            nanos: 123_456_789,
        });
        assert_eq!(roundtrip(input.clone()), input);
    }

    #[test]
    fn duration_negative_components_roundtrip() {
        let input = Property::Duration(Duration {
            months: -1,
            days: -2,
            seconds: -3,
            nanos: -4,
        });
        assert_eq!(roundtrip(input.clone()), input);
    }
}
