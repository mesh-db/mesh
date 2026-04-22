//! `apoc.convert.*` — JSON serialization helpers.
//!
//! Two directions, three shipped functions:
//!
//! - `apoc.convert.toJson(any)` — serializes a `Property` to a
//!   JSON string. Null, Bool, Int64, Float64, String, List, and
//!   Map all round-trip cleanly. Temporal (`DateTime`,
//!   `LocalDateTime`, `Date`, `Time`, `Duration`) and spatial
//!   (`Point`) values are rejected with [`ApocError::TypeMismatch`]
//!   in this release — the canonical string formatters live in
//!   `meshdb-executor`, and this crate can't depend on the
//!   executor without creating a cycle. A follow-up that lifts
//!   those formatters into `meshdb-core` will unlock the full
//!   Neo4j APOC surface.
//!
//! - `apoc.convert.fromJsonMap(String)` — parses a JSON object
//!   into a [`Property::Map`]. Errors if the parse succeeds but
//!   the top level is not an object.
//!
//! - `apoc.convert.fromJsonList(String)` — same shape, but expects
//!   a top-level JSON array and returns [`Property::List`].
//!
//! Integer coercion on parse: JSON numbers that fit in `i64` lower
//! to [`Property::Int64`]; otherwise they fall through to
//! [`Property::Float64`]. This matches Cypher's own "prefer
//! integer" rule for numeric literals.

use crate::ApocError;
use meshdb_core::Property;
use serde_json::{Map as JsonMap, Number, Value as Json};

pub fn call(suffix: &str, args: &[Property]) -> Result<Property, ApocError> {
    match suffix {
        "tojson" => to_json(args),
        "fromjsonmap" => from_json_map(args),
        "fromjsonlist" => from_json_list(args),
        _ => Err(ApocError::UnknownFunction(format!("apoc.convert.{suffix}"))),
    }
}

// ---------------------------------------------------------------
// Property → JSON
// ---------------------------------------------------------------

fn to_json(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("toJson", args, 1)?;
    let j = property_to_json(&args[0])?;
    // `serde_json::to_string` can only fail on non-string map keys,
    // which our conversion already rejects — any error here is a
    // bug, not a user-visible condition.
    Ok(Property::String(j.to_string()))
}

fn property_to_json(p: &Property) -> Result<Json, ApocError> {
    match p {
        Property::Null => Ok(Json::Null),
        Property::Bool(b) => Ok(Json::Bool(*b)),
        Property::Int64(n) => Ok(Json::Number(Number::from(*n))),
        Property::Float64(f) => Number::from_f64(*f).map(Json::Number).ok_or_else(|| {
            // NaN / Infinity can't be represented in JSON. The Neo4j
            // APOC convention here is to emit `null`; we surface a
            // type error instead so the caller isn't silently lied to.
            ApocError::TypeMismatch {
                name: "apoc.convert.toJson".into(),
                details: format!("non-finite float {f} cannot be JSON-encoded"),
            }
        }),
        Property::String(s) => Ok(Json::String(s.clone())),
        Property::List(items) => items
            .iter()
            .map(property_to_json)
            .collect::<Result<Vec<_>, _>>()
            .map(Json::Array),
        Property::Map(entries) => {
            let mut out = JsonMap::with_capacity(entries.len());
            for (k, v) in entries {
                out.insert(k.clone(), property_to_json(v)?);
            }
            Ok(Json::Object(out))
        }
        other => Err(ApocError::TypeMismatch {
            name: "apoc.convert.toJson".into(),
            details: format!(
                "{} values are not supported by toJson yet",
                other.type_name()
            ),
        }),
    }
}

// ---------------------------------------------------------------
// JSON → Property
// ---------------------------------------------------------------

fn from_json_map(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("fromJsonMap", args, 1)?;
    match &args[0] {
        Property::Null => Ok(Property::Null),
        Property::String(s) => {
            let j = parse_json(s, "fromJsonMap")?;
            match j {
                Json::Object(obj) => Ok(Property::Map(
                    obj.into_iter()
                        .map(|(k, v)| Ok((k, json_to_property(v)?)))
                        .collect::<Result<_, ApocError>>()?,
                )),
                other => Err(ApocError::TypeMismatch {
                    name: "apoc.convert.fromJsonMap".into(),
                    details: format!(
                        "top-level JSON value must be an object, got {}",
                        json_kind(&other)
                    ),
                }),
            }
        }
        other => Err(ApocError::TypeMismatch {
            name: "apoc.convert.fromJsonMap".into(),
            details: format!("expected a string, got {}", other.type_name()),
        }),
    }
}

fn from_json_list(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("fromJsonList", args, 1)?;
    match &args[0] {
        Property::Null => Ok(Property::Null),
        Property::String(s) => {
            let j = parse_json(s, "fromJsonList")?;
            match j {
                Json::Array(items) => Ok(Property::List(
                    items
                        .into_iter()
                        .map(json_to_property)
                        .collect::<Result<_, _>>()?,
                )),
                other => Err(ApocError::TypeMismatch {
                    name: "apoc.convert.fromJsonList".into(),
                    details: format!(
                        "top-level JSON value must be an array, got {}",
                        json_kind(&other)
                    ),
                }),
            }
        }
        other => Err(ApocError::TypeMismatch {
            name: "apoc.convert.fromJsonList".into(),
            details: format!("expected a string, got {}", other.type_name()),
        }),
    }
}

fn json_to_property(j: Json) -> Result<Property, ApocError> {
    Ok(match j {
        Json::Null => Property::Null,
        Json::Bool(b) => Property::Bool(b),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                Property::Int64(i)
            } else if let Some(f) = n.as_f64() {
                Property::Float64(f)
            } else {
                // u64 overflow — JSON allows it, we don't.
                return Err(ApocError::TypeMismatch {
                    name: "apoc.convert.fromJson".into(),
                    details: format!("number {n} does not fit in i64 or f64"),
                });
            }
        }
        Json::String(s) => Property::String(s),
        Json::Array(items) => Property::List(
            items
                .into_iter()
                .map(json_to_property)
                .collect::<Result<_, _>>()?,
        ),
        Json::Object(obj) => Property::Map(
            obj.into_iter()
                .map(|(k, v)| Ok::<_, ApocError>((k, json_to_property(v)?)))
                .collect::<Result<_, _>>()?,
        ),
    })
}

// ---------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------

fn parse_json(s: &str, name: &str) -> Result<Json, ApocError> {
    serde_json::from_str(s).map_err(|e| ApocError::TypeMismatch {
        name: format!("apoc.convert.{name}"),
        details: format!("invalid JSON: {e}"),
    })
}

fn json_kind(j: &Json) -> &'static str {
    match j {
        Json::Null => "null",
        Json::Bool(_) => "boolean",
        Json::Number(_) => "number",
        Json::String(_) => "string",
        Json::Array(_) => "array",
        Json::Object(_) => "object",
    }
}

fn check_arity(name: &str, args: &[Property], expected: usize) -> Result<(), ApocError> {
    if args.len() != expected {
        return Err(ApocError::Arity {
            name: format!("apoc.convert.{name}"),
            expected,
            got: args.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn s(v: &str) -> Property {
        Property::String(v.to_string())
    }

    #[test]
    fn to_json_primitives_round_trip() {
        assert_eq!(to_json(&[Property::Null]).unwrap(), s("null"));
        assert_eq!(to_json(&[Property::Bool(true)]).unwrap(), s("true"));
        assert_eq!(to_json(&[Property::Int64(42)]).unwrap(), s("42"));
        assert_eq!(to_json(&[Property::Float64(1.5)]).unwrap(), s("1.5"));
        assert_eq!(to_json(&[s("hi")]).unwrap(), s("\"hi\""));
    }

    #[test]
    fn to_json_list_and_nested() {
        let list = Property::List(vec![
            Property::Int64(1),
            Property::Bool(false),
            s("x"),
            Property::Null,
        ]);
        assert_eq!(to_json(&[list]).unwrap(), s("[1,false,\"x\",null]"));
    }

    #[test]
    fn to_json_map_is_parseable_back() {
        // HashMap iteration order is non-deterministic so we check
        // round-trip rather than exact string.
        let mut m = HashMap::new();
        m.insert("a".to_string(), Property::Int64(1));
        m.insert("b".to_string(), s("hello"));
        let encoded = to_json(&[Property::Map(m)]).unwrap();
        let s_enc = match encoded {
            Property::String(s) => s,
            other => panic!("expected string, got {other:?}"),
        };
        let decoded = from_json_map(&[Property::String(s_enc)]).unwrap();
        match decoded {
            Property::Map(out) => {
                assert_eq!(out.len(), 2);
                assert!(matches!(out.get("a"), Some(Property::Int64(1))));
                assert!(matches!(out.get("b"), Some(Property::String(v)) if v == "hello"));
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn to_json_rejects_nan() {
        let err = to_json(&[Property::Float64(f64::NAN)]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn to_json_rejects_temporal_for_now() {
        let err = to_json(&[Property::Date(0)]).unwrap_err();
        match err {
            ApocError::TypeMismatch { details, .. } => {
                assert!(
                    details.contains("Date"),
                    "expected Date in details, got {details}"
                );
            }
            other => panic!("expected TypeMismatch, got {other:?}"),
        }
    }

    #[test]
    fn from_json_map_parses_object() {
        let out = from_json_map(&[s(r#"{"n": 1, "b": true, "s": "x"}"#)]).unwrap();
        match out {
            Property::Map(m) => {
                assert!(matches!(m.get("n"), Some(Property::Int64(1))));
                assert!(matches!(m.get("b"), Some(Property::Bool(true))));
                assert!(matches!(m.get("s"), Some(Property::String(v)) if v == "x"));
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn from_json_map_rejects_array_top_level() {
        let err = from_json_map(&[s("[1, 2, 3]")]).unwrap_err();
        match err {
            ApocError::TypeMismatch { details, .. } => {
                assert!(details.contains("array"), "unexpected details: {details}");
            }
            other => panic!("expected TypeMismatch, got {other:?}"),
        }
    }

    #[test]
    fn from_json_list_parses_array() {
        let out = from_json_list(&[s(r#"[1, "two", 3.5, null]"#)]).unwrap();
        match out {
            Property::List(items) => {
                assert_eq!(items.len(), 4);
                assert!(matches!(items[0], Property::Int64(1)));
                assert!(matches!(&items[1], Property::String(v) if v == "two"));
                assert!(matches!(items[2], Property::Float64(f) if f == 3.5));
                assert!(matches!(items[3], Property::Null));
            }
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn from_json_list_rejects_object_top_level() {
        let err = from_json_list(&[s(r#"{"a": 1}"#)]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn from_json_surfaces_parse_errors() {
        let err = from_json_map(&[s("not json")]).unwrap_err();
        match err {
            ApocError::TypeMismatch { details, .. } => {
                assert!(details.starts_with("invalid JSON"));
            }
            other => panic!("expected TypeMismatch, got {other:?}"),
        }
    }

    #[test]
    fn null_input_passes_through_on_from_json() {
        assert_eq!(from_json_map(&[Property::Null]).unwrap(), Property::Null,);
        assert_eq!(from_json_list(&[Property::Null]).unwrap(), Property::Null,);
    }

    #[test]
    fn nested_round_trip_preserves_shape() {
        let src = r#"{"xs": [1, 2, 3], "meta": {"name": "Ada", "active": true}}"#;
        let parsed = from_json_map(&[s(src)]).unwrap();
        let re_encoded = to_json(&[parsed]).unwrap();
        let again = from_json_map(&[re_encoded]).unwrap();
        match again {
            Property::Map(m) => {
                assert!(m.contains_key("xs"));
                assert!(m.contains_key("meta"));
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn arity_errors_name_the_function() {
        let err = to_json(&[]).unwrap_err();
        match err {
            ApocError::Arity {
                name,
                expected,
                got,
            } => {
                assert_eq!(name, "apoc.convert.toJson");
                assert_eq!(expected, 1);
                assert_eq!(got, 0);
            }
            other => panic!("expected Arity, got {other:?}"),
        }
    }

    #[test]
    fn unknown_function_surfaces_full_name() {
        let err = call("frobnicate", &[Property::Null]).unwrap_err();
        match err {
            ApocError::UnknownFunction(n) => assert_eq!(n, "apoc.convert.frobnicate"),
            other => panic!("expected UnknownFunction, got {other:?}"),
        }
    }
}
