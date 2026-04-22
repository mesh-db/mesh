//! `apoc.meta.*` — value type introspection.
//!
//! This is the scalar slice of Neo4j's `apoc.meta.*` surface —
//! the *procedure* side (`apoc.meta.schema`,
//! `apoc.meta.nodeTypeProperties`, …) walks the live graph and
//! belongs behind the executor's procedure registry, not this
//! crate. Everything here is a side-effect-free transform over a
//! single [`Property`] and carries no dependency of its own, so
//! the `meta` Cargo feature is pure code gating.
//!
//! Shipped functions:
//!
//! - `apoc.meta.type(value)` — upper-snake-case type name as a
//!   string (`"STRING"`, `"INTEGER"`, `"LIST"`, `"DATE_TIME"`, …).
//!   The spelling matches what the Neo4j APOC library returns so
//!   ported queries and dashboards keep rendering the same labels.
//! - `apoc.meta.isType(value, typeName)` — case-insensitive
//!   comparison between `type(value)` and the caller-supplied
//!   name. Matches both `"STRING"` and `"string"`.
//! - `apoc.meta.types(value)` — if `value` is a `Map`, returns a
//!   `Map<String, String>` of each entry's type; for non-map
//!   inputs, wraps the value under an empty-string key (matching
//!   the Neo4j APOC convention).
//!
//! Null handling follows the rest of the crate: `Null` in →
//! `"NULL"` out for `type` (so the function is total), `Null`
//! pair value preserved as `"NULL"` in the `types` map, and
//! `isType` returns `false` rather than `Null` when either
//! argument is `Null` — matching APOC's truthy-boolean behavior
//! for this specific function.

use crate::ApocError;
use meshdb_core::Property;
use std::collections::HashMap;

pub fn call(suffix: &str, args: &[Property]) -> Result<Property, ApocError> {
    match suffix {
        "type" => type_of(args),
        "istype" => is_type(args),
        "types" => types(args),
        _ => Err(ApocError::UnknownFunction(format!("apoc.meta.{suffix}"))),
    }
}

fn type_of(args: &[Property]) -> Result<Property, ApocError> {
    if args.len() != 1 {
        return Err(ApocError::Arity {
            name: "apoc.meta.type".into(),
            expected: 1,
            got: args.len(),
        });
    }
    Ok(Property::String(apoc_type_name(&args[0]).to_string()))
}

fn is_type(args: &[Property]) -> Result<Property, ApocError> {
    if args.len() != 2 {
        return Err(ApocError::Arity {
            name: "apoc.meta.isType".into(),
            expected: 2,
            got: args.len(),
        });
    }
    let expected = match &args[1] {
        Property::String(s) => s,
        Property::Null => return Ok(Property::Bool(false)),
        other => {
            return Err(ApocError::TypeMismatch {
                name: "apoc.meta.isType".into(),
                details: format!(
                    "second argument must be a string, got {}",
                    other.type_name()
                ),
            });
        }
    };
    let actual = apoc_type_name(&args[0]);
    Ok(Property::Bool(expected.eq_ignore_ascii_case(actual)))
}

fn types(args: &[Property]) -> Result<Property, ApocError> {
    if args.len() != 1 {
        return Err(ApocError::Arity {
            name: "apoc.meta.types".into(),
            expected: 1,
            got: args.len(),
        });
    }
    let out = match &args[0] {
        Property::Map(m) => m
            .iter()
            .map(|(k, v)| (k.clone(), Property::String(apoc_type_name(v).to_string())))
            .collect::<HashMap<_, _>>(),
        other => {
            let mut m = HashMap::with_capacity(1);
            m.insert(
                String::new(),
                Property::String(apoc_type_name(other).to_string()),
            );
            m
        }
    };
    Ok(Property::Map(out))
}

/// Upper-snake-case type name for a `Property`, matching the
/// spelling Neo4j's APOC library returns from `apoc.meta.type`.
/// Not `Property::type_name` because Mesh's internal names
/// (`"Int64"`, `"DateTime"`) differ from the APOC convention
/// (`"INTEGER"`, `"DATE_TIME"`).
fn apoc_type_name(p: &Property) -> &'static str {
    match p {
        Property::Null => "NULL",
        Property::String(_) => "STRING",
        Property::Int64(_) => "INTEGER",
        Property::Float64(_) => "FLOAT",
        Property::Bool(_) => "BOOLEAN",
        Property::List(_) => "LIST",
        Property::Map(_) => "MAP",
        Property::DateTime { .. } => "DATE_TIME",
        Property::LocalDateTime(_) => "LOCAL_DATE_TIME",
        Property::Date(_) => "DATE",
        Property::Time { .. } => "TIME",
        Property::Duration(_) => "DURATION",
        Property::Point(_) => "POINT",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Property {
        Property::String(v.to_string())
    }

    #[test]
    fn type_of_each_scalar_kind() {
        let cases: &[(Property, &str)] = &[
            (Property::Null, "NULL"),
            (s("hello"), "STRING"),
            (Property::Int64(7), "INTEGER"),
            (Property::Float64(1.5), "FLOAT"),
            (Property::Bool(true), "BOOLEAN"),
            (Property::List(vec![Property::Int64(1)]), "LIST"),
            (Property::Map(HashMap::new()), "MAP"),
        ];
        for (input, expected) in cases {
            let out = call("type", &[input.clone()]).unwrap();
            assert_eq!(out, s(expected), "type({input:?})");
        }
    }

    #[test]
    fn type_of_temporals_and_point() {
        use meshdb_core::{Duration, Point, SRID_CARTESIAN_2D};
        let cases: &[(Property, &str)] = &[
            (
                Property::DateTime {
                    nanos: 0,
                    tz_offset_secs: None,
                    tz_name: None,
                },
                "DATE_TIME",
            ),
            (Property::LocalDateTime(0), "LOCAL_DATE_TIME"),
            (Property::Date(0), "DATE"),
            (
                Property::Time {
                    nanos: 0,
                    tz_offset_secs: None,
                },
                "TIME",
            ),
            (
                Property::Duration(Duration {
                    months: 0,
                    days: 0,
                    seconds: 0,
                    nanos: 0,
                }),
                "DURATION",
            ),
            (
                Property::Point(Point {
                    srid: SRID_CARTESIAN_2D,
                    x: 0.0,
                    y: 0.0,
                    z: None,
                }),
                "POINT",
            ),
        ];
        for (input, expected) in cases {
            let out = call("type", &[input.clone()]).unwrap();
            assert_eq!(out, s(expected), "type({input:?})");
        }
    }

    #[test]
    fn is_type_is_case_insensitive() {
        let cases = [
            ("STRING", true),
            ("string", true),
            ("String", true),
            ("INTEGER", false),
        ];
        for (name, expected) in cases {
            let out = call("istype", &[s("hi"), s(name)]).unwrap();
            assert_eq!(out, Property::Bool(expected), "isType(\"hi\", {name:?})");
        }
    }

    #[test]
    fn is_type_null_second_arg_is_false() {
        // `Null` as the type name returns false, not Null — matches
        // APOC which always yields a boolean.
        let out = call("istype", &[Property::Int64(1), Property::Null]).unwrap();
        assert_eq!(out, Property::Bool(false));
    }

    #[test]
    fn is_type_non_string_second_arg_type_mismatches() {
        let err = call("istype", &[s("x"), Property::Int64(7)]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn types_of_map_returns_per_key_types() {
        let mut m = HashMap::new();
        m.insert("name".to_string(), s("alice"));
        m.insert("age".to_string(), Property::Int64(30));
        m.insert("score".to_string(), Property::Float64(0.5));
        let out = call("types", &[Property::Map(m)]).unwrap();
        let Property::Map(got) = out else {
            panic!("expected Map, got {out:?}");
        };
        assert_eq!(got.get("name"), Some(&s("STRING")));
        assert_eq!(got.get("age"), Some(&s("INTEGER")));
        assert_eq!(got.get("score"), Some(&s("FLOAT")));
    }

    #[test]
    fn types_of_non_map_wraps_under_empty_key() {
        let out = call("types", &[Property::Int64(42)]).unwrap();
        let Property::Map(got) = out else {
            panic!("expected Map, got {out:?}");
        };
        assert_eq!(got.len(), 1);
        assert_eq!(got.get(""), Some(&s("INTEGER")));
    }

    #[test]
    fn arity_errors_name_the_function() {
        let err = call("type", &[]).unwrap_err();
        match err {
            ApocError::Arity {
                name,
                expected,
                got,
            } => {
                assert_eq!(name, "apoc.meta.type");
                assert_eq!(expected, 1);
                assert_eq!(got, 0);
            }
            other => panic!("expected Arity, got {other:?}"),
        }
    }

    #[test]
    fn unknown_function_surfaces_full_name() {
        let err = call("schema", &[]).unwrap_err();
        match err {
            ApocError::UnknownFunction(n) => assert_eq!(n, "apoc.meta.schema"),
            other => panic!("expected UnknownFunction, got {other:?}"),
        }
    }
}
