//! `apoc.date.*` — epoch-millis helpers.
//!
//! Shipped functions:
//!
//! - `apoc.date.currentTimestamp()` — Unix epoch milliseconds
//!   (wall-clock). Not cached per statement; each call reads
//!   `SystemTime::now()` live.
//! - `apoc.date.toISO8601(millis)` — UTC ISO 8601 string, e.g.
//!   `"2026-04-22T14:30:00.123Z"`. Always emits the `Z` suffix.
//! - `apoc.date.fromISO8601(string)` — parses an RFC 3339 / ISO
//!   8601 string and returns Unix epoch milliseconds.
//! - `apoc.date.convert(value, from_unit, to_unit)` — pure integer
//!   unit conversion (truncating on narrowing).
//! - `apoc.date.add(value, unit, addValue, addUnit)` — adds
//!   `addValue addUnit` to `value unit`, returning the result
//!   back in `unit`.
//!
//! Supported unit strings: `ms`, `s`, `m` / `min`, `h`, `d`, `w`.
//! Matching is case-insensitive. Millisecond resolution is the
//! lingua franca; everything else converts to/from ms on the way
//! through.
//!
//! Null propagation: any function taking a primary value returns
//! `Null` if that value is `Null`. Unit-string arguments must be
//! present and valid — passing `null` for a unit is a type error.

use crate::ApocError;
use chrono::{DateTime, SecondsFormat, Utc};
use meshdb_core::Property;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn call(suffix: &str, args: &[Property]) -> Result<Property, ApocError> {
    match suffix {
        "currenttimestamp" => current_timestamp(args),
        "toiso8601" => to_iso8601(args),
        "fromiso8601" => from_iso8601(args),
        "convert" => convert(args),
        "add" => add(args),
        _ => Err(ApocError::UnknownFunction(format!("apoc.date.{suffix}"))),
    }
}

// ---------------------------------------------------------------
// Now / format / parse.
// ---------------------------------------------------------------

fn current_timestamp(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("currentTimestamp", args, 0)?;
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    Ok(Property::Int64(ms))
}

fn to_iso8601(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("toISO8601", args, 1)?;
    match &args[0] {
        Property::Null => Ok(Property::Null),
        Property::Int64(ms) => {
            let dt = DateTime::<Utc>::from_timestamp_millis(*ms).ok_or_else(|| {
                ApocError::TypeMismatch {
                    name: "apoc.date.toISO8601".into(),
                    details: format!("millis value {ms} is out of range for a DateTime"),
                }
            })?;
            Ok(Property::String(
                dt.to_rfc3339_opts(SecondsFormat::Millis, true),
            ))
        }
        other => Err(ApocError::TypeMismatch {
            name: "apoc.date.toISO8601".into(),
            details: format!("expected integer millis, got {}", other.type_name()),
        }),
    }
}

fn from_iso8601(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("fromISO8601", args, 1)?;
    match &args[0] {
        Property::Null => Ok(Property::Null),
        Property::String(s) => {
            let dt = DateTime::parse_from_rfc3339(s).map_err(|e| ApocError::TypeMismatch {
                name: "apoc.date.fromISO8601".into(),
                details: format!("invalid ISO 8601 / RFC 3339 string: {e}"),
            })?;
            Ok(Property::Int64(dt.timestamp_millis()))
        }
        other => Err(ApocError::TypeMismatch {
            name: "apoc.date.fromISO8601".into(),
            details: format!("expected a string, got {}", other.type_name()),
        }),
    }
}

// ---------------------------------------------------------------
// Unit conversion / arithmetic.
// ---------------------------------------------------------------

fn convert(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("convert", args, 3)?;
    if matches!(&args[0], Property::Null) {
        return Ok(Property::Null);
    }
    let value = expect_int("convert", &args[0], "value")?;
    let from = expect_unit("convert", &args[1], "fromUnit")?;
    let to = expect_unit("convert", &args[2], "toUnit")?;
    Ok(Property::Int64(convert_value(value, from, to)))
}

fn add(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("add", args, 4)?;
    if matches!(&args[0], Property::Null) {
        return Ok(Property::Null);
    }
    let value = expect_int("add", &args[0], "value")?;
    let unit = expect_unit("add", &args[1], "unit")?;
    let add_value = expect_int("add", &args[2], "addValue")?;
    let add_unit = expect_unit("add", &args[3], "addUnit")?;
    // Convert the delta into `unit` before adding so the result
    // stays in the caller's unit.
    let delta = convert_value(add_value, add_unit, unit);
    Ok(Property::Int64(value.saturating_add(delta)))
}

fn convert_value(value: i64, from: Unit, to: Unit) -> i64 {
    if from == to {
        return value;
    }
    let ms = value.saturating_mul(from.to_ms());
    ms / to.to_ms()
}

// ---------------------------------------------------------------
// Units.
// ---------------------------------------------------------------

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Unit {
    Ms,
    Sec,
    Min,
    Hour,
    Day,
    Week,
}

impl Unit {
    fn to_ms(self) -> i64 {
        match self {
            Unit::Ms => 1,
            Unit::Sec => 1_000,
            Unit::Min => 60 * 1_000,
            Unit::Hour => 60 * 60 * 1_000,
            Unit::Day => 24 * 60 * 60 * 1_000,
            Unit::Week => 7 * 24 * 60 * 60 * 1_000,
        }
    }

    fn parse(s: &str) -> Option<Unit> {
        match s.to_ascii_lowercase().as_str() {
            "ms" | "millis" | "milliseconds" => Some(Unit::Ms),
            "s" | "sec" | "secs" | "second" | "seconds" => Some(Unit::Sec),
            "m" | "min" | "mins" | "minute" | "minutes" => Some(Unit::Min),
            "h" | "hr" | "hrs" | "hour" | "hours" => Some(Unit::Hour),
            "d" | "day" | "days" => Some(Unit::Day),
            "w" | "wk" | "wks" | "week" | "weeks" => Some(Unit::Week),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------
// Argument helpers.
// ---------------------------------------------------------------

fn check_arity(name: &str, args: &[Property], expected: usize) -> Result<(), ApocError> {
    if args.len() != expected {
        return Err(ApocError::Arity {
            name: format!("apoc.date.{name}"),
            expected,
            got: args.len(),
        });
    }
    Ok(())
}

fn expect_int(fn_name: &str, p: &Property, arg_name: &str) -> Result<i64, ApocError> {
    match p {
        Property::Int64(n) => Ok(*n),
        other => Err(ApocError::TypeMismatch {
            name: format!("apoc.date.{fn_name}"),
            details: format!("{arg_name}: expected integer, got {}", other.type_name()),
        }),
    }
}

fn expect_unit(fn_name: &str, p: &Property, arg_name: &str) -> Result<Unit, ApocError> {
    match p {
        Property::String(s) => Unit::parse(s).ok_or_else(|| ApocError::TypeMismatch {
            name: format!("apoc.date.{fn_name}"),
            details: format!(
                "{arg_name}: unknown time unit {s:?} (expected one of ms, s, m, h, d, w)"
            ),
        }),
        other => Err(ApocError::TypeMismatch {
            name: format!("apoc.date.{fn_name}"),
            details: format!(
                "{arg_name}: expected a unit string, got {}",
                other.type_name()
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Property {
        Property::String(v.to_string())
    }

    #[test]
    fn current_timestamp_is_recent_and_positive() {
        // Just sanity: value fits in i64 and is after 2020-01-01 (1577836800000 ms).
        let out = current_timestamp(&[]).unwrap();
        match out {
            Property::Int64(ms) => {
                assert!(ms > 1_577_836_800_000, "implausibly old: {ms}");
            }
            other => panic!("expected Int64, got {other:?}"),
        }
    }

    #[test]
    fn current_timestamp_rejects_args() {
        let err = current_timestamp(&[Property::Int64(0)]).unwrap_err();
        assert!(matches!(err, ApocError::Arity { expected: 0, .. }));
    }

    #[test]
    fn to_iso8601_formats_utc_with_z() {
        // 1700000000000 ms = 2023-11-14T22:13:20Z
        let out = to_iso8601(&[Property::Int64(1_700_000_000_000)]).unwrap();
        assert_eq!(out, s("2023-11-14T22:13:20.000Z"));
    }

    #[test]
    fn to_iso8601_millis_precision() {
        let out = to_iso8601(&[Property::Int64(1_700_000_000_123)]).unwrap();
        assert_eq!(out, s("2023-11-14T22:13:20.123Z"));
    }

    #[test]
    fn to_iso8601_null_propagates() {
        assert_eq!(to_iso8601(&[Property::Null]).unwrap(), Property::Null);
    }

    #[test]
    fn from_iso8601_parses_z_suffix() {
        let out = from_iso8601(&[s("2023-11-14T22:13:20Z")]).unwrap();
        assert_eq!(out, Property::Int64(1_700_000_000_000));
    }

    #[test]
    fn from_iso8601_parses_offset() {
        // 2023-11-14T23:13:20+01:00 == 2023-11-14T22:13:20Z
        let out = from_iso8601(&[s("2023-11-14T23:13:20+01:00")]).unwrap();
        assert_eq!(out, Property::Int64(1_700_000_000_000));
    }

    #[test]
    fn from_iso8601_round_trips_through_to_iso8601() {
        let ms = 1_700_000_000_456_i64;
        let enc = to_iso8601(&[Property::Int64(ms)]).unwrap();
        let dec = from_iso8601(&[enc]).unwrap();
        assert_eq!(dec, Property::Int64(ms));
    }

    #[test]
    fn from_iso8601_rejects_junk() {
        let err = from_iso8601(&[s("not a date")]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn convert_widens_and_narrows() {
        // 1 hour → 3_600_000 ms
        let out = convert(&[Property::Int64(1), s("h"), s("ms")]).unwrap();
        assert_eq!(out, Property::Int64(3_600_000));
        // 3_600_000 ms → 1 hour (truncating)
        let out = convert(&[Property::Int64(3_600_000), s("ms"), s("h")]).unwrap();
        assert_eq!(out, Property::Int64(1));
        // 90 seconds → 1 minute (truncating)
        let out = convert(&[Property::Int64(90), s("s"), s("min")]).unwrap();
        assert_eq!(out, Property::Int64(1));
    }

    #[test]
    fn convert_same_unit_is_identity() {
        let out = convert(&[Property::Int64(42), s("ms"), s("ms")]).unwrap();
        assert_eq!(out, Property::Int64(42));
    }

    #[test]
    fn convert_accepts_unit_aliases() {
        // "minutes" / "hours" / "days" / "weeks" etc.
        let out = convert(&[Property::Int64(2), s("hours"), s("minutes")]).unwrap();
        assert_eq!(out, Property::Int64(120));
        let out = convert(&[Property::Int64(1), s("week"), s("days")]).unwrap();
        assert_eq!(out, Property::Int64(7));
    }

    #[test]
    fn convert_rejects_bad_unit() {
        let err = convert(&[Property::Int64(1), s("fortnight"), s("ms")]).unwrap_err();
        match err {
            ApocError::TypeMismatch { details, .. } => {
                assert!(details.contains("fortnight"));
            }
            other => panic!("expected TypeMismatch, got {other:?}"),
        }
    }

    #[test]
    fn add_applies_delta_in_source_unit() {
        // 1000 ms + 2 s = 3000 ms
        let out = add(&[Property::Int64(1000), s("ms"), Property::Int64(2), s("s")]).unwrap();
        assert_eq!(out, Property::Int64(3000));
        // 5 min + 1 h = 65 min
        let out = add(&[Property::Int64(5), s("min"), Property::Int64(1), s("h")]).unwrap();
        assert_eq!(out, Property::Int64(65));
    }

    #[test]
    fn add_negative_delta() {
        let out = add(&[Property::Int64(10), s("s"), Property::Int64(-3), s("s")]).unwrap();
        assert_eq!(out, Property::Int64(7));
    }

    #[test]
    fn null_primary_value_propagates() {
        assert_eq!(
            convert(&[Property::Null, s("s"), s("ms")]).unwrap(),
            Property::Null,
        );
        assert_eq!(
            add(&[Property::Null, s("s"), Property::Int64(1), s("s")]).unwrap(),
            Property::Null,
        );
    }

    #[test]
    fn arity_errors_name_the_function() {
        let err = to_iso8601(&[]).unwrap_err();
        match err {
            ApocError::Arity {
                name,
                expected,
                got,
            } => {
                assert_eq!(name, "apoc.date.toISO8601");
                assert_eq!(expected, 1);
                assert_eq!(got, 0);
            }
            other => panic!("expected Arity, got {other:?}"),
        }
    }

    #[test]
    fn unknown_function_surfaces_full_name() {
        let err = call("reticulate", &[]).unwrap_err();
        match err {
            ApocError::UnknownFunction(n) => assert_eq!(n, "apoc.date.reticulate"),
            other => panic!("expected UnknownFunction, got {other:?}"),
        }
    }
}
