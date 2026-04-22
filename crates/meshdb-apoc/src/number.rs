//! `apoc.number.*` — number parsing, formatting, and roman numerals.
//!
//! Shipped functions:
//!
//! - `apoc.number.parseInt(text, radix?)` — parses a string to
//!   [`Property::Int64`]. Radix defaults to 10; supported range is
//!   2..=36. Returns [`Property::Null`] on any parse failure so
//!   the function matches Cypher's own `toInteger`-on-bad-input
//!   null-return convention.
//! - `apoc.number.parseFloat(text)` — string → [`Property::Float64`],
//!   null on failure.
//! - `apoc.number.arabicToRoman(n)` — integer (1..=3999) to classic
//!   roman numeral string. Out-of-range inputs surface as a type
//!   error.
//! - `apoc.number.romanToArabic(s)` — inverse; null on any invalid
//!   roman string so the caller doesn't need a try/catch.
//! - `apoc.number.format(n, decimals?)` — groups with `,` and pads
//!   to a fixed decimal count. `decimals` defaults to 0 for integer
//!   inputs and to "natural" for floats (trailing zeros trimmed).
//!   Java DecimalFormat pattern strings are *not* supported — that
//!   surface can ship later if a user asks for it.
//!
//! Null propagation: the primary value argument (`text` / `n` / `s`)
//! propagates null on the way out; optional arguments (`radix`,
//! `decimals`) must be present-and-valid when supplied.

use crate::ApocError;
use meshdb_core::Property;

pub fn call(suffix: &str, args: &[Property]) -> Result<Property, ApocError> {
    match suffix {
        "parseint" => parse_int(args),
        "parsefloat" => parse_float(args),
        "arabictoroman" => arabic_to_roman(args),
        "romantoarabic" => roman_to_arabic(args),
        "format" => format(args),
        _ => Err(ApocError::UnknownFunction(format!("apoc.number.{suffix}"))),
    }
}

// ---------------------------------------------------------------
// Parsing.
// ---------------------------------------------------------------

fn parse_int(args: &[Property]) -> Result<Property, ApocError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ApocError::Arity {
            name: "apoc.number.parseInt".into(),
            expected: 1,
            got: args.len(),
        });
    }
    let radix = match args.get(1) {
        None | Some(Property::Null) => 10_u32,
        Some(Property::Int64(r)) => {
            let r = *r;
            if !(2..=36).contains(&r) {
                return Err(ApocError::TypeMismatch {
                    name: "apoc.number.parseInt".into(),
                    details: format!("radix {r} out of range (expected 2..=36)"),
                });
            }
            r as u32
        }
        Some(other) => {
            return Err(ApocError::TypeMismatch {
                name: "apoc.number.parseInt".into(),
                details: format!("radix: expected integer, got {}", other.type_name()),
            })
        }
    };
    match &args[0] {
        Property::Null => Ok(Property::Null),
        Property::String(s) => Ok(match i64::from_str_radix(s.trim(), radix) {
            Ok(n) => Property::Int64(n),
            Err(_) => Property::Null,
        }),
        other => Err(ApocError::TypeMismatch {
            name: "apoc.number.parseInt".into(),
            details: format!("expected a string, got {}", other.type_name()),
        }),
    }
}

fn parse_float(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("parseFloat", args, 1)?;
    match &args[0] {
        Property::Null => Ok(Property::Null),
        Property::String(s) => Ok(match s.trim().parse::<f64>() {
            Ok(f) => Property::Float64(f),
            Err(_) => Property::Null,
        }),
        other => Err(ApocError::TypeMismatch {
            name: "apoc.number.parseFloat".into(),
            details: format!("expected a string, got {}", other.type_name()),
        }),
    }
}

// ---------------------------------------------------------------
// Roman numerals.
// ---------------------------------------------------------------

const ROMAN_VALUES: &[(i64, &str)] = &[
    (1000, "M"),
    (900, "CM"),
    (500, "D"),
    (400, "CD"),
    (100, "C"),
    (90, "XC"),
    (50, "L"),
    (40, "XL"),
    (10, "X"),
    (9, "IX"),
    (5, "V"),
    (4, "IV"),
    (1, "I"),
];

fn arabic_to_roman(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("arabicToRoman", args, 1)?;
    let n = match &args[0] {
        Property::Null => return Ok(Property::Null),
        Property::Int64(n) => *n,
        other => {
            return Err(ApocError::TypeMismatch {
                name: "apoc.number.arabicToRoman".into(),
                details: format!("expected integer, got {}", other.type_name()),
            })
        }
    };
    if !(1..=3999).contains(&n) {
        return Err(ApocError::TypeMismatch {
            name: "apoc.number.arabicToRoman".into(),
            details: format!("value {n} out of range (expected 1..=3999)"),
        });
    }
    let mut remaining = n;
    let mut out = String::new();
    for &(v, sym) in ROMAN_VALUES {
        while remaining >= v {
            out.push_str(sym);
            remaining -= v;
        }
    }
    Ok(Property::String(out))
}

fn roman_to_arabic(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("romanToArabic", args, 1)?;
    let s = match &args[0] {
        Property::Null => return Ok(Property::Null),
        Property::String(s) => s.as_str(),
        other => {
            return Err(ApocError::TypeMismatch {
                name: "apoc.number.romanToArabic".into(),
                details: format!("expected a string, got {}", other.type_name()),
            })
        }
    };
    let s = s.trim();
    if s.is_empty() {
        return Ok(Property::Null);
    }
    let up = s.to_ascii_uppercase();
    let mut total: i64 = 0;
    let mut remaining = up.as_str();
    // Greedy longest-token match. If the loop body ever fails to
    // consume a character, the string isn't valid roman — return
    // Null so callers can branch without a try/catch.
    while !remaining.is_empty() {
        let mut matched = false;
        for &(v, sym) in ROMAN_VALUES {
            if let Some(rest) = remaining.strip_prefix(sym) {
                total += v;
                remaining = rest;
                matched = true;
                break;
            }
        }
        if !matched {
            return Ok(Property::Null);
        }
    }
    // Sanity: classic roman caps out at 3999 — anything above was
    // built from invalid repetitions (e.g. "MMMM").
    if total > 3999 {
        return Ok(Property::Null);
    }
    Ok(Property::Int64(total))
}

// ---------------------------------------------------------------
// Formatting with grouping.
// ---------------------------------------------------------------

fn format(args: &[Property]) -> Result<Property, ApocError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ApocError::Arity {
            name: "apoc.number.format".into(),
            expected: 1,
            got: args.len(),
        });
    }
    // Optional decimals arg.
    let decimals = match args.get(1) {
        None | Some(Property::Null) => None,
        Some(Property::Int64(d)) => {
            let d = *d;
            if !(0..=20).contains(&d) {
                return Err(ApocError::TypeMismatch {
                    name: "apoc.number.format".into(),
                    details: format!("decimals {d} out of range (expected 0..=20)"),
                });
            }
            Some(d as usize)
        }
        Some(other) => {
            return Err(ApocError::TypeMismatch {
                name: "apoc.number.format".into(),
                details: format!("decimals: expected integer, got {}", other.type_name()),
            })
        }
    };
    match &args[0] {
        Property::Null => Ok(Property::Null),
        Property::Int64(n) => Ok(Property::String(format_number(
            *n as f64,
            decimals.unwrap_or(0),
            /* trim_trailing_zeros */ false,
            /* is_integer_input */ true,
        ))),
        Property::Float64(f) => {
            if !f.is_finite() {
                return Err(ApocError::TypeMismatch {
                    name: "apoc.number.format".into(),
                    details: format!("cannot format non-finite value {f}"),
                });
            }
            let d = decimals.unwrap_or(6);
            let trim = decimals.is_none();
            Ok(Property::String(format_number(*f, d, trim, false)))
        }
        other => Err(ApocError::TypeMismatch {
            name: "apoc.number.format".into(),
            details: format!("expected a number, got {}", other.type_name()),
        }),
    }
}

/// Formats `value` with `,` grouping and exactly `decimals` digits
/// after the decimal point. When `trim_trailing_zeros` is true and
/// a fractional part is present, strips any trailing zeros (and a
/// dangling `.`). When `is_integer_input` is true, the decimal
/// point is omitted entirely if `decimals == 0`.
fn format_number(
    value: f64,
    decimals: usize,
    trim_trailing_zeros: bool,
    is_integer_input: bool,
) -> String {
    let negative = value < 0.0 || (value == 0.0 && value.is_sign_negative());
    let abs = if negative { -value } else { value };
    // Use a formatted decimal string as the canonical source of
    // digits so rounding matches what the user wrote.
    let formatted = format!("{abs:.*}", decimals);
    let (int_part, frac_part) = match formatted.split_once('.') {
        Some((i, f)) => (i.to_string(), f.to_string()),
        None => (formatted, String::new()),
    };
    let mut grouped = String::with_capacity(int_part.len() + int_part.len() / 3);
    for (i, ch) in int_part.chars().rev().enumerate() {
        if i != 0 && i % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(ch);
    }
    let int_out: String = grouped.chars().rev().collect();

    let mut out = String::new();
    if negative {
        out.push('-');
    }
    out.push_str(&int_out);
    if !frac_part.is_empty() {
        if trim_trailing_zeros {
            let trimmed = frac_part.trim_end_matches('0');
            if !trimmed.is_empty() {
                out.push('.');
                out.push_str(trimmed);
            }
        } else {
            out.push('.');
            out.push_str(&frac_part);
        }
    } else if decimals > 0 && !is_integer_input {
        out.push('.');
        for _ in 0..decimals {
            out.push('0');
        }
    }
    out
}

// ---------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------

fn check_arity(name: &str, args: &[Property], expected: usize) -> Result<(), ApocError> {
    if args.len() != expected {
        return Err(ApocError::Arity {
            name: format!("apoc.number.{name}"),
            expected,
            got: args.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Property {
        Property::String(v.to_string())
    }

    #[test]
    fn parse_int_decimal_default() {
        assert_eq!(parse_int(&[s("42")]).unwrap(), Property::Int64(42));
        assert_eq!(parse_int(&[s("-100")]).unwrap(), Property::Int64(-100));
    }

    #[test]
    fn parse_int_with_radix() {
        assert_eq!(
            parse_int(&[s("ff"), Property::Int64(16)]).unwrap(),
            Property::Int64(255),
        );
        assert_eq!(
            parse_int(&[s("1010"), Property::Int64(2)]).unwrap(),
            Property::Int64(10),
        );
    }

    #[test]
    fn parse_int_returns_null_on_junk() {
        assert_eq!(parse_int(&[s("not a number")]).unwrap(), Property::Null);
        assert_eq!(
            parse_int(&[s("12.5")]).unwrap(),
            Property::Null,
            "integer parser should reject floats",
        );
    }

    #[test]
    fn parse_int_trims_whitespace() {
        assert_eq!(parse_int(&[s("  42 ")]).unwrap(), Property::Int64(42));
    }

    #[test]
    fn parse_int_bad_radix_errors() {
        let err = parse_int(&[s("1"), Property::Int64(37)]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn parse_float_handles_scientific() {
        assert_eq!(
            parse_float(&[s("1.5e3")]).unwrap(),
            Property::Float64(1500.0),
        );
    }

    #[test]
    fn parse_float_returns_null_on_junk() {
        assert_eq!(parse_float(&[s("nope")]).unwrap(), Property::Null);
    }

    #[test]
    fn arabic_to_roman_canonical() {
        // Classic canonical examples.
        assert_eq!(arabic_to_roman(&[Property::Int64(1)]).unwrap(), s("I"),);
        assert_eq!(arabic_to_roman(&[Property::Int64(4)]).unwrap(), s("IV"),);
        assert_eq!(
            arabic_to_roman(&[Property::Int64(1994)]).unwrap(),
            s("MCMXCIV"),
        );
        assert_eq!(
            arabic_to_roman(&[Property::Int64(3999)]).unwrap(),
            s("MMMCMXCIX"),
        );
    }

    #[test]
    fn arabic_to_roman_range_check() {
        let err = arabic_to_roman(&[Property::Int64(0)]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
        let err = arabic_to_roman(&[Property::Int64(4000)]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn roman_to_arabic_round_trip() {
        for n in [1, 4, 9, 49, 1994, 3999] {
            let r = arabic_to_roman(&[Property::Int64(n)]).unwrap();
            assert_eq!(roman_to_arabic(&[r]).unwrap(), Property::Int64(n));
        }
    }

    #[test]
    fn roman_to_arabic_accepts_lowercase() {
        assert_eq!(
            roman_to_arabic(&[s("mcmxciv")]).unwrap(),
            Property::Int64(1994),
        );
    }

    #[test]
    fn roman_to_arabic_rejects_junk() {
        assert_eq!(roman_to_arabic(&[s("NOTROMAN")]).unwrap(), Property::Null);
        assert_eq!(roman_to_arabic(&[s("")]).unwrap(), Property::Null);
        assert_eq!(
            roman_to_arabic(&[s("MMMM")]).unwrap(),
            Property::Null,
            "four M's exceeds the 3999 cap",
        );
    }

    #[test]
    fn format_integer_default() {
        assert_eq!(format(&[Property::Int64(1234567)]).unwrap(), s("1,234,567"),);
        assert_eq!(format(&[Property::Int64(-1000)]).unwrap(), s("-1,000"),);
        assert_eq!(format(&[Property::Int64(42)]).unwrap(), s("42"));
    }

    #[test]
    fn format_integer_with_decimals() {
        // An explicit decimals argument is honored even for integer
        // inputs — the caller asked for two decimal places, so
        // trailing zeros are kept.
        assert_eq!(
            format(&[Property::Int64(1000), Property::Int64(2)]).unwrap(),
            s("1,000.00"),
        );
        // Default (no decimals arg) on an integer omits the dot.
        assert_eq!(format(&[Property::Int64(1000)]).unwrap(), s("1,000"),);
    }

    #[test]
    fn format_float_default_trims_trailing_zeros() {
        assert_eq!(format(&[Property::Float64(1234.5)]).unwrap(), s("1,234.5"),);
    }

    #[test]
    fn format_float_with_explicit_decimals_keeps_zeros() {
        assert_eq!(
            format(&[Property::Float64(1000.0), Property::Int64(2)]).unwrap(),
            s("1,000.00"),
        );
        assert_eq!(
            format(&[Property::Float64(1234.5678), Property::Int64(2)]).unwrap(),
            s("1,234.57"),
        );
    }

    #[test]
    fn format_float_rejects_non_finite() {
        let err = format(&[Property::Float64(f64::NAN)]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn null_primary_value_propagates() {
        for fname in ["parseInt", "parseFloat", "arabicToRoman", "romanToArabic"] {
            let suf = fname.to_ascii_lowercase();
            let out = call(&suf, &[Property::Null]).unwrap();
            assert_eq!(out, Property::Null, "{fname} should null-propagate");
        }
        assert_eq!(format(&[Property::Null]).unwrap(), Property::Null);
    }

    #[test]
    fn arity_errors_name_the_function() {
        let err = parse_float(&[]).unwrap_err();
        match err {
            ApocError::Arity {
                name,
                expected,
                got,
            } => {
                assert_eq!(name, "apoc.number.parseFloat");
                assert_eq!(expected, 1);
                assert_eq!(got, 0);
            }
            other => panic!("expected Arity, got {other:?}"),
        }
    }

    #[test]
    fn unknown_function_surfaces_full_name() {
        let err = call("wavelet", &[]).unwrap_err();
        match err {
            ApocError::UnknownFunction(n) => assert_eq!(n, "apoc.number.wavelet"),
            other => panic!("expected UnknownFunction, got {other:?}"),
        }
    }
}
