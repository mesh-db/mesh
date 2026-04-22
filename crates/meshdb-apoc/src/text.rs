//! `apoc.text.*` — string manipulation helpers.
//!
//! Every function here is null-propagating on its primary string
//! argument: passing `Property::Null` returns `Property::Null`
//! without inspecting the other arguments. Matches the Neo4j APOC
//! convention — a `null` input never raises, it just passes through
//! so chained calls stay total.
//!
//! Regex-based helpers (`split`, `replace`, `regexGroups`) use the
//! `regex` crate; invalid patterns surface as [`ApocError::TypeMismatch`]
//! with the original regex compile error in `details` so the user
//! can see what's wrong with their pattern. `urlencode` / `urldecode`
//! are hand-rolled percent-escape implementations (RFC 3986 unreserved
//! set + `%XX` escapes); rolling our own avoids a `percent-encoding`
//! dependency for the ~30 lines of code actually needed.

use crate::ApocError;
use meshdb_core::Property;

pub fn call(suffix: &str, args: &[Property]) -> Result<Property, ApocError> {
    match suffix {
        "join" => join(args),
        "split" => split(args),
        "replace" => replace(args),
        "indexof" => index_of(args),
        "lpad" => pad(args, PadSide::Left),
        "rpad" => pad(args, PadSide::Right),
        "capitalize" => map_string(args, "capitalize", capitalize_first),
        "capitalizeall" => map_string(args, "capitalizeAll", capitalize_all),
        "decapitalize" => map_string(args, "decapitalize", decapitalize_first),
        "swapcase" => map_string(args, "swapCase", swap_case),
        "camelcase" => map_string(args, "camelCase", to_camel_case),
        "snakecase" => map_string(args, "snakeCase", to_snake_case),
        "uppercamelcase" => map_string(args, "upperCamelCase", to_upper_camel_case),
        "repeat" => repeat(args),
        "reverse" => map_string(args, "reverse", |s| s.chars().rev().collect()),
        "urlencode" => map_string(args, "urlencode", url_encode),
        "urldecode" => url_decode(args),
        "regexgroups" => regex_groups(args),
        "hexvalue" => hex_value(args),
        "base64encode" => map_string(args, "base64Encode", base64_encode),
        "base64decode" => base64_decode(args),
        "bytecount" => byte_count(args),
        "clean" => map_string(args, "clean", clean),
        "levenshteindistance" => levenshtein_distance(args),
        _ => Err(ApocError::UnknownFunction(format!("apoc.text.{suffix}"))),
    }
}

// ---------------------------------------------------------------
// Shared argument helpers.
// ---------------------------------------------------------------

fn check_arity(name: &str, args: &[Property], expected: usize) -> Result<(), ApocError> {
    if args.len() != expected {
        return Err(ApocError::Arity {
            name: format!("apoc.text.{name}"),
            expected,
            got: args.len(),
        });
    }
    Ok(())
}

/// Extract the primary string arg, short-circuiting on `Null`. The
/// caller receives `None` for null input (meaning "return Null") or
/// `Some(&str)` for the string to operate on.
fn as_string<'a>(name: &str, p: &'a Property) -> Result<Option<&'a str>, ApocError> {
    match p {
        Property::Null => Ok(None),
        Property::String(s) => Ok(Some(s.as_str())),
        other => Err(ApocError::TypeMismatch {
            name: format!("apoc.text.{name}"),
            details: format!("expected a string, got {}", other.type_name()),
        }),
    }
}

fn expect_int(name: &str, p: &Property) -> Result<i64, ApocError> {
    match p {
        Property::Int64(n) => Ok(*n),
        other => Err(ApocError::TypeMismatch {
            name: format!("apoc.text.{name}"),
            details: format!("expected integer, got {}", other.type_name()),
        }),
    }
}

/// Thin wrapper for the "single-string in, single-string out"
/// shape — covers most of the casing / reversal helpers. The
/// transform closure takes `&str` and returns the `String` output.
fn map_string<F>(args: &[Property], name: &str, transform: F) -> Result<Property, ApocError>
where
    F: FnOnce(&str) -> String,
{
    check_arity(name, args, 1)?;
    let Some(s) = as_string(name, &args[0])? else {
        return Ok(Property::Null);
    };
    Ok(Property::String(transform(s)))
}

// ---------------------------------------------------------------
// join / split / replace / indexOf
// ---------------------------------------------------------------

fn join(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("join", args, 2)?;
    let items = match &args[0] {
        Property::Null => return Ok(Property::Null),
        Property::List(items) => items,
        other => {
            return Err(ApocError::TypeMismatch {
                name: "apoc.text.join".into(),
                details: format!("expected a list, got {}", other.type_name()),
            })
        }
    };
    let sep = match as_string("join", &args[1])? {
        Some(s) => s,
        None => return Ok(Property::Null),
    };
    // Stringify each element. Null elements render as the empty
    // string — matches how Neo4j's `apoc.text.join` handles them.
    let mut out = String::new();
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push_str(sep);
        }
        stringify_element(item, &mut out);
    }
    Ok(Property::String(out))
}

/// Best-effort string rendering for `join`. Numbers / bools print
/// via `Display`; lists / maps / nested collections render as their
/// default `{:?}` debug form since APOC's `toString` story on
/// composites is itself approximate.
fn stringify_element(p: &Property, out: &mut String) {
    use std::fmt::Write;
    match p {
        Property::Null => {}
        Property::String(s) => out.push_str(s),
        Property::Int64(n) => {
            let _ = write!(out, "{n}");
        }
        Property::Float64(f) => {
            let _ = write!(out, "{f}");
        }
        Property::Bool(b) => {
            let _ = write!(out, "{b}");
        }
        other => {
            let _ = write!(out, "{other:?}");
        }
    }
}

fn split(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("split", args, 2)?;
    let Some(s) = as_string("split", &args[0])? else {
        return Ok(Property::Null);
    };
    let pattern = match as_string("split", &args[1])? {
        Some(p) => p,
        None => return Ok(Property::Null),
    };
    let re = compile_regex("split", pattern)?;
    let parts: Vec<Property> = re
        .split(s)
        .map(|p| Property::String(p.to_string()))
        .collect();
    Ok(Property::List(parts))
}

fn replace(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("replace", args, 3)?;
    let Some(s) = as_string("replace", &args[0])? else {
        return Ok(Property::Null);
    };
    let pattern = match as_string("replace", &args[1])? {
        Some(p) => p,
        None => return Ok(Property::Null),
    };
    let replacement = match as_string("replace", &args[2])? {
        Some(r) => r,
        None => return Ok(Property::Null),
    };
    let re = compile_regex("replace", pattern)?;
    Ok(Property::String(re.replace_all(s, replacement).to_string()))
}

fn index_of(args: &[Property]) -> Result<Property, ApocError> {
    if args.len() != 2 && args.len() != 3 {
        return Err(ApocError::Arity {
            name: "apoc.text.indexOf".into(),
            expected: 2,
            got: args.len(),
        });
    }
    let Some(s) = as_string("indexOf", &args[0])? else {
        return Ok(Property::Null);
    };
    let needle = match as_string("indexOf", &args[1])? {
        Some(n) => n,
        None => return Ok(Property::Null),
    };
    let from = if args.len() == 3 {
        expect_int("indexOf", &args[2])?.max(0) as usize
    } else {
        0
    };
    // Byte indices — Neo4j's APOC semantics count bytes, not
    // codepoints. Bail out with -1 when `from` is past the end so
    // `get()` doesn't panic.
    let result = if from >= s.len() {
        -1
    } else {
        match s[from..].find(needle) {
            Some(rel) => (from + rel) as i64,
            None => -1,
        }
    };
    Ok(Property::Int64(result))
}

// ---------------------------------------------------------------
// lpad / rpad
// ---------------------------------------------------------------

enum PadSide {
    Left,
    Right,
}

fn pad(args: &[Property], side: PadSide) -> Result<Property, ApocError> {
    let name = match side {
        PadSide::Left => "lpad",
        PadSide::Right => "rpad",
    };
    check_arity(name, args, 3)?;
    let Some(s) = as_string(name, &args[0])? else {
        return Ok(Property::Null);
    };
    let target_len = expect_int(name, &args[1])?.max(0) as usize;
    let pad_str = match as_string(name, &args[2])? {
        Some(p) => p,
        None => return Ok(Property::Null),
    };
    let current = s.chars().count();
    if current >= target_len || pad_str.is_empty() {
        return Ok(Property::String(s.to_string()));
    }
    let missing = target_len - current;
    // Build the pad fill by cycling the pad string one char at a
    // time so a multi-char pad (`"-*"`) fills from its first char.
    let pad_chars: Vec<char> = pad_str.chars().cycle().take(missing).collect();
    let pad_string: String = pad_chars.into_iter().collect();
    let out = match side {
        PadSide::Left => format!("{pad_string}{s}"),
        PadSide::Right => format!("{s}{pad_string}"),
    };
    Ok(Property::String(out))
}

// ---------------------------------------------------------------
// Casing helpers.
// ---------------------------------------------------------------

fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => {
            let mut out = c.to_uppercase().collect::<String>();
            out.push_str(chars.as_str());
            out
        }
    }
}

fn decapitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => {
            let mut out = c.to_lowercase().collect::<String>();
            out.push_str(chars.as_str());
            out
        }
    }
}

/// Uppercase the first letter of each whitespace-delimited word.
/// Interior chars are left untouched — matches Neo4j's
/// `apoc.text.capitalizeAll` which is definitely a
/// "Title-Case Words" transform, not a "title case everything".
fn capitalize_all(s: &str) -> String {
    s.split(' ')
        .map(capitalize_first)
        .collect::<Vec<_>>()
        .join(" ")
}

fn swap_case(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_uppercase() {
                c.to_lowercase().collect::<String>()
            } else if c.is_lowercase() {
                c.to_uppercase().collect::<String>()
            } else {
                c.to_string()
            }
        })
        .collect()
}

/// Split on whitespace / hyphens / underscores into word tokens.
/// Used by the camelCase / snake_case / PascalCase transforms so
/// they share one tokenizer.
fn tokenize_words(s: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    for c in s.chars() {
        if c.is_whitespace() || c == '-' || c == '_' {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
        } else {
            current.push(c);
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

fn to_camel_case(s: &str) -> String {
    let mut words = tokenize_words(s).into_iter();
    let mut out = String::new();
    if let Some(first) = words.next() {
        out.push_str(&first.to_lowercase());
    }
    for w in words {
        out.push_str(&capitalize_first(&w.to_lowercase()));
    }
    out
}

fn to_upper_camel_case(s: &str) -> String {
    tokenize_words(s)
        .into_iter()
        .map(|w| capitalize_first(&w.to_lowercase()))
        .collect()
}

fn to_snake_case(s: &str) -> String {
    tokenize_words(s)
        .into_iter()
        .map(|w| w.to_lowercase())
        .collect::<Vec<_>>()
        .join("_")
}

// ---------------------------------------------------------------
// repeat.
// ---------------------------------------------------------------

fn repeat(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("repeat", args, 2)?;
    let Some(s) = as_string("repeat", &args[0])? else {
        return Ok(Property::Null);
    };
    let n = expect_int("repeat", &args[1])?;
    if n < 0 {
        return Err(ApocError::TypeMismatch {
            name: "apoc.text.repeat".into(),
            details: format!("negative repeat count {n}"),
        });
    }
    Ok(Property::String(s.repeat(n as usize)))
}

// ---------------------------------------------------------------
// URL encode / decode — RFC 3986 percent-escaping.
// ---------------------------------------------------------------

fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                // Standard `%XX` with uppercase hex digits. Neo4j
                // APOC uses uppercase too, matching most URL libs.
                out.push('%');
                out.push(hex_upper(b >> 4));
                out.push(hex_upper(b & 0x0F));
            }
        }
    }
    out
}

fn hex_upper(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + nibble - 10) as char,
    }
}

fn url_decode(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("urldecode", args, 1)?;
    let Some(s) = as_string("urldecode", &args[0])? else {
        return Ok(Property::Null);
    };
    let mut bytes = Vec::with_capacity(s.len());
    let src = s.as_bytes();
    let mut i = 0;
    while i < src.len() {
        match src[i] {
            b'%' => {
                if i + 2 >= src.len() {
                    return Err(ApocError::TypeMismatch {
                        name: "apoc.text.urldecode".into(),
                        details: "truncated percent-escape".into(),
                    });
                }
                let hi = from_hex(src[i + 1])?;
                let lo = from_hex(src[i + 2])?;
                bytes.push((hi << 4) | lo);
                i += 3;
            }
            b'+' => {
                // Some URL-encoding schemes treat `+` as a space.
                // APOC's urldecode is form-style, so we honour that.
                bytes.push(b' ');
                i += 1;
            }
            b => {
                bytes.push(b);
                i += 1;
            }
        }
    }
    match String::from_utf8(bytes) {
        Ok(s) => Ok(Property::String(s)),
        Err(e) => Err(ApocError::TypeMismatch {
            name: "apoc.text.urldecode".into(),
            details: format!("invalid UTF-8 after decode: {e}"),
        }),
    }
}

fn from_hex(b: u8) -> Result<u8, ApocError> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(ApocError::TypeMismatch {
            name: "apoc.text.urldecode".into(),
            details: format!("invalid hex digit {:?}", b as char),
        }),
    }
}

// ---------------------------------------------------------------
// regexGroups + hexValue.
// ---------------------------------------------------------------

fn regex_groups(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("regexGroups", args, 2)?;
    let Some(s) = as_string("regexGroups", &args[0])? else {
        return Ok(Property::Null);
    };
    let pattern = match as_string("regexGroups", &args[1])? {
        Some(p) => p,
        None => return Ok(Property::Null),
    };
    let re = compile_regex("regexGroups", pattern)?;
    // Neo4j's `regexGroups` returns `List<List<String>>`: one inner
    // list per match, where the first element is the whole match
    // and the rest are the captured groups. Non-participating
    // groups render as empty strings (APOC's convention — `null`
    // would break the outer list's scalar invariant).
    let mut outer: Vec<Property> = Vec::new();
    for caps in re.captures_iter(s) {
        let mut inner: Vec<Property> = Vec::with_capacity(caps.len());
        for i in 0..caps.len() {
            let chunk = caps.get(i).map(|m| m.as_str()).unwrap_or("");
            inner.push(Property::String(chunk.to_string()));
        }
        outer.push(Property::List(inner));
    }
    Ok(Property::List(outer))
}

fn hex_value(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("hexValue", args, 1)?;
    match &args[0] {
        Property::Null => Ok(Property::Null),
        Property::Int64(n) => Ok(Property::String(format!("{:X}", *n as u64))),
        other => Err(ApocError::TypeMismatch {
            name: "apoc.text.hexValue".into(),
            details: format!("expected integer, got {}", other.type_name()),
        }),
    }
}

fn compile_regex(name: &str, pattern: &str) -> Result<regex::Regex, ApocError> {
    regex::Regex::new(pattern).map_err(|e| ApocError::TypeMismatch {
        name: format!("apoc.text.{name}"),
        details: format!("invalid regex: {e}"),
    })
}

// ---------------------------------------------------------------
// Base64 (RFC 4648, standard alphabet `+/` with `=` padding).
// Hand-rolled for the same reason as the UUID base64 in
// `create.rs`: a dedicated `base64` crate would be overkill for a
// 20-line implementation that only needs one alphabet and
// handles arbitrary input lengths correctly.
// ---------------------------------------------------------------

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(((bytes.len() + 2) / 3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = if chunk.len() > 1 { chunk[1] } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] } else { 0 };
        out.push(BASE64_ALPHABET[(b0 >> 2) as usize] as char);
        out.push(BASE64_ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        match chunk.len() {
            3 => {
                out.push(BASE64_ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
                out.push(BASE64_ALPHABET[(b2 & 0x3f) as usize] as char);
            }
            2 => {
                out.push(BASE64_ALPHABET[((b1 & 0x0f) << 2) as usize] as char);
                out.push('=');
            }
            _ => {
                out.push('=');
                out.push('=');
            }
        }
    }
    out
}

fn base64_decode(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("base64Decode", args, 1)?;
    let Property::String(s) = &args[0] else {
        return match &args[0] {
            Property::Null => Ok(Property::Null),
            other => Err(ApocError::TypeMismatch {
                name: "apoc.text.base64Decode".into(),
                details: format!("expected string, got {}", other.type_name()),
            }),
        };
    };
    let trimmed = s.trim_end_matches('=');
    let mut bytes: Vec<u8> = Vec::with_capacity((trimmed.len() * 3) / 4);
    let mut buf: u32 = 0;
    let mut acc_bits: u32 = 0;
    for c in trimmed.chars() {
        let val: u32 = match c {
            'A'..='Z' => (c as u32) - ('A' as u32),
            'a'..='z' => (c as u32) - ('a' as u32) + 26,
            '0'..='9' => (c as u32) - ('0' as u32) + 52,
            '+' | '-' => 62,
            '/' | '_' => 63,
            c if c.is_whitespace() => continue,
            bad => {
                return Err(ApocError::TypeMismatch {
                    name: "apoc.text.base64Decode".into(),
                    details: format!("invalid base64 character: {bad:?}"),
                });
            }
        };
        buf = (buf << 6) | val;
        acc_bits += 6;
        if acc_bits >= 8 {
            acc_bits -= 8;
            bytes.push((buf >> acc_bits) as u8);
            buf &= (1 << acc_bits) - 1;
        }
    }
    match std::str::from_utf8(&bytes) {
        Ok(s) => Ok(Property::String(s.to_string())),
        Err(_) => Err(ApocError::TypeMismatch {
            name: "apoc.text.base64Decode".into(),
            details: "decoded bytes are not valid UTF-8".into(),
        }),
    }
}

/// UTF-8 byte length — useful when callers need storage-cost
/// estimates that `size()` (character count) doesn't give.
fn byte_count(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("byteCount", args, 1)?;
    match &args[0] {
        Property::Null => Ok(Property::Null),
        Property::String(s) => Ok(Property::Int64(s.len() as i64)),
        other => Err(ApocError::TypeMismatch {
            name: "apoc.text.byteCount".into(),
            details: format!("expected string, got {}", other.type_name()),
        }),
    }
}

/// Canonicalise a string: strip non-alphanumeric characters and
/// lowercase the result. Matches Neo4j APOC's `apoc.text.clean` —
/// used for fuzzy-match / dedupe pipelines where punctuation and
/// case should be ignored.
fn clean(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// Classic Levenshtein edit distance (insertions, deletions,
/// single-character substitutions). O(len1 * len2) time, O(min)
/// space via the two-row trick. Unicode-safe: operates on chars,
/// not bytes, so multi-byte characters count as one edit.
fn levenshtein_distance(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("levenshteinDistance", args, 2)?;
    let a = match &args[0] {
        Property::String(s) => s.as_str(),
        Property::Null => return Ok(Property::Null),
        other => {
            return Err(ApocError::TypeMismatch {
                name: "apoc.text.levenshteinDistance".into(),
                details: format!("expected string, got {}", other.type_name()),
            });
        }
    };
    let b = match &args[1] {
        Property::String(s) => s.as_str(),
        Property::Null => return Ok(Property::Null),
        other => {
            return Err(ApocError::TypeMismatch {
                name: "apoc.text.levenshteinDistance".into(),
                details: format!("expected string, got {}", other.type_name()),
            });
        }
    };
    let ac: Vec<char> = a.chars().collect();
    let bc: Vec<char> = b.chars().collect();
    if ac.is_empty() {
        return Ok(Property::Int64(bc.len() as i64));
    }
    if bc.is_empty() {
        return Ok(Property::Int64(ac.len() as i64));
    }
    let mut prev: Vec<usize> = (0..=bc.len()).collect();
    let mut curr: Vec<usize> = vec![0; bc.len() + 1];
    for (i, ca) in ac.iter().enumerate() {
        curr[0] = i + 1;
        for (j, cb) in bc.iter().enumerate() {
            let substitute = prev[j] + if ca == cb { 0 } else { 1 };
            let insert = curr[j] + 1;
            let delete = prev[j + 1] + 1;
            curr[j + 1] = substitute.min(insert).min(delete);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    Ok(Property::Int64(prev[bc.len()] as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Property {
        Property::String(v.into())
    }
    fn i(n: i64) -> Property {
        Property::Int64(n)
    }

    #[test]
    fn join_basic() {
        assert_eq!(
            join(&[Property::List(vec![s("a"), s("b"), s("c")]), s("-")]).unwrap(),
            s("a-b-c"),
        );
    }

    #[test]
    fn join_stringifies_mixed_elements() {
        assert_eq!(
            join(&[
                Property::List(vec![i(1), s("x"), Property::Bool(true)]),
                s(",")
            ])
            .unwrap(),
            s("1,x,true"),
        );
    }

    #[test]
    fn join_null_list_returns_null() {
        assert_eq!(join(&[Property::Null, s(",")]).unwrap(), Property::Null);
    }

    #[test]
    fn split_regex() {
        assert_eq!(
            split(&[s("a,b;c"), s("[,;]")]).unwrap(),
            Property::List(vec![s("a"), s("b"), s("c")]),
        );
    }

    #[test]
    fn replace_regex() {
        assert_eq!(
            replace(&[s("hello world"), s("o"), s("0")]).unwrap(),
            s("hell0 w0rld"),
        );
    }

    #[test]
    fn replace_invalid_regex_is_type_mismatch() {
        assert!(matches!(
            replace(&[s("x"), s("["), s("_")]).unwrap_err(),
            ApocError::TypeMismatch { .. }
        ));
    }

    #[test]
    fn index_of_hit_and_miss() {
        assert_eq!(index_of(&[s("hello"), s("ll")]).unwrap(), i(2));
        assert_eq!(index_of(&[s("hello"), s("zz")]).unwrap(), i(-1));
    }

    #[test]
    fn index_of_with_from_offset() {
        assert_eq!(index_of(&[s("abcabc"), s("b"), i(3)]).unwrap(), i(4));
    }

    #[test]
    fn lpad_pads_left() {
        assert_eq!(
            pad(&[s("7"), i(4), s("0")], PadSide::Left).unwrap(),
            s("0007")
        );
    }

    #[test]
    fn rpad_pads_right() {
        assert_eq!(
            pad(&[s("x"), i(5), s(".-")], PadSide::Right).unwrap(),
            s("x.-.-"),
        );
    }

    #[test]
    fn pad_noop_when_already_long_enough() {
        assert_eq!(
            pad(&[s("long"), i(2), s("_")], PadSide::Left).unwrap(),
            s("long"),
        );
    }

    #[test]
    fn capitalize_uppercases_first_char() {
        assert_eq!(capitalize_first("hello"), "Hello");
    }

    #[test]
    fn capitalize_all_title_cases_words() {
        assert_eq!(capitalize_all("hello world"), "Hello World");
    }

    #[test]
    fn decapitalize_lowercases_first() {
        assert_eq!(decapitalize_first("Hello"), "hello");
    }

    #[test]
    fn swap_case_flips_all() {
        assert_eq!(swap_case("HeLLo"), "hEllO");
    }

    #[test]
    fn camel_case_from_snake_input() {
        assert_eq!(to_camel_case("hello world"), "helloWorld");
        assert_eq!(to_camel_case("api_key_name"), "apiKeyName");
    }

    #[test]
    fn snake_case_from_mixed_input() {
        assert_eq!(to_snake_case("Hello World"), "hello_world");
        assert_eq!(to_snake_case("api-key-name"), "api_key_name");
    }

    #[test]
    fn upper_camel_case_produces_pascal() {
        assert_eq!(to_upper_camel_case("hello world"), "HelloWorld");
    }

    #[test]
    fn repeat_concatenates_n_times() {
        assert_eq!(repeat(&[s("ab"), i(3)]).unwrap(), s("ababab"),);
    }

    #[test]
    fn repeat_negative_errors() {
        assert!(matches!(
            repeat(&[s("a"), i(-1)]).unwrap_err(),
            ApocError::TypeMismatch { .. }
        ));
    }

    #[test]
    fn url_encode_roundtrips_reserved_chars() {
        let encoded = url_encode("a b&c=1");
        assert_eq!(encoded, "a%20b%26c%3D1");
    }

    #[test]
    fn url_decode_roundtrips_encoded_input() {
        let decoded = url_decode(&[s("a%20b%26c%3D1")]).unwrap();
        assert_eq!(decoded, s("a b&c=1"));
    }

    #[test]
    fn url_decode_plus_is_space() {
        assert_eq!(url_decode(&[s("a+b")]).unwrap(), s("a b"));
    }

    #[test]
    fn url_decode_truncated_escape_errors() {
        assert!(matches!(
            url_decode(&[s("a%2")]).unwrap_err(),
            ApocError::TypeMismatch { .. }
        ));
    }

    #[test]
    fn regex_groups_extracts_captures() {
        let r = regex_groups(&[s("abc123def456"), s(r"([a-z]+)(\d+)")]).unwrap();
        assert_eq!(
            r,
            Property::List(vec![
                Property::List(vec![s("abc123"), s("abc"), s("123")]),
                Property::List(vec![s("def456"), s("def"), s("456")]),
            ]),
        );
    }

    #[test]
    fn regex_groups_no_match_is_empty_list() {
        let r = regex_groups(&[s("hello"), s(r"\d+")]).unwrap();
        assert_eq!(r, Property::List(vec![]));
    }

    #[test]
    fn hex_value_formats_integer_uppercase() {
        assert_eq!(hex_value(&[i(255)]).unwrap(), s("FF"));
    }

    #[test]
    fn null_input_returns_null() {
        assert_eq!(
            map_string(&[Property::Null], "reverse", |s| s.to_string()).unwrap(),
            Property::Null,
        );
    }

    #[test]
    fn base64_encode_decodes_round_trip() {
        let cases = ["", "a", "ab", "abc", "hello, world", "π☆"];
        for input in cases {
            let encoded = base64_encode(input);
            let decoded = base64_decode(&[s(&encoded)]).unwrap();
            assert_eq!(decoded, s(input), "round-trip for {input:?}");
        }
    }

    #[test]
    fn base64_encode_matches_known_vector() {
        // RFC 4648 §10: "foobar" → "Zm9vYmFy"
        assert_eq!(base64_encode("foobar"), "Zm9vYmFy");
        // Padding cases
        assert_eq!(base64_encode("fo"), "Zm8=");
        assert_eq!(base64_encode("f"), "Zg==");
    }

    #[test]
    fn base64_decode_rejects_invalid_char() {
        let err = base64_decode(&[s("****")]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn byte_count_vs_size() {
        // ASCII: byte count equals character count.
        assert_eq!(byte_count(&[s("hello")]).unwrap(), Property::Int64(5));
        // Multi-byte UTF-8: byte count exceeds character count.
        assert_eq!(byte_count(&[s("π")]).unwrap(), Property::Int64(2));
        assert_eq!(byte_count(&[s("☆")]).unwrap(), Property::Int64(3));
    }

    #[test]
    fn clean_strips_non_alphanumeric_and_lowercases() {
        assert_eq!(clean("Hello, World!"), "helloworld");
        assert_eq!(clean("A-B_C.D 1-2-3"), "abcd123");
    }

    #[test]
    fn levenshtein_distance_known_cases() {
        let d = |a: &str, b: &str| levenshtein_distance(&[s(a), s(b)]).unwrap();
        assert_eq!(d("kitten", "sitting"), Property::Int64(3));
        assert_eq!(d("", "abc"), Property::Int64(3));
        assert_eq!(d("abc", ""), Property::Int64(3));
        assert_eq!(d("same", "same"), Property::Int64(0));
        // Unicode: insert a multi-byte char counts as one edit.
        assert_eq!(d("abc", "aπbc"), Property::Int64(1));
    }

    #[test]
    fn levenshtein_null_arg_propagates_null() {
        assert_eq!(
            levenshtein_distance(&[Property::Null, s("x")]).unwrap(),
            Property::Null,
        );
        assert_eq!(
            levenshtein_distance(&[s("x"), Property::Null]).unwrap(),
            Property::Null,
        );
    }
}
