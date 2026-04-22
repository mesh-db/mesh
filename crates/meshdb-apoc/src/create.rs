//! `apoc.create.*` — scalar UUID helpers.
//!
//! The full Neo4j `apoc.create.*` surface includes *procedures*
//! (`apoc.create.node`, `apoc.create.relationship`, …) that live
//! behind `CALL` and mutate the graph. Those belong in the
//! executor's procedure registry, not this crate — they need the
//! write path and aren't pure scalars. What this module ships is
//! the four UUID-generation / UUID-format scalars:
//!
//! - `apoc.create.uuid()` — fresh UUID v4 string in canonical
//!   hyphenated form (`"xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx"`).
//!   Each call is live; no statement-level memoization.
//! - `apoc.create.uuidBase64()` — fresh UUID v4 encoded as
//!   URL-safe base64 without padding (22 chars).
//! - `apoc.create.uuidBase64ToHex(b64)` — converts a 22-char
//!   base64 UUID back to its canonical hyphenated hex form.
//! - `apoc.create.uuidHexToBase64(hex)` — the inverse: hyphenated
//!   or bare 32-hex string to the 22-char base64 form.
//!
//! Both conversion helpers accept either the URL-safe (`-_`) or
//! the standard (`+/`) base64 alphabets on decode, and accept the
//! UUID with or without hyphens on hex decode. Malformed inputs
//! surface as [`ApocError::TypeMismatch`] — callers who prefer a
//! lax null-on-garbage response can wrap with `COALESCE`.

use crate::ApocError;
use meshdb_core::Property;
use uuid::Uuid;

pub fn call(suffix: &str, args: &[Property]) -> Result<Property, ApocError> {
    match suffix {
        "uuid" => uuid_v4(args),
        "uuidbase64" => uuid_v4_base64(args),
        "uuidbase64tohex" => uuid_base64_to_hex(args),
        "uuidhextobase64" => uuid_hex_to_base64(args),
        _ => Err(ApocError::UnknownFunction(format!("apoc.create.{suffix}"))),
    }
}

// ---------------------------------------------------------------
// Generators.
// ---------------------------------------------------------------

fn uuid_v4(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("uuid", args, 0)?;
    Ok(Property::String(Uuid::new_v4().hyphenated().to_string()))
}

fn uuid_v4_base64(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("uuidBase64", args, 0)?;
    let bytes = *Uuid::new_v4().as_bytes();
    Ok(Property::String(encode_base64_16(&bytes)))
}

// ---------------------------------------------------------------
// Format conversions.
// ---------------------------------------------------------------

fn uuid_base64_to_hex(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("uuidBase64ToHex", args, 1)?;
    match &args[0] {
        Property::Null => Ok(Property::Null),
        Property::String(s) => {
            let bytes = decode_base64_16(s.trim()).ok_or_else(|| ApocError::TypeMismatch {
                name: "apoc.create.uuidBase64ToHex".into(),
                details: format!("invalid 22-char base64 UUID: {s:?}"),
            })?;
            Ok(Property::String(
                Uuid::from_bytes(bytes).hyphenated().to_string(),
            ))
        }
        other => Err(ApocError::TypeMismatch {
            name: "apoc.create.uuidBase64ToHex".into(),
            details: format!("expected a string, got {}", other.type_name()),
        }),
    }
}

fn uuid_hex_to_base64(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("uuidHexToBase64", args, 1)?;
    match &args[0] {
        Property::Null => Ok(Property::Null),
        Property::String(s) => {
            let parsed = Uuid::parse_str(s.trim()).map_err(|e| ApocError::TypeMismatch {
                name: "apoc.create.uuidHexToBase64".into(),
                details: format!("invalid UUID string: {e}"),
            })?;
            Ok(Property::String(encode_base64_16(parsed.as_bytes())))
        }
        other => Err(ApocError::TypeMismatch {
            name: "apoc.create.uuidHexToBase64".into(),
            details: format!("expected a string, got {}", other.type_name()),
        }),
    }
}

// ---------------------------------------------------------------
// Base64 (16-byte specialization, URL-safe alphabet, no padding).
// ---------------------------------------------------------------

const B64_URLSAFE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Encodes exactly 16 bytes to a 22-char URL-safe base64 string
/// (no padding, no newlines). The third group of 3 bytes is only
/// 1 byte, so the final block is 2 chars instead of 4, yielding
/// 21 + 1 = 22 output chars.
fn encode_base64_16(bytes: &[u8; 16]) -> String {
    let mut out = String::with_capacity(22);
    // Five full 3-byte chunks → 20 chars.
    for chunk in bytes[..15].chunks_exact(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk[1] as u32;
        let b2 = chunk[2] as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64_URLSAFE[((n >> 18) & 0x3f) as usize] as char);
        out.push(B64_URLSAFE[((n >> 12) & 0x3f) as usize] as char);
        out.push(B64_URLSAFE[((n >> 6) & 0x3f) as usize] as char);
        out.push(B64_URLSAFE[(n & 0x3f) as usize] as char);
    }
    // Final trailing byte → 2 output chars.
    let last = bytes[15] as u32;
    out.push(B64_URLSAFE[((last >> 2) & 0x3f) as usize] as char);
    out.push(B64_URLSAFE[((last << 4) & 0x3f) as usize] as char);
    out
}

/// Decodes a 22-char base64 string (URL-safe or standard alphabet,
/// any trailing `=` padding trimmed) back to the 16-byte UUID
/// payload. Returns `None` for any non-alphabet character, wrong
/// length, or non-zero low bits that the 2-char tail can't
/// represent.
fn decode_base64_16(s: &str) -> Option<[u8; 16]> {
    // Accept either URL-safe ('-'/'_') or standard ('+'/'/') alphabet
    // and ignore any trailing '=' padding the caller might include.
    let trimmed = s.trim_end_matches('=');
    if trimmed.len() != 22 {
        return None;
    }
    let mut out = [0u8; 16];
    let bytes = trimmed.as_bytes();
    // Five full 4-char blocks → 15 output bytes.
    for i in 0..5 {
        let n = (b64_decode_char(bytes[i * 4])? << 18)
            | (b64_decode_char(bytes[i * 4 + 1])? << 12)
            | (b64_decode_char(bytes[i * 4 + 2])? << 6)
            | b64_decode_char(bytes[i * 4 + 3])?;
        out[i * 3] = ((n >> 16) & 0xff) as u8;
        out[i * 3 + 1] = ((n >> 8) & 0xff) as u8;
        out[i * 3 + 2] = (n & 0xff) as u8;
    }
    // Tail: 2 chars → 1 byte. The second char's low 4 bits must be
    // zero or the encoding isn't a valid 16-byte payload.
    let a = b64_decode_char(bytes[20])?;
    let b = b64_decode_char(bytes[21])?;
    if b & 0x0f != 0 {
        return None;
    }
    out[15] = (((a << 2) | (b >> 4)) & 0xff) as u8;
    Some(out)
}

fn b64_decode_char(c: u8) -> Option<u32> {
    Some(match c {
        b'A'..=b'Z' => (c - b'A') as u32,
        b'a'..=b'z' => (c - b'a' + 26) as u32,
        b'0'..=b'9' => (c - b'0' + 52) as u32,
        b'+' | b'-' => 62,
        b'/' | b'_' => 63,
        _ => return None,
    })
}

// ---------------------------------------------------------------
// Argument helpers.
// ---------------------------------------------------------------

fn check_arity(name: &str, args: &[Property], expected: usize) -> Result<(), ApocError> {
    if args.len() != expected {
        return Err(ApocError::Arity {
            name: format!("apoc.create.{name}"),
            expected,
            got: args.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s_str(p: Property) -> String {
        match p {
            Property::String(s) => s,
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn uuid_shape_and_version() {
        let v = s_str(uuid_v4(&[]).unwrap());
        assert_eq!(v.len(), 36, "hyphenated UUID is 36 chars");
        // version nibble at index 14 (8-4-4-4-12 layout, 3rd group first char)
        assert_eq!(&v[14..15], "4", "UUID v4 version nibble");
        // variant nibble at index 19, first char of 4th group — must be 8/9/a/b
        let variant = v.chars().nth(19).unwrap();
        assert!(matches!(variant, '8' | '9' | 'a' | 'b'));
    }

    #[test]
    fn uuid_base64_has_stable_length() {
        let v = s_str(uuid_v4_base64(&[]).unwrap());
        assert_eq!(v.len(), 22);
        // Alphabet check — every char must be URL-safe base64.
        for c in v.bytes() {
            assert!(
                matches!(c, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_'),
                "unexpected char {:?}",
                c as char,
            );
        }
    }

    #[test]
    fn base64_round_trip() {
        let hex = s_str(uuid_v4(&[]).unwrap());
        let b64 = uuid_hex_to_base64(&[Property::String(hex.clone())]).unwrap();
        let back = uuid_base64_to_hex(&[b64]).unwrap();
        assert_eq!(back, Property::String(hex));
    }

    #[test]
    fn hex_to_base64_accepts_unhyphenated_input() {
        let hex = "550e8400e29b41d4a716446655440000";
        let b64 = s_str(uuid_hex_to_base64(&[Property::String(hex.into())]).unwrap());
        let back = s_str(uuid_base64_to_hex(&[Property::String(b64)]).unwrap());
        assert_eq!(back, "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn base64_to_hex_accepts_standard_alphabet() {
        // Encode with URL-safe, then substitute `-_` → `+/` to prove
        // the decoder accepts either alphabet.
        let b64 = s_str(uuid_v4_base64(&[]).unwrap());
        let std = b64.replace('-', "+").replace('_', "/");
        let via_urlsafe = uuid_base64_to_hex(&[Property::String(b64)]).unwrap();
        let via_standard = uuid_base64_to_hex(&[Property::String(std)]).unwrap();
        assert_eq!(via_urlsafe, via_standard);
    }

    #[test]
    fn base64_to_hex_strips_trailing_padding() {
        // Pad with `==` — a Java-style encoder sometimes emits it.
        let b64 = s_str(uuid_v4_base64(&[]).unwrap());
        let padded = format!("{b64}==");
        assert!(uuid_base64_to_hex(&[Property::String(padded)]).is_ok());
    }

    #[test]
    fn base64_to_hex_rejects_wrong_length() {
        let err = uuid_base64_to_hex(&[Property::String("tooshort".into())]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn base64_to_hex_rejects_bad_alphabet() {
        // 22 chars but contains '!' which isn't in either alphabet.
        let err = uuid_base64_to_hex(&[Property::String("!".repeat(22))]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn hex_to_base64_rejects_garbage() {
        let err = uuid_hex_to_base64(&[Property::String("not a uuid".into())]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn null_propagates_on_conversions() {
        assert_eq!(
            uuid_base64_to_hex(&[Property::Null]).unwrap(),
            Property::Null,
        );
        assert_eq!(
            uuid_hex_to_base64(&[Property::Null]).unwrap(),
            Property::Null,
        );
    }

    #[test]
    fn generators_reject_args() {
        let err = uuid_v4(&[Property::Int64(1)]).unwrap_err();
        assert!(matches!(err, ApocError::Arity { expected: 0, .. }));
        let err = uuid_v4_base64(&[Property::Int64(1)]).unwrap_err();
        assert!(matches!(err, ApocError::Arity { expected: 0, .. }));
    }

    #[test]
    fn unknown_function_surfaces_full_name() {
        let err = call("quaff", &[]).unwrap_err();
        match err {
            ApocError::UnknownFunction(n) => assert_eq!(n, "apoc.create.quaff"),
            other => panic!("expected UnknownFunction, got {other:?}"),
        }
    }
}
