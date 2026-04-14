//! PackStream, Bolt's binary serialization format. Similar in spirit to
//! MessagePack but with Bolt-specific marker bytes and an extra `Struct`
//! type for graph values (Node, Relationship, Path, temporals, ...).
//!
//! This module is serialization-only — higher layers translate Bolt
//! messages (HELLO, RUN, RECORD, ...) to and from [`BoltValue::Struct`]
//! and then call [`encode`] / [`decode`] here.

use crate::error::{BoltError, Result};
use crate::value::BoltValue;

// --- PackStream markers (subset used by Bolt 4.4) -------------------------

const MARKER_NULL: u8 = 0xC0;
const MARKER_FLOAT_64: u8 = 0xC1;
const MARKER_FALSE: u8 = 0xC2;
const MARKER_TRUE: u8 = 0xC3;

const MARKER_INT_8: u8 = 0xC8;
const MARKER_INT_16: u8 = 0xC9;
const MARKER_INT_32: u8 = 0xCA;
const MARKER_INT_64: u8 = 0xCB;

const MARKER_BYTES_8: u8 = 0xCC;
const MARKER_BYTES_16: u8 = 0xCD;
const MARKER_BYTES_32: u8 = 0xCE;

// TINY_STRING    0x80..0x8F  (low nibble = length 0..15)
const MARKER_STRING_8: u8 = 0xD0;
const MARKER_STRING_16: u8 = 0xD1;
const MARKER_STRING_32: u8 = 0xD2;

// TINY_LIST      0x90..0x9F
const MARKER_LIST_8: u8 = 0xD4;
const MARKER_LIST_16: u8 = 0xD5;
const MARKER_LIST_32: u8 = 0xD6;

// TINY_MAP       0xA0..0xAF
const MARKER_MAP_8: u8 = 0xD8;
const MARKER_MAP_16: u8 = 0xD9;
const MARKER_MAP_32: u8 = 0xDA;

// TINY_STRUCT    0xB0..0xBF  (low nibble = field count 0..15; followed by tag byte)
const MARKER_STRUCT_8: u8 = 0xDC;
const MARKER_STRUCT_16: u8 = 0xDD;

/// Serialize a [`BoltValue`] into a fresh byte buffer.
pub fn encode(value: &BoltValue) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    encode_into(value, &mut out);
    out
}

/// Serialize a [`BoltValue`] into `out`, appending bytes.
pub fn encode_into(value: &BoltValue, out: &mut Vec<u8>) {
    match value {
        BoltValue::Null => out.push(MARKER_NULL),
        BoltValue::Bool(false) => out.push(MARKER_FALSE),
        BoltValue::Bool(true) => out.push(MARKER_TRUE),
        BoltValue::Int(i) => encode_int(*i, out),
        BoltValue::Float(f) => {
            out.push(MARKER_FLOAT_64);
            out.extend_from_slice(&f.to_be_bytes());
        }
        BoltValue::String(s) => encode_string(s, out),
        BoltValue::Bytes(b) => encode_bytes(b, out),
        BoltValue::List(items) => {
            encode_list_header(items.len(), out);
            for item in items {
                encode_into(item, out);
            }
        }
        BoltValue::Map(entries) => {
            encode_map_header(entries.len(), out);
            for (k, v) in entries {
                encode_string(k, out);
                encode_into(v, out);
            }
        }
        BoltValue::Struct { tag, fields } => {
            encode_struct_header(fields.len(), *tag, out);
            for f in fields {
                encode_into(f, out);
            }
        }
    }
}

fn encode_int(value: i64, out: &mut Vec<u8>) {
    // Tiny ints: -16..=127 fit in one byte (two's complement). The
    // negative tiny-int range 0xF0..0xFF encodes -16..-1; the positive
    // tiny-int range 0x00..0x7F encodes 0..127.
    if (-16..=127).contains(&value) {
        out.push(value as u8);
    } else if (i8::MIN as i64..=i8::MAX as i64).contains(&value) {
        out.push(MARKER_INT_8);
        out.push(value as i8 as u8);
    } else if (i16::MIN as i64..=i16::MAX as i64).contains(&value) {
        out.push(MARKER_INT_16);
        out.extend_from_slice(&(value as i16).to_be_bytes());
    } else if (i32::MIN as i64..=i32::MAX as i64).contains(&value) {
        out.push(MARKER_INT_32);
        out.extend_from_slice(&(value as i32).to_be_bytes());
    } else {
        out.push(MARKER_INT_64);
        out.extend_from_slice(&value.to_be_bytes());
    }
}

fn encode_string(s: &str, out: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len <= 0x0F {
        out.push(0x80 | (len as u8));
    } else if len <= u8::MAX as usize {
        out.push(MARKER_STRING_8);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(MARKER_STRING_16);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(MARKER_STRING_32);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

fn encode_bytes(b: &[u8], out: &mut Vec<u8>) {
    let len = b.len();
    if len <= u8::MAX as usize {
        out.push(MARKER_BYTES_8);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(MARKER_BYTES_16);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(MARKER_BYTES_32);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(b);
}

fn encode_list_header(len: usize, out: &mut Vec<u8>) {
    if len <= 0x0F {
        out.push(0x90 | (len as u8));
    } else if len <= u8::MAX as usize {
        out.push(MARKER_LIST_8);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(MARKER_LIST_16);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(MARKER_LIST_32);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

fn encode_map_header(len: usize, out: &mut Vec<u8>) {
    if len <= 0x0F {
        out.push(0xA0 | (len as u8));
    } else if len <= u8::MAX as usize {
        out.push(MARKER_MAP_8);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(MARKER_MAP_16);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(MARKER_MAP_32);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

fn encode_struct_header(field_count: usize, tag: u8, out: &mut Vec<u8>) {
    if field_count <= 0x0F {
        out.push(0xB0 | (field_count as u8));
    } else if field_count <= u8::MAX as usize {
        out.push(MARKER_STRUCT_8);
        out.push(field_count as u8);
    } else {
        // STRUCT_16: up to 65535 fields. Larger is not a thing Bolt
        // needs — messages and records have at most a handful.
        assert!(
            field_count <= u16::MAX as usize,
            "struct field count {} exceeds PackStream limit",
            field_count
        );
        out.push(MARKER_STRUCT_16);
        out.extend_from_slice(&(field_count as u16).to_be_bytes());
    }
    out.push(tag);
}

// --- Decoder --------------------------------------------------------------

/// Parse a single [`BoltValue`] from the start of `input` and return it
/// alongside the number of bytes consumed. A message chunk always
/// contains exactly one top-level value (usually a Struct), so callers
/// can assert `consumed == input.len()` when decoding a complete chunk.
pub fn decode(input: &[u8]) -> Result<(BoltValue, usize)> {
    let mut cursor = Cursor::new(input);
    let value = decode_value(&mut cursor)?;
    Ok((value, cursor.pos))
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        if self.pos >= self.buf.len() {
            return Err(BoltError::Truncated(self.pos));
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_exact(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(BoltError::Truncated(self.pos));
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_u16(&mut self) -> Result<u16> {
        let b = self.read_exact(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let b = self.read_exact(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_i8(&mut self) -> Result<i8> {
        Ok(self.read_u8()? as i8)
    }

    fn read_i16(&mut self) -> Result<i16> {
        let b = self.read_exact(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    fn read_i32(&mut self) -> Result<i32> {
        let b = self.read_exact(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_i64(&mut self) -> Result<i64> {
        let b = self.read_exact(8)?;
        Ok(i64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn read_f64(&mut self) -> Result<f64> {
        let b = self.read_exact(8)?;
        Ok(f64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

fn decode_value(cursor: &mut Cursor) -> Result<BoltValue> {
    let marker = cursor.read_u8()?;

    // Fast-path: tiny containers and tiny ints fit in the marker byte itself.
    match marker {
        MARKER_NULL => return Ok(BoltValue::Null),
        MARKER_TRUE => return Ok(BoltValue::Bool(true)),
        MARKER_FALSE => return Ok(BoltValue::Bool(false)),
        MARKER_FLOAT_64 => return Ok(BoltValue::Float(cursor.read_f64()?)),
        MARKER_INT_8 => return Ok(BoltValue::Int(cursor.read_i8()? as i64)),
        MARKER_INT_16 => return Ok(BoltValue::Int(cursor.read_i16()? as i64)),
        MARKER_INT_32 => return Ok(BoltValue::Int(cursor.read_i32()? as i64)),
        MARKER_INT_64 => return Ok(BoltValue::Int(cursor.read_i64()?)),
        MARKER_BYTES_8 => {
            let len = cursor.read_u8()? as usize;
            return Ok(BoltValue::Bytes(cursor.read_exact(len)?.to_vec()));
        }
        MARKER_BYTES_16 => {
            let len = cursor.read_u16()? as usize;
            return Ok(BoltValue::Bytes(cursor.read_exact(len)?.to_vec()));
        }
        MARKER_BYTES_32 => {
            let len = cursor.read_u32()? as usize;
            return Ok(BoltValue::Bytes(cursor.read_exact(len)?.to_vec()));
        }
        MARKER_STRING_8 => {
            let len = cursor.read_u8()? as usize;
            return Ok(BoltValue::String(read_string(cursor, len)?));
        }
        MARKER_STRING_16 => {
            let len = cursor.read_u16()? as usize;
            return Ok(BoltValue::String(read_string(cursor, len)?));
        }
        MARKER_STRING_32 => {
            let len = cursor.read_u32()? as usize;
            return Ok(BoltValue::String(read_string(cursor, len)?));
        }
        MARKER_LIST_8 => {
            let len = cursor.read_u8()? as usize;
            return read_list(cursor, len);
        }
        MARKER_LIST_16 => {
            let len = cursor.read_u16()? as usize;
            return read_list(cursor, len);
        }
        MARKER_LIST_32 => {
            let len = cursor.read_u32()? as usize;
            return read_list(cursor, len);
        }
        MARKER_MAP_8 => {
            let len = cursor.read_u8()? as usize;
            return read_map(cursor, len);
        }
        MARKER_MAP_16 => {
            let len = cursor.read_u16()? as usize;
            return read_map(cursor, len);
        }
        MARKER_MAP_32 => {
            let len = cursor.read_u32()? as usize;
            return read_map(cursor, len);
        }
        MARKER_STRUCT_8 => {
            let count = cursor.read_u8()? as usize;
            let tag = cursor.read_u8()?;
            return read_struct(cursor, count, tag);
        }
        MARKER_STRUCT_16 => {
            let count = cursor.read_u16()? as usize;
            let tag = cursor.read_u8()?;
            return read_struct(cursor, count, tag);
        }
        _ => {}
    }

    // Marker-encoded values: tiny ints (+/- range), tiny strings, lists,
    // maps, and structs.
    //
    // The positive tiny-int range 0x00..=0x7F encodes 0..=127; the
    // negative tiny-int range 0xF0..=0xFF encodes -16..=-1 via two's
    // complement of the whole byte. Everything in between falls
    // through into the tiny-container ranges below.
    if marker <= 0x7F {
        return Ok(BoltValue::Int(marker as i64));
    }
    if marker >= 0xF0 {
        return Ok(BoltValue::Int(marker as i8 as i64));
    }
    match marker & 0xF0 {
        0x80 => {
            let len = (marker & 0x0F) as usize;
            Ok(BoltValue::String(read_string(cursor, len)?))
        }
        0x90 => {
            let len = (marker & 0x0F) as usize;
            read_list(cursor, len)
        }
        0xA0 => {
            let len = (marker & 0x0F) as usize;
            read_map(cursor, len)
        }
        0xB0 => {
            let count = (marker & 0x0F) as usize;
            let tag = cursor.read_u8()?;
            read_struct(cursor, count, tag)
        }
        _ => Err(BoltError::InvalidMarker(marker)),
    }
}

fn read_string(cursor: &mut Cursor, len: usize) -> Result<String> {
    let bytes = cursor.read_exact(len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| BoltError::InvalidUtf8)
}

fn read_list(cursor: &mut Cursor, len: usize) -> Result<BoltValue> {
    let mut items = Vec::with_capacity(len);
    for _ in 0..len {
        items.push(decode_value(cursor)?);
    }
    Ok(BoltValue::List(items))
}

fn read_map(cursor: &mut Cursor, len: usize) -> Result<BoltValue> {
    let mut entries = Vec::with_capacity(len);
    for _ in 0..len {
        let key = match decode_value(cursor)? {
            BoltValue::String(s) => s,
            other => {
                return Err(BoltError::Malformed(format!(
                    "map key must be string, got {:?}",
                    other
                )))
            }
        };
        let value = decode_value(cursor)?;
        entries.push((key, value));
    }
    Ok(BoltValue::Map(entries))
}

fn read_struct(cursor: &mut Cursor, count: usize, tag: u8) -> Result<BoltValue> {
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        fields.push(decode_value(cursor)?);
    }
    Ok(BoltValue::Struct { tag, fields })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(value: BoltValue) {
        let bytes = encode(&value);
        let (decoded, consumed) = decode(&bytes).expect("decode");
        assert_eq!(consumed, bytes.len(), "decoder left trailing bytes");
        assert_eq!(decoded, value);
    }

    #[test]
    fn null_and_bools() {
        roundtrip(BoltValue::Null);
        roundtrip(BoltValue::Bool(true));
        roundtrip(BoltValue::Bool(false));
    }

    #[test]
    fn tiny_ints_fit_in_one_byte() {
        for i in -16..=127_i64 {
            let bytes = encode(&BoltValue::Int(i));
            assert_eq!(bytes.len(), 1, "int {i} should be tiny");
            roundtrip(BoltValue::Int(i));
        }
    }

    #[test]
    fn int_8_range() {
        roundtrip(BoltValue::Int(-17));
        roundtrip(BoltValue::Int(-128));
        roundtrip(BoltValue::Int(128));
        roundtrip(BoltValue::Int(255));
    }

    #[test]
    fn int_16_range() {
        roundtrip(BoltValue::Int(256));
        roundtrip(BoltValue::Int(-129));
        roundtrip(BoltValue::Int(i16::MAX as i64));
        roundtrip(BoltValue::Int(i16::MIN as i64));
    }

    #[test]
    fn int_32_range() {
        roundtrip(BoltValue::Int(i32::MAX as i64));
        roundtrip(BoltValue::Int(i32::MIN as i64));
    }

    #[test]
    fn int_64_range() {
        roundtrip(BoltValue::Int(i64::MAX));
        roundtrip(BoltValue::Int(i64::MIN));
    }

    #[test]
    fn float_roundtrip() {
        roundtrip(BoltValue::Float(0.0));
        roundtrip(BoltValue::Float(3.14));
        roundtrip(BoltValue::Float(-1.5e200));
    }

    #[test]
    fn tiny_string() {
        roundtrip(BoltValue::String("".into()));
        roundtrip(BoltValue::String("hello".into()));
        let fifteen = "x".repeat(15);
        roundtrip(BoltValue::String(fifteen));
    }

    #[test]
    fn string_8_range() {
        let s = "x".repeat(16);
        let bytes = encode(&BoltValue::String(s.clone()));
        assert_eq!(bytes[0], MARKER_STRING_8);
        roundtrip(BoltValue::String(s));
        roundtrip(BoltValue::String("x".repeat(255)));
    }

    #[test]
    fn string_16_range() {
        let s = "x".repeat(256);
        let bytes = encode(&BoltValue::String(s.clone()));
        assert_eq!(bytes[0], MARKER_STRING_16);
        roundtrip(BoltValue::String(s));
    }

    #[test]
    fn utf8_string() {
        roundtrip(BoltValue::String("héllo 世界 🦀".into()));
    }

    #[test]
    fn tiny_list_of_mixed_values() {
        let list = BoltValue::List(vec![
            BoltValue::Int(1),
            BoltValue::String("two".into()),
            BoltValue::Bool(true),
            BoltValue::Null,
        ]);
        roundtrip(list);
    }

    #[test]
    fn nested_list() {
        let list = BoltValue::List(vec![
            BoltValue::List(vec![BoltValue::Int(1), BoltValue::Int(2)]),
            BoltValue::List(vec![BoltValue::Int(3), BoltValue::Int(4)]),
        ]);
        roundtrip(list);
    }

    #[test]
    fn tiny_map_preserves_order() {
        let map = BoltValue::Map(vec![
            ("b".into(), BoltValue::Int(1)),
            ("a".into(), BoltValue::Int(2)),
            ("c".into(), BoltValue::Int(3)),
        ]);
        let encoded = encode(&map);
        let (decoded, _) = decode(&encoded).unwrap();
        // Map key order is preserved across round-trip.
        match decoded {
            BoltValue::Map(entries) => {
                assert_eq!(
                    entries.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
                    vec!["b", "a", "c"]
                );
            }
            _ => panic!("expected map"),
        }
    }

    #[test]
    fn struct_roundtrip() {
        let s = BoltValue::Struct {
            tag: 0x4E, // Node
            fields: vec![
                BoltValue::Int(42),
                BoltValue::List(vec![BoltValue::String("Person".into())]),
                BoltValue::Map(vec![
                    ("name".into(), BoltValue::String("Ada".into())),
                    ("age".into(), BoltValue::Int(37)),
                ]),
                BoltValue::String("element-42".into()),
            ],
        };
        roundtrip(s);
    }

    #[test]
    fn struct_16_field_count() {
        // 16 fields → must use STRUCT_8 marker since tiny struct only
        // covers 0..=15 fields.
        let fields: Vec<BoltValue> = (0..16).map(BoltValue::Int).collect();
        let s = BoltValue::Struct { tag: 0x10, fields };
        let bytes = encode(&s);
        assert_eq!(bytes[0], MARKER_STRUCT_8);
        roundtrip(s);
    }

    #[test]
    fn reject_invalid_marker() {
        let bytes = vec![0xC4]; // unused marker byte in Bolt 4.4 PackStream
        assert!(matches!(
            decode(&bytes),
            Err(BoltError::InvalidMarker(0xC4))
        ));
    }
}
