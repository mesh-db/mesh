//! Typed wrappers around the Bolt 4.4 message set. Each variant is
//! encoded on the wire as a PackStream `Struct` with a well-known tag
//! byte and a fixed-arity field list.
//!
//! Tag byte reference (Bolt 4.4):
//!
//! | Message       | Tag    | Direction | Fields                              |
//! |---------------|--------|-----------|-------------------------------------|
//! | HELLO         | 0x01   | C → S     | `{extra}`                           |
//! | GOODBYE       | 0x02   | C → S     | `{}`                                |
//! | RESET         | 0x0F   | C → S     | `{}`                                |
//! | RUN           | 0x10   | C → S     | `query`, `params`, `extra`          |
//! | BEGIN         | 0x11   | C → S     | `{extra}`                           |
//! | COMMIT        | 0x12   | C → S     | `{}`                                |
//! | ROLLBACK      | 0x13   | C → S     | `{}`                                |
//! | DISCARD       | 0x2F   | C → S     | `{extra}`                           |
//! | PULL          | 0x3F   | C → S     | `{extra}`                           |
//! | SUCCESS       | 0x70   | S → C     | `{metadata}`                        |
//! | RECORD        | 0x71   | S → C     | `fields` (as a List)                |
//! | IGNORED       | 0x7E   | S → C     | `{}`                                |
//! | FAILURE       | 0x7F   | S → C     | `{metadata}` — `code` + `message`   |

use crate::error::{BoltError, Result};
use crate::packstream;
use crate::value::BoltValue;

pub const TAG_HELLO: u8 = 0x01;
pub const TAG_GOODBYE: u8 = 0x02;
pub const TAG_RESET: u8 = 0x0F;
pub const TAG_RUN: u8 = 0x10;
pub const TAG_BEGIN: u8 = 0x11;
pub const TAG_COMMIT: u8 = 0x12;
pub const TAG_ROLLBACK: u8 = 0x13;
pub const TAG_DISCARD: u8 = 0x2F;
pub const TAG_PULL: u8 = 0x3F;
pub const TAG_SUCCESS: u8 = 0x70;
pub const TAG_RECORD: u8 = 0x71;
pub const TAG_IGNORED: u8 = 0x7E;
pub const TAG_FAILURE: u8 = 0x7F;

/// Well-known PackStream struct tags for graph values.
pub const TAG_NODE: u8 = 0x4E;
pub const TAG_RELATIONSHIP: u8 = 0x52;
pub const TAG_UNBOUND_RELATIONSHIP: u8 = 0x72;
/// Bolt 4.4 Path struct. Fields are `{nodes: List<Node>,
/// rels: List<UnboundRelationship>, sequence: List<Int>}` — the
/// sequence alternates `(rel_index, node_index)` pairs so the
/// client can reconstruct the traversal with deduplicated nodes
/// and rels. `value_conv::path_to_bolt` in mesh-server packs our
/// `Value::Path` into this shape.
pub const TAG_PATH: u8 = 0x50;
/// Bolt 4.4 Date struct. Single field `[days: Int]` — days
/// since the UNIX epoch (1970-01-01), UTC.
pub const TAG_DATE: u8 = 0x44;
/// Bolt 4.4 LocalDateTime struct. Fields `[seconds: Int, nanos: Int]`
/// — nanosecond-precision timestamp since the UNIX epoch without
/// timezone information. We always emit this UTC-only form in v1;
/// the timezone-aware `DateTime` variants (`0x46` / `0x66`) are
/// follow-ups.
pub const TAG_LOCAL_DATE_TIME: u8 = 0x64;
/// Bolt 4.4 Duration struct. Fields
/// `[months: Int, days: Int, seconds: Int, nanos: Int]`.
pub const TAG_DURATION: u8 = 0x45;

/// One logical Bolt message. The coarse-grained variants are carried as
/// `BoltValue::Map`s rather than typed structs so the library stays
/// small and callers can pass through arbitrary metadata / extras
/// without schema churn as new Bolt versions add fields.
#[derive(Debug, Clone, PartialEq)]
pub enum BoltMessage {
    Hello {
        extra: BoltValue,
    },
    Goodbye,
    Reset,
    Run {
        query: String,
        params: BoltValue,
        extra: BoltValue,
    },
    Begin {
        extra: BoltValue,
    },
    Commit,
    Rollback,
    Discard {
        extra: BoltValue,
    },
    Pull {
        extra: BoltValue,
    },
    Success {
        metadata: BoltValue,
    },
    Record {
        fields: Vec<BoltValue>,
    },
    Ignored,
    Failure {
        metadata: BoltValue,
    },
}

impl BoltMessage {
    /// Serialize this message into a PackStream payload suitable for
    /// wrapping in [`crate::framing::write_message`].
    pub fn encode(&self) -> Vec<u8> {
        let (tag, fields) = self.as_struct();
        let value = BoltValue::Struct { tag, fields };
        packstream::encode(&value)
    }

    /// Convert this message into the raw tag + field vector it occupies
    /// on the wire. The `BoltValue::Map` extras are passed through
    /// verbatim — no schema validation.
    pub fn as_struct(&self) -> (u8, Vec<BoltValue>) {
        match self {
            BoltMessage::Hello { extra } => (TAG_HELLO, vec![extra.clone()]),
            BoltMessage::Goodbye => (TAG_GOODBYE, vec![]),
            BoltMessage::Reset => (TAG_RESET, vec![]),
            BoltMessage::Run {
                query,
                params,
                extra,
            } => (
                TAG_RUN,
                vec![
                    BoltValue::String(query.clone()),
                    params.clone(),
                    extra.clone(),
                ],
            ),
            BoltMessage::Begin { extra } => (TAG_BEGIN, vec![extra.clone()]),
            BoltMessage::Commit => (TAG_COMMIT, vec![]),
            BoltMessage::Rollback => (TAG_ROLLBACK, vec![]),
            BoltMessage::Discard { extra } => (TAG_DISCARD, vec![extra.clone()]),
            BoltMessage::Pull { extra } => (TAG_PULL, vec![extra.clone()]),
            BoltMessage::Success { metadata } => (TAG_SUCCESS, vec![metadata.clone()]),
            BoltMessage::Record { fields } => (TAG_RECORD, vec![BoltValue::List(fields.clone())]),
            BoltMessage::Ignored => (TAG_IGNORED, vec![]),
            BoltMessage::Failure { metadata } => (TAG_FAILURE, vec![metadata.clone()]),
        }
    }

    /// Parse a PackStream payload previously produced by [`encode`].
    pub fn decode(payload: &[u8]) -> Result<Self> {
        let (value, _) = packstream::decode(payload)?;
        let (tag, fields) = match value {
            BoltValue::Struct { tag, fields } => (tag, fields),
            other => {
                return Err(BoltError::Malformed(format!(
                    "expected top-level Struct, got {:?}",
                    other
                )))
            }
        };

        match tag {
            TAG_HELLO => {
                let extra = one_field(fields, "HELLO")?;
                Ok(BoltMessage::Hello { extra })
            }
            TAG_GOODBYE => Ok(BoltMessage::Goodbye),
            TAG_RESET => Ok(BoltMessage::Reset),
            TAG_RUN => {
                let [query_v, params, extra] = three_fields(fields, "RUN")?;
                let query = match query_v {
                    BoltValue::String(s) => s,
                    other => {
                        return Err(BoltError::Malformed(format!(
                            "RUN query must be String, got {:?}",
                            other
                        )))
                    }
                };
                Ok(BoltMessage::Run {
                    query,
                    params,
                    extra,
                })
            }
            TAG_BEGIN => {
                let extra = one_field(fields, "BEGIN")?;
                Ok(BoltMessage::Begin { extra })
            }
            TAG_COMMIT => Ok(BoltMessage::Commit),
            TAG_ROLLBACK => Ok(BoltMessage::Rollback),
            TAG_DISCARD => {
                let extra = one_field(fields, "DISCARD")?;
                Ok(BoltMessage::Discard { extra })
            }
            TAG_PULL => {
                let extra = one_field(fields, "PULL")?;
                Ok(BoltMessage::Pull { extra })
            }
            TAG_SUCCESS => {
                let metadata = one_field(fields, "SUCCESS")?;
                Ok(BoltMessage::Success { metadata })
            }
            TAG_RECORD => {
                let list = one_field(fields, "RECORD")?;
                match list {
                    BoltValue::List(items) => Ok(BoltMessage::Record { fields: items }),
                    other => Err(BoltError::Malformed(format!(
                        "RECORD fields must be List, got {:?}",
                        other
                    ))),
                }
            }
            TAG_IGNORED => Ok(BoltMessage::Ignored),
            TAG_FAILURE => {
                let metadata = one_field(fields, "FAILURE")?;
                Ok(BoltMessage::Failure { metadata })
            }
            _ => Err(BoltError::UnknownMessageTag(tag)),
        }
    }
}

fn one_field(mut fields: Vec<BoltValue>, message: &'static str) -> Result<BoltValue> {
    if fields.len() != 1 {
        return Err(BoltError::Malformed(format!(
            "{} expects 1 field, got {}",
            message,
            fields.len()
        )));
    }
    Ok(fields.remove(0))
}

fn three_fields(fields: Vec<BoltValue>, message: &'static str) -> Result<[BoltValue; 3]> {
    let len = fields.len();
    let array: [BoltValue; 3] = fields
        .try_into()
        .map_err(|_| BoltError::Malformed(format!("{} expects 3 fields, got {}", message, len)))?;
    Ok(array)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: BoltMessage) {
        let bytes = msg.encode();
        let decoded = BoltMessage::decode(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn run_round_trip() {
        roundtrip(BoltMessage::Run {
            query: "MATCH (n) RETURN n".into(),
            params: BoltValue::Map(vec![]),
            extra: BoltValue::Map(vec![]),
        });
    }

    #[test]
    fn hello_with_metadata() {
        roundtrip(BoltMessage::Hello {
            extra: BoltValue::map([
                ("user_agent", BoltValue::String("mesh-test/0.1".into())),
                ("scheme", BoltValue::String("none".into())),
            ]),
        });
    }

    #[test]
    fn success_and_failure() {
        roundtrip(BoltMessage::Success {
            metadata: BoltValue::map([
                (
                    "fields",
                    BoltValue::List(vec![BoltValue::String("n".into())]),
                ),
                ("qid", BoltValue::Int(0)),
            ]),
        });
        roundtrip(BoltMessage::Failure {
            metadata: BoltValue::map([
                ("code", BoltValue::String("Mesh.ClientError.Parse".into())),
                ("message", BoltValue::String("unexpected token".into())),
            ]),
        });
    }

    #[test]
    fn record_with_mixed_fields() {
        roundtrip(BoltMessage::Record {
            fields: vec![
                BoltValue::Int(1),
                BoltValue::String("hello".into()),
                BoltValue::Null,
            ],
        });
    }

    #[test]
    fn pull_and_discard_use_one_field() {
        roundtrip(BoltMessage::Pull {
            extra: BoltValue::map([("n", BoltValue::Int(-1))]),
        });
        roundtrip(BoltMessage::Discard {
            extra: BoltValue::Map(vec![]),
        });
    }

    #[test]
    fn zero_arg_messages() {
        roundtrip(BoltMessage::Goodbye);
        roundtrip(BoltMessage::Reset);
        roundtrip(BoltMessage::Commit);
        roundtrip(BoltMessage::Rollback);
        roundtrip(BoltMessage::Ignored);
    }

    #[test]
    fn unknown_tag_rejected() {
        let bogus = BoltValue::Struct {
            tag: 0xAA,
            fields: vec![],
        };
        let bytes = packstream::encode(&bogus);
        let err = BoltMessage::decode(&bytes).unwrap_err();
        matches!(err, BoltError::UnknownMessageTag(0xAA));
    }
}
