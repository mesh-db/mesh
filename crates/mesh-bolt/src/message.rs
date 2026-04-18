//! Typed wrappers around the Bolt 4.4 and 5.x message set. Each variant
//! is encoded on the wire as a PackStream `Struct` with a well-known
//! tag byte and a fixed-arity field list.
//!
//! Tag byte reference:
//!
//! | Message       | Tag    | Version | Direction | Fields                              |
//! |---------------|--------|---------|-----------|-------------------------------------|
//! | HELLO         | 0x01   | 4.4+    | C → S     | `{extra}`                           |
//! | GOODBYE       | 0x02   | 4.4+    | C → S     | `{}`                                |
//! | RESET         | 0x0F   | 4.4+    | C → S     | `{}`                                |
//! | RUN           | 0x10   | 4.4+    | C → S     | `query`, `params`, `extra`          |
//! | BEGIN         | 0x11   | 4.4+    | C → S     | `{extra}`                           |
//! | COMMIT        | 0x12   | 4.4+    | C → S     | `{}`                                |
//! | ROLLBACK      | 0x13   | 4.4+    | C → S     | `{}`                                |
//! | DISCARD       | 0x2F   | 4.4+    | C → S     | `{extra}`                           |
//! | PULL          | 0x3F   | 4.4+    | C → S     | `{extra}`                           |
//! | ROUTE         | 0x66   | 4.4+    | C → S     | `routing`, `bookmarks`, `extra`     |
//! | LOGON         | 0x6A   | 5.1+    | C → S     | `{auth}`                            |
//! | LOGOFF        | 0x6B   | 5.1+    | C → S     | `{}`                                |
//! | TELEMETRY     | 0x54   | 5.4+    | C → S     | `{api}`                             |
//! | SUCCESS       | 0x70   | 4.4+    | S → C     | `{metadata}`                        |
//! | RECORD        | 0x71   | 4.4+    | S → C     | `fields` (as a List)                |
//! | IGNORED       | 0x7E   | 4.4+    | S → C     | `{}`                                |
//! | FAILURE       | 0x7F   | 4.4+    | S → C     | `{metadata}` — `code` + `message`   |

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
/// TELEMETRY message introduced in Bolt 5.4. Reports driver API
/// usage for observability. Server can safely no-op and SUCCESS.
pub const TAG_TELEMETRY: u8 = 0x54;
/// ROUTE message introduced in Bolt 4.4 for causal-cluster routing.
/// Clients ask the server for a routing table; single-node Mesh
/// replies with a synthetic table pointing at itself.
pub const TAG_ROUTE: u8 = 0x66;
/// LOGON message introduced in Bolt 5.1. Separates authentication
/// from the initial HELLO so credentials can be refreshed mid-session.
/// When present, the driver sends HELLO → (server SUCCESS) → LOGON.
pub const TAG_LOGON: u8 = 0x6A;
/// LOGOFF message introduced in Bolt 5.1. Clears session
/// authentication; the connection returns to an unauthenticated
/// state waiting for a fresh LOGON.
pub const TAG_LOGOFF: u8 = 0x6B;
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
/// Bolt LocalDateTime struct (tag 0x64). Fields `[seconds: Int, nanos: Int]`
/// — nanosecond-precision timestamp since the UNIX epoch without
/// timezone information. Used when the value has no timezone (a
/// naive wall-clock time).
pub const TAG_LOCAL_DATE_TIME: u8 = 0x64;
/// Bolt DateTime with timezone offset (Bolt 5.0+, tag 0x49).
/// Fields `[seconds: Int, nanos: Int, tz_offset_seconds: Int]`.
/// In Bolt 5.0+, `seconds`/`nanos` are UTC instant; clients compute
/// local wall-clock time from the offset.
pub const TAG_DATE_TIME: u8 = 0x49;
/// Legacy DateTime (Bolt 4.4, tag 0x46): same fields, but
/// `seconds`/`nanos` represent local wall-clock time, not UTC.
/// Kept for compatibility with drivers negotiating Bolt 4.4.
pub const TAG_DATE_TIME_LEGACY: u8 = 0x46;
/// Bolt DateTimeZoneId (Bolt 5.0+, tag 0x69). Fields
/// `[seconds: Int, nanos: Int, tz_id: String]` — timezone carried as
/// a named region (e.g. "Europe/Stockholm").
pub const TAG_DATE_TIME_ZONE_ID: u8 = 0x69;
/// Legacy DateTimeZoneId (Bolt 4.4, tag 0x66): same fields, local
/// wall-clock semantics.
pub const TAG_DATE_TIME_ZONE_ID_LEGACY: u8 = 0x66;
/// Bolt Time struct (tag 0x54 — NOT the TELEMETRY tag; context disambiguates).
/// Fields `[nanos: Int, tz_offset_seconds: Int]` — nanoseconds since
/// midnight with timezone offset. TELEMETRY (0x54) is a message,
/// Time (0x54) is a value; they never collide because messages and
/// values have separate decoding contexts.
pub const TAG_TIME: u8 = 0x54;
/// Bolt LocalTime struct (tag 0x74). Fields `[nanos: Int]` —
/// nanoseconds since midnight, no timezone.
pub const TAG_LOCAL_TIME: u8 = 0x74;
/// Bolt Duration struct. Fields
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
    /// Bolt 4.4+ routing request: `[routing: Map, bookmarks: List,
    /// extra: Map]`. Returns a routing table in SUCCESS metadata.
    Route {
        routing: BoltValue,
        bookmarks: BoltValue,
        extra: BoltValue,
    },
    /// Bolt 5.1+ authentication: `{auth}` — the auth dict that used to
    /// live in HELLO extra in earlier versions.
    Logon {
        auth: BoltValue,
    },
    /// Bolt 5.1+ clear auth.
    Logoff,
    /// Bolt 5.4+ driver telemetry: `{api: Int}`. Server can no-op.
    Telemetry {
        api: BoltValue,
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
            BoltMessage::Route {
                routing,
                bookmarks,
                extra,
            } => (
                TAG_ROUTE,
                vec![routing.clone(), bookmarks.clone(), extra.clone()],
            ),
            BoltMessage::Logon { auth } => (TAG_LOGON, vec![auth.clone()]),
            BoltMessage::Logoff => (TAG_LOGOFF, vec![]),
            BoltMessage::Telemetry { api } => (TAG_TELEMETRY, vec![api.clone()]),
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
            TAG_ROUTE => {
                let [routing, bookmarks, extra] = three_fields(fields, "ROUTE")?;
                Ok(BoltMessage::Route {
                    routing,
                    bookmarks,
                    extra,
                })
            }
            TAG_LOGON => {
                let auth = one_field(fields, "LOGON")?;
                Ok(BoltMessage::Logon { auth })
            }
            TAG_LOGOFF => Ok(BoltMessage::Logoff),
            TAG_TELEMETRY => {
                let api = one_field(fields, "TELEMETRY")?;
                Ok(BoltMessage::Telemetry { api })
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

    #[test]
    fn bolt_5_logon_round_trip() {
        roundtrip(BoltMessage::Logon {
            auth: BoltValue::map([
                ("scheme", BoltValue::String("basic".into())),
                ("principal", BoltValue::String("neo4j".into())),
                ("credentials", BoltValue::String("s3cret".into())),
            ]),
        });
    }

    #[test]
    fn bolt_5_logoff_round_trip() {
        roundtrip(BoltMessage::Logoff);
    }

    #[test]
    fn bolt_5_telemetry_round_trip() {
        roundtrip(BoltMessage::Telemetry {
            api: BoltValue::map([("api", BoltValue::Int(1))]),
        });
    }

    #[test]
    fn bolt_route_round_trip() {
        roundtrip(BoltMessage::Route {
            routing: BoltValue::map([("address", BoltValue::String("localhost:7687".into()))]),
            bookmarks: BoltValue::List(vec![]),
            extra: BoltValue::Map(vec![]),
        });
    }
}
