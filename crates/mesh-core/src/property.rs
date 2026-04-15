use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A Cypher duration — months, days, seconds, nanoseconds. Matches
/// the Bolt 4.4 Duration struct (tag `0x45`) field-for-field so no
/// conversion is needed at the wire boundary. Stored as four
/// separate components because the calendar units (`months`, `days`)
/// can't be reduced to seconds without knowing a reference date,
/// while the exact units (`seconds`, `nanos`) can.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Duration {
    pub months: i64,
    pub days: i64,
    pub seconds: i64,
    pub nanos: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum Property {
    Null,
    String(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
    List(Vec<Property>),
    Map(HashMap<String, Property>),
    /// UTC epoch milliseconds. Maps to Bolt's `LocalDateTime`
    /// (struct tag `0x64`) on the wire — we're UTC-only in v1,
    /// so the local and timezone-aware forms are interchangeable
    /// and we pick the simpler one. Range: ±292 million years
    /// from 1970, more than sufficient for any real workload.
    DateTime(i64),
    /// Days since the UNIX epoch (1970-01-01, UTC). `i32` gives
    /// ±5.9 million years of range — far more than any realistic
    /// calendar application. Maps to Bolt `Date` (struct tag
    /// `0x44`).
    Date(i32),
    /// A Cypher duration value — see [`Duration`].
    Duration(Duration),
}

impl Property {
    pub fn type_name(&self) -> &'static str {
        match self {
            Property::Null => "Null",
            Property::String(_) => "String",
            Property::Int64(_) => "Int64",
            Property::Float64(_) => "Float64",
            Property::Bool(_) => "Bool",
            Property::List(_) => "List",
            Property::Map(_) => "Map",
            Property::DateTime(_) => "DateTime",
            Property::Date(_) => "Date",
            Property::Duration(_) => "Duration",
        }
    }
}

impl From<String> for Property {
    fn from(v: String) -> Self {
        Property::String(v)
    }
}

impl From<&str> for Property {
    fn from(v: &str) -> Self {
        Property::String(v.to_string())
    }
}

impl From<i64> for Property {
    fn from(v: i64) -> Self {
        Property::Int64(v)
    }
}

impl From<i32> for Property {
    fn from(v: i32) -> Self {
        Property::Int64(v as i64)
    }
}

impl From<f64> for Property {
    fn from(v: f64) -> Self {
        Property::Float64(v)
    }
}

impl From<bool> for Property {
    fn from(v: bool) -> Self {
        Property::Bool(v)
    }
}

impl<T: Into<Property>> From<Vec<T>> for Property {
    fn from(v: Vec<T>) -> Self {
        Property::List(v.into_iter().map(Into::into).collect())
    }
}
