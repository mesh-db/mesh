/// The set of values that can appear inside a Bolt message, including the
/// graph-specific `Struct` variant (Nodes, Relationships, etc. are just
/// PackStream structs with a well-known tag byte).
///
/// Maps keep insertion order because Bolt message metadata (`SUCCESS`
/// fields, `RUN` extras) depends on it.
#[derive(Debug, Clone, PartialEq)]
pub enum BoltValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<BoltValue>),
    Map(Vec<(String, BoltValue)>),
    Struct { tag: u8, fields: Vec<BoltValue> },
}

impl BoltValue {
    /// Convenience builder for the common `{"key": value, ...}` pattern
    /// used in Bolt message metadata.
    pub fn map<I, S>(entries: I) -> Self
    where
        I: IntoIterator<Item = (S, BoltValue)>,
        S: Into<String>,
    {
        BoltValue::Map(entries.into_iter().map(|(k, v)| (k.into(), v)).collect())
    }

    /// Look up a map entry by key. Returns `None` on non-maps or missing
    /// keys; never panics.
    pub fn get(&self, key: &str) -> Option<&BoltValue> {
        match self {
            BoltValue::Map(entries) => entries.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            BoltValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            BoltValue::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            BoltValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[BoltValue]> {
        match self {
            BoltValue::List(items) => Some(items.as_slice()),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&[(String, BoltValue)]> {
        match self {
            BoltValue::Map(entries) => Some(entries.as_slice()),
            _ => None,
        }
    }
}
