use crate::error::{Error, Result};
use mesh_core::{EdgeId, NodeId, Property};

pub(crate) const ID_LEN: usize = 16;
pub(crate) const ADJ_KEY_LEN: usize = 32;
const LEN_PREFIX: usize = 2;

/// Type tags for index-value encoding. Chosen so that keys for different
/// property types can never alias even when their byte widths happen to
/// match. Kept narrow on purpose — Float64/List/Map are rejected at the
/// index layer because equality on them is either unsound (Float) or
/// ill-defined (List, Map).
pub(crate) const INDEX_VALUE_STRING: u8 = 0x01;
pub(crate) const INDEX_VALUE_INT: u8 = 0x02;
pub(crate) const INDEX_VALUE_BOOL: u8 = 0x03;
pub(crate) const INDEX_VALUE_DATETIME: u8 = 0x04;
pub(crate) const INDEX_VALUE_DATE: u8 = 0x05;

pub(crate) fn adj_key(node: NodeId, edge: EdgeId) -> [u8; ADJ_KEY_LEN] {
    let mut key = [0u8; ADJ_KEY_LEN];
    key[..ID_LEN].copy_from_slice(node.as_bytes());
    key[ID_LEN..].copy_from_slice(edge.as_bytes());
    key
}

pub(crate) fn edge_from_adj_key(cf: &'static str, key: &[u8]) -> Result<EdgeId> {
    if key.len() != ADJ_KEY_LEN {
        return Err(Error::CorruptBytes {
            cf,
            expected: ADJ_KEY_LEN,
            actual: key.len(),
        });
    }
    let mut bytes = [0u8; ID_LEN];
    bytes.copy_from_slice(&key[ID_LEN..]);
    Ok(EdgeId::from_bytes(bytes))
}

pub(crate) fn node_from_adj_value(cf: &'static str, value: &[u8]) -> Result<NodeId> {
    if value.len() != ID_LEN {
        return Err(Error::CorruptBytes {
            cf,
            expected: ID_LEN,
            actual: value.len(),
        });
    }
    let mut bytes = [0u8; ID_LEN];
    bytes.copy_from_slice(value);
    Ok(NodeId::from_bytes(bytes))
}

pub(crate) fn label_index_key(label: &str, node: NodeId) -> Vec<u8> {
    make_str_id_key(label, node.as_bytes())
}

pub(crate) fn label_index_prefix(label: &str) -> Vec<u8> {
    make_str_prefix(label)
}

pub(crate) fn type_index_key(edge_type: &str, edge: EdgeId) -> Vec<u8> {
    make_str_id_key(edge_type, edge.as_bytes())
}

pub(crate) fn type_index_prefix(edge_type: &str) -> Vec<u8> {
    make_str_prefix(edge_type)
}

pub(crate) fn id_from_str_index_key(
    cf: &'static str,
    key: &[u8],
    str_len: usize,
) -> Result<[u8; ID_LEN]> {
    let expected = LEN_PREFIX + str_len + ID_LEN;
    if key.len() != expected {
        return Err(Error::CorruptBytes {
            cf,
            expected,
            actual: key.len(),
        });
    }
    let mut bytes = [0u8; ID_LEN];
    bytes.copy_from_slice(&key[LEN_PREFIX + str_len..]);
    Ok(bytes)
}

fn make_str_prefix(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let len = u16::try_from(bytes.len()).expect("string length fits in u16");
    let mut key = Vec::with_capacity(LEN_PREFIX + bytes.len());
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(bytes);
    key
}

fn make_str_id_key(s: &str, id: &[u8; ID_LEN]) -> Vec<u8> {
    let bytes = s.as_bytes();
    let len = u16::try_from(bytes.len()).expect("string length fits in u16");
    let mut key = Vec::with_capacity(LEN_PREFIX + bytes.len() + ID_LEN);
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(bytes);
    key.extend_from_slice(id);
    key
}

/// Encode a [`Property`] into the canonical index-value byte form.
/// The first byte is a type tag, so a String `"42"` and an Int64 `42`
/// produce disjoint prefixes and can coexist under the same
/// `(label, prop)` in the index without aliasing.
///
/// Returns `None` for types the index layer refuses to store:
/// `Float64` (equality on f64 is a footgun), `List`, `Map`, and the
/// logical "no value" case `Null` (callers should simply not index a
/// missing property).
pub(crate) fn encode_index_value(value: &Property) -> Option<Vec<u8>> {
    match value {
        Property::String(s) => {
            let mut out = Vec::with_capacity(1 + s.len());
            out.push(INDEX_VALUE_STRING);
            out.extend_from_slice(s.as_bytes());
            Some(out)
        }
        Property::Int64(n) => {
            let mut out = Vec::with_capacity(1 + 8);
            out.push(INDEX_VALUE_INT);
            out.extend_from_slice(&n.to_be_bytes());
            Some(out)
        }
        Property::Bool(b) => Some(vec![INDEX_VALUE_BOOL, u8::from(*b)]),
        Property::DateTime { nanos, .. } => {
            // Big-endian epoch-nanos so lexicographic byte order
            // matches temporal order. Timezone offset is not part of
            // the index key: two datetimes at the same instant compare
            // equal regardless of their presentation offset.
            let mut out = Vec::with_capacity(1 + 16);
            out.push(INDEX_VALUE_DATETIME);
            out.extend_from_slice(&nanos.to_be_bytes());
            Some(out)
        }
        Property::Date(days) => {
            let mut out = Vec::with_capacity(1 + 4);
            out.push(INDEX_VALUE_DATE);
            out.extend_from_slice(&days.to_be_bytes());
            Some(out)
        }
        // Duration has no total ordering (3 months = ? days, depends
        // on the reference date), so we refuse to index it — matches
        // how Neo4j treats `Duration` in schema indexes.
        Property::Duration(_) => None,
        Property::Float64(_) | Property::List(_) | Property::Map(_) | Property::Null
        | Property::Time { .. } | Property::LocalDateTime(_) => None,
    }
}

/// Full key format for a property-index entry:
///
///   `[label_len:u16][label][prop_len:u16][prop][value_len:u32][value][node_id:16]`
///
/// The label + prop prefix lets us scan all entries for a specific
/// index with a single `iterator_cf(From(prefix, Forward))` call, and
/// the trailing node id lets the scan emit matching nodes in one pass.
pub(crate) fn property_index_key(
    label: &str,
    prop_key: &str,
    value_bytes: &[u8],
    node: NodeId,
) -> Vec<u8> {
    let label_bytes = label.as_bytes();
    let prop_bytes = prop_key.as_bytes();
    let label_len = u16::try_from(label_bytes.len()).expect("label length fits in u16");
    let prop_len = u16::try_from(prop_bytes.len()).expect("prop key length fits in u16");
    let value_len = u32::try_from(value_bytes.len()).expect("value length fits in u32");
    let mut key = Vec::with_capacity(
        LEN_PREFIX
            + label_bytes.len()
            + LEN_PREFIX
            + prop_bytes.len()
            + 4
            + value_bytes.len()
            + ID_LEN,
    );
    key.extend_from_slice(&label_len.to_be_bytes());
    key.extend_from_slice(label_bytes);
    key.extend_from_slice(&prop_len.to_be_bytes());
    key.extend_from_slice(prop_bytes);
    key.extend_from_slice(&value_len.to_be_bytes());
    key.extend_from_slice(value_bytes);
    key.extend_from_slice(node.as_bytes());
    key
}

/// Prefix that selects every index entry for a specific
/// `(label, prop, value)` triple. Used by `nodes_by_property` to
/// iterate the matching node ids.
pub(crate) fn property_index_value_prefix(
    label: &str,
    prop_key: &str,
    value_bytes: &[u8],
) -> Vec<u8> {
    let label_bytes = label.as_bytes();
    let prop_bytes = prop_key.as_bytes();
    let label_len = u16::try_from(label_bytes.len()).expect("label length fits in u16");
    let prop_len = u16::try_from(prop_bytes.len()).expect("prop key length fits in u16");
    let value_len = u32::try_from(value_bytes.len()).expect("value length fits in u32");
    let mut key = Vec::with_capacity(
        LEN_PREFIX + label_bytes.len() + LEN_PREFIX + prop_bytes.len() + 4 + value_bytes.len(),
    );
    key.extend_from_slice(&label_len.to_be_bytes());
    key.extend_from_slice(label_bytes);
    key.extend_from_slice(&prop_len.to_be_bytes());
    key.extend_from_slice(prop_bytes);
    key.extend_from_slice(&value_len.to_be_bytes());
    key.extend_from_slice(value_bytes);
    key
}

/// Prefix covering every entry for a specific `(label, prop)` index,
/// across all values. Used by `drop_property_index` to sweep the CF.
pub(crate) fn property_index_name_prefix(label: &str, prop_key: &str) -> Vec<u8> {
    let label_bytes = label.as_bytes();
    let prop_bytes = prop_key.as_bytes();
    let label_len = u16::try_from(label_bytes.len()).expect("label length fits in u16");
    let prop_len = u16::try_from(prop_bytes.len()).expect("prop key length fits in u16");
    let mut key =
        Vec::with_capacity(LEN_PREFIX + label_bytes.len() + LEN_PREFIX + prop_bytes.len());
    key.extend_from_slice(&label_len.to_be_bytes());
    key.extend_from_slice(label_bytes);
    key.extend_from_slice(&prop_len.to_be_bytes());
    key.extend_from_slice(prop_bytes);
    key
}

/// Extract the trailing node id from a property-index key whose
/// prefix length (label+prop+value) is already known. Used by the
/// `nodes_by_property` scan path.
pub(crate) fn node_id_from_property_index_key(
    key: &[u8],
    prefix_len: usize,
) -> Result<[u8; ID_LEN]> {
    let expected = prefix_len + ID_LEN;
    if key.len() != expected {
        return Err(Error::CorruptBytes {
            cf: "property_index",
            expected,
            actual: key.len(),
        });
    }
    let mut bytes = [0u8; ID_LEN];
    bytes.copy_from_slice(&key[prefix_len..]);
    Ok(bytes)
}
