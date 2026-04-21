use crate::engine::{
    ConstraintScope, PropertyConstraintKind, PropertyConstraintSpec, PropertyType,
};
use crate::error::{Error, Result};
use meshdb_core::{EdgeId, NodeId, Property};

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
        Property::Float64(_)
        | Property::List(_)
        | Property::Map(_)
        | Property::Null
        | Property::Time { .. }
        | Property::LocalDateTime(_)
        | Property::Point(_) => None,
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
    build_property_index_key(label, prop_key, value_bytes, node.as_bytes())
}

/// Prefix that selects every index entry for a specific
/// `(label, prop, value)` triple. Used by `nodes_by_property` to
/// iterate the matching node ids.
pub(crate) fn property_index_value_prefix(
    label: &str,
    prop_key: &str,
    value_bytes: &[u8],
) -> Vec<u8> {
    build_property_index_value_prefix(label, prop_key, value_bytes)
}

/// Prefix covering every entry for a specific `(label, prop)` index,
/// across all values. Used by `drop_property_index` to sweep the CF.
pub(crate) fn property_index_name_prefix(label: &str, prop_key: &str) -> Vec<u8> {
    build_property_index_name_prefix(label, prop_key)
}

/// Type tags for constraint-kind encoding in `CF_CONSTRAINT_META`.
/// Chosen so future kinds can extend without renumbering. The tags are
/// part of the on-disk format — do NOT renumber without a migration.
pub(crate) const CONSTRAINT_KIND_UNIQUE: u8 = 0x01;
pub(crate) const CONSTRAINT_KIND_NOT_NULL: u8 = 0x02;
pub(crate) const CONSTRAINT_KIND_PROPERTY_TYPE: u8 = 0x03;
pub(crate) const CONSTRAINT_KIND_NODE_KEY: u8 = 0x04;

/// Sub-tags identifying the target type of a `PROPERTY_TYPE` kind.
/// Only meaningful when the outer tag is `CONSTRAINT_KIND_PROPERTY_TYPE`.
pub(crate) const PROPERTY_TYPE_STRING: u8 = 0x01;
pub(crate) const PROPERTY_TYPE_INTEGER: u8 = 0x02;
pub(crate) const PROPERTY_TYPE_FLOAT: u8 = 0x03;
pub(crate) const PROPERTY_TYPE_BOOLEAN: u8 = 0x04;

/// Scope tags for constraint meta. `NODE` predates
/// `RELATIONSHIP` — the original format stored only a label,
/// implicitly node-scoped; the new format prefixes every entry with a
/// scope tag so on-disk reads can tell the two apart. Old entries
/// written before this tag existed aren't automatically readable —
/// users on pre-relationship builds will need to re-declare their
/// constraints after upgrading.
pub(crate) const SCOPE_NODE: u8 = 0x01;
pub(crate) const SCOPE_RELATIONSHIP: u8 = 0x02;

/// Encode a constraint spec's value bytes for `CF_CONSTRAINT_META`.
/// Layout:
///   `[kind:u8][?type:u8][scope:u8][target_len:u16][target][prop_count:u16]([prop_len:u16][prop])*`
/// The `type` byte is only emitted when `kind == PROPERTY_TYPE`;
/// the property list is always length-prefixed with a count and
/// per-entry length so `NodeKey`'s composite tuple round-trips. The
/// scope byte distinguishes node-label from relationship-type targets.
pub(crate) fn constraint_meta_encode(spec: &PropertyConstraintSpec) -> Vec<u8> {
    let target_bytes = spec.scope.target().as_bytes();
    let target_len = u16::try_from(target_bytes.len()).expect("scope target length fits in u16");
    let prop_count = u16::try_from(spec.properties.len()).expect("property count fits in u16");
    let props_total: usize = spec.properties.iter().map(|p| p.len()).sum();
    let mut out = Vec::with_capacity(
        3 + LEN_PREFIX
            + target_bytes.len()
            + LEN_PREFIX
            + props_total
            + LEN_PREFIX * spec.properties.len(),
    );
    match spec.kind {
        PropertyConstraintKind::Unique => out.push(CONSTRAINT_KIND_UNIQUE),
        PropertyConstraintKind::NotNull => out.push(CONSTRAINT_KIND_NOT_NULL),
        PropertyConstraintKind::PropertyType(t) => {
            out.push(CONSTRAINT_KIND_PROPERTY_TYPE);
            out.push(property_type_tag(t));
        }
        PropertyConstraintKind::NodeKey => out.push(CONSTRAINT_KIND_NODE_KEY),
    }
    out.push(match spec.scope {
        ConstraintScope::Node(_) => SCOPE_NODE,
        ConstraintScope::Relationship(_) => SCOPE_RELATIONSHIP,
    });
    out.extend_from_slice(&target_len.to_be_bytes());
    out.extend_from_slice(target_bytes);
    out.extend_from_slice(&prop_count.to_be_bytes());
    for prop in &spec.properties {
        let bytes = prop.as_bytes();
        let len = u16::try_from(bytes.len()).expect("property length fits in u16");
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(bytes);
    }
    out
}

fn property_type_tag(t: PropertyType) -> u8 {
    match t {
        PropertyType::String => PROPERTY_TYPE_STRING,
        PropertyType::Integer => PROPERTY_TYPE_INTEGER,
        PropertyType::Float => PROPERTY_TYPE_FLOAT,
        PropertyType::Boolean => PROPERTY_TYPE_BOOLEAN,
    }
}

fn property_type_from_tag(cf: &'static str, tag: u8) -> Result<PropertyType> {
    match tag {
        PROPERTY_TYPE_STRING => Ok(PropertyType::String),
        PROPERTY_TYPE_INTEGER => Ok(PropertyType::Integer),
        PROPERTY_TYPE_FLOAT => Ok(PropertyType::Float),
        PROPERTY_TYPE_BOOLEAN => Ok(PropertyType::Boolean),
        _ => Err(Error::CorruptBytes {
            cf,
            expected: 1,
            actual: 1,
        }),
    }
}

/// Decode a constraint-meta value back into a [`PropertyConstraintSpec`].
/// The name is passed in by the caller because it lives in the key,
/// not the value. Corrupt bytes surface as `Error::CorruptBytes` so
/// the caller can report the offending CF.
pub(crate) fn constraint_meta_decode(
    cf: &'static str,
    name: String,
    bytes: &[u8],
) -> Result<PropertyConstraintSpec> {
    if bytes.is_empty() {
        return Err(Error::CorruptBytes {
            cf,
            expected: 1,
            actual: 0,
        });
    }
    let (kind, kind_header_len) = match bytes[0] {
        CONSTRAINT_KIND_UNIQUE => (PropertyConstraintKind::Unique, 1usize),
        CONSTRAINT_KIND_NOT_NULL => (PropertyConstraintKind::NotNull, 1usize),
        CONSTRAINT_KIND_PROPERTY_TYPE => {
            if bytes.len() < 2 {
                return Err(Error::CorruptBytes {
                    cf,
                    expected: 2,
                    actual: bytes.len(),
                });
            }
            let t = property_type_from_tag(cf, bytes[1])?;
            (PropertyConstraintKind::PropertyType(t), 2usize)
        }
        CONSTRAINT_KIND_NODE_KEY => (PropertyConstraintKind::NodeKey, 1usize),
        _ => {
            return Err(Error::CorruptBytes {
                cf,
                expected: 1,
                actual: bytes.len(),
            });
        }
    };
    let mut cursor = kind_header_len;
    if cursor >= bytes.len() {
        return Err(Error::CorruptBytes {
            cf,
            expected: cursor + 1,
            actual: bytes.len(),
        });
    }
    let scope_tag = bytes[cursor];
    cursor += 1;
    let target = read_len_prefixed_string(cf, bytes, &mut cursor)?;
    let scope = match scope_tag {
        SCOPE_NODE => ConstraintScope::Node(target),
        SCOPE_RELATIONSHIP => ConstraintScope::Relationship(target),
        _ => {
            return Err(Error::CorruptBytes {
                cf,
                expected: 1,
                actual: 1,
            });
        }
    };
    // Property list: u16 count followed by `count` length-prefixed
    // strings. Zero-count is accepted by the decoder but the storage
    // layer rejects it at create time — keeping the decode side
    // permissive lets future kinds with no properties slot in without
    // a format bump.
    if cursor + LEN_PREFIX > bytes.len() {
        return Err(Error::CorruptBytes {
            cf,
            expected: cursor + LEN_PREFIX,
            actual: bytes.len(),
        });
    }
    let prop_count = u16::from_be_bytes([bytes[cursor], bytes[cursor + 1]]) as usize;
    cursor += LEN_PREFIX;
    let mut properties = Vec::with_capacity(prop_count);
    for _ in 0..prop_count {
        properties.push(read_len_prefixed_string(cf, bytes, &mut cursor)?);
    }
    if cursor != bytes.len() {
        return Err(Error::CorruptBytes {
            cf,
            expected: cursor,
            actual: bytes.len(),
        });
    }
    Ok(PropertyConstraintSpec {
        name,
        scope,
        properties,
        kind,
    })
}

fn read_len_prefixed_string(cf: &'static str, bytes: &[u8], cursor: &mut usize) -> Result<String> {
    if *cursor + LEN_PREFIX > bytes.len() {
        return Err(Error::CorruptBytes {
            cf,
            expected: *cursor + LEN_PREFIX,
            actual: bytes.len(),
        });
    }
    let len = u16::from_be_bytes([bytes[*cursor], bytes[*cursor + 1]]) as usize;
    *cursor += LEN_PREFIX;
    if *cursor + len > bytes.len() {
        return Err(Error::CorruptBytes {
            cf,
            expected: *cursor + len,
            actual: bytes.len(),
        });
    }
    let s = std::str::from_utf8(&bytes[*cursor..*cursor + len])
        .map_err(|_| Error::CorruptBytes {
            cf,
            expected: len,
            actual: len,
        })?
        .to_string();
    *cursor += len;
    Ok(s)
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

/// Full key format for an edge-property-index entry:
///
///   `[type_len:u16][edge_type][prop_len:u16][prop][value_len:u32][value][edge_id:16]`
///
/// Byte-identical to [`property_index_key`] modulo the trailing id
/// interpretation. Lives in its own column family so the iterator
/// range scans stay tight and the two index families can be dropped
/// independently.
pub(crate) fn edge_property_index_key(
    edge_type: &str,
    prop_key: &str,
    value_bytes: &[u8],
    edge: EdgeId,
) -> Vec<u8> {
    build_property_index_key(edge_type, prop_key, value_bytes, edge.as_bytes())
}

/// Prefix selecting every entry for a specific `(edge_type, prop, value)`
/// triple. Used by `edges_by_property` to iterate the matching edge ids.
pub(crate) fn edge_property_index_value_prefix(
    edge_type: &str,
    prop_key: &str,
    value_bytes: &[u8],
) -> Vec<u8> {
    build_property_index_value_prefix(edge_type, prop_key, value_bytes)
}

/// Prefix covering every entry for a specific `(edge_type, prop)` edge
/// index, across all values. Used by `drop_edge_property_index` to
/// sweep the CF.
pub(crate) fn edge_property_index_name_prefix(edge_type: &str, prop_key: &str) -> Vec<u8> {
    build_property_index_name_prefix(edge_type, prop_key)
}

/// Extract the trailing edge id from an edge-property-index key whose
/// prefix length is already known. Mirror of
/// [`node_id_from_property_index_key`] for the relationship side.
pub(crate) fn edge_id_from_property_index_key(
    key: &[u8],
    prefix_len: usize,
) -> Result<[u8; ID_LEN]> {
    let expected = prefix_len + ID_LEN;
    if key.len() != expected {
        return Err(Error::CorruptBytes {
            cf: "edge_property_index",
            expected,
            actual: key.len(),
        });
    }
    let mut bytes = [0u8; ID_LEN];
    bytes.copy_from_slice(&key[prefix_len..]);
    Ok(bytes)
}

fn build_property_index_key(
    name: &str,
    prop_key: &str,
    value_bytes: &[u8],
    id_bytes: &[u8; ID_LEN],
) -> Vec<u8> {
    let name_bytes = name.as_bytes();
    let prop_bytes = prop_key.as_bytes();
    let name_len = u16::try_from(name_bytes.len()).expect("label/type length fits in u16");
    let prop_len = u16::try_from(prop_bytes.len()).expect("prop key length fits in u16");
    let value_len = u32::try_from(value_bytes.len()).expect("value length fits in u32");
    let mut key = Vec::with_capacity(
        LEN_PREFIX
            + name_bytes.len()
            + LEN_PREFIX
            + prop_bytes.len()
            + 4
            + value_bytes.len()
            + ID_LEN,
    );
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name_bytes);
    key.extend_from_slice(&prop_len.to_be_bytes());
    key.extend_from_slice(prop_bytes);
    key.extend_from_slice(&value_len.to_be_bytes());
    key.extend_from_slice(value_bytes);
    key.extend_from_slice(id_bytes);
    key
}

fn build_property_index_value_prefix(name: &str, prop_key: &str, value_bytes: &[u8]) -> Vec<u8> {
    let name_bytes = name.as_bytes();
    let prop_bytes = prop_key.as_bytes();
    let name_len = u16::try_from(name_bytes.len()).expect("label/type length fits in u16");
    let prop_len = u16::try_from(prop_bytes.len()).expect("prop key length fits in u16");
    let value_len = u32::try_from(value_bytes.len()).expect("value length fits in u32");
    let mut key = Vec::with_capacity(
        LEN_PREFIX + name_bytes.len() + LEN_PREFIX + prop_bytes.len() + 4 + value_bytes.len(),
    );
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name_bytes);
    key.extend_from_slice(&prop_len.to_be_bytes());
    key.extend_from_slice(prop_bytes);
    key.extend_from_slice(&value_len.to_be_bytes());
    key.extend_from_slice(value_bytes);
    key
}

fn build_property_index_name_prefix(name: &str, prop_key: &str) -> Vec<u8> {
    let name_bytes = name.as_bytes();
    let prop_bytes = prop_key.as_bytes();
    let name_len = u16::try_from(name_bytes.len()).expect("label/type length fits in u16");
    let prop_len = u16::try_from(prop_bytes.len()).expect("prop key length fits in u16");
    let mut key = Vec::with_capacity(LEN_PREFIX + name_bytes.len() + LEN_PREFIX + prop_bytes.len());
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name_bytes);
    key.extend_from_slice(&prop_len.to_be_bytes());
    key.extend_from_slice(prop_bytes);
    key
}
