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

/// Encode the tuple of values for a composite index spec by
/// pulling each property from `props` (a node's or edge's property
/// map) and running it through [`encode_index_value`]. Returns
/// `None` if any property is missing or carries an unindexable
/// value — composite indexes are all-or-nothing: a partial tuple
/// isn't a valid key, so the entry is omitted entirely.
pub(crate) fn encode_index_tuple(
    props: &std::collections::HashMap<String, Property>,
    keys: &[String],
) -> Option<Vec<Vec<u8>>> {
    let mut values = Vec::with_capacity(keys.len());
    for key in keys {
        let val = props.get(key)?;
        values.push(encode_index_value(val)?);
    }
    Some(values)
}

/// Full key format for a property-index entry. Property names and
/// values interleave so a partial-tuple value-prefix is a real byte
/// prefix of the full key:
///
///   `[label_len:u16][label]([prop_len:u16][prop][value_len:u32][value])^N[node_id:16]`
///
/// Length-1 inputs produce byte-identical output to the
/// pre-composite layout. Partial-prefix seeks (a length-K prefix of
/// a length-N index) use `property_index_composite_value_prefix`
/// against the same interleaved encoding.
pub(crate) fn property_index_composite_key(
    label: &str,
    prop_keys: &[&str],
    value_bytes_list: &[&[u8]],
    node: NodeId,
) -> Vec<u8> {
    build_property_index_composite_key(label, prop_keys, value_bytes_list, node.as_bytes())
}

/// Prefix that selects every index entry for a specific
/// `(label, properties..., values...)` tuple. Used by
/// `nodes_by_properties` to iterate the matching node ids.
pub(crate) fn property_index_composite_value_prefix(
    label: &str,
    prop_keys: &[&str],
    value_bytes_list: &[&[u8]],
) -> Vec<u8> {
    build_property_index_composite_value_prefix(label, prop_keys, value_bytes_list)
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

/// Extract the trailing node id from a property-index key. The id
/// is always the last `ID_LEN` bytes regardless of how much of the
/// key the matching prefix consumed — a partial-prefix seek on a
/// composite index leaves extra `[prop][value]` pairs between the
/// seek prefix and the id, and the tail-based extraction handles
/// that naturally. `prefix_len` is retained in the error path so
/// callers can still report the expected shape when a stored key
/// is too short to contain an id at all.
pub(crate) fn node_id_from_property_index_key(
    key: &[u8],
    prefix_len: usize,
) -> Result<[u8; ID_LEN]> {
    if key.len() < prefix_len + ID_LEN {
        return Err(Error::CorruptBytes {
            cf: "property_index",
            expected: prefix_len + ID_LEN,
            actual: key.len(),
        });
    }
    let mut bytes = [0u8; ID_LEN];
    bytes.copy_from_slice(&key[key.len() - ID_LEN..]);
    Ok(bytes)
}

/// Relationship-scope analogue of [`property_index_composite_key`].
/// Lives in its own column family so the iterator range scans stay
/// tight and the two index families can be dropped independently.
pub(crate) fn edge_property_index_composite_key(
    edge_type: &str,
    prop_keys: &[&str],
    value_bytes_list: &[&[u8]],
    edge: EdgeId,
) -> Vec<u8> {
    build_property_index_composite_key(edge_type, prop_keys, value_bytes_list, edge.as_bytes())
}

pub(crate) fn edge_property_index_composite_value_prefix(
    edge_type: &str,
    prop_keys: &[&str],
    value_bytes_list: &[&[u8]],
) -> Vec<u8> {
    build_property_index_composite_value_prefix(edge_type, prop_keys, value_bytes_list)
}

/// Extract the trailing edge id from an edge-property-index key whose
/// prefix length is already known. Mirror of
/// [`node_id_from_property_index_key`] for the relationship side.
/// Uses the same tail-based extraction so partial-prefix seeks work
/// for composite edge indexes (once the edge-scoped planner learns
/// to emit them).
pub(crate) fn edge_id_from_property_index_key(
    key: &[u8],
    prefix_len: usize,
) -> Result<[u8; ID_LEN]> {
    if key.len() < prefix_len + ID_LEN {
        return Err(Error::CorruptBytes {
            cf: "edge_property_index",
            expected: prefix_len + ID_LEN,
            actual: key.len(),
        });
    }
    let mut bytes = [0u8; ID_LEN];
    bytes.copy_from_slice(&key[key.len() - ID_LEN..]);
    Ok(bytes)
}

/// Composite key layout used by both node and edge property
/// indexes. Property names and values interleave so a partial-tuple
/// value-prefix is a real byte prefix of the full key:
///
///   `[name_len:u16][name]([prop_len:u16][prop][value_len:u32][value])^N[id]`
///
/// Length-1 inputs produce byte-identical output to the
/// pre-composite layout — the interleaved form is equivalent to
/// the old `[name][prop][value][id]` when N=1.
fn build_property_index_composite_key(
    name: &str,
    prop_keys: &[&str],
    value_bytes_list: &[&[u8]],
    id_bytes: &[u8; ID_LEN],
) -> Vec<u8> {
    debug_assert_eq!(
        prop_keys.len(),
        value_bytes_list.len(),
        "composite index key: props and values must have equal length"
    );
    let name_bytes = name.as_bytes();
    let name_len = u16::try_from(name_bytes.len()).expect("label/type length fits in u16");
    let props_total: usize = prop_keys.iter().map(|p| p.len()).sum();
    let values_total: usize = value_bytes_list.iter().map(|v| v.len()).sum();
    let mut key = Vec::with_capacity(
        LEN_PREFIX
            + name_bytes.len()
            + LEN_PREFIX * prop_keys.len()
            + props_total
            + 4 * value_bytes_list.len()
            + values_total
            + ID_LEN,
    );
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name_bytes);
    for (prop, value) in prop_keys.iter().zip(value_bytes_list.iter()) {
        let prop_bytes = prop.as_bytes();
        let prop_len = u16::try_from(prop_bytes.len()).expect("prop key length fits in u16");
        key.extend_from_slice(&prop_len.to_be_bytes());
        key.extend_from_slice(prop_bytes);
        let value_len = u32::try_from(value.len()).expect("value length fits in u32");
        key.extend_from_slice(&value_len.to_be_bytes());
        key.extend_from_slice(value);
    }
    key.extend_from_slice(id_bytes);
    key
}

/// Prefix selecting every index entry that matches
/// `[name][prop1][val1]...[propK][valK]` where `K` equals the input
/// slice lengths. A length-K prefix of a composite index's property
/// list is a real byte prefix of the full key, so partial-tuple
/// seeks use this directly with `iterator_cf(From(prefix, Forward))`.
fn build_property_index_composite_value_prefix(
    name: &str,
    prop_keys: &[&str],
    value_bytes_list: &[&[u8]],
) -> Vec<u8> {
    debug_assert_eq!(prop_keys.len(), value_bytes_list.len());
    let name_bytes = name.as_bytes();
    let name_len = u16::try_from(name_bytes.len()).expect("label/type length fits in u16");
    let props_total: usize = prop_keys.iter().map(|p| p.len()).sum();
    let values_total: usize = value_bytes_list.iter().map(|v| v.len()).sum();
    let mut key = Vec::with_capacity(
        LEN_PREFIX
            + name_bytes.len()
            + LEN_PREFIX * prop_keys.len()
            + props_total
            + 4 * value_bytes_list.len()
            + values_total,
    );
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name_bytes);
    for (prop, value) in prop_keys.iter().zip(value_bytes_list.iter()) {
        let prop_bytes = prop.as_bytes();
        let prop_len = u16::try_from(prop_bytes.len()).expect("prop key length fits in u16");
        key.extend_from_slice(&prop_len.to_be_bytes());
        key.extend_from_slice(prop_bytes);
        let value_len = u32::try_from(value.len()).expect("value length fits in u32");
        key.extend_from_slice(&value_len.to_be_bytes());
        key.extend_from_slice(value);
    }
    key
}

/// Prefix covering every property-index entry scoped to `name`
/// (label for node indexes, edge type for edge indexes), across all
/// properties and values. Used by `DROP INDEX` to sweep every entry
/// whose parsed spec matches — with the interleaved key layout a
/// single-property name-prefix like `[name][prop1]` could collide
/// with composite indexes whose first property is `prop1`, so DROP
/// iterates the broader `[name]` prefix and filters by spec.
pub(crate) fn property_index_label_prefix(name: &str) -> Vec<u8> {
    let name_bytes = name.as_bytes();
    let name_len = u16::try_from(name_bytes.len()).expect("label/type length fits in u16");
    let mut key = Vec::with_capacity(LEN_PREFIX + name_bytes.len());
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name_bytes);
    key
}

/// Parse the property-name sequence from an index-CF entry so
/// callers can decide whether the entry was produced by a given
/// spec. Returns `Some(props)` when the key decodes cleanly, or
/// `None` when the bytes don't look like a well-formed entry —
/// the DROP path treats undecodable entries as "not my spec" and
/// leaves them alone rather than aborting.
pub(crate) fn parse_property_index_entry_props(key: &[u8]) -> Option<Vec<String>> {
    if key.len() < LEN_PREFIX + ID_LEN {
        return None;
    }
    let name_len = u16::from_be_bytes([key[0], key[1]]) as usize;
    let mut cursor = LEN_PREFIX + name_len;
    if cursor + ID_LEN > key.len() {
        return None;
    }
    let payload_end = key.len() - ID_LEN;
    let mut props: Vec<String> = Vec::new();
    while cursor < payload_end {
        if cursor + LEN_PREFIX > payload_end {
            return None;
        }
        let prop_len = u16::from_be_bytes([key[cursor], key[cursor + 1]]) as usize;
        cursor += LEN_PREFIX;
        if cursor + prop_len > payload_end {
            return None;
        }
        let prop = std::str::from_utf8(&key[cursor..cursor + prop_len]).ok()?;
        props.push(prop.to_string());
        cursor += prop_len;
        // Skip over the value segment: [value_len:u32][value_bytes].
        if cursor + 4 > payload_end {
            return None;
        }
        let value_len = u32::from_be_bytes([
            key[cursor],
            key[cursor + 1],
            key[cursor + 2],
            key[cursor + 3],
        ]) as usize;
        cursor += 4;
        if cursor + value_len > payload_end {
            return None;
        }
        cursor += value_len;
    }
    if cursor != payload_end {
        return None;
    }
    Some(props)
}
