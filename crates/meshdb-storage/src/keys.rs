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
pub(crate) fn property_index_composite_key<P, V>(
    label: &str,
    prop_keys: &[P],
    value_bytes_list: &[V],
    node: NodeId,
) -> Vec<u8>
where
    P: AsRef<str>,
    V: AsRef<[u8]>,
{
    build_property_index_composite_key(label, prop_keys, value_bytes_list, node.as_bytes())
}

/// Prefix that selects every index entry for a specific
/// `(label, properties..., values...)` tuple. Used by
/// `nodes_by_properties` to iterate the matching node ids.
pub(crate) fn property_index_composite_value_prefix<P, V>(
    label: &str,
    prop_keys: &[P],
    value_bytes_list: &[V],
) -> Vec<u8>
where
    P: AsRef<str>,
    V: AsRef<[u8]>,
{
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
pub(crate) fn edge_property_index_composite_key<P, V>(
    edge_type: &str,
    prop_keys: &[P],
    value_bytes_list: &[V],
    edge: EdgeId,
) -> Vec<u8>
where
    P: AsRef<str>,
    V: AsRef<[u8]>,
{
    build_property_index_composite_key(edge_type, prop_keys, value_bytes_list, edge.as_bytes())
}

pub(crate) fn edge_property_index_composite_value_prefix<P, V>(
    edge_type: &str,
    prop_keys: &[P],
    value_bytes_list: &[V],
) -> Vec<u8>
where
    P: AsRef<str>,
    V: AsRef<[u8]>,
{
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
fn build_property_index_composite_key<P, V>(
    name: &str,
    prop_keys: &[P],
    value_bytes_list: &[V],
    id_bytes: &[u8; ID_LEN],
) -> Vec<u8>
where
    P: AsRef<str>,
    V: AsRef<[u8]>,
{
    debug_assert_eq!(
        prop_keys.len(),
        value_bytes_list.len(),
        "composite index key: props and values must have equal length"
    );
    let name_bytes = name.as_bytes();
    let name_len = u16::try_from(name_bytes.len()).expect("label/type length fits in u16");
    let props_total: usize = prop_keys.iter().map(|p| p.as_ref().len()).sum();
    let values_total: usize = value_bytes_list.iter().map(|v| v.as_ref().len()).sum();
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
        let prop_bytes = prop.as_ref().as_bytes();
        let prop_len = u16::try_from(prop_bytes.len()).expect("prop key length fits in u16");
        key.extend_from_slice(&prop_len.to_be_bytes());
        key.extend_from_slice(prop_bytes);
        let value_bytes = value.as_ref();
        let value_len = u32::try_from(value_bytes.len()).expect("value length fits in u32");
        key.extend_from_slice(&value_len.to_be_bytes());
        key.extend_from_slice(value_bytes);
    }
    key.extend_from_slice(id_bytes);
    key
}

/// Prefix selecting every index entry that matches
/// `[name][prop1][val1]...[propK][valK]` where `K` equals the input
/// slice lengths. A length-K prefix of a composite index's property
/// list is a real byte prefix of the full key, so partial-tuple
/// seeks use this directly with `iterator_cf(From(prefix, Forward))`.
fn build_property_index_composite_value_prefix<P, V>(
    name: &str,
    prop_keys: &[P],
    value_bytes_list: &[V],
) -> Vec<u8>
where
    P: AsRef<str>,
    V: AsRef<[u8]>,
{
    debug_assert_eq!(prop_keys.len(), value_bytes_list.len());
    let name_bytes = name.as_bytes();
    let name_len = u16::try_from(name_bytes.len()).expect("label/type length fits in u16");
    let props_total: usize = prop_keys.iter().map(|p| p.as_ref().len()).sum();
    let values_total: usize = value_bytes_list.iter().map(|v| v.as_ref().len()).sum();
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
        let prop_bytes = prop.as_ref().as_bytes();
        let prop_len = u16::try_from(prop_bytes.len()).expect("prop key length fits in u16");
        key.extend_from_slice(&prop_len.to_be_bytes());
        key.extend_from_slice(prop_bytes);
        let value_bytes = value.as_ref();
        let value_len = u32::try_from(value_bytes.len()).expect("value length fits in u32");
        key.extend_from_slice(&value_len.to_be_bytes());
        key.extend_from_slice(value_bytes);
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

// ---------------------------------------------------------------
// Point index — Z-order (Morton) cell keys.
//
// Layout:
//   `<label>\0<property>\0<srid:i32 BE><cell:u64 BE><node_id:16>`
//
// The label/property NUL-split header mirrors the property-index
// meta format. The `srid` is folded into the key (not just the
// metadata) so a bbox scan over one CRS can't accidentally sweep
// points tagged with a different CRS — the i32 BE prefix confines
// each SRID's entries to their own sorted neighbourhood.
//
// `cell` is the Morton code produced by `point_cell()`. Its position
// between the SRID and the node_id means rows under one SRID sort in
// Z-order, which is what makes a bbox → cell-range scan work.
//
// The stored value is a small fixed-shape record produced by
// `point_index_value()` — `[has_z: u8][x: f64 BE 8][y: f64 BE 8]
// [z: f64 BE 8]?`. Keeping coords in the value lets the per-row
// precision filter run without fetching the full node.
// ---------------------------------------------------------------

const POINT_INDEX_CELL_LEN: usize = 8;

/// Per-SRID spatial domain for the Morton quantizer, as
/// `(xmin, xmax, ymin, ymax)`. Geographic CRSs have fixed bounds
/// (lon/lat); Cartesian gets a generous symmetric window — points
/// outside it clamp to the edge and still get an index entry, just
/// one at the boundary cell (the precision filter on stored coords
/// still accepts the real value on lookup).
fn point_domain(srid: i32) -> (f64, f64, f64, f64) {
    match srid {
        // WGS-84 2D / 3D — longitude, latitude.
        4326 | 4979 => (-180.0, 180.0, -90.0, 90.0),
        // Cartesian and unknown SRIDs share one generous window.
        _ => (-1.0e9, 1.0e9, -1.0e9, 1.0e9),
    }
}

fn normalize_axis(v: f64, lo: f64, hi: f64) -> u32 {
    let t = ((v - lo) / (hi - lo)).clamp(0.0, 1.0);
    // `u32::MAX as f64` rounds to 2^32 (one past the max u32) because
    // f64 can't represent 2^32 - 1 exactly. Clamp on the way back.
    let scaled = (t * (u32::MAX as f64)).round();
    if scaled >= u32::MAX as f64 {
        u32::MAX
    } else {
        scaled as u32
    }
}

/// Interleave the bits of two u32s into a single u64. Standard
/// "Magic bits" expansion — x goes to even positions, y to odd.
fn interleave_u32(x: u32, y: u32) -> u64 {
    fn spread(v: u32) -> u64 {
        let mut v = v as u64;
        v = (v | (v << 16)) & 0x0000_FFFF_0000_FFFF;
        v = (v | (v << 8)) & 0x00FF_00FF_00FF_00FF;
        v = (v | (v << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
        v = (v | (v << 2)) & 0x3333_3333_3333_3333;
        v = (v | (v << 1)) & 0x5555_5555_5555_5555;
        v
    }
    spread(x) | (spread(y) << 1)
}

/// Morton-code cell ID for `(x, y)` under the domain of `srid`.
/// Coordinates outside the domain clamp — they still index, but at
/// the boundary cell.
pub(crate) fn point_cell(srid: i32, x: f64, y: f64) -> u64 {
    let (xmin, xmax, ymin, ymax) = point_domain(srid);
    let nx = normalize_axis(x, xmin, xmax);
    let ny = normalize_axis(y, ymin, ymax);
    interleave_u32(nx, ny)
}

/// Cell range that covers a bbox. Returns `(min_cell, max_cell)`
/// such that every cell whose point lies inside the bbox satisfies
/// `min_cell <= cell <= max_cell`. The range overshoots — Morton
/// order zigzags across cell boundaries — so the caller must apply
/// a precise bbox filter on the stored coords after the scan.
pub(crate) fn point_cell_range(srid: i32, xlo: f64, ylo: f64, xhi: f64, yhi: f64) -> (u64, u64) {
    let (xmin, xmax, ymin, ymax) = point_domain(srid);
    let nxlo = normalize_axis(xlo.min(xhi), xmin, xmax);
    let nxhi = normalize_axis(xlo.max(xhi), xmin, xmax);
    let nylo = normalize_axis(ylo.min(yhi), ymin, ymax);
    let nyhi = normalize_axis(ylo.max(yhi), ymin, ymax);
    // The min/max Morton for the axis-aligned rectangle [nxlo..nxhi] ×
    // [nylo..nyhi] are the interleavings of the min-min and max-max
    // corners. That's a property of Morton order: within a rectangle,
    // the lex-min cell is at the SW corner and lex-max at the NE.
    let lo = interleave_u32(nxlo, nylo);
    let hi = interleave_u32(nxhi, nyhi);
    (lo, hi)
}

/// Label+property header shared by every point-index key — without
/// the SRID or cell suffix. Used when dropping a whole index: one
/// range scan covers every SRID variant at once.
pub(crate) fn point_index_label_prop_prefix(label: &str, property: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(label.len() + property.len() + 2);
    k.extend_from_slice(label.as_bytes());
    k.push(0);
    k.extend_from_slice(property.as_bytes());
    k.push(0);
    k
}

/// Header including the SRID — every entry for one `(label,
/// property, srid)` tuple shares this prefix. Bbox scans seek
/// relative to it.
pub(crate) fn point_index_srid_prefix(label: &str, property: &str, srid: i32) -> Vec<u8> {
    let mut k = point_index_label_prop_prefix(label, property);
    k.extend_from_slice(&srid.to_be_bytes());
    k
}

/// Full point-index key for one indexed point. `id` is the 16-byte
/// UUID tail — accepts both `NodeId::as_bytes()` and
/// `EdgeId::as_bytes()`, since the node-scope and edge-scope point
/// indexes share this key layout (they live in separate CFs, so the
/// overlap is contained).
pub(crate) fn point_index_key(
    label_or_type: &str,
    property: &str,
    srid: i32,
    cell: u64,
    id: &[u8; ID_LEN],
) -> Vec<u8> {
    let mut k = point_index_srid_prefix(label_or_type, property, srid);
    k.extend_from_slice(&cell.to_be_bytes());
    k.extend_from_slice(id);
    k
}

/// Value record for a point-index entry: the point's coordinates,
/// plus a 1-byte `has_z` discriminator. Kept in the value so bbox /
/// distance filters don't have to fetch the full node.
pub(crate) fn point_index_value(x: f64, y: f64, z: Option<f64>) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + 16 + z.map_or(0, |_| 8));
    v.push(if z.is_some() { 1 } else { 0 });
    v.extend_from_slice(&x.to_be_bytes());
    v.extend_from_slice(&y.to_be_bytes());
    if let Some(zv) = z {
        v.extend_from_slice(&zv.to_be_bytes());
    }
    v
}

pub(crate) fn decode_point_index_value(
    cf: &'static str,
    bytes: &[u8],
) -> Result<(f64, f64, Option<f64>)> {
    if bytes.is_empty() {
        return Err(Error::CorruptBytes {
            cf,
            expected: 17,
            actual: 0,
        });
    }
    let has_z = bytes[0] != 0;
    let expected = if has_z { 25 } else { 17 };
    if bytes.len() != expected {
        return Err(Error::CorruptBytes {
            cf,
            expected,
            actual: bytes.len(),
        });
    }
    let x = f64::from_be_bytes(bytes[1..9].try_into().unwrap());
    let y = f64::from_be_bytes(bytes[9..17].try_into().unwrap());
    let z = if has_z {
        Some(f64::from_be_bytes(bytes[17..25].try_into().unwrap()))
    } else {
        None
    };
    Ok((x, y, z))
}

/// Extract the trailing node id from a point-index key. The key
/// layout puts the id in the last [`ID_LEN`] bytes regardless of the
/// header's length, so tail extraction is position-independent.
pub(crate) fn node_id_from_point_index_key(cf: &'static str, key: &[u8]) -> Result<[u8; ID_LEN]> {
    if key.len() < ID_LEN {
        return Err(Error::CorruptBytes {
            cf,
            expected: ID_LEN,
            actual: key.len(),
        });
    }
    let mut bytes = [0u8; ID_LEN];
    bytes.copy_from_slice(&key[key.len() - ID_LEN..]);
    Ok(bytes)
}

/// Extract the `u64 BE` cell ID from a point-index key. Positioned
/// immediately before the trailing node id and after the
/// `<label>\0<prop>\0<srid:4>` header. Used by `nodes_in_bbox` to
/// break out of a forward iterator once the cell passes the upper
/// bound of the query's cell range.
pub(crate) fn cell_from_point_index_key(
    cf: &'static str,
    key: &[u8],
    header_len: usize,
) -> Result<u64> {
    if key.len() < header_len + POINT_INDEX_CELL_LEN + ID_LEN {
        return Err(Error::CorruptBytes {
            cf,
            expected: header_len + POINT_INDEX_CELL_LEN + ID_LEN,
            actual: key.len(),
        });
    }
    let start = header_len;
    let cell_bytes: [u8; POINT_INDEX_CELL_LEN] =
        key[start..start + POINT_INDEX_CELL_LEN].try_into().unwrap();
    Ok(u64::from_be_bytes(cell_bytes))
}

#[cfg(test)]
mod point_index_tests {
    use super::*;

    #[test]
    fn morton_cell_range_encloses_contained_point() {
        let srid = 4326;
        let (lo, hi) = point_cell_range(srid, -10.0, -5.0, 10.0, 5.0);
        let inside = point_cell(srid, 0.0, 0.0);
        assert!(lo <= inside && inside <= hi);
    }

    #[test]
    fn morton_cell_range_corners_are_extrema() {
        let srid = 4326;
        let (lo, hi) = point_cell_range(srid, -10.0, -5.0, 10.0, 5.0);
        let sw = point_cell(srid, -10.0, -5.0);
        let ne = point_cell(srid, 10.0, 5.0);
        assert_eq!(lo, sw);
        assert_eq!(hi, ne);
    }

    #[test]
    fn point_index_value_roundtrips_2d_and_3d() {
        let v2 = point_index_value(1.5, 2.5, None);
        let (x, y, z) = decode_point_index_value("point_index", &v2).unwrap();
        assert_eq!((x, y, z), (1.5, 2.5, None));
        let v3 = point_index_value(1.5, 2.5, Some(3.5));
        let (x, y, z) = decode_point_index_value("point_index", &v3).unwrap();
        assert_eq!((x, y, z), (1.5, 2.5, Some(3.5)));
    }

    #[test]
    fn point_index_key_tail_is_node_id() {
        let id = NodeId::new();
        let k = point_index_key("L", "p", 4326, 0, id.as_bytes());
        let tail = node_id_from_point_index_key("point_index", &k).unwrap();
        assert_eq!(tail, *id.as_bytes());
    }

    #[test]
    fn clamp_keeps_out_of_domain_points_in_range() {
        // A WGS-84 coord way out of range still yields a valid cell
        // at the domain boundary (not a crash / wraparound).
        let cell = point_cell(4326, 1e9, 1e9);
        let (_, hi) = point_cell_range(4326, -180.0, -90.0, 180.0, 90.0);
        assert_eq!(cell, hi);
    }
}
