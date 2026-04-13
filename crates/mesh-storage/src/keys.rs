use crate::error::{Error, Result};
use mesh_core::{EdgeId, NodeId};

pub(crate) const ID_LEN: usize = 16;
pub(crate) const ADJ_KEY_LEN: usize = 32;
const LEN_PREFIX: usize = 2;

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
