use crate::error::{Error, Result};
use mesh_core::{EdgeId, NodeId};

pub(crate) const ID_LEN: usize = 16;
pub(crate) const ADJ_KEY_LEN: usize = 32;

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
