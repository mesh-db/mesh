use mesh_core::NodeId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PartitionId(pub u32);

impl std::fmt::Display for PartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "p{}", self.0)
    }
}

/// Deterministic hash-based partitioner. Uses FNV-1a over the node's 16 raw
/// bytes so partition assignment is stable across processes and Rust versions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Partitioner {
    num_partitions: u32,
}

impl Partitioner {
    pub fn new(num_partitions: u32) -> Self {
        assert!(num_partitions > 0, "num_partitions must be > 0");
        Self { num_partitions }
    }

    pub fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    pub fn partition_for(&self, node: NodeId) -> PartitionId {
        let h = fnv1a(node.as_bytes());
        PartitionId((h % self.num_partitions as u64) as u32)
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce4_84222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}
