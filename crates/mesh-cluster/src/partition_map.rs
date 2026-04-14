use crate::{Error, PartitionId, PeerId, Result};

/// Maps each partition id to the peer that currently owns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMap {
    assignments: Vec<PeerId>,
}

impl PartitionMap {
    pub fn new(assignments: Vec<PeerId>) -> Self {
        Self { assignments }
    }

    /// Round-robin partitions across the provided peer list.
    pub fn round_robin(peers: &[PeerId], num_partitions: u32) -> Result<Self> {
        if peers.is_empty() {
            return Err(Error::NoPeers);
        }
        if num_partitions == 0 {
            return Err(Error::ZeroPartitions);
        }
        let assignments = (0..num_partitions)
            .map(|p| peers[(p as usize) % peers.len()])
            .collect();
        Ok(Self { assignments })
    }

    pub fn num_partitions(&self) -> u32 {
        self.assignments.len() as u32
    }

    pub fn owner(&self, partition: PartitionId) -> PeerId {
        self.assignments[partition.0 as usize]
    }

    pub fn assignments(&self) -> &[PeerId] {
        &self.assignments
    }
}
