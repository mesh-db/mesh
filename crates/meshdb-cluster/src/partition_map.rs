use crate::{Error, PartitionId, PeerId, Result};
use serde::{Deserialize, Serialize};

/// Maps each partition id to the peer that currently owns it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// Maps each partition id to the *set* of peers that hold a replica
/// of that partition's Raft group, used by `mode = "multi-raft"`.
///
/// In single-Raft and routing modes only one peer is associated with
/// each partition (the leader / owner), so [`PartitionMap`] is the
/// right shape. Multi-Raft replicates each partition's Raft log
/// across `replication_factor` peers, so the partition→peer mapping
/// is one-to-many.
///
/// The replica set for partition `P` is contiguous in the peer list
/// starting at `P % peers.len()` — chosen so adjacent partitions
/// share most of their replicas, which keeps placement deterministic
/// without requiring a global heuristic. Real systems with peer
/// heterogeneity (different cores / disk) eventually want weighted
/// placement, but meshdb assumes homogeneous peers per CLAUDE.md so
/// round-robin is sufficient for v1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionReplicaMap {
    /// One entry per partition, each a non-empty list of peer ids
    /// (the replica set). Length == `num_partitions`.
    replicas: Vec<Vec<PeerId>>,
}

impl PartitionReplicaMap {
    pub fn new(replicas: Vec<Vec<PeerId>>) -> Self {
        Self { replicas }
    }

    /// Round-robin replica placement for `num_partitions` partitions
    /// across `peers`, with `replication_factor` peers per partition.
    /// Partition `P`'s replicas are `peers[(P+0)%N], peers[(P+1)%N],
    /// … peers[(P+R-1)%N]` where `N = peers.len()` and `R =
    /// replication_factor`.
    ///
    /// Errors:
    ///   * `NoPeers` — `peers` is empty.
    ///   * `ZeroPartitions` — `num_partitions == 0`.
    ///   * `InvalidReplicationFactor` — `replication_factor == 0` or
    ///     `replication_factor > peers.len()`.
    pub fn round_robin_replicas(
        peers: &[PeerId],
        num_partitions: u32,
        replication_factor: usize,
    ) -> Result<Self> {
        if peers.is_empty() {
            return Err(Error::NoPeers);
        }
        if num_partitions == 0 {
            return Err(Error::ZeroPartitions);
        }
        if replication_factor == 0 || replication_factor > peers.len() {
            return Err(Error::InvalidReplicationFactor {
                replication_factor,
                peer_count: peers.len(),
            });
        }
        let n = peers.len();
        let replicas = (0..num_partitions as usize)
            .map(|p| {
                (0..replication_factor)
                    .map(|r| peers[(p + r) % n])
                    .collect()
            })
            .collect();
        Ok(Self { replicas })
    }

    pub fn num_partitions(&self) -> u32 {
        self.replicas.len() as u32
    }

    /// Replica set for `partition`. Always non-empty.
    pub fn replicas(&self, partition: PartitionId) -> &[PeerId] {
        &self.replicas[partition.0 as usize]
    }

    /// True when `peer` is in the replica set of `partition`. Used
    /// by the bootstrap path on each peer to decide which partition
    /// Raft groups to instantiate locally.
    pub fn contains(&self, partition: PartitionId, peer: PeerId) -> bool {
        self.replicas[partition.0 as usize].contains(&peer)
    }

    /// All distinct partition ids where `peer` holds a replica.
    /// Cheap because partition counts are small (default 4, typical
    /// production tens to low hundreds).
    pub fn partitions_for(&self, peer: PeerId) -> Vec<PartitionId> {
        self.replicas
            .iter()
            .enumerate()
            .filter_map(|(idx, set)| {
                if set.contains(&peer) {
                    Some(PartitionId(idx as u32))
                } else {
                    None
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peers(ids: &[u64]) -> Vec<PeerId> {
        ids.iter().copied().map(PeerId).collect()
    }

    #[test]
    fn replica_map_round_robin_assigns_contiguous_replicas() {
        // 4 partitions, 4 peers, rf=3 → each partition wraps around
        // the peer list starting at its id modulo peer count.
        let map = PartitionReplicaMap::round_robin_replicas(&peers(&[1, 2, 3, 4]), 4, 3).unwrap();
        assert_eq!(map.replicas(PartitionId(0)), &peers(&[1, 2, 3])[..]);
        assert_eq!(map.replicas(PartitionId(1)), &peers(&[2, 3, 4])[..]);
        assert_eq!(map.replicas(PartitionId(2)), &peers(&[3, 4, 1])[..]);
        assert_eq!(map.replicas(PartitionId(3)), &peers(&[4, 1, 2])[..]);
    }

    #[test]
    fn replica_map_partitions_for_returns_every_partition_a_peer_replicates() {
        let map = PartitionReplicaMap::round_robin_replicas(&peers(&[1, 2, 3, 4]), 4, 3).unwrap();
        // Peer 1 replicates partitions where peer 1 falls in
        // [P, P+1, P+2] (mod 4): P ∈ {0, 2, 3}.
        assert_eq!(
            map.partitions_for(PeerId(1)),
            vec![PartitionId(0), PartitionId(2), PartitionId(3)]
        );
    }

    #[test]
    fn replica_map_rf_equal_to_peers_replicates_every_partition_everywhere() {
        let map = PartitionReplicaMap::round_robin_replicas(&peers(&[1, 2, 3]), 2, 3).unwrap();
        for p in 0..2 {
            let mut set: Vec<_> = map.replicas(PartitionId(p)).to_vec();
            set.sort_by_key(|p| p.0);
            assert_eq!(set, peers(&[1, 2, 3]));
        }
    }

    #[test]
    fn replica_map_rejects_zero_replication_factor() {
        let err = PartitionReplicaMap::round_robin_replicas(&peers(&[1, 2, 3]), 4, 0).unwrap_err();
        assert!(matches!(err, Error::InvalidReplicationFactor { .. }));
    }

    #[test]
    fn replica_map_rejects_replication_factor_above_peer_count() {
        let err = PartitionReplicaMap::round_robin_replicas(&peers(&[1, 2]), 4, 3).unwrap_err();
        assert!(matches!(err, Error::InvalidReplicationFactor { .. }));
    }

    #[test]
    fn replica_map_rejects_empty_peer_list() {
        let err = PartitionReplicaMap::round_robin_replicas(&[], 4, 1).unwrap_err();
        assert!(matches!(err, Error::NoPeers));
    }

    #[test]
    fn replica_map_rejects_zero_partitions() {
        let err = PartitionReplicaMap::round_robin_replicas(&peers(&[1]), 0, 1).unwrap_err();
        assert!(matches!(err, Error::ZeroPartitions));
    }
}
