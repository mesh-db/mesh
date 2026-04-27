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

    /// Weighted replica placement: each peer carries a positive
    /// weight; partition slots are distributed so each peer ends
    /// up with approximately `weight_i / sum(weights) * num_partitions
    /// * replication_factor` placements. Useful for heterogeneous
    /// clusters (peers with different cores / disk capacity).
    ///
    /// Algorithm: greedy lowest-score, where `score(peer) = (count
    /// + 1) / weight`. For each partition, pick `replication_factor`
    /// distinct peers in ascending score order, breaking ties by
    /// peer id for determinism. The same `weighted_peers` slice
    /// always yields the same map.
    ///
    /// When every weight is equal this produces a placement that
    /// matches `round_robin_replicas` modulo tie-breaks; the round-
    /// robin path is still preferred at config-resolve time when
    /// no peer has a non-default weight, to keep wire-compatible
    /// with v1 clusters.
    ///
    /// Errors:
    ///   * `NoPeers` — `weighted_peers` is empty.
    ///   * `ZeroPartitions` — `num_partitions == 0`.
    ///   * `InvalidReplicationFactor` — out of `[1, weighted_peers.len()]`.
    ///   * `InvalidWeight` — any weight ≤ 0 or non-finite.
    pub fn weighted_replicas(
        weighted_peers: &[(PeerId, f64)],
        num_partitions: u32,
        replication_factor: usize,
    ) -> Result<Self> {
        if weighted_peers.is_empty() {
            return Err(Error::NoPeers);
        }
        if num_partitions == 0 {
            return Err(Error::ZeroPartitions);
        }
        if replication_factor == 0 || replication_factor > weighted_peers.len() {
            return Err(Error::InvalidReplicationFactor {
                replication_factor,
                peer_count: weighted_peers.len(),
            });
        }
        for (peer, w) in weighted_peers {
            if !w.is_finite() || *w <= 0.0 {
                return Err(Error::InvalidWeight {
                    peer: *peer,
                    weight: *w,
                });
            }
        }
        let n = weighted_peers.len();
        let mut counts: Vec<f64> = vec![0.0; n];
        let mut replicas: Vec<Vec<PeerId>> = Vec::with_capacity(num_partitions as usize);
        for _p in 0..num_partitions {
            let mut chosen: Vec<PeerId> = Vec::with_capacity(replication_factor);
            let mut used: Vec<bool> = vec![false; n];
            for _r in 0..replication_factor {
                // Find unused peer with lowest (count + 1) / weight.
                // Tie-break by peer id for determinism.
                let best = (0..n)
                    .filter(|i| !used[*i])
                    .min_by(|a, b| {
                        let sa = (counts[*a] + 1.0) / weighted_peers[*a].1;
                        let sb = (counts[*b] + 1.0) / weighted_peers[*b].1;
                        sa.partial_cmp(&sb)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| weighted_peers[*a].0.cmp(&weighted_peers[*b].0))
                    })
                    .expect("rf <= n means at least one peer is unused");
                counts[best] += 1.0;
                used[best] = true;
                chosen.push(weighted_peers[best].0);
            }
            replicas.push(chosen);
        }
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

    fn weighted(pairs: &[(u64, f64)]) -> Vec<(PeerId, f64)> {
        pairs.iter().map(|(id, w)| (PeerId(*id), *w)).collect()
    }

    #[test]
    fn weighted_replicas_uniform_weights_balances_evenly() {
        // 6 partitions × rf=2 = 12 placements; 3 peers each weight 1.0
        // → 4 placements per peer. Greedy lowest-score lays them down
        // evenly.
        let map = PartitionReplicaMap::weighted_replicas(
            &weighted(&[(1, 1.0), (2, 1.0), (3, 1.0)]),
            6,
            2,
        )
        .unwrap();
        let mut counts = std::collections::HashMap::new();
        for p in 0..6 {
            for peer in map.replicas(PartitionId(p)) {
                *counts.entry(peer.0).or_insert(0) += 1;
            }
        }
        assert_eq!(counts.get(&1), Some(&4));
        assert_eq!(counts.get(&2), Some(&4));
        assert_eq!(counts.get(&3), Some(&4));
    }

    #[test]
    fn weighted_replicas_skewed_weights_skews_load() {
        // Peer 1 has weight 4, peers 2 & 3 weight 1. 6 partitions × rf=2
        // = 12 placements; expected ratio 4:1:1 → ~8:2:2. Allow ±1
        // slack since greedy isn't perfectly continuous.
        let map = PartitionReplicaMap::weighted_replicas(
            &weighted(&[(1, 4.0), (2, 1.0), (3, 1.0)]),
            6,
            2,
        )
        .unwrap();
        let mut counts = std::collections::HashMap::new();
        for p in 0..6 {
            for peer in map.replicas(PartitionId(p)) {
                *counts.entry(peer.0).or_insert(0) += 1;
            }
        }
        let p1 = *counts.get(&1).unwrap_or(&0);
        let p2 = *counts.get(&2).unwrap_or(&0);
        let p3 = *counts.get(&3).unwrap_or(&0);
        assert!(
            p1 > p2 && p1 > p3,
            "peer 1 should dominate: counts = ({p1}, {p2}, {p3})"
        );
        assert_eq!(p1 + p2 + p3, 12, "every placement must be assigned");
    }

    #[test]
    fn weighted_replicas_each_partition_has_distinct_replicas() {
        let map =
            PartitionReplicaMap::weighted_replicas(&weighted(&[(1, 2.0), (2, 1.0)]), 3, 2).unwrap();
        for p in 0..3 {
            let r = map.replicas(PartitionId(p));
            let mut sorted = r.to_vec();
            sorted.sort_by_key(|p| p.0);
            sorted.dedup();
            assert_eq!(
                sorted.len(),
                r.len(),
                "duplicates in partition {p}'s replicas"
            );
        }
    }

    #[test]
    fn weighted_replicas_is_deterministic() {
        let a = PartitionReplicaMap::weighted_replicas(
            &weighted(&[(1, 2.5), (2, 1.0), (3, 1.5)]),
            8,
            2,
        )
        .unwrap();
        let b = PartitionReplicaMap::weighted_replicas(
            &weighted(&[(1, 2.5), (2, 1.0), (3, 1.5)]),
            8,
            2,
        )
        .unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn weighted_replicas_rejects_non_positive_weight() {
        let err = PartitionReplicaMap::weighted_replicas(&weighted(&[(1, 0.0), (2, 1.0)]), 4, 1)
            .unwrap_err();
        assert!(matches!(err, Error::InvalidWeight { .. }));
        let err = PartitionReplicaMap::weighted_replicas(&weighted(&[(1, -1.0), (2, 1.0)]), 4, 1)
            .unwrap_err();
        assert!(matches!(err, Error::InvalidWeight { .. }));
        let err =
            PartitionReplicaMap::weighted_replicas(&weighted(&[(1, f64::NAN), (2, 1.0)]), 4, 1)
                .unwrap_err();
        assert!(matches!(err, Error::InvalidWeight { .. }));
    }
}
