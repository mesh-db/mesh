//! Multi-raft cluster wrapper.
//!
//! In `mode = "multi-raft"` each peer hosts:
//!
//!   * One **metadata** Raft group, spanning every peer. Replicates
//!     DDL (`CreateIndex`, `CreateConstraint`, `InstallTrigger`, ...)
//!     and cluster-membership entries.
//!   * One Raft group **per partition** this peer is a replica of —
//!     determined deterministically by [`PartitionReplicaMap`].
//!     Replicates the data writes targeting that partition.
//!
//! This module holds the [`MultiRaftCluster`] wrapper that bundles
//! those handles together plus a [`PartitionLeaderCache`] used by the
//! write-routing path to find the right peer for a partition.

use crate::multi_raft_applier::PartitionGraphApplier;
use crate::raft_service::RaftGroupRegistry;
use meshdb_cluster::raft::{NodeId, RaftCluster};
use meshdb_cluster::{PartitionId, PartitionReplicaMap};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Default interval for the periodic in-doubt recovery loop. Matches
/// the spirit of [`crate::DEFAULT_ROTATION_INTERVAL`] — frequent
/// enough to catch a stale PREPARE within a minute, infrequent
/// enough that an idle cluster's recovery polls don't overwhelm
/// `ResolveTransaction` traffic.
pub const DEFAULT_RECOVERY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Default deadline for the synchronous-DDL gate — `CREATE INDEX` /
/// `CREATE CONSTRAINT` / `apoc.trigger.install` won't return to the
/// caller until every peer's meta replica has applied the entry, or
/// this deadline expires (in which case the DDL is durably committed
/// but the user gets a `Status::DeadlineExceeded` so they know one
/// or more peers are lagging). 5 seconds is loose enough to absorb
/// normal Raft replication + apply latency, tight enough that a
/// single dead peer can't make every DDL hang indefinitely.
pub const DEFAULT_DDL_STRICT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Bundle of every Raft group this peer is a replica of in
/// `mode = "multi-raft"`.
pub struct MultiRaftCluster {
    /// This peer's NodeId. Stored so leader-resolution can
    /// short-circuit when we lead a partition and don't need to
    /// proxy the write.
    pub self_id: NodeId,
    /// Metadata Raft group — every peer is a member.
    pub meta: Arc<RaftCluster>,
    /// Per-partition Raft groups this peer is a replica of. Not
    /// every partition appears here — only the ones whose replica
    /// set includes `self_id`. RwLock-wrapped so a runtime
    /// rebalance can grow the set without restart (see
    /// `instantiate_partition_group`).
    partitions: Arc<RwLock<HashMap<PartitionId, Arc<RaftCluster>>>>,
    /// Concrete applier handles for the partitions hosted on this
    /// peer. Same RwLock semantics as `partitions` — kept in lockstep
    /// so a partition addition updates both atomically from the
    /// caller's perspective.
    partition_appliers: Arc<RwLock<HashMap<PartitionId, Arc<PartitionGraphApplier>>>>,
    /// The deterministic placement map shared by every peer.
    /// Lookup-only — multi-raft v1 does not rebalance at runtime.
    pub replica_map: Arc<PartitionReplicaMap>,
    /// Lazy cache of partition leadership, refreshed from each
    /// partition Raft's own metrics. See [`PartitionLeaderCache`].
    pub leader_cache: PartitionLeaderCache,
    /// Highest meta-Raft log index this peer has either proposed or
    /// learned about from a forwarded write. Partition writes await
    /// the local meta replica to apply at least up to this index
    /// before proposing — guarantees DDL committed before the write
    /// is visible at apply time, so e.g. a CREATE INDEX followed by
    /// a write through any peer doesn't observe a stale index on
    /// the partition leader.
    ///
    /// Updated by:
    ///   * Successful local DDL propose (sets to leader's
    ///     `last_applied.index` post-commit).
    ///   * Inbound forward_write / forward_ddl carrying a `min_meta_index`
    ///     value greater than the current.
    /// Read by partition write paths via [`await_meta_barrier`].
    pub min_meta_index: AtomicU64,
    /// Clone of the `RaftGroupRegistry` handed to
    /// [`MeshRaftService::with_registry`]. The two share the same
    /// `Arc<RwLock<HashMap>>` for partitions, so a runtime spin-up
    /// can register the new partition Raft via this handle and the
    /// dispatch path picks it up immediately. `None` until the
    /// bootstrap path attaches one (test harnesses that build a
    /// registry post-hoc don't always set it; their missing
    /// registry is non-fatal — the runtime spin-up just can't
    /// register inbound RPCs without it).
    pub dispatch_registry: RwLock<Option<RaftGroupRegistry>>,
    /// Synchronous DDL gate strict timeout. After a meta-Raft
    /// proposal commits locally, the gate polls every other peer's
    /// `meta_last_applied` until they catch up to the proposal's
    /// commit index, or this duration elapses. Default
    /// [`DEFAULT_DDL_STRICT_TIMEOUT`]; tests override via
    /// [`Self::set_ddl_strict_timeout`] to exercise the timeout path
    /// in bounded wall time.
    pub ddl_strict_timeout: std::sync::RwLock<std::time::Duration>,
}

impl std::fmt::Debug for MultiRaftCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiRaftCluster")
            .field("self_id", &self.self_id)
            .field(
                "partitions",
                &self
                    .partitions
                    .read()
                    .map(|g| g.keys().copied().collect::<Vec<_>>())
                    .unwrap_or_default(),
            )
            .field("num_replicas", &self.replica_map.num_partitions())
            .finish_non_exhaustive()
    }
}

impl MultiRaftCluster {
    pub fn new(
        self_id: NodeId,
        meta: Arc<RaftCluster>,
        partitions: HashMap<PartitionId, Arc<RaftCluster>>,
        partition_appliers: HashMap<PartitionId, Arc<PartitionGraphApplier>>,
        replica_map: Arc<PartitionReplicaMap>,
    ) -> Self {
        let leader_cache = PartitionLeaderCache::new(replica_map.num_partitions());
        Self {
            self_id,
            meta,
            partitions: Arc::new(RwLock::new(partitions)),
            partition_appliers: Arc::new(RwLock::new(partition_appliers)),
            replica_map,
            leader_cache,
            min_meta_index: AtomicU64::new(0),
            dispatch_registry: RwLock::new(None),
            ddl_strict_timeout: std::sync::RwLock::new(DEFAULT_DDL_STRICT_TIMEOUT),
        }
    }

    /// Returns the currently-configured synchronous DDL gate strict
    /// timeout. Defaults to [`DEFAULT_DDL_STRICT_TIMEOUT`].
    pub fn ddl_strict_timeout(&self) -> std::time::Duration {
        *self
            .ddl_strict_timeout
            .read()
            .expect("ddl_strict_timeout lock poisoned")
    }

    /// Override the synchronous DDL gate strict timeout. Test-only
    /// hook for exercising the deadline-exceeded path in bounded
    /// wall time. Production sites use the default.
    pub fn set_ddl_strict_timeout(&self, timeout: std::time::Duration) {
        *self
            .ddl_strict_timeout
            .write()
            .expect("ddl_strict_timeout lock poisoned") = timeout;
    }

    /// Stash a clone of the dispatch [`RaftGroupRegistry`] so
    /// runtime partition spin-up can register the new group. Pass
    /// the same registry instance handed to
    /// [`MeshRaftService::with_registry`] — the registry's
    /// `partitions` field is `Arc<RwLock<...>>`, so the clone shares
    /// the live dispatch table.
    pub fn attach_dispatch_registry(&self, registry: RaftGroupRegistry) {
        *self
            .dispatch_registry
            .write()
            .expect("dispatch_registry lock poisoned") = Some(registry);
    }

    /// Look up the partition Raft handle for `partition`, returning
    /// `None` when this peer doesn't host the partition's replica.
    /// Replaces the previous public `partitions.get(&p)` access —
    /// hides the RwLock.
    pub fn partition(&self, partition: PartitionId) -> Option<Arc<RaftCluster>> {
        self.partitions
            .read()
            .expect("partitions lock poisoned")
            .get(&partition)
            .cloned()
    }

    /// Snapshot every `(PartitionId, Arc<RaftCluster>)` pair this
    /// peer hosts. Used by callers that need to walk every group
    /// (registry construction, recovery, leader-readiness loops in
    /// tests). Holds the read lock only for the snapshot duration.
    pub fn partitions_snapshot(&self) -> Vec<(PartitionId, Arc<RaftCluster>)> {
        let guard = self.partitions.read().expect("partitions lock poisoned");
        guard.iter().map(|(k, v)| (*k, v.clone())).collect()
    }

    /// Snapshot every `(PartitionId, Arc<PartitionGraphApplier>)`
    /// pair. Used by `recover_multi_raft_in_doubt` to walk every
    /// partition's `pending_tx_ids`.
    pub fn partition_appliers_snapshot(&self) -> Vec<(PartitionId, Arc<PartitionGraphApplier>)> {
        let guard = self
            .partition_appliers
            .read()
            .expect("partition_appliers lock poisoned");
        guard.iter().map(|(k, v)| (*k, v.clone())).collect()
    }

    /// `true` when this peer hosts a Raft replica for `partition`.
    pub fn hosts_partition(&self, partition: PartitionId) -> bool {
        self.partitions
            .read()
            .expect("partitions lock poisoned")
            .contains_key(&partition)
    }

    /// Insert a runtime-instantiated partition Raft group. Called
    /// post-rebalance when a peer is added as a replica of a
    /// partition it didn't bootstrap with. Idempotent — re-adding
    /// the same partition replaces the prior entry, which is how
    /// a re-issued rebalance reconciles after a transient failure.
    ///
    /// Caller is responsible for opening the rocksdb dir, building
    /// the `RaftCluster` and `PartitionGraphApplier`, and (if the
    /// new group needs to receive RPCs from existing peers)
    /// updating the dispatch registry — see
    /// [`MeshRaftService::register_partition_group`]. Future
    /// orchestration will roll all of these into one helper.
    pub fn add_runtime_partition_group(
        &self,
        partition: PartitionId,
        raft: Arc<RaftCluster>,
        applier: Arc<PartitionGraphApplier>,
    ) {
        self.partitions
            .write()
            .expect("partitions lock poisoned")
            .insert(partition, raft);
        self.partition_appliers
            .write()
            .expect("partition_appliers lock poisoned")
            .insert(partition, applier);
    }

    /// Snapshot the local meta-Raft applier's `last_applied` index.
    /// Returns 0 when nothing has applied yet (fresh cluster pre-
    /// any-DDL). Cheap — reads the watch-channel metrics.
    pub fn meta_last_applied(&self) -> u64 {
        self.meta
            .raft
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0)
    }

    /// Advance the cluster's `min_meta_index` to `at_least` if it's
    /// higher than the current value. Idempotent and lock-free.
    pub fn observe_meta_barrier(&self, at_least: u64) {
        let mut current = self.min_meta_index.load(Ordering::SeqCst);
        while at_least > current {
            match self.min_meta_index.compare_exchange_weak(
                current,
                at_least,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// Wait until the local meta-Raft applier has applied at least
    /// `min_meta_index`. Returns immediately if already caught up;
    /// returns `Err` if the deadline expires (deadline-exceeded
    /// surfaces to the caller as a retryable Status::Unavailable).
    /// Polls every 5ms — meta-Raft apply lag is sub-millisecond
    /// under normal load, so this is hot-path-cheap.
    pub async fn await_meta_barrier(&self, timeout: Duration) -> Result<(), &'static str> {
        let target = self.min_meta_index.load(Ordering::SeqCst);
        if target == 0 {
            return Ok(());
        }
        let deadline = Instant::now() + timeout;
        loop {
            if self.meta_last_applied() >= target {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err("meta barrier timeout");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Build a [`RaftGroupRegistry`] that lets [`MeshRaftService`]
    /// dispatch incoming RPCs to the right group on this peer.
    pub fn build_registry(&self) -> RaftGroupRegistry {
        let mut reg = RaftGroupRegistry::new().with_meta(Arc::new(self.meta.raft.clone()));
        for (p, raft) in self.partitions_snapshot() {
            reg = reg.with_partition(p, Arc::new(raft.raft.clone()));
        }
        reg
    }

    /// Returns the cached or freshly-polled leader of `partition`.
    /// `None` means this peer doesn't know the leader yet (election
    /// in progress, freshly-restarted follower, etc.). Callers
    /// should treat that as "retry shortly" — the write path's
    /// server-side forwarding wraps this with a refresh-on-stale
    /// retry loop.
    pub fn leader_of(&self, partition: PartitionId) -> Option<NodeId> {
        // Fast path: cached.
        if let Some(leader) = self.leader_cache.get(partition) {
            return Some(leader);
        }
        // Slow path: poll the local Raft replica's metrics if we
        // host this partition. If we don't, the cache miss is the
        // best we can do — caller should ask another replica.
        if let Some(raft) = self.partition(partition) {
            if let Some(leader) = raft.current_leader() {
                self.leader_cache.set(partition, leader.0);
                return Some(leader.0);
            }
        }
        None
    }

    /// True when this peer leads `partition`. Used by the write
    /// path to decide between local-propose and forward-to-leader.
    pub fn is_local_leader(&self, partition: PartitionId) -> bool {
        self.leader_of(partition) == Some(self.self_id)
    }

    /// Returns the current leader of the metadata Raft group, or
    /// `None` if no leader is known on this peer (election in
    /// progress, freshly-restarted follower, etc.).
    pub fn meta_leader(&self) -> Option<NodeId> {
        self.meta.current_leader().map(|id| id.0)
    }

    /// Like [`Self::leader_of`] but bypasses the leader cache and
    /// asks the local Raft replica directly. Useful when callers
    /// know the cached entry may be stale (e.g. just observed the
    /// previous leader crash). Returns `None` if this peer doesn't
    /// host the partition or its replica doesn't yet know who leads.
    pub fn partition_current_leader(&self, partition: PartitionId) -> Option<NodeId> {
        let raft = self.partition(partition)?;
        let leader = raft.current_leader()?.0;
        self.leader_cache.set(partition, leader);
        Some(leader)
    }

    /// Stop the local replica of `partition`. After return, this
    /// peer's openraft handle for that partition is shut down — it
    /// stops sending heartbeats and rejects proposals. Other
    /// replicas will time out and elect a new leader. Test-only.
    pub async fn shutdown_partition(&self, partition: PartitionId) -> Result<(), String> {
        let raft = self
            .partition(partition)
            .ok_or_else(|| format!("partition {} not hosted on this peer", partition.0))?;
        raft.shutdown_in_place()
            .await
            .map_err(|e| format!("partition {} shutdown: {e}", partition.0))
    }

    /// Stop the local replica of the metadata Raft group. After
    /// return, this peer no longer participates in meta-Raft —
    /// surviving peers will elect a new leader. Test-only.
    pub async fn shutdown_meta(&self) -> Result<(), String> {
        self.meta
            .shutdown_in_place()
            .await
            .map_err(|e| format!("meta shutdown: {e}"))
    }

    /// Add a peer as a voter of `partition`'s Raft group. Wraps
    /// openraft's `change_membership` with `ChangeMembers::AddVoters`.
    /// The new peer's address is looked up from the cluster's meta
    /// state — any peer in the cluster's `Membership` is eligible.
    ///
    /// Useful for growing a partition's replica set (e.g. promoting
    /// a learner to handle increased read traffic, or recovering
    /// after a permanent peer loss).
    ///
    /// **v1 limitations** — same as [`remove_partition_replica`]:
    /// * Caller is responsible for updating
    ///   [`PartitionReplicaMap`] separately so write routing sees
    ///   the new replica.
    /// * The new peer must already be running with the partition's
    ///   Raft group instantiated locally (i.e. it bootstrapped
    ///   with that partition in its replica set, even if it isn't
    ///   currently a voter). Runtime group spin-up is a v2 concern.
    pub async fn add_partition_replica(
        &self,
        partition: PartitionId,
        peer_id: NodeId,
    ) -> Result<(), String> {
        let raft = self
            .partition(partition)
            .ok_or_else(|| format!("partition {} not hosted on this peer", partition.0))?;
        let state = self.meta.current_state().await;
        let addr = state
            .membership
            .iter()
            .find_map(|(id, addr)| {
                if id.0 == peer_id {
                    Some(addr.to_string())
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                format!("peer {peer_id} not in cluster membership; add to meta group first")
            })?;
        drop(state);
        let mut members = std::collections::BTreeMap::new();
        members.insert(peer_id, openraft::BasicNode::new(addr));
        raft.raft
            .change_membership(openraft::ChangeMembers::AddVoters(members), false)
            .await
            .map(|_| ())
            .map_err(|e| format!("partition {} AddVoters: {e}", partition.0))?;
        self.publish_partition_voters(partition).await;
        Ok(())
    }

    /// Remove a peer as a voter of `partition`'s Raft group. Wraps
    /// `ChangeMembers::RemoveVoters`. Useful for shrinking a replica
    /// set or evacuating a peer scheduled for decommissioning.
    ///
    /// Caller is responsible for updating [`PartitionReplicaMap`]
    /// separately so write routing stops targeting the removed
    /// peer. Runtime group teardown on the removed peer (so its
    /// rocksdb instance can be reclaimed) is a v2 concern; for now
    /// the disk artifacts stay until the operator deletes them.
    pub async fn remove_partition_replica(
        &self,
        partition: PartitionId,
        peer_id: NodeId,
    ) -> Result<(), String> {
        let raft = self
            .partition(partition)
            .ok_or_else(|| format!("partition {} not hosted on this peer", partition.0))?;
        let mut to_remove = std::collections::BTreeSet::new();
        to_remove.insert(peer_id);
        raft.raft
            .change_membership(openraft::ChangeMembers::RemoveVoters(to_remove), false)
            .await
            .map(|_| ())
            .map_err(|e| format!("partition {} RemoveVoters: {e}", partition.0))?;
        self.publish_partition_voters(partition).await;
        Ok(())
    }

    /// Publish the partition's current voter set through the meta
    /// Raft as a `ClusterCommand::SetPartitionReplicas` so the
    /// cluster's persisted view of placement matches the partition
    /// Raft's actual membership. Called after a successful
    /// `change_membership` on the partition.
    ///
    /// Failures here are logged but not propagated — the rebalance
    /// itself succeeded; only the meta-side bookkeeping fell behind.
    /// An operator can re-run the rebalance API to reconcile, or
    /// the periodic recovery loop will eventually surface the gap
    /// (future work; for v1 the operator handles it).
    async fn publish_partition_voters(&self, partition: PartitionId) {
        let voters = self.current_partition_voters(partition).await;
        if let Err(e) = self
            .meta
            .propose(meshdb_cluster::ClusterCommand::SetPartitionReplicas {
                partition: partition.0,
                replicas: voters.iter().map(|p| p.0).collect(),
            })
            .await
        {
            tracing::warn!(
                partition = ?partition,
                error = %e,
                "post-rebalance SetPartitionReplicas propose failed; \
                 partition Raft membership advanced but cluster view did not — \
                 operator should re-run the rebalance API to reconcile"
            );
        }
    }

    /// Snapshot the partition Raft's current voter set as observed
    /// by this peer's local replica.
    async fn current_partition_voters(
        &self,
        partition: PartitionId,
    ) -> Vec<meshdb_cluster::PeerId> {
        let Some(raft) = self.partition(partition) else {
            return Vec::new();
        };
        let metrics = raft.raft.metrics().borrow().clone();
        metrics
            .membership_config
            .membership()
            .voter_ids()
            .map(meshdb_cluster::PeerId)
            .collect()
    }
}

/// Lazy cache of `PartitionId -> Option<NodeId>` mapping the current
/// leader for each partition group. Updated on demand by
/// [`MultiRaftCluster::leader_of`]; staleness is acceptable because
/// the write-routing path retries once on a `leader_hint` mismatch.
#[derive(Clone)]
pub struct PartitionLeaderCache {
    inner: Arc<RwLock<Vec<Option<NodeId>>>>,
}

impl PartitionLeaderCache {
    pub fn new(num_partitions: u32) -> Self {
        Self {
            inner: Arc::new(RwLock::new(vec![None; num_partitions as usize])),
        }
    }

    pub fn get(&self, partition: PartitionId) -> Option<NodeId> {
        let cache = self.inner.read().expect("leader cache poisoned");
        *cache.get(partition.0 as usize)?
    }

    pub fn set(&self, partition: PartitionId, leader: NodeId) {
        let mut cache = self.inner.write().expect("leader cache poisoned");
        if let Some(slot) = cache.get_mut(partition.0 as usize) {
            *slot = Some(leader);
        }
    }

    pub fn invalidate(&self, partition: PartitionId) {
        let mut cache = self.inner.write().expect("leader cache poisoned");
        if let Some(slot) = cache.get_mut(partition.0 as usize) {
            *slot = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_cache_round_trips_set_get() {
        let cache = PartitionLeaderCache::new(4);
        assert_eq!(cache.get(PartitionId(0)), None);
        cache.set(PartitionId(0), 7);
        assert_eq!(cache.get(PartitionId(0)), Some(7));
        cache.invalidate(PartitionId(0));
        assert_eq!(cache.get(PartitionId(0)), None);
    }
}
