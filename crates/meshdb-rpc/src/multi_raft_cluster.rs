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

    /// Force this peer's local replica of `partition` to start an
    /// election right now, bypassing the election timer. Wraps
    /// openraft's `Raft::trigger().elect()`. Does NOT guarantee
    /// leadership transfer — see [`RaftCluster::force_election`]
    /// for the caveats. Returns `Err("partition not hosted")`
    /// when this peer doesn't host the partition's replica.
    ///
    /// Operator-facing primitive for leader balancing: pair this
    /// with `shutdown_partition` on the current leader (or a
    /// transient `change_membership` round-trip) to land
    /// leadership where you want it.
    pub async fn force_partition_election(&self, partition: PartitionId) -> Result<(), String> {
        let raft = self
            .partition(partition)
            .ok_or_else(|| format!("partition {} not hosted on this peer", partition.0))?;
        raft.force_election()
            .await
            .map_err(|e| format!("force election on partition {}: {e}", partition.0))
    }

    /// Force this peer's local replica of the metadata Raft group
    /// to start an election right now. Same caveats as
    /// [`Self::force_partition_election`] — doesn't guarantee
    /// transfer.
    pub async fn force_meta_election(&self) -> Result<(), String> {
        self.meta
            .force_election()
            .await
            .map_err(|e| format!("force election on meta: {e}"))
    }

    /// Returns the partitions this peer currently leads. Used by
    /// the graceful-shutdown path to decide which leadership
    /// transfers to perform. Bypasses the leader cache and reads
    /// each partition Raft's live metrics — the cache may be stale
    /// (e.g. mid-shutdown the replica reports it's no longer leader,
    /// but the cache still names self).
    pub fn partitions_led_locally(&self) -> Vec<PartitionId> {
        self.partitions_snapshot()
            .into_iter()
            .filter_map(|(p, raft)| match raft.current_leader() {
                Some(leader) if leader.0 == self.self_id => Some(p),
                _ => None,
            })
            .collect()
    }

    /// Add `peer_id` as a non-voting *learner* of `partition`'s
    /// Raft group. Learners receive every committed log entry but
    /// don't participate in elections or counted toward quorum.
    /// They're the canonical scaling story for read-heavy
    /// workloads — adding learners absorbs read traffic without
    /// adding write-quorum overhead.
    ///
    /// The same caveats as [`Self::add_partition_replica`] apply:
    /// the new peer must already be running with a partition Raft
    /// instance for this partition (use
    /// `meshdb_server::instantiate_partition_group`), and the
    /// caller is responsible for updating
    /// [`PartitionReplicaMap`] separately if read routing should
    /// see the new learner.
    pub async fn add_partition_learner(
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
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(peer_id, openraft::BasicNode::new(addr));
        raft.raft
            .change_membership(openraft::ChangeMembers::AddNodes(nodes), false)
            .await
            .map(|_| ())
            .map_err(|e| format!("partition {} AddNodes (learner): {e}", partition.0))
    }

    /// Remove `peer_id` from `partition`'s Raft group entirely
    /// (learner or voter). If the peer is currently a voter
    /// openraft returns `LearnerNotFound`; demote it via
    /// [`Self::remove_partition_replica`] first.
    pub async fn remove_partition_learner(
        &self,
        partition: PartitionId,
        peer_id: NodeId,
    ) -> Result<(), String> {
        let raft = self
            .partition(partition)
            .ok_or_else(|| format!("partition {} not hosted on this peer", partition.0))?;
        let mut nodes = std::collections::BTreeSet::new();
        nodes.insert(peer_id);
        raft.raft
            .change_membership(openraft::ChangeMembers::RemoveNodes(nodes), false)
            .await
            .map(|_| ())
            .map_err(|e| format!("partition {} RemoveNodes (learner): {e}", partition.0))
    }

    /// Demote an existing voter of `partition` to a learner —
    /// the peer keeps receiving log entries but no longer counts
    /// toward quorum or votes in elections. Useful for shifting a
    /// peer from "active replica" to "read-only" without losing
    /// the data it already has on disk.
    ///
    /// Composes openraft's `RemoveVoters` with `retain = true`
    /// (demote rather than evict). Inverse operation:
    /// [`Self::add_partition_replica`] (which upgrades a learner
    /// to a voter via `AddVoters`).
    pub async fn demote_partition_voter_to_learner(
        &self,
        partition: PartitionId,
        peer_id: NodeId,
    ) -> Result<(), String> {
        let raft = self
            .partition(partition)
            .ok_or_else(|| format!("partition {} not hosted on this peer", partition.0))?;
        let mut to_demote = std::collections::BTreeSet::new();
        to_demote.insert(peer_id);
        raft.raft
            .change_membership(openraft::ChangeMembers::RemoveVoters(to_demote), true)
            .await
            .map(|_| ())
            .map_err(|e| format!("partition {} demote-to-learner: {e}", partition.0))
    }

    /// Returns the set of `(NodeId, role)` pairs for `partition`'s
    /// Raft group — voters and learners. Useful for tests and
    /// observability. Returns `None` when this peer doesn't host
    /// the partition.
    pub async fn partition_membership(
        &self,
        partition: PartitionId,
    ) -> Option<(Vec<NodeId>, Vec<NodeId>)> {
        let raft = self.partition(partition)?;
        let m = raft.raft.metrics().borrow().clone();
        let voter_ids: std::collections::HashSet<u64> =
            m.membership_config.membership().voter_ids().collect();
        let voters: Vec<u64> = voter_ids.iter().copied().collect();
        let learners: Vec<u64> = m
            .membership_config
            .membership()
            .nodes()
            .map(|(id, _)| *id)
            .filter(|id| !voter_ids.contains(id))
            .collect();
        Some((voters, learners))
    }

    /// Linearizable-read primitive for `partition`. Calls openraft's
    /// `ensure_linearizable` on the local partition Raft replica,
    /// which:
    ///   1. Verifies this peer is still the leader of `partition`
    ///      (sends heartbeats to a quorum of followers).
    ///   2. Waits for the local state machine's last_applied to
    ///      catch up to the read_log_id implied by step 1.
    ///
    /// After this returns Ok, a subsequent read of the local store
    /// for keys owned by this partition observes every write that
    /// committed before the call — the canonical Raft linearizability
    /// guarantee.
    ///
    /// Errors:
    ///   * `Err("partition not hosted")` — this peer doesn't replicate
    ///     `partition`. Caller should route the read to a peer that
    ///     does (look up `replica_map`).
    ///   * `Err("not leader: …")` — this peer's replica isn't the
    ///     current leader. Caller should retry against the leader
    ///     (look up `leader_of(partition)`). The error string carries
    ///     the leader hint when openraft surfaces one.
    ///   * `Err("…")` — any other openraft error (election in flight,
    ///     shutting down, etc.).
    ///
    /// Production sites that want linearizable reads should:
    ///   1. Identify the partition for the keys they're reading.
    ///   2. Resolve the partition leader via `leader_of`.
    ///   3. Issue the read RPC to that leader; the leader calls
    ///      this method before serving from its local store.
    ///
    /// The full read-path rewire that does this transparently across
    /// the executor is future work — for now this primitive is
    /// exposed so callers that opt in (a config knob, a Bolt-level
    /// consistency hint, etc.) can build on it.
    pub async fn ensure_partition_linearizable(
        &self,
        partition: PartitionId,
    ) -> Result<(), String> {
        let raft = self
            .partition(partition)
            .ok_or_else(|| format!("partition {} not hosted on this peer", partition.0))?;
        raft.raft
            .ensure_linearizable()
            .await
            .map(|_| ())
            .map_err(|e| format!("ensure_linearizable on partition {}: {e}", partition.0))
    }

    /// Linearizable-read primitive for the metadata Raft group.
    /// Same shape as [`Self::ensure_partition_linearizable`] — must
    /// be called on the meta leader. Useful before reads against
    /// the meta-replicated `ClusterState` (replica map, partition
    /// leader gossip) when stale views aren't acceptable.
    pub async fn ensure_meta_linearizable(&self) -> Result<(), String> {
        self.meta
            .raft
            .ensure_linearizable()
            .await
            .map(|_| ())
            .map_err(|e| format!("ensure_linearizable on meta: {e}"))
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

    /// Drain a peer: remove `peer_id` as a voter from every
    /// partition Raft group it currently belongs to. Called against
    /// any peer in the cluster (typically a healthy survivor); the
    /// drain target itself does not need to be reachable. Returns
    /// the list of partitions that were successfully drained, plus
    /// per-partition errors for the rest. Partial failures are
    /// non-fatal — the operator can rerun the call to retry the
    /// remaining partitions.
    ///
    /// **Doesn't touch the meta group.** Removing the peer from
    /// meta would risk taking down the cluster's DDL path if the
    /// drain target was the meta leader; the operator is expected
    /// to verify with `meta_leader()` before draining the meta
    /// member separately. Future work: a single all-in-one
    /// `decommission` that handles meta + partitions atomically.
    ///
    /// **Caller is responsible for the post-drain rocksdb cleanup**
    /// on the drained peer. The on-disk partition data sticks
    /// around until manually deleted, mirroring `remove_partition_replica`.
    pub async fn drain_peer(
        &self,
        peer_id: NodeId,
    ) -> (Vec<PartitionId>, Vec<(PartitionId, String)>) {
        let mut drained = Vec::new();
        let mut errors = Vec::new();
        // Collect the partition list under the read lock, then
        // release before doing any async work.
        let partitions: Vec<PartitionId> = self
            .partitions
            .read()
            .expect("partitions lock poisoned")
            .keys()
            .copied()
            .collect();
        for partition in partitions {
            // Only attempt to remove if the peer is currently a
            // voter — otherwise change_membership would surface a
            // confusing "not a voter" error.
            let voters = self.current_partition_voters(partition).await;
            if !voters.iter().any(|p| p.0 == peer_id) {
                continue;
            }
            match self.remove_partition_replica(partition, peer_id).await {
                Ok(()) => drained.push(partition),
                Err(e) => errors.push((partition, e)),
            }
        }
        (drained, errors)
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
