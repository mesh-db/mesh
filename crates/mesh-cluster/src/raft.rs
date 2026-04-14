//! openraft integration.
//!
//! Implements the v1 [`RaftStorage`] trait on a single [`MemStore`]
//! backed by an `Arc<RwLock<MemStoreInner>>`, then hands it to
//! [`openraft::storage::Adaptor`] to get the v2 `RaftLogStorage` +
//! `RaftStateMachine` split openraft 0.9's `Raft::new` requires.
//!
//! Includes a [`NoOpNetwork`] suitable for single-node clusters and a
//! [`RaftCluster`] wrapper that bootstraps the Raft instance and exposes
//! `propose` / `current_state`.

use crate::{ClusterCommand, ClusterState, Error, GraphCommand, MeshLogEntry, Result};
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{Adaptor, LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot};
pub use openraft::BasicNode;
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, Raft, RaftStorage, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, Vote,
};
use rocksdb::{Options, WriteBatch, WriteOptions, DB};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug};
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

// Persistent-storage key schema — a single rocksdb namespace is enough for
// everything Raft needs durable:
//   "vote"              → Vote<NodeId>                              (JSON)
//   "last_purged"       → LogId<NodeId>                             (JSON)
//   "log/<be64>"        → Entry<MeshRaftConfig>                     (JSON)
//   "cluster_state"     → ClusterState                              (JSON)
//   "last_applied"      → LogId<NodeId>                             (JSON)
//   "stored_membership" → StoredMembership<NodeId, BasicNode>       (JSON)
//   "snapshot_meta"     → SnapshotMeta<NodeId, BasicNode>           (JSON)
//   "snapshot_data"     → raw snapshot bytes
const VOTE_KEY: &[u8] = b"vote";
const LAST_PURGED_KEY: &[u8] = b"last_purged";
const LOG_PREFIX: &[u8] = b"log/";
const CLUSTER_STATE_KEY: &[u8] = b"cluster_state";
const LAST_APPLIED_KEY: &[u8] = b"last_applied";
const STORED_MEMBERSHIP_KEY: &[u8] = b"stored_membership";
const SNAPSHOT_META_KEY: &[u8] = b"snapshot_meta";
const SNAPSHOT_DATA_KEY: &[u8] = b"snapshot_data";

fn log_key(index: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(LOG_PREFIX.len() + 8);
    k.extend_from_slice(LOG_PREFIX);
    k.extend_from_slice(&index.to_be_bytes());
    k
}

fn synced_writes() -> WriteOptions {
    let mut opts = WriteOptions::default();
    opts.set_sync(true);
    opts
}

fn write_logs_err<E: std::error::Error + 'static>(e: E) -> StorageError<NodeId> {
    StorageIOError::write_logs(&e).into()
}

fn write_vote_err<E: std::error::Error + 'static>(e: E) -> StorageError<NodeId> {
    StorageIOError::write_vote(&e).into()
}

fn write_sm_err<E: std::error::Error + 'static>(e: E) -> StorageError<NodeId> {
    StorageIOError::write_state_machine(&e).into()
}

/// Cluster-wide peer identifier as seen by openraft.
pub type NodeId = u64;

/// Response returned from the state machine when an entry is applied.
/// `error: Some(_)` means the entry committed through Raft (so every replica
/// will observe it) but the local apply step rejected it — for example, a
/// `ClusterCommand::AddPeer` for a peer that's already a member. Callers
/// should treat a populated `error` as a write failure even though the
/// underlying `client_write` returned Ok.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyResponse {
    pub error: Option<String>,
}

impl ApplyResponse {
    pub fn ok() -> Self {
        Self { error: None }
    }

    pub fn err(msg: impl Into<String>) -> Self {
        Self {
            error: Some(msg.into()),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.error.is_none()
    }
}

/// Wire format for a Raft snapshot. Carries both the cluster-metadata
/// state and the opaque graph blob produced by [`GraphStateMachine::snapshot`].
/// On install we split it back apart, hand the graph blob to
/// [`GraphStateMachine::restore`], and replace the cluster state in place.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedSnapshot {
    pub cluster: ClusterState,
    pub graph: Vec<u8>,
}

openraft::declare_raft_types!(
    /// Type binding for Mesh's Raft consensus layer. `D = MeshLogEntry`
    /// so a single Raft group replicates both cluster metadata changes
    /// and graph mutations.
    pub MeshRaftConfig:
        D = MeshLogEntry,
        R = ApplyResponse,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = Entry<MeshRaftConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Pluggable applier for [`GraphCommand`] entries. mesh-cluster doesn't
/// know about [`mesh_storage::Store`] — downstream crates implement this
/// trait and pass an instance to [`RaftCluster::new_with_applier`].
pub trait GraphStateMachine: std::fmt::Debug + Send + Sync + 'static {
    /// Apply a single graph mutation. Errors are surfaced as strings for
    /// simplicity; a future step will introduce a typed error.
    fn apply(&self, command: &GraphCommand) -> std::result::Result<(), String>;

    /// Serialize the current graph state as an opaque snapshot blob. Called
    /// by [`MemSnapshotBuilder::build_snapshot`] so a snapshot also captures
    /// the full graph data (not just cluster metadata).
    ///
    /// Default impl returns an empty blob, suitable for cluster-state-only
    /// callers (tests).
    fn snapshot(&self) -> std::result::Result<Vec<u8>, String> {
        Ok(Vec::new())
    }

    /// Replace the local graph state with the contents of `snapshot`. Called
    /// by `install_snapshot` when a follower receives a snapshot from the
    /// leader. Implementations should treat the snapshot as authoritative
    /// — wipe any prior state before applying.
    fn restore(&self, _snapshot: &[u8]) -> std::result::Result<(), String> {
        Ok(())
    }
}

impl From<crate::PeerId> for NodeId {
    fn from(p: crate::PeerId) -> Self {
        p.0
    }
}

impl From<NodeId> for crate::PeerId {
    fn from(id: NodeId) -> Self {
        crate::PeerId(id)
    }
}

/// Default Raft config tuned for tests — short heartbeats and elections so
/// the single-node bootstrap path resolves quickly. Also enables automatic
/// snapshots after every 16 committed log entries and aggressive log
/// compaction so older entries can be purged once a snapshot covers them.
pub fn default_config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 250,
        election_timeout_min: 500,
        election_timeout_max: 1000,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(16),
        max_in_snapshot_log_to_keep: 0,
        ..openraft::Config::default()
    }
}

// ============================================================================
// MemStore: in-memory RaftStorage (v1) — Adaptor converts it to v2 traits.
// ============================================================================

pub struct MemStoreInner {
    // Log
    pub log: BTreeMap<u64, Entry<MeshRaftConfig>>,
    pub last_purged: Option<LogId<NodeId>>,
    pub vote: Option<Vote<NodeId>>,

    // State machine
    pub state: ClusterState,
    pub last_applied: Option<LogId<NodeId>>,
    pub stored_membership: StoredMembership<NodeId, BasicNode>,
    pub graph_applier: Option<Arc<dyn GraphStateMachine>>,

    // Snapshot
    pub snapshot: Option<(SnapshotMeta<NodeId, BasicNode>, Vec<u8>)>,
    pub snapshot_counter: u64,

    // Durable backing. When `Some`, every mutation to vote / log / last_purged
    // is also written through to this rocksdb handle with sync=true so Raft
    // safety invariants survive process restarts. The state machine itself
    // stays in-memory for now and is rebuilt on restart via log replay.
    pub persistent: Option<Arc<DB>>,
}

impl Debug for MemStoreInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemStoreInner")
            .field("log_entries", &self.log.len())
            .field("last_purged", &self.last_purged)
            .field("vote", &self.vote)
            .field("last_applied", &self.last_applied)
            .field("persistent", &self.persistent.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct MemStore {
    inner: Arc<RwLock<MemStoreInner>>,
}

impl MemStore {
    pub fn new(initial_state: ClusterState) -> Self {
        Self::new_with_applier(initial_state, None)
    }

    pub fn new_with_applier(
        initial_state: ClusterState,
        graph_applier: Option<Arc<dyn GraphStateMachine>>,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(MemStoreInner {
                log: BTreeMap::new(),
                last_purged: None,
                vote: None,
                state: initial_state,
                last_applied: None,
                stored_membership: StoredMembership::default(),
                graph_applier,
                snapshot: None,
                snapshot_counter: 0,
                persistent: None,
            })),
        }
    }

    /// Open a [`MemStore`] backed by a rocksdb instance at `path`. On
    /// restart this hydrates the vote, log entries, last-purged pointer,
    /// state-machine state (cluster state + last_applied + stored
    /// membership), and the most recent snapshot from disk. openraft can
    /// then resume without re-applying committed log entries.
    ///
    /// `initial_state` is only used as the fallback for the very first
    /// open against a fresh data directory.
    ///
    /// The rocksdb handle is opened with `create_if_missing = true`, so a
    /// fresh data directory also works — it'll simply start empty.
    pub fn open_persistent<P: AsRef<Path>>(
        path: P,
        initial_state: ClusterState,
        graph_applier: Option<Arc<dyn GraphStateMachine>>,
    ) -> std::result::Result<Self, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, path)?);

        let vote: Option<Vote<NodeId>> = db
            .get(VOTE_KEY)?
            .and_then(|v| serde_json::from_slice(&v).ok());

        let last_purged: Option<LogId<NodeId>> = db
            .get(LAST_PURGED_KEY)?
            .and_then(|v| serde_json::from_slice(&v).ok());

        let mut log = BTreeMap::new();
        for kv in db.prefix_iterator(LOG_PREFIX) {
            let (key, value) = kv?;
            if !key.starts_with(LOG_PREFIX) {
                break;
            }
            let idx_bytes = &key[LOG_PREFIX.len()..];
            if idx_bytes.len() != 8 {
                continue;
            }
            let idx = u64::from_be_bytes(idx_bytes.try_into().unwrap());
            if let Ok(entry) = serde_json::from_slice::<Entry<MeshRaftConfig>>(&value) {
                log.insert(idx, entry);
            }
        }

        let state: ClusterState = db
            .get(CLUSTER_STATE_KEY)?
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or(initial_state);

        let last_applied: Option<LogId<NodeId>> = db
            .get(LAST_APPLIED_KEY)?
            .and_then(|b| serde_json::from_slice(&b).ok());

        let stored_membership: StoredMembership<NodeId, BasicNode> = db
            .get(STORED_MEMBERSHIP_KEY)?
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();

        let snapshot: Option<(SnapshotMeta<NodeId, BasicNode>, Vec<u8>)> =
            match (db.get(SNAPSHOT_META_KEY)?, db.get(SNAPSHOT_DATA_KEY)?) {
                (Some(meta_bytes), Some(data)) => serde_json::from_slice::<
                    SnapshotMeta<NodeId, BasicNode>,
                >(&meta_bytes)
                .ok()
                .map(|meta| (meta, data)),
                _ => None,
            };

        Ok(Self {
            inner: Arc::new(RwLock::new(MemStoreInner {
                log,
                last_purged,
                vote,
                state,
                last_applied,
                stored_membership,
                graph_applier,
                snapshot,
                snapshot_counter: 0,
                persistent: Some(db),
            })),
        })
    }

    pub fn inner(&self) -> Arc<RwLock<MemStoreInner>> {
        self.inner.clone()
    }
}

impl RaftLogReader<MeshRaftConfig> for MemStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<Entry<MeshRaftConfig>>, StorageError<NodeId>>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let inner = self.inner.read().await;
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftStorage<MeshRaftConfig> for MemStore {
    type LogReader = Self;
    type SnapshotBuilder = MemSnapshotBuilder;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_log_state(
        &mut self,
    ) -> std::result::Result<LogState<MeshRaftConfig>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        let last_purged = inner.last_purged;
        let last = inner
            .log
            .values()
            .next_back()
            .map(|e| e.log_id)
            .or(last_purged);
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        if let Some(db) = inner.persistent.as_ref() {
            let bytes = serde_json::to_vec(vote).map_err(write_vote_err)?;
            db.put_opt(VOTE_KEY, &bytes, &synced_writes())
                .map_err(write_vote_err)?;
        }
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> std::result::Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.read().await.vote)
    }

    async fn append_to_log<I>(
        &mut self,
        entries: I,
    ) -> std::result::Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<MeshRaftConfig>> + OptionalSend,
    {
        let mut inner = self.inner.write().await;
        let entries: Vec<Entry<MeshRaftConfig>> = entries.into_iter().collect();
        if let Some(db) = inner.persistent.as_ref() {
            let mut batch = WriteBatch::default();
            for entry in &entries {
                let value = serde_json::to_vec(entry).map_err(write_logs_err)?;
                batch.put(log_key(entry.log_id.index), value);
            }
            db.write_opt(batch, &synced_writes())
                .map_err(write_logs_err)?;
        }
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        if let Some(db) = inner.persistent.as_ref() {
            let mut batch = WriteBatch::default();
            for idx in inner.log.range(log_id.index..).map(|(k, _)| *k) {
                batch.delete(log_key(idx));
            }
            db.write_opt(batch, &synced_writes())
                .map_err(write_logs_err)?;
        }
        inner.log.retain(|idx, _| *idx < log_id.index);
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        if let Some(db) = inner.persistent.as_ref() {
            let mut batch = WriteBatch::default();
            for idx in inner
                .log
                .range(..=log_id.index)
                .map(|(k, _)| *k)
            {
                batch.delete(log_key(idx));
            }
            let last_purged_bytes =
                serde_json::to_vec(&log_id).map_err(write_logs_err)?;
            batch.put(LAST_PURGED_KEY, last_purged_bytes);
            db.write_opt(batch, &synced_writes())
                .map_err(write_logs_err)?;
        }
        inner.log.retain(|idx, _| *idx > log_id.index);
        inner.last_purged = Some(log_id);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> std::result::Result<
        (Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>),
        StorageError<NodeId>,
    > {
        let inner = self.inner.read().await;
        Ok((inner.last_applied, inner.stored_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<MeshRaftConfig>],
    ) -> std::result::Result<Vec<ApplyResponse>, StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        let graph_applier = inner.graph_applier.clone();
        let mut results = Vec::with_capacity(entries.len());
        for entry in entries {
            inner.last_applied = Some(entry.log_id);
            // Per-entry apply result. Raft progress is unaffected — every
            // replica sees the same Result for the same entry, so we get
            // deterministic divergence rather than silent drift. The
            // proposing client can then turn the error into a typed
            // failure via `propose_entry`.
            let response = match &entry.payload {
                EntryPayload::Blank => ApplyResponse::ok(),
                EntryPayload::Normal(log_entry) => match log_entry {
                    MeshLogEntry::Cluster(cmd) => match inner.state.apply(cmd) {
                        Ok(()) => ApplyResponse::ok(),
                        Err(e) => ApplyResponse::err(e.to_string()),
                    },
                    MeshLogEntry::Graph(cmd) => match &graph_applier {
                        Some(applier) => match applier.apply(cmd) {
                            Ok(()) => ApplyResponse::ok(),
                            Err(e) => ApplyResponse::err(e),
                        },
                        None => ApplyResponse::ok(),
                    },
                },
                EntryPayload::Membership(m) => {
                    inner.stored_membership =
                        StoredMembership::new(Some(entry.log_id), m.clone());
                    ApplyResponse::ok()
                }
            };
            if let Some(err) = response.error.as_ref() {
                tracing::warn!(
                    log_index = entry.log_id.index,
                    error = %err,
                    "state machine apply rejected entry"
                );
            }
            results.push(response);
        }
        // Flush state-machine state in one synced rocksdb batch. Persisting
        // last_applied + cluster_state + stored_membership together means a
        // restart can pick up exactly where we left off, instead of
        // re-applying every committed log entry from the beginning.
        if let Some(db) = inner.persistent.as_ref() {
            let mut batch = WriteBatch::default();
            batch.put(
                CLUSTER_STATE_KEY,
                serde_json::to_vec(&inner.state).map_err(write_sm_err)?,
            );
            if let Some(la) = inner.last_applied.as_ref() {
                batch.put(
                    LAST_APPLIED_KEY,
                    serde_json::to_vec(la).map_err(write_sm_err)?,
                );
            }
            batch.put(
                STORED_MEMBERSHIP_KEY,
                serde_json::to_vec(&inner.stored_membership).map_err(write_sm_err)?,
            );
            db.write_opt(batch, &synced_writes())
                .map_err(write_sm_err)?;
        }
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MemSnapshotBuilder {
            inner: self.inner.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let payload: PersistedSnapshot =
            serde_json::from_slice(&data).map_err(|e| {
                StorageError::from(StorageIOError::read_snapshot(
                    Some(meta.signature()),
                    &e,
                ))
            })?;
        let mut inner = self.inner.write().await;
        inner.state = payload.cluster;
        inner.last_applied = meta.last_log_id;
        inner.stored_membership = meta.last_membership.clone();
        inner.snapshot = Some((meta.clone(), data.clone()));

        // Hand the opaque graph blob to the applier so the local store
        // ends up exactly matching the leader's. Done while holding the
        // inner lock so concurrent applies can't interleave with restore.
        if let Some(applier) = &inner.graph_applier {
            applier.restore(&payload.graph).map_err(|e| {
                StorageError::from(StorageIOError::read_snapshot(
                    Some(meta.signature()),
                    &std::io::Error::other(e),
                ))
            })?;
        }
        tracing::info!(
            snapshot_id = %meta.snapshot_id,
            last_log = ?meta.last_log_id,
            graph_bytes = payload.graph.len(),
            "installed raft snapshot from leader"
        );

        if let Some(db) = inner.persistent.as_ref() {
            let mut batch = WriteBatch::default();
            batch.put(
                CLUSTER_STATE_KEY,
                serde_json::to_vec(&inner.state).map_err(write_sm_err)?,
            );
            if let Some(la) = inner.last_applied.as_ref() {
                batch.put(
                    LAST_APPLIED_KEY,
                    serde_json::to_vec(la).map_err(write_sm_err)?,
                );
            }
            batch.put(
                STORED_MEMBERSHIP_KEY,
                serde_json::to_vec(&inner.stored_membership).map_err(write_sm_err)?,
            );
            batch.put(
                SNAPSHOT_META_KEY,
                serde_json::to_vec(meta).map_err(write_sm_err)?,
            );
            batch.put(SNAPSHOT_DATA_KEY, &data);
            db.write_opt(batch, &synced_writes())
                .map_err(write_sm_err)?;
        }
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<MeshRaftConfig>>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        Ok(inner.snapshot.as_ref().map(|(meta, data)| Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(data.clone())),
        }))
    }
}

pub struct MemSnapshotBuilder {
    inner: Arc<RwLock<MemStoreInner>>,
}

impl RaftSnapshotBuilder<MeshRaftConfig> for MemSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<MeshRaftConfig>, StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        inner.snapshot_counter += 1;
        // Bundle cluster metadata + the graph applier's opaque blob into
        // a single PersistedSnapshot. A snapshot received by a follower
        // restores BOTH halves so a brand-new replica can catch up from
        // an InstallSnapshot alone, without needing the leader's full log.
        let graph = if let Some(applier) = &inner.graph_applier {
            applier
                .snapshot()
                .map_err(|e| StorageIOError::read_state_machine(&std::io::Error::other(e)))?
        } else {
            Vec::new()
        };
        let payload = PersistedSnapshot {
            cluster: inner.state.clone(),
            graph,
        };
        let data = serde_json::to_vec(&payload).map_err(|e| {
            StorageError::from(StorageIOError::read_state_machine(&e))
        })?;
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.stored_membership.clone(),
            snapshot_id: format!("mesh-snapshot-{}", inner.snapshot_counter),
        };
        inner.snapshot = Some((meta.clone(), data.clone()));

        if let Some(db) = inner.persistent.as_ref() {
            let mut batch = WriteBatch::default();
            batch.put(
                SNAPSHOT_META_KEY,
                serde_json::to_vec(&meta).map_err(write_sm_err)?,
            );
            batch.put(SNAPSHOT_DATA_KEY, &data);
            db.write_opt(batch, &synced_writes())
                .map_err(write_sm_err)?;
        }

        tracing::info!(
            snapshot_id = %meta.snapshot_id,
            last_applied = ?meta.last_log_id,
            graph_bytes = payload.graph.len(),
            total_bytes = data.len(),
            "built raft snapshot"
        );

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

// ============================================================================
// NoOpNetwork: single-node stub
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct NoOpNetwork;

impl RaftNetworkFactory<MeshRaftConfig> for NoOpNetwork {
    type Network = NoOpNetworkClient;

    async fn new_client(&mut self, _target: NodeId, _node: &BasicNode) -> Self::Network {
        NoOpNetworkClient
    }
}

#[derive(Debug, Default)]
pub struct NoOpNetworkClient;

fn unreachable<E>() -> RPCError<NodeId, BasicNode, E>
where
    E: std::error::Error,
{
    RPCError::Network(NetworkError::new(&std::io::Error::new(
        std::io::ErrorKind::ConnectionRefused,
        "single-node cluster has no peers",
    )))
}

impl RaftNetwork<MeshRaftConfig> for NoOpNetworkClient {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<MeshRaftConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        Err(unreachable())
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<MeshRaftConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        Err(unreachable())
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> std::result::Result<
        VoteResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        Err(unreachable())
    }
}

// ============================================================================
// RaftCluster: wraps a Raft instance with its state handle
// ============================================================================

pub struct RaftCluster {
    pub id: NodeId,
    pub raft: Raft<MeshRaftConfig>,
    store: MemStore,
}

impl RaftCluster {
    /// Create a Raft instance with the given network factory. Does **not**
    /// call `initialize` — the caller is responsible for bootstrapping the
    /// cluster exactly once on the designated seed peer via [`initialize`].
    pub async fn new<N>(
        id: NodeId,
        initial_state: ClusterState,
        network: N,
    ) -> Result<Self>
    where
        N: RaftNetworkFactory<MeshRaftConfig>,
    {
        Self::new_with_applier(id, initial_state, network, None).await
    }

    /// Like [`new`], but accepts a [`GraphStateMachine`] that handles
    /// `MeshLogEntry::Graph` entries on top of the cluster-metadata state.
    pub async fn new_with_applier<N>(
        id: NodeId,
        initial_state: ClusterState,
        network: N,
        graph_applier: Option<Arc<dyn GraphStateMachine>>,
    ) -> Result<Self>
    where
        N: RaftNetworkFactory<MeshRaftConfig>,
    {
        let store = MemStore::new_with_applier(initial_state, graph_applier);
        Self::from_store(id, store, network).await
    }

    /// Open a [`RaftCluster`] whose storage is backed by a rocksdb instance
    /// at `path`. The log and vote are hydrated from disk on startup; the
    /// state machine rebuilds via log replay as openraft re-applies
    /// committed entries.
    pub async fn open_persistent<N, P>(
        id: NodeId,
        path: P,
        initial_state: ClusterState,
        network: N,
        graph_applier: Option<Arc<dyn GraphStateMachine>>,
    ) -> Result<Self>
    where
        N: RaftNetworkFactory<MeshRaftConfig>,
        P: AsRef<Path>,
    {
        let store = MemStore::open_persistent(path, initial_state, graph_applier)
            .map_err(|e| Error::Raft(format!("opening persistent raft store: {e}")))?;
        Self::from_store(id, store, network).await
    }

    async fn from_store<N>(id: NodeId, store: MemStore, network: N) -> Result<Self>
    where
        N: RaftNetworkFactory<MeshRaftConfig>,
    {
        let config = Arc::new(
            default_config()
                .validate()
                .map_err(|e| Error::Raft(e.to_string()))?,
        );
        let (log_store, state_machine) = Adaptor::new(store.clone());
        let raft = Raft::new(id, config, network, log_store, state_machine)
            .await
            .map_err(|e| Error::Raft(e.to_string()))?;
        Ok(Self { id, raft, store })
    }

    /// Bootstrap the cluster with the provided member set. Only call on the
    /// seed peer.
    pub async fn initialize(&self, members: BTreeMap<NodeId, BasicNode>) -> Result<()> {
        self.raft
            .initialize(members)
            .await
            .map_err(|e| Error::Raft(e.to_string()))
    }

    /// Convenience: create a single-node cluster and initialize it in one step.
    pub async fn new_single_node(id: NodeId, initial_state: ClusterState) -> Result<Self> {
        let cluster = Self::new(id, initial_state, NoOpNetwork).await?;
        let mut members = BTreeSet::new();
        members.insert(id);
        cluster
            .raft
            .initialize(members)
            .await
            .map_err(|e| Error::Raft(e.to_string()))?;
        Ok(cluster)
    }

    /// Propose a [`ClusterCommand`] entry. Wraps internally as
    /// [`MeshLogEntry::Cluster`] so existing callers don't need to touch
    /// the entry type.
    pub async fn propose(&self, command: ClusterCommand) -> Result<ApplyResponse> {
        self.propose_entry(MeshLogEntry::Cluster(command)).await
    }

    /// Propose a [`GraphCommand`] entry that the configured
    /// [`GraphStateMachine`] will apply on every replica.
    pub async fn propose_graph(&self, command: GraphCommand) -> Result<ApplyResponse> {
        self.propose_entry(MeshLogEntry::Graph(command)).await
    }

    /// Propose an arbitrary [`MeshLogEntry`]. Returns
    /// [`Error::ForwardToLeader`] when called on a non-leader peer so the
    /// caller can route the request to the actual leader, and
    /// [`Error::Apply`] when the entry committed through Raft but the
    /// local state machine rejected it (e.g. duplicate AddPeer).
    #[tracing::instrument(level = "debug", skip_all, fields(node_id = self.id))]
    pub async fn propose_entry(&self, entry: MeshLogEntry) -> Result<ApplyResponse> {
        use openraft::error::{ClientWriteError, ForwardToLeader, RaftError};
        match self.raft.client_write(entry).await {
            Ok(response) => match response.data.error {
                Some(err) => Err(Error::Apply(err)),
                None => Ok(response.data),
            },
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(
                ForwardToLeader {
                    leader_id,
                    leader_node,
                    ..
                },
            ))) => Err(Error::ForwardToLeader {
                leader_id: leader_id.map(crate::PeerId),
                leader_address: leader_node.map(|n| n.addr),
            }),
            Err(e) => Err(Error::Raft(e.to_string())),
        }
    }

    pub async fn current_state(&self) -> ClusterState {
        self.store.inner().read().await.state.clone()
    }

    pub async fn shutdown(self) -> Result<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| Error::Raft(e.to_string()))
    }
}
