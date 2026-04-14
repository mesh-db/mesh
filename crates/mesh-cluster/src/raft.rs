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

use crate::{ClusterCommand, ClusterState, Error, Result};
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
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Cluster-wide peer identifier as seen by openraft.
pub type NodeId = u64;

/// Response returned from the state machine when an entry is applied.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyResponse;

openraft::declare_raft_types!(
    /// Type binding for Mesh's Raft consensus layer.
    pub MeshRaftConfig:
        D = ClusterCommand,
        R = ApplyResponse,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = Entry<MeshRaftConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

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
/// the single-node bootstrap path resolves quickly.
pub fn default_config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 250,
        election_timeout_min: 500,
        election_timeout_max: 1000,
        ..openraft::Config::default()
    }
}

// ============================================================================
// MemStore: in-memory RaftStorage (v1) — Adaptor converts it to v2 traits.
// ============================================================================

#[derive(Debug)]
pub struct MemStoreInner {
    // Log
    pub log: BTreeMap<u64, Entry<MeshRaftConfig>>,
    pub last_purged: Option<LogId<NodeId>>,
    pub vote: Option<Vote<NodeId>>,

    // State machine
    pub state: ClusterState,
    pub last_applied: Option<LogId<NodeId>>,
    pub stored_membership: StoredMembership<NodeId, BasicNode>,

    // Snapshot
    pub snapshot: Option<(SnapshotMeta<NodeId, BasicNode>, Vec<u8>)>,
    pub snapshot_counter: u64,
}

#[derive(Debug, Clone)]
pub struct MemStore {
    inner: Arc<RwLock<MemStoreInner>>,
}

impl MemStore {
    pub fn new(initial_state: ClusterState) -> Self {
        Self {
            inner: Arc::new(RwLock::new(MemStoreInner {
                log: BTreeMap::new(),
                last_purged: None,
                vote: None,
                state: initial_state,
                last_applied: None,
                stored_membership: StoredMembership::default(),
                snapshot: None,
                snapshot_counter: 0,
            })),
        }
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
        self.inner.write().await.vote = Some(*vote);
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
        inner.log.retain(|idx, _| *idx < log_id.index);
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
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
        let mut results = Vec::with_capacity(entries.len());
        for entry in entries {
            inner.last_applied = Some(entry.log_id);
            match &entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(cmd) => {
                    // Apply errors are swallowed so Raft keeps making progress;
                    // a follow-up step will surface them through ApplyResponse.
                    let _ = inner.state.apply(cmd);
                }
                EntryPayload::Membership(m) => {
                    inner.stored_membership =
                        StoredMembership::new(Some(entry.log_id), m.clone());
                }
            }
            results.push(ApplyResponse);
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
        let state: ClusterState = serde_json::from_slice(&data).map_err(|e| {
            StorageError::from(StorageIOError::read_snapshot(Some(meta.signature()), &e))
        })?;
        let mut inner = self.inner.write().await;
        inner.state = state;
        inner.last_applied = meta.last_log_id;
        inner.stored_membership = meta.last_membership.clone();
        inner.snapshot = Some((meta.clone(), data));
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
        let data = serde_json::to_vec(&inner.state).map_err(|e| {
            StorageError::from(StorageIOError::read_state_machine(&e))
        })?;
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.stored_membership.clone(),
            snapshot_id: format!("mesh-snapshot-{}", inner.snapshot_counter),
        };
        inner.snapshot = Some((meta.clone(), data.clone()));
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
        let config = Arc::new(
            default_config()
                .validate()
                .map_err(|e| Error::Raft(e.to_string()))?,
        );
        let store = MemStore::new(initial_state);
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

    pub async fn propose(&self, command: ClusterCommand) -> Result<ApplyResponse> {
        let response = self
            .raft
            .client_write(command)
            .await
            .map_err(|e| Error::Raft(e.to_string()))?;
        Ok(response.data)
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
