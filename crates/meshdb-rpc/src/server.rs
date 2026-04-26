use crate::convert::{
    edge_from_proto, edge_id_from_proto, edge_to_proto, node_from_proto, node_id_from_proto,
    node_to_proto, uuid_to_proto,
};
use crate::executor_writer::BufferingGraphWriter;
use crate::partitioned_reader::PartitionedGraphReader;
use crate::proto::mesh_query_server::{MeshQuery, MeshQueryServer};
use crate::proto::mesh_write_client::MeshWriteClient;
use crate::proto::mesh_write_server::{MeshWrite, MeshWriteServer};
use crate::proto::{
    AllNodeIdsRequest, AllNodeIdsResponse, BatchPhase, BatchWriteRequest, BatchWriteResponse,
    ConstraintKind as ProtoConstraintKind, ConstraintScopeKind as ProtoConstraintScopeKind,
    CreateEdgePointIndexRequest, CreateEdgePointIndexResponse, CreateEdgePropertyIndexRequest,
    CreateEdgePropertyIndexResponse, CreatePointIndexRequest, CreatePointIndexResponse,
    CreatePropertyConstraintRequest, CreatePropertyConstraintResponse, CreatePropertyIndexRequest,
    CreatePropertyIndexResponse, DeleteEdgeRequest, DeleteEdgeResponse, DetachDeleteNodeRequest,
    DetachDeleteNodeResponse, DropEdgePointIndexRequest, DropEdgePointIndexResponse,
    DropEdgePropertyIndexRequest, DropEdgePropertyIndexResponse, DropPointIndexRequest,
    DropPointIndexResponse, DropPropertyConstraintRequest, DropPropertyConstraintResponse,
    DropPropertyIndexRequest, DropPropertyIndexResponse, EdgesByPropertyRequest,
    EdgesByPropertyResponse, ExecuteCypherRequest, ExecuteCypherResponse, GetEdgeRequest,
    GetEdgeResponse, GetNodeRequest, GetNodeResponse, HealthRequest, HealthResponse, NeighborInfo,
    NeighborRequest, NeighborResponse, NodesByLabelRequest, NodesByLabelResponse,
    NodesByPropertyRequest, NodesByPropertyResponse, PropertyTypeKind as ProtoPropertyTypeKind,
    PutEdgeRequest, PutEdgeResponse, PutNodeRequest, PutNodeResponse, ResolveTransactionRequest,
    ResolveTransactionResponse,
};
use crate::raft_applier::{storage_kind, storage_scope};
use crate::routing::Routing;
use crate::tx_coordinator::TxCoordinator;
use meshdb_cluster::raft::RaftCluster;
use meshdb_cluster::{
    resolved_constraint_name, ConstraintKind as ClusterConstraintKind,
    ConstraintScope as ClusterConstraintScope, Error as ClusterError, GraphCommand, PeerId,
    PropertyType as ClusterPropertyType,
};
use meshdb_executor::{
    execute_with_reader_and_procs, GraphReader, GraphWriter, ProcedureRegistry,
    StorageReaderAdapter,
};
use meshdb_storage::StorageEngine;
use std::collections::HashSet;
use std::sync::Arc;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Response, Status};

/// Default registry factory. Constructs a fresh `ProcedureRegistry`
/// with the built-ins registered — matches the behaviour that
/// existed before [`MeshService::with_procedure_registry_factory`]
/// existed, so call sites that don't need a custom registry don't
/// have to think about it.
fn default_registry_factory() -> Arc<dyn Fn() -> ProcedureRegistry + Send + Sync> {
    Arc::new(|| {
        let mut p = ProcedureRegistry::new();
        p.register_defaults();
        p
    })
}

/// Compute the pre-commit trigger diff from a prepared command
/// batch. `created_*` entries come from `PutNode` / `PutEdge`
/// commands whose id wasn't yet in the store; `deleted_*`
/// entries come from `DeleteEdge` / `DetachDeleteNode` commands
/// resolved against the pre-commit store snapshot. When a tx
/// issues multiple writes for the same id (e.g. CREATE then
/// SET), last-write wins — trigger bodies see the final shape.
#[cfg(feature = "apoc-trigger")]
fn compute_trigger_diff(
    store: &dyn StorageEngine,
    commands: &[GraphCommand],
) -> meshdb_executor::apoc_trigger::TriggerDiff {
    use meshdb_executor::apoc_trigger::TriggerDiff;
    let mut diff = TriggerDiff::default();
    // Flatten Batch variants and restrict to the four
    // data-mutation types — DDL / index / constraint commands
    // aren't part of the trigger surface.
    let mut flat: Vec<&GraphCommand> = Vec::with_capacity(commands.len());
    fn collect<'a>(out: &mut Vec<&'a GraphCommand>, cmds: &'a [GraphCommand]) {
        for c in cmds {
            match c {
                GraphCommand::Batch(inner) => collect(out, inner),
                _ => out.push(c),
            }
        }
    }
    collect(&mut flat, commands);
    // Track last-write-wins per node/edge id so CREATE+SET in
    // the same tx yields one entry with the final shape.
    let mut created_nodes: std::collections::HashMap<meshdb_core::NodeId, meshdb_core::Node> =
        std::collections::HashMap::new();
    let mut created_edges: std::collections::HashMap<meshdb_core::EdgeId, meshdb_core::Edge> =
        std::collections::HashMap::new();
    for cmd in flat {
        match cmd {
            GraphCommand::PutNode(n) => match store.get_node(n.id) {
                Ok(Some(prev)) => {
                    diff_node_changes(&prev, n, &mut diff);
                }
                Ok(None) => {
                    created_nodes.insert(n.id, n.clone());
                }
                Err(_) => {
                    created_nodes.insert(n.id, n.clone());
                }
            },
            GraphCommand::PutEdge(e) => match store.get_edge(e.id) {
                Ok(Some(prev)) => {
                    diff_edge_changes(&prev, e, &mut diff);
                }
                Ok(None) => {
                    created_edges.insert(e.id, e.clone());
                }
                Err(_) => {
                    created_edges.insert(e.id, e.clone());
                }
            },
            GraphCommand::DeleteEdge(id) => {
                if let Ok(Some(prev)) = store.get_edge(*id) {
                    diff.deleted_relationships.push(prev);
                }
            }
            GraphCommand::DetachDeleteNode(id) => {
                if let Ok(Some(prev)) = store.get_node(*id) {
                    diff.deleted_nodes.push(prev);
                }
                if let Ok(outs) = store.outgoing(*id) {
                    for (eid, _) in outs {
                        if let Ok(Some(e)) = store.get_edge(eid) {
                            diff.deleted_relationships.push(e);
                        }
                    }
                }
                if let Ok(ins) = store.incoming(*id) {
                    for (eid, _) in ins {
                        if let Ok(Some(e)) = store.get_edge(eid) {
                            diff.deleted_relationships.push(e);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    diff.created_nodes = created_nodes.into_values().collect();
    diff.created_relationships = created_edges.into_values().collect();
    diff
}

/// Compare pre-tx and post-tx node values, pushing per-property
/// and per-label change records to the diff. Used by
/// [`compute_trigger_diff`] for `PutNode`s targeting existing
/// ids — these are property/label updates rather than creates.
#[cfg(feature = "apoc-trigger")]
fn diff_node_changes(
    prev: &meshdb_core::Node,
    next: &meshdb_core::Node,
    diff: &mut meshdb_executor::apoc_trigger::TriggerDiff,
) {
    use meshdb_executor::apoc_trigger::{LabelChange, PropertyChange};
    use meshdb_executor::Value;
    let element = Value::Node(next.clone());
    // Properties: detect added, changed, and removed keys.
    for (key, new_val) in &next.properties {
        let old_val = prev.properties.get(key);
        if old_val != Some(new_val) {
            diff.assigned_node_properties.push(PropertyChange {
                key: key.clone(),
                old: old_val.cloned(),
                new: Some(new_val.clone()),
                element: element.clone(),
            });
        }
    }
    for (key, old_val) in &prev.properties {
        if !next.properties.contains_key(key) {
            diff.removed_node_properties.push(PropertyChange {
                key: key.clone(),
                old: Some(old_val.clone()),
                new: None,
                element: element.clone(),
            });
        }
    }
    // Labels: O(n*m) is fine — label sets are tiny.
    for label in &next.labels {
        if !prev.labels.iter().any(|l| l == label) {
            diff.assigned_labels.push(LabelChange {
                label: label.clone(),
                element: element.clone(),
            });
        }
    }
    for label in &prev.labels {
        if !next.labels.iter().any(|l| l == label) {
            diff.removed_labels.push(LabelChange {
                label: label.clone(),
                element: element.clone(),
            });
        }
    }
}

/// Edge counterpart of [`diff_node_changes`]. Edges don't have
/// labels, so only the property side fires.
#[cfg(feature = "apoc-trigger")]
fn diff_edge_changes(
    prev: &meshdb_core::Edge,
    next: &meshdb_core::Edge,
    diff: &mut meshdb_executor::apoc_trigger::TriggerDiff,
) {
    use meshdb_executor::apoc_trigger::PropertyChange;
    use meshdb_executor::Value;
    let element = Value::Edge(next.clone());
    for (key, new_val) in &next.properties {
        let old_val = prev.properties.get(key);
        if old_val != Some(new_val) {
            diff.assigned_relationship_properties.push(PropertyChange {
                key: key.clone(),
                old: old_val.cloned(),
                new: Some(new_val.clone()),
                element: element.clone(),
            });
        }
    }
    for (key, old_val) in &prev.properties {
        if !next.properties.contains_key(key) {
            diff.removed_relationship_properties.push(PropertyChange {
                key: key.clone(),
                old: Some(old_val.clone()),
                new: None,
                element: element.clone(),
            });
        }
    }
}

#[derive(Clone)]
pub struct MeshService {
    store: Arc<dyn StorageEngine>,
    routing: Option<Arc<Routing>>,
    raft: Option<Arc<RaftCluster>>,
    /// Multi-raft cluster handle for `mode = "multi-raft"`. When
    /// present, single-partition writes are routed through the
    /// partition's Raft group (forwarded to the leader's peer when
    /// this isn't it); cross-partition writes ride a Spanner-style
    /// 2PC over partition Rafts. Mutually exclusive with `routing`
    /// and `raft` — at most one of the three is set.
    multi_raft: Option<Arc<crate::MultiRaftCluster>>,
    /// Per-service 2PC participant staging. Bounded-lifetime map of
    /// txid → staged commands with a background sweeper that drops
    /// entries older than the configured TTL. Shared across all
    /// routing-mode peers so a `BatchWrite(Prepare)` on any gRPC
    /// worker lands in the same map the sweeper walks.
    pending_batches: Arc<crate::ParticipantStaging>,
    /// Durable 2PC coordinator log. Only populated in routing mode,
    /// where multi-peer transactions need a crash-recovery record.
    coordinator_log: Option<Arc<crate::CoordinatorLog>>,
    /// Durable 2PC participant log. Every PREPARE / COMMIT / ABORT
    /// that reaches this peer gets an fsync'd entry here before the
    /// RPC ACKs, so a crash between PREPARE ACK and COMMIT doesn't
    /// lose the staged commands. `None` falls back to the in-memory-
    /// only behaviour — acceptable for single-node tests but not for
    /// a production routing cluster.
    participant_log: Option<Arc<crate::ParticipantLog>>,
    /// Per-phase deadlines applied to every peer RPC the coordinator
    /// issues on this service. Defaulted at construction; overrides
    /// exist for tests that need millisecond-scale deadlines to
    /// exercise the timeout paths.
    tx_timeouts: crate::TxCoordinatorTimeouts,
    /// Client TLS config used when this service builds ad-hoc gRPC
    /// endpoints (leader forwarding in Raft mode, for example). `None`
    /// means inter-peer gRPC traffic is plaintext. The `Routing` /
    /// `GrpcNetwork` channels carry their own TLS config; this field
    /// exists for the callsites that construct endpoints from a bare
    /// `&str` address rather than looking them up in `Routing`.
    client_tls: Option<ClientTlsConfig>,
    /// Factory for the per-request `ProcedureRegistry`. Default
    /// registers the built-ins that ship unconditionally
    /// (`db.labels()` and the APOC features that compiled in).
    /// meshdb-server overrides this at startup to splice in the
    /// operator-configured [`ImportConfig`] for `apoc.load.*`,
    /// without any feature-gated surface leaking into
    /// `MeshService` itself. Arc so cheap-to-clone for the
    /// spawned per-request closures.
    procedure_registry_factory: Arc<dyn Fn() -> ProcedureRegistry + Send + Sync>,
    /// Test-only fault-injection handle. `None` in release builds
    /// (the field itself is cfg-gated). Threaded into every
    /// [`TxCoordinator`] spawned off this service and consulted
    /// inside the participant `batch_write` and
    /// `resolve_transaction` handlers.
    #[cfg(any(test, feature = "fault-inject"))]
    fault_points: Option<Arc<crate::FaultPoints>>,
}

impl MeshService {
    /// Local-only service: every request is answered from the local store.
    pub fn new(store: Arc<dyn StorageEngine>) -> Self {
        Self {
            store,
            routing: None,
            multi_raft: None,
            raft: None,
            pending_batches: crate::ParticipantStaging::with_default_ttl(),
            coordinator_log: None,
            participant_log: None,
            tx_timeouts: crate::TxCoordinatorTimeouts::default(),
            client_tls: None,
            procedure_registry_factory: default_registry_factory(),
            #[cfg(any(test, feature = "fault-inject"))]
            fault_points: None,
        }
    }

    /// Routed service without a durable coordinator log. Equivalent
    /// to [`with_routing_and_log`] passing `None`.
    pub fn with_routing(store: Arc<dyn StorageEngine>, routing: Arc<Routing>) -> Self {
        Self::with_routing_and_log(store, routing, None)
    }

    /// Routed service with an optional durable coordinator log. The
    /// log records 2PC progress so a coordinator crash between PREPARE
    /// and COMMIT can be recovered via
    /// [`Self::recover_pending_transactions`] at startup.
    pub fn with_routing_and_log(
        store: Arc<dyn StorageEngine>,
        routing: Arc<Routing>,
        coordinator_log: Option<Arc<crate::CoordinatorLog>>,
    ) -> Self {
        Self {
            store,
            routing: Some(routing),
            multi_raft: None,
            raft: None,
            pending_batches: crate::ParticipantStaging::with_default_ttl(),
            coordinator_log,
            participant_log: None,
            tx_timeouts: crate::TxCoordinatorTimeouts::default(),
            client_tls: None,
            procedure_registry_factory: default_registry_factory(),
            #[cfg(any(test, feature = "fault-inject"))]
            fault_points: None,
        }
    }

    /// Attach a [`ParticipantLog`] so PREPARE / COMMIT / ABORT
    /// transitions are fsync'd before the RPC ACKs. Required for
    /// durable participant-side 2PC in routing mode; omit (or pass
    /// `None`) for local-only services that don't run the 2PC path.
    pub fn with_participant_log(mut self, log: Option<Arc<crate::ParticipantLog>>) -> Self {
        self.participant_log = log;
        self
    }

    /// Override the per-phase 2PC coordinator deadlines applied to
    /// every peer RPC this service issues as coordinator. Defaults
    /// come from [`crate::TxCoordinatorTimeouts::default`]; tests
    /// that want to exercise the timeout branch pass shorter values.
    pub fn with_tx_timeouts(mut self, timeouts: crate::TxCoordinatorTimeouts) -> Self {
        self.tx_timeouts = timeouts;
        self
    }

    /// Raft-backed service: writes go through `RaftCluster::propose_graph`
    /// so every replica's local store ends up with the same data via the
    /// state machine's apply path. Reads go straight to the local store
    /// (every peer holds the full graph in this single-Raft-group model).
    pub fn with_raft(store: Arc<dyn StorageEngine>, raft: Arc<RaftCluster>) -> Self {
        Self {
            store,
            routing: None,
            multi_raft: None,
            raft: Some(raft),
            pending_batches: crate::ParticipantStaging::with_default_ttl(),
            coordinator_log: None,
            participant_log: None,
            tx_timeouts: crate::TxCoordinatorTimeouts::default(),
            client_tls: None,
            procedure_registry_factory: default_registry_factory(),
            #[cfg(any(test, feature = "fault-inject"))]
            fault_points: None,
        }
    }

    /// Multi-raft-backed service: per-partition Raft groups for data
    /// writes, a metadata Raft group for DDL. Single-partition writes
    /// arriving on a non-leader peer are server-side proxied to the
    /// partition leader via `MeshWrite::ForwardWrite` — clients see
    /// one consistent endpoint for the lifetime of a session, even
    /// when partition leadership shifts.
    pub fn with_multi_raft(
        store: Arc<dyn StorageEngine>,
        multi_raft: Arc<crate::MultiRaftCluster>,
    ) -> Self {
        Self {
            store,
            routing: None,
            raft: None,
            multi_raft: Some(multi_raft),
            pending_batches: crate::ParticipantStaging::with_default_ttl(),
            coordinator_log: None,
            participant_log: None,
            tx_timeouts: crate::TxCoordinatorTimeouts::default(),
            client_tls: None,
            procedure_registry_factory: default_registry_factory(),
            #[cfg(any(test, feature = "fault-inject"))]
            fault_points: None,
        }
    }

    /// Attach a coordinator log to a multi-raft service so cross-
    /// partition decisions are durable. The log records `Prepared` /
    /// `CommitDecision` / `AbortDecision` / `Completed` entries; new
    /// partition leaders use `ResolveTransaction` to read each peer's
    /// log when resolving in-doubt PREPAREs after a leader change.
    pub fn with_coordinator_log(mut self, log: Option<Arc<crate::CoordinatorLog>>) -> Self {
        self.coordinator_log = log;
        self
    }

    /// Override the per-request [`ProcedureRegistry`] factory. The
    /// closure is called every time a Cypher query needs a
    /// registry; meshdb-server uses this to bake the operator-
    /// configured `ImportConfig` in without leaking an apoc-load
    /// cfg cascade into `MeshService`.
    pub fn with_procedure_registry_factory<F>(mut self, factory: F) -> Self
    where
        F: Fn() -> ProcedureRegistry + Send + Sync + 'static,
    {
        self.procedure_registry_factory = Arc::new(factory);
        self
    }

    /// Set the client TLS config used for ad-hoc gRPC endpoints built
    /// inside the service — currently the two leader-forwarding sites
    /// that construct an [`Endpoint`] from a peer address each call.
    /// `None` (the default) means inter-peer gRPC traffic is plaintext
    /// and URIs use `http://`; `Some(cfg)` switches to `https://` and
    /// applies the TLS config on every outgoing channel.
    pub fn with_client_tls(mut self, tls: Option<ClientTlsConfig>) -> Self {
        self.client_tls = tls;
        self
    }

    /// Build an [`Endpoint`] for a peer address, applying this
    /// service's client TLS config when one is configured. Used by the
    /// leader-forwarding helpers below.
    fn peer_endpoint(&self, addr: &str) -> Result<Endpoint, Status> {
        let scheme = if self.client_tls.is_some() {
            "https"
        } else {
            "http"
        };
        let uri = format!("{scheme}://{addr}");
        let mut endpoint = Endpoint::from_shared(uri)
            .map_err(|e| Status::internal(format!("invalid peer address {addr}: {e}")))?;
        if let Some(tls) = self.client_tls.clone() {
            endpoint = endpoint.tls_config(tls).map_err(|e| {
                Status::internal(format!("applying tls to peer endpoint {addr}: {e}"))
            })?;
        }
        Ok(endpoint)
    }

    /// Replace the default participant staging with a custom one.
    /// Used by tests that want a short TTL (milliseconds) so the
    /// sweeper behavior is observable without waiting 60s.
    pub fn with_staging(mut self, staging: Arc<crate::ParticipantStaging>) -> Self {
        self.pending_batches = staging;
        self
    }

    /// Attach a shared [`FaultPoints`](crate::FaultPoints) handle so
    /// integration tests can drive deterministic crash paths
    /// through the coordinator and participant 2PC code. Zero cost
    /// in release builds — both the field and the checkpoint
    /// branches are cfg-gated behind `fault-inject`.
    #[cfg(any(test, feature = "fault-inject"))]
    pub fn with_fault_points(mut self, fp: Option<Arc<crate::FaultPoints>>) -> Self {
        self.fault_points = fp;
        self
    }

    /// Borrow the currently-attached [`FaultPoints`], if any. The
    /// meshdb-server test harness uses this to pull the handle back
    /// out of a freshly-built [`ServerComponents`] so the test can
    /// flip flags mid-run.
    #[cfg(any(test, feature = "fault-inject"))]
    pub fn fault_points(&self) -> Option<Arc<crate::FaultPoints>> {
        self.fault_points.clone()
    }

    /// Spawn the participant-staging TTL sweeper as a background
    /// task. Returns the `JoinHandle` so the caller can abort it at
    /// shutdown. Safe to call zero or one times per service; calling
    /// more than once leaks the earlier sweeper's handle.
    pub fn spawn_staging_sweeper(
        &self,
        interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        self.pending_batches.clone().spawn_sweeper(interval)
    }

    /// Spawn a periodic in-doubt-recovery loop for multi-raft mode.
    /// Calls [`Self::recover_multi_raft_in_doubt`] every `interval`
    /// so a coordinator that crashes after fsyncing CommitDecision —
    /// while the rest of the cluster keeps running — still has its
    /// in-doubt PREPAREs resolved without operator intervention.
    /// Returns `None` when the service isn't running multi-raft;
    /// the caller is responsible for aborting the returned handle
    /// on shutdown.
    pub fn spawn_multi_raft_recovery_loop(
        &self,
        interval: std::time::Duration,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if self.multi_raft.is_none() {
            return None;
        }
        let svc = self.clone();
        Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            // Skip the immediate first tick — startup recovery already
            // ran inline through `serve()` before the listener bound.
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(e) = svc.recover_multi_raft_in_doubt().await {
                    tracing::warn!(error = %e, "periodic multi-raft recovery failed; will retry on next tick");
                }
            }
        }))
    }

    /// Spawn the coordinator-log rotator as a background task.
    /// Returns `None` when the service has no coordinator log (i.e.
    /// single-node or Raft mode), since there's nothing to rotate.
    /// The caller is responsible for aborting the returned handle on
    /// shutdown.
    pub fn spawn_log_rotator(
        &self,
        interval: std::time::Duration,
        min_completed: usize,
    ) -> Option<tokio::task::JoinHandle<()>> {
        self.coordinator_log
            .as_ref()
            .map(|log| log.clone().spawn_rotator(interval, min_completed))
    }

    pub fn into_query_server(self) -> MeshQueryServer<Self> {
        MeshQueryServer::new(self)
    }

    pub fn into_write_server(self) -> MeshWriteServer<Self> {
        MeshWriteServer::new(self)
    }

    /// Run a Cypher query end-to-end against this service and return the
    /// raw [`meshdb_executor::Row`]s. Shared between the gRPC
    /// `execute_cypher` handler and the Bolt protocol listener so both
    /// entry points drive the exact same parsing, planning, execution,
    /// routing, and 2PC commit logic.
    ///
    /// On cluster-mode Raft writes where this peer isn't the leader,
    /// the helper transparently forwards the original query to the
    /// leader via gRPC and deserializes the rows back — identical
    /// semantics to the direct gRPC call path.
    pub async fn execute_cypher_local(
        &self,
        query: String,
        params: meshdb_executor::ParamMap,
    ) -> std::result::Result<Vec<meshdb_executor::Row>, Status> {
        // Two-step auto-commit: run the executor against a buffer,
        // then dispatch the buffered writes through the active
        // backend. The two halves are public on their own so the Bolt
        // explicit-transaction handler can interleave multiple buffered
        // RUNs and commit them as one batch.
        let (rows, commands) = self
            .execute_cypher_buffered(query.clone(), params.clone())
            .await?;
        if !commands.is_empty() {
            // Raft mode needs to forward the *original query string* to
            // the leader on a ForwardToLeader error, not the buffered
            // commands (whose ids would clash with anything the leader
            // already minted). Detect that case here and re-issue the
            // gRPC call with params; the leader runs the whole pipeline
            // on its end and returns the resulting rows.
            match self.commit_buffered_commands(commands).await {
                Ok(()) => {}
                Err(status) => {
                    if let Some(addr) = leader_redirect_address(&status) {
                        return self
                            .forward_execute_cypher_to_leader(&addr, query, params)
                            .await;
                    }
                    return Err(status);
                }
            }
        }
        Ok(rows)
    }

    /// Run a Cypher query end-to-end without committing — returns the
    /// projection rows alongside the buffered `GraphCommand`s the
    /// executor produced. Shared by [`execute_cypher_local`] (which
    /// commits immediately) and the Bolt explicit-transaction handler
    /// (which accumulates the commands across multiple `RUN`s and
    /// commits them in one batch at COMMIT time).
    pub async fn execute_cypher_buffered(
        &self,
        query: String,
        params: meshdb_executor::ParamMap,
    ) -> std::result::Result<(Vec<meshdb_executor::Row>, Vec<GraphCommand>), Status> {
        // Auto-commit path — equivalent to running an explicit tx
        // with an empty accumulator and no surrounding Bolt tx,
        // so `CALL ... IN TRANSACTIONS` is allowed and gets
        // dispatched to the batched-commit path internally.
        self.execute_cypher_in_tx(query, params, Vec::new(), false)
            .await
    }

    /// Run a Cypher query with a read-your-writes overlay derived
    /// from `prev_commands`. Used by the Bolt explicit-transaction
    /// handler so subsequent `RUN`s inside a `BEGIN` / `COMMIT`
    /// block see the writes from earlier RUNs in the same tx.
    ///
    /// Returns `(rows, new_commands)` where `new_commands` are *this*
    /// RUN's writes only; the caller is responsible for appending
    /// them to its accumulator for subsequent RUNs.
    ///
    /// With an empty `prev_commands`, this collapses to the normal
    /// auto-commit read path — the overlay is a no-op and every read
    /// hits the base reader directly.
    #[tracing::instrument(
        skip_all,
        fields(query_len = query.len(), prev_commands = prev_commands.len())
    )]
    pub async fn execute_cypher_in_tx(
        &self,
        query: String,
        params: meshdb_executor::ParamMap,
        prev_commands: Vec<GraphCommand>,
        in_explicit_tx: bool,
    ) -> std::result::Result<(Vec<meshdb_executor::Row>, Vec<GraphCommand>), Status> {
        let statement = meshdb_cypher::parse(&query).map_err(bad_request)?;

        // Schema DDL replication is wired up across every mode now:
        // - Single-node: applied directly through the store.
        // - Raft: replicated via `propose_graph(GraphCommand::CreateIndex)`,
        //   each peer's `StoreGraphApplier` runs the local create.
        // - Routing: parallel fan-out with rollback in
        //   `replicate_index_ddl_routing`, called from
        //   `commit_buffered_commands`.

        // Populate the planner context with the registered indexes so
        // `MATCH (n:Label {prop: ...})` can rewrite to `IndexSeek`
        // when a matching index exists. In Raft/routing modes this is
        // currently always empty because DDL is rejected above; once
        // phases B/C land the same call will surface the full set.
        let planner_ctx = meshdb_cypher::PlannerContext {
            outer_bindings: Vec::new(),
            indexes: self
                .store
                .list_property_indexes()
                .into_iter()
                .map(|s| (s.label, s.properties))
                .collect(),
            edge_indexes: self
                .store
                .list_edge_property_indexes()
                .into_iter()
                .map(|s| (s.edge_type, s.properties))
                .collect(),
            point_indexes: self
                .store
                .list_point_indexes()
                .into_iter()
                .map(|s| (s.label, s.property))
                .collect(),
            edge_point_indexes: self
                .store
                .list_edge_point_indexes()
                .into_iter()
                .map(|s| (s.edge_type, s.property))
                .collect(),
        };
        let plan =
            meshdb_cypher::plan_with_context(&statement, &planner_ctx).map_err(bad_request)?;

        // CALL { ... } IN TRANSACTIONS commits each batch
        // independently, which conflicts with an enclosing
        // explicit transaction (whose whole point is one atomic
        // commit at the end). Reject early with a clear error so
        // clients see a protocol-level failure rather than
        // mysterious partial commits. Auto-commit callers
        // (`execute_cypher_buffered`) pass `in_explicit_tx =
        // false` and route to the batched-execution path.
        if plan_contains_in_transactions(&plan) {
            if in_explicit_tx {
                return Err(Status::failed_precondition(
                    "CALL { ... } IN TRANSACTIONS is not allowed inside an explicit \
                     transaction (BEGIN / COMMIT). Run the statement as auto-commit \
                     instead.",
                ));
            }
            return self
                .execute_call_in_transactions(plan, params, prev_commands)
                .await;
        }
        // apoc.periodic.iterate has the same explicit-tx
        // restriction as IN TRANSACTIONS for the same reason —
        // each batch commits independently.
        if plan_contains_apoc_periodic_iterate(&plan) {
            if in_explicit_tx {
                return Err(Status::failed_precondition(
                    "apoc.periodic.iterate is not allowed inside an explicit \
                     transaction (BEGIN / COMMIT). Run the statement as auto-commit \
                     instead.",
                ));
            }
            return self
                .execute_apoc_periodic_iterate(plan, params, prev_commands)
                .await;
        }

        // Metric increments. The mode label is set once per query
        // and reused for both the counter and the latency
        // histogram so dashboards can compute a per-mode mean
        // latency (sum / count). The IndexSeek count walks the
        // plan tree once — cheap relative to the query itself.
        let mode_label = if self.multi_raft.is_some() {
            crate::metrics::MODE_MULTI_RAFT
        } else if self.routing.is_some() {
            crate::metrics::MODE_ROUTING
        } else if self.raft.is_some() {
            crate::metrics::MODE_RAFT
        } else {
            crate::metrics::MODE_SINGLE
        };
        crate::metrics::CYPHER_QUERIES_TOTAL
            .with_label_values(&[mode_label])
            .inc();
        let _timer = crate::metrics::CYPHER_QUERY_DURATION_SECONDS
            .with_label_values(&[mode_label])
            .start_timer();
        let seek_count = count_index_seeks(&plan);
        if seek_count > 0 {
            crate::metrics::CYPHER_INDEX_SEEKS_TOTAL.inc_by(seek_count);
        }

        let store = self.store.clone();
        let routing = self.routing.clone();
        let exec_params = params;
        let registry_factory = self.procedure_registry_factory.clone();

        let (rows, commands) = tokio::task::spawn_blocking(
            move || -> std::result::Result<
                (Vec<meshdb_executor::Row>, Vec<GraphCommand>),
                meshdb_executor::Error,
            > {
                let store_ref: &dyn StorageEngine = store.as_ref();
                let storage_reader = StorageReaderAdapter(store_ref);
                let writer = BufferingGraphWriter::new();
                // Fold the previously-buffered tx writes into an
                // overlay state. Empty for auto-commit RUNs; populated
                // for in-tx RUNs so the executor sees the prior writes.
                let overlay = crate::TxOverlayState::from_commands(&prev_commands);

                let procs = registry_factory();
                let rows = if let Some(r) = routing.as_ref() {
                    // Routing mode: reads go through a partitioned
                    // reader. Single-node and Raft modes use the local
                    // store directly (Raft replicates the full graph).
                    let partitioned =
                        PartitionedGraphReader::new(store.clone(), r.clone());
                    let base: &dyn GraphReader = &partitioned;
                    let reader = crate::OverlayGraphReader::new(base, &overlay);
                    execute_with_reader_and_procs(
                        &plan,
                        &reader as &dyn GraphReader,
                        &writer as &dyn GraphWriter,
                        &exec_params,
                        &procs,
                    )?
                } else {
                    let base: &dyn GraphReader = &storage_reader;
                    let reader = crate::OverlayGraphReader::new(base, &overlay);
                    execute_with_reader_and_procs(
                        &plan,
                        &reader as &dyn GraphReader,
                        &writer as &dyn GraphWriter,
                        &exec_params,
                        &procs,
                    )?
                };
                Ok((rows, writer.into_commands()))
            },
        )
        .await
        .map_err(|e| Status::internal(format!("executor task panicked: {e}")))?
        .map_err(|e| Status::internal(format!("cypher execution failed: {e}")))?;

        Ok((rows, commands))
    }

    /// Execute a plan whose top-level node is
    /// [`meshdb_cypher::LogicalPlan::CallSubqueryInTransactions`].
    /// Drains the input subtree once, then runs the body for
    /// each input row in batches of `batch_size`, committing
    /// each batch as its own transaction.
    ///
    /// Each batch:
    ///   1. Fresh `BufferingGraphWriter`.
    ///   2. For every input row in the chunk, run the body via
    ///      [`meshdb_executor::execute_with_seed`] (which folds
    ///      the row into the operator pipeline as outer-scope).
    ///   3. Merge body outputs with their outer rows.
    ///   4. Commit the writer's accumulated commands through
    ///      [`Self::commit_buffered_commands`].
    ///
    /// On commit failure, the remainder of the IN TRANSACTIONS
    /// run aborts; already-committed batches stay durably
    /// persisted — matches Neo4j 5's default `ON ERROR FAIL`.
    ///
    /// `prev_commands` must be empty (the auto-commit caller
    /// passes `Vec::new()`); if it isn't, that's a wiring bug
    /// surfaced as a protocol-level failure.
    #[allow(clippy::too_many_lines)]
    async fn execute_call_in_transactions(
        &self,
        plan: meshdb_cypher::LogicalPlan,
        params: meshdb_executor::ParamMap,
        prev_commands: Vec<GraphCommand>,
    ) -> std::result::Result<(Vec<meshdb_executor::Row>, Vec<GraphCommand>), Status> {
        if !prev_commands.is_empty() {
            return Err(Status::failed_precondition(
                "CALL { ... } IN TRANSACTIONS dispatched with a non-empty buffered \
                 command set — the in-explicit-tx check upstream should have rejected \
                 this case earlier",
            ));
        }
        // Locate the IN TRANSACTIONS node anywhere in the plan
        // tree. Cloning so the original plan tree stays intact —
        // we'll reuse it later via execute_with_in_tx_substitute
        // to evaluate any wrapping clauses (Project / OrderBy /
        // Limit / Aggregate / Filter) on top of the
        // batched-and-committed body output rows.
        let (input_plan, body_plan, batch_size, error_mode, report_status_as) =
            match find_in_transactions_node(&plan) {
                Some(parts) => parts,
                None => {
                    return Err(Status::failed_precondition(
                        "execute_call_in_transactions called on a plan without an \
                         IN TRANSACTIONS node — the dispatcher upstream miswired this",
                    ));
                }
            };

        // Drain the input plan into a Vec<Row>. Run on a
        // blocking thread because the operator pipeline is sync.
        let input_rows: Vec<meshdb_executor::Row> = {
            let store = self.store.clone();
            let routing = self.routing.clone();
            let input_plan = input_plan;
            let params = params.clone();
            let registry_factory = self.procedure_registry_factory.clone();
            tokio::task::spawn_blocking(
                move || -> std::result::Result<_, meshdb_executor::Error> {
                    let storage_reader = StorageReaderAdapter(store.as_ref() as &dyn StorageEngine);
                    let writer = BufferingGraphWriter::new();
                    let procs = registry_factory();
                    if let Some(r) = routing.as_ref() {
                        let partitioned = PartitionedGraphReader::new(store.clone(), r.clone());
                        meshdb_executor::execute_with_reader_and_procs(
                            &input_plan,
                            &partitioned as &dyn GraphReader,
                            &writer as &dyn GraphWriter,
                            &params,
                            &procs,
                        )
                    } else {
                        meshdb_executor::execute_with_reader_and_procs(
                            &input_plan,
                            &storage_reader as &dyn GraphReader,
                            &writer as &dyn GraphWriter,
                            &params,
                            &procs,
                        )
                    }
                },
            )
            .await
            .map_err(|e| Status::internal(format!("input drain panicked: {e}")))?
            .map_err(|e| Status::internal(format!("input drain failed: {e}")))?
        };

        let mut all_output: Vec<meshdb_executor::Row> = Vec::new();
        let bs = batch_size.max(1) as usize;
        let mut batch_idx: usize = 0;
        for chunk in input_rows.chunks(bs) {
            batch_idx += 1;
            let chunk_rows: Vec<meshdb_executor::Row> = chunk.to_vec();
            let body_plan = body_plan.clone();
            let store = self.store.clone();
            let routing = self.routing.clone();
            let params = params.clone();
            let registry_factory = self.procedure_registry_factory.clone();
            let outcome_result = tokio::task::spawn_blocking(
                move || -> std::result::Result<
                    (Vec<meshdb_executor::Row>, Vec<GraphCommand>),
                    meshdb_executor::Error,
                > {
                    let storage_reader = StorageReaderAdapter(store.as_ref() as &dyn StorageEngine);
                    let writer = BufferingGraphWriter::new();
                    let procs = registry_factory();
                    let mut batch_output: Vec<meshdb_executor::Row> = Vec::new();
                    for outer_row in chunk_rows {
                        let body_rows = if let Some(r) = routing.as_ref() {
                            let partitioned =
                                PartitionedGraphReader::new(store.clone(), r.clone());
                            meshdb_executor::execute_with_seed(
                                &body_plan,
                                Some(&outer_row),
                                &partitioned as &dyn GraphReader,
                                &writer as &dyn GraphWriter,
                                &params,
                                &procs,
                            )?
                        } else {
                            meshdb_executor::execute_with_seed(
                                &body_plan,
                                Some(&outer_row),
                                &storage_reader as &dyn GraphReader,
                                &writer as &dyn GraphWriter,
                                &params,
                                &procs,
                            )?
                        };
                        if body_rows.is_empty() {
                            // Write-only body (CREATE / SET / DELETE / etc.
                            // with no RETURN) emits zero rows. IN
                            // TRANSACTIONS has FOREACH-like semantics —
                            // the outer row still passes through so that
                            // any wrapping `RETURN x` after the CALL
                            // surfaces one row per input row, matching
                            // Neo4j 5.
                            batch_output.push(outer_row);
                        } else {
                            for body_row in body_rows {
                                let mut merged = outer_row.clone();
                                for (k, v) in body_row {
                                    merged.insert(k, v);
                                }
                                batch_output.push(merged);
                            }
                        }
                    }
                    Ok((batch_output, writer.into_commands()))
                },
            )
            .await
            .map_err(|e| {
                Status::internal(format!("batch {batch_idx} executor panicked: {e}"))
            })?;

            // Mint a fresh transaction id for the batch — used
            // by the REPORT STATUS row when set, and harmlessly
            // unused otherwise.
            let tx_id = uuid::Uuid::now_v7().to_string();

            // Body execution result: error here means a row in
            // the batch failed (parse, type, constraint, etc.).
            // No commit was attempted, so nothing landed.
            let (batch_output, batch_commands) = match outcome_result {
                Ok(out) => out,
                Err(e) => {
                    if let Some(var) = report_status_as.as_deref() {
                        all_output.push(make_status_row(var, &tx_id, false, Some(&e.to_string())));
                    }
                    match error_mode {
                        meshdb_cypher::OnErrorMode::Fail => {
                            return Err(Status::internal(format!(
                                "batch {batch_idx} execution failed: {e}"
                            )));
                        }
                        meshdb_cypher::OnErrorMode::Continue => {
                            tracing::warn!(
                                batch = batch_idx,
                                error = %e,
                                "ON ERROR CONTINUE — body execution failed; skipping batch"
                            );
                            continue;
                        }
                        meshdb_cypher::OnErrorMode::Break => {
                            tracing::warn!(
                                batch = batch_idx,
                                error = %e,
                                "ON ERROR BREAK — body execution failed; halting"
                            );
                            break;
                        }
                    }
                }
            };

            // Commit attempt: failure here means the body
            // succeeded but the underlying commit
            // (single-node / Raft / 2PC) couldn't persist.
            match self.commit_buffered_commands(batch_commands).await {
                Ok(()) => {
                    if let Some(var) = report_status_as.as_deref() {
                        all_output.push(make_status_row(var, &tx_id, true, None));
                    } else {
                        all_output.extend(batch_output);
                    }
                }
                Err(e) => {
                    if let Some(var) = report_status_as.as_deref() {
                        all_output.push(make_status_row(var, &tx_id, false, Some(e.message())));
                    }
                    match error_mode {
                        meshdb_cypher::OnErrorMode::Fail => return Err(e),
                        meshdb_cypher::OnErrorMode::Continue => {
                            tracing::warn!(
                                batch = batch_idx,
                                error = %e.message(),
                                "ON ERROR CONTINUE — commit failed; skipping batch"
                            );
                            continue;
                        }
                        meshdb_cypher::OnErrorMode::Break => {
                            tracing::warn!(
                                batch = batch_idx,
                                error = %e.message(),
                                "ON ERROR BREAK — commit failed; halting"
                            );
                            break;
                        }
                    }
                }
            }
        }
        // Fold the accumulated body output back into the
        // wrapping plan via execute_with_in_tx_substitute. The
        // executor builds the operator tree for `plan` and,
        // when it reaches the IN TRANSACTIONS node, substitutes
        // a `RowsLiteralOp` over `all_output` instead of
        // panicking. Wrapping clauses (Project / OrderBy /
        // Limit / Aggregate / Filter) run untouched, so
        // queries like
        //   UNWIND ... CALL { ... } IN TRANSACTIONS RETURN ...
        // surface the projected rows to the caller.
        //
        // For the bare-form `CALL { ... } IN TRANSACTIONS` with
        // no wrapping clause, the plan tree IS the
        // CallSubqueryInTransactions node — the substitute path
        // yields all_output directly, and the write-only
        // suppression check inside execute_with_in_tx_substitute
        // (`is_write_only_plan`) drops them, matching pre-
        // surfacing behaviour.
        let store = self.store.clone();
        let routing = self.routing.clone();
        let plan_for_substitution = plan;
        let registry_factory = self.procedure_registry_factory.clone();
        let final_rows = tokio::task::spawn_blocking(
            move || -> std::result::Result<Vec<meshdb_executor::Row>, meshdb_executor::Error> {
                let storage_reader = StorageReaderAdapter(store.as_ref() as &dyn StorageEngine);
                let writer = BufferingGraphWriter::new();
                let procs = registry_factory();
                if let Some(r) = routing.as_ref() {
                    let partitioned = PartitionedGraphReader::new(store.clone(), r.clone());
                    meshdb_executor::execute_with_in_tx_substitute(
                        &plan_for_substitution,
                        all_output,
                        &partitioned as &dyn GraphReader,
                        &writer as &dyn GraphWriter,
                        &params,
                        &procs,
                    )
                } else {
                    meshdb_executor::execute_with_in_tx_substitute(
                        &plan_for_substitution,
                        all_output,
                        &storage_reader as &dyn GraphReader,
                        &writer as &dyn GraphWriter,
                        &params,
                        &procs,
                    )
                }
            },
        )
        .await
        .map_err(|e| Status::internal(format!("post-batch projection panicked: {e}")))?
        .map_err(|e| Status::internal(format!("post-batch projection failed: {e}")))?;
        Ok((final_rows, Vec::new()))
    }

    /// True when `plan` carries a `CallSubqueryInTransactions`
    /// node anywhere in its tree — used by `execute_cypher_in_tx`
    /// to detect the IN TRANSACTIONS form before dispatching.

    /// Execute a plan whose top-level node is (or wraps) an
    /// [`meshdb_cypher::LogicalPlan::ApocPeriodicIterate`].
    /// Drains the iterate plan into rows, then for each batch
    /// runs the action plan once per input row with the row's
    /// columns injected as `$<column>` parameters, committing
    /// each batch as its own transaction (ON ERROR
    /// CONTINUE-style — failed batches are recorded but don't
    /// abort the run). Aggregates per-batch outcomes into a
    /// single APOC-shape result row that gets folded back into
    /// any wrapping plan via the substitute mechanism.
    #[allow(clippy::too_many_lines)]
    async fn execute_apoc_periodic_iterate(
        &self,
        plan: meshdb_cypher::LogicalPlan,
        params: meshdb_executor::ParamMap,
        prev_commands: Vec<GraphCommand>,
    ) -> std::result::Result<(Vec<meshdb_executor::Row>, Vec<GraphCommand>), Status> {
        if !prev_commands.is_empty() {
            return Err(Status::failed_precondition(
                "apoc.periodic.iterate dispatched with a non-empty buffered command set \
                 — the in-explicit-tx check upstream should have rejected this case",
            ));
        }
        let (
            iterate_plan,
            action_plan,
            batch_size,
            extra_params_exprs,
            max_retries,
            failed_params_cap,
            iterate_list,
            parallel,
            concurrency,
        ) = match find_apoc_periodic_iterate_node(&plan) {
            Some(parts) => parts,
            None => {
                return Err(Status::failed_precondition(
                    "execute_apoc_periodic_iterate called on a plan without an \
                         ApocPeriodicIterate node — the dispatcher upstream miswired this",
                ));
            }
        };

        // Evaluate any `params` config entries once at start
        // against the existing `params` ParamMap so per-row
        // execution sees a stable extra-param baseline.
        let extra_params: meshdb_executor::ParamMap = {
            let store = self.store.clone();
            let routing = self.routing.clone();
            let base_params = params.clone();
            tokio::task::spawn_blocking(
                move || -> std::result::Result<meshdb_executor::ParamMap, meshdb_executor::Error> {
                    eval_extra_params(&extra_params_exprs, &base_params, &store, routing.as_ref())
                },
            )
            .await
            .map_err(|e| Status::internal(format!("extra_params eval panicked: {e}")))?
            .map_err(|e| Status::internal(format!("extra_params eval failed: {e}")))?
        };

        // Drain the iterate query into Vec<Row>. Iterate runs
        // read-only — its writes (if any, e.g. side-effect
        // procedures) go to a throwaway BufferingGraphWriter
        // that's never committed, matching Neo4j semantics.
        let input_rows: Vec<meshdb_executor::Row> = {
            let store = self.store.clone();
            let routing = self.routing.clone();
            let iterate_plan = iterate_plan;
            let params = params.clone();
            let registry_factory = self.procedure_registry_factory.clone();
            tokio::task::spawn_blocking(
                move || -> std::result::Result<_, meshdb_executor::Error> {
                    let storage_reader = StorageReaderAdapter(store.as_ref() as &dyn StorageEngine);
                    let writer = BufferingGraphWriter::new();
                    let procs = registry_factory();
                    if let Some(r) = routing.as_ref() {
                        let partitioned = PartitionedGraphReader::new(store.clone(), r.clone());
                        meshdb_executor::execute_with_reader_and_procs(
                            &iterate_plan,
                            &partitioned as &dyn GraphReader,
                            &writer as &dyn GraphWriter,
                            &params,
                            &procs,
                        )
                    } else {
                        meshdb_executor::execute_with_reader_and_procs(
                            &iterate_plan,
                            &storage_reader as &dyn GraphReader,
                            &writer as &dyn GraphWriter,
                            &params,
                            &procs,
                        )
                    }
                },
            )
            .await
            .map_err(|e| Status::internal(format!("apoc.periodic.iterate iterate panicked: {e}")))?
            .map_err(|e| Status::internal(format!("apoc.periodic.iterate iterate failed: {e}")))?
        };

        let started_at = std::time::Instant::now();
        let shared = Arc::new(SharedApocStats::new());
        let bs = batch_size.max(1) as usize;

        // Materialize the batches up front so the spawned
        // parallel tasks own their chunks without borrowing
        // across the task boundary.
        let chunks: Vec<Vec<meshdb_executor::Row>> =
            input_rows.chunks(bs).map(|c| c.to_vec()).collect();

        if parallel {
            let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
            let mut handles = Vec::with_capacity(chunks.len());
            for chunk in chunks {
                // Acquire OUTSIDE the spawn so the outer loop
                // provides back-pressure — we never queue more
                // tasks than the permit count allows at any one
                // moment. Dropping the permit at task end
                // releases it for the next chunk.
                let permit = sem.clone().acquire_owned().await.map_err(|e| {
                    Status::internal(format!("parallel-batch semaphore closed: {e}"))
                })?;
                let this = self.clone();
                let action_plan = action_plan.clone();
                let extra_params = extra_params.clone();
                let shared = shared.clone();
                handles.push(tokio::spawn(async move {
                    let _permit = permit;
                    this.run_apoc_periodic_batch(
                        chunk,
                        action_plan,
                        extra_params,
                        max_retries,
                        iterate_list,
                        failed_params_cap,
                        shared,
                    )
                    .await
                }));
            }
            // Drive every task to completion. Individual batch
            // failures already landed in `shared`; only a task-
            // level panic (JoinError) or an unrecoverable
            // executor panic surfaces here as an internal error.
            for handle in handles {
                handle.await.map_err(|e| {
                    Status::internal(format!("parallel batch task panicked: {e}"))
                })??;
            }
        } else {
            for chunk in chunks {
                self.run_apoc_periodic_batch(
                    chunk,
                    action_plan.clone(),
                    extra_params.clone(),
                    max_retries,
                    iterate_list,
                    failed_params_cap,
                    shared.clone(),
                )
                .await?;
            }
        }

        let time_taken = started_at.elapsed().as_millis() as i64;

        // Snapshot the shared state into owned values so the
        // result-row builder doesn't hold any mutexes — after
        // this point the outer mutexes are effectively dropped.
        let (batches_count, total, committed_ops, failed_ops, failed_batches, retries_count) = {
            let c = shared.counters.lock().unwrap();
            (
                c.batches,
                c.total,
                c.committed_ops,
                c.failed_ops,
                c.failed_batches,
                c.retries,
            )
        };
        let error_messages = shared.errors.lock().unwrap().clone();
        let failed_params = shared.failed_params.lock().unwrap().clone();
        let update_stats = shared.update_stats.lock().unwrap().clone();

        // Build the standard APOC result row.
        let result_row = build_apoc_periodic_iterate_row(
            batches_count,
            total,
            committed_ops,
            failed_ops,
            failed_batches,
            retries_count,
            &failed_params,
            &update_stats,
            time_taken,
            &error_messages,
        );

        // Fold back into the wrapping plan via the substitute
        // mechanism. The plan's ApocPeriodicIterate node gets
        // swapped for a RowsLiteralOp yielding our single
        // result row, and any wrapping clauses run untouched.
        let store = self.store.clone();
        let routing = self.routing.clone();
        let plan_for_substitution = plan;
        let registry_factory = self.procedure_registry_factory.clone();
        let final_rows = tokio::task::spawn_blocking(
            move || -> std::result::Result<Vec<meshdb_executor::Row>, meshdb_executor::Error> {
                let storage_reader = StorageReaderAdapter(store.as_ref() as &dyn StorageEngine);
                let writer = BufferingGraphWriter::new();
                let procs = registry_factory();
                if let Some(r) = routing.as_ref() {
                    let partitioned = PartitionedGraphReader::new(store.clone(), r.clone());
                    meshdb_executor::execute_with_in_tx_substitute(
                        &plan_for_substitution,
                        vec![result_row],
                        &partitioned as &dyn GraphReader,
                        &writer as &dyn GraphWriter,
                        &params,
                        &procs,
                    )
                } else {
                    meshdb_executor::execute_with_in_tx_substitute(
                        &plan_for_substitution,
                        vec![result_row],
                        &storage_reader as &dyn GraphReader,
                        &writer as &dyn GraphWriter,
                        &params,
                        &procs,
                    )
                }
            },
        )
        .await
        .map_err(|e| Status::internal(format!("apoc.periodic.iterate projection panicked: {e}")))?
        .map_err(|e| Status::internal(format!("apoc.periodic.iterate projection failed: {e}")))?;
        Ok((final_rows, Vec::new()))
    }

    /// Execute one batch of `apoc.periodic.iterate`: run the
    /// action query up to `max_retries + 1` times, commit any
    /// resulting writes, and fold the outcome into `shared`.
    /// Called once per batch from the dispatcher — back-to-back
    /// in the default sequential path, concurrently through a
    /// `tokio::spawn` pool when `parallel: true`. Returns `Ok`
    /// even when the batch ultimately fails (the failure is
    /// recorded in `shared.errors` / `shared.failed_params`);
    /// only an executor-task panic surfaces as `Err`.
    #[allow(clippy::too_many_arguments)]
    async fn run_apoc_periodic_batch(
        &self,
        chunk_rows: Vec<meshdb_executor::Row>,
        action_plan: meshdb_cypher::LogicalPlan,
        extra_params: meshdb_executor::ParamMap,
        max_retries: i64,
        iterate_list: bool,
        failed_params_cap: i64,
        shared: Arc<SharedApocStats>,
    ) -> std::result::Result<(), Status> {
        let chunk_size = chunk_rows.len() as i64;
        let max_attempts = max_retries.max(0) + 1;

        // Increment the batches / total counters up front so
        // the batch index we log on failure stays coherent with
        // the final report, even under parallel dispatch.
        let batch_index = {
            let mut c = shared.counters.lock().unwrap();
            c.batches += 1;
            c.total += chunk_size;
            c.batches
        };

        let mut last_err: Option<String> = None;
        let mut succeeded_commands: Option<Vec<GraphCommand>> = None;
        let mut retries_incurred: i64 = 0;

        for attempt in 0..max_attempts {
            if attempt > 0 {
                retries_incurred += 1;
            }
            let chunk_rows_attempt = chunk_rows.clone();
            let action_plan_attempt = action_plan.clone();
            let store = self.store.clone();
            let routing = self.routing.clone();
            let extra_params_attempt = extra_params.clone();
            let registry_factory = self.procedure_registry_factory.clone();
            let outcome_result = tokio::task::spawn_blocking(
                move || -> std::result::Result<Vec<GraphCommand>, meshdb_executor::Error> {
                    let storage_reader = StorageReaderAdapter(store.as_ref() as &dyn StorageEngine);
                    let writer = BufferingGraphWriter::new();
                    let procs = registry_factory();
                    if iterate_list {
                        // Batch-as-list mode: bind the whole
                        // chunk to `$_batch` (a list of
                        // row-Maps) and run the action ONCE.
                        let mut params = extra_params_attempt.clone();
                        let batch_list: Vec<meshdb_core::Property> = chunk_rows_attempt
                            .iter()
                            .map(|row| meshdb_core::Property::Map(row_to_property_map(row)))
                            .collect();
                        params.insert(
                            "_batch".into(),
                            meshdb_executor::Value::Property(meshdb_core::Property::List(
                                batch_list,
                            )),
                        );
                        if let Some(r) = routing.as_ref() {
                            let partitioned = PartitionedGraphReader::new(store.clone(), r.clone());
                            meshdb_executor::execute_with_reader_and_procs(
                                &action_plan_attempt,
                                &partitioned as &dyn GraphReader,
                                &writer as &dyn GraphWriter,
                                &params,
                                &procs,
                            )?;
                        } else {
                            meshdb_executor::execute_with_reader_and_procs(
                                &action_plan_attempt,
                                &storage_reader as &dyn GraphReader,
                                &writer as &dyn GraphWriter,
                                &params,
                                &procs,
                            )?;
                        }
                    } else {
                        for row in chunk_rows_attempt {
                            let mut row_params = extra_params_attempt.clone();
                            for (k, v) in row {
                                row_params.insert(k, v);
                            }
                            if let Some(r) = routing.as_ref() {
                                let partitioned =
                                    PartitionedGraphReader::new(store.clone(), r.clone());
                                meshdb_executor::execute_with_reader_and_procs(
                                    &action_plan_attempt,
                                    &partitioned as &dyn GraphReader,
                                    &writer as &dyn GraphWriter,
                                    &row_params,
                                    &procs,
                                )?;
                            } else {
                                meshdb_executor::execute_with_reader_and_procs(
                                    &action_plan_attempt,
                                    &storage_reader as &dyn GraphReader,
                                    &writer as &dyn GraphWriter,
                                    &row_params,
                                    &procs,
                                )?;
                            }
                        }
                    }
                    Ok(writer.into_commands())
                },
            )
            .await
            .map_err(|e| Status::internal(format!("batch {batch_index} executor panicked: {e}")))?;

            match outcome_result {
                Ok(commands) => match self.commit_buffered_commands(commands.clone()).await {
                    Ok(()) => {
                        succeeded_commands = Some(commands);
                        break;
                    }
                    Err(e) => {
                        last_err = Some(e.message().to_string());
                    }
                },
                Err(e) => {
                    last_err = Some(e.to_string());
                }
            }
        }

        if let Some(commands) = succeeded_commands {
            {
                let mut s = shared.update_stats.lock().unwrap();
                s.absorb(&commands);
            }
            let mut c = shared.counters.lock().unwrap();
            c.committed_ops += chunk_size;
            c.retries += retries_incurred;
        } else {
            {
                let mut c = shared.counters.lock().unwrap();
                c.failed_ops += chunk_size;
                c.failed_batches += 1;
                c.retries += retries_incurred;
            }
            let err_str = last_err.unwrap_or_else(|| "<no error captured>".into());
            {
                let mut errs = shared.errors.lock().unwrap();
                *errs.entry(err_str.clone()).or_insert(0) += 1;
            }
            if failed_params_cap != 0 {
                let mut fp = shared.failed_params.lock().unwrap();
                let bucket = fp.entry(err_str.clone()).or_default();
                let cap = if failed_params_cap < 0 {
                    usize::MAX
                } else {
                    failed_params_cap as usize
                };
                for row in &chunk_rows {
                    if bucket.len() >= cap {
                        break;
                    }
                    bucket.push(row_to_property_map(row));
                }
            }
            tracing::warn!(
                batch = batch_index,
                error = %err_str,
                attempts = max_attempts,
                "apoc.periodic.iterate — batch failed after retries"
            );
        }
        Ok(())
    }

    /// Commit a batch of `GraphCommand`s through the active backend.
    /// Used by both the auto-commit single-RUN path and the Bolt
    /// explicit-transaction COMMIT phase. No-op for an empty batch.
    ///
    /// In Raft mode this is a single `propose_graph(Batch)` call, so a
    /// multi-RUN tx commits atomically as one log entry. In routing
    /// mode the [`TxCoordinator`] groups by destination peer and runs
    /// the existing 2PC protocol. Single-node mode writes directly to
    /// the local store.
    #[tracing::instrument(skip_all, fields(cmd_count = commands.len()))]
    pub async fn commit_buffered_commands(
        &self,
        commands: Vec<GraphCommand>,
    ) -> std::result::Result<(), Status> {
        self.commit_buffered_commands_inner(commands, false).await
    }

    /// Internal entry point. `from_trigger` flips on for the
    /// recursive commit that follows trigger firing — it
    /// Commit `commands` through the multi-raft path. Single-partition
    /// writes are proposed through the partition's Raft (locally if
    /// this peer is the leader, otherwise proxied via
    /// `MeshWrite::ForwardWrite` to the leader's peer). Multi-partition
    /// writes ride a Spanner-style 2PC over partition Rafts —
    /// implementation lands in Phase 7; for now they error.
    async fn commit_multi_raft(
        &self,
        multi_raft: &Arc<crate::MultiRaftCluster>,
        commands: Vec<GraphCommand>,
    ) -> std::result::Result<(), Status> {
        // Phase 9 (DDL through meta group) wires this branch up; for
        // Phase 6 we surface a clear error if a DDL command shows up
        // alongside data writes. DDL-only batches will land on the
        // meta group via the same routing once Phase 9 ships.
        let (ddl, graph) = split_ddl(commands);
        if !ddl.is_empty() {
            // DDL goes through the meta Raft group. Local-leader
            // path proposes directly; non-leader proxies via
            // `MeshWrite::ForwardDdl` to the meta leader so the
            // user sees a transparent commit regardless of which
            // peer they hit.
            for entry in ddl {
                if let Err(e) = self.propose_meta_command(multi_raft, entry).await {
                    return Err(e);
                }
            }
            if graph.is_empty() {
                return Ok(());
            }
        }

        // Determine which partitions the data writes touch.
        let cluster = self.store.as_ref();
        let _ = cluster;
        let partitioner = meshdb_cluster::Partitioner::new(multi_raft.replica_map.num_partitions());
        let mut touched: std::collections::BTreeSet<meshdb_cluster::PartitionId> =
            std::collections::BTreeSet::new();
        for cmd in &graph {
            partitions_touched_by_command(cmd, &partitioner, &mut touched);
        }

        if touched.is_empty() {
            return Ok(());
        }

        if touched.len() == 1 {
            let partition = *touched.iter().next().unwrap();
            return self
                .commit_multi_raft_single_partition(multi_raft, partition, graph)
                .await;
        }

        // Multi-partition write — Spanner-style 2PC over partition
        // Rafts. PREPARE *and* COMMIT both ride the partition Raft so
        // staged state is replicated by the time PREPARE-ACK returns
        // (no in-doubt window dependent on a participant log fsync).
        self.commit_multi_raft_cross_partition(multi_raft, &touched, graph)
            .await
    }

    /// Resolve any in-doubt PREPAREd transactions left behind by a
    /// coordinator crash mid-2PC. For each partition this peer leads,
    /// poll `pending_tx_ids()` from the local applier, then ask every
    /// peer's `ResolveTransaction` RPC for the coordinator's recorded
    /// decision. Propose `CommitTx` / `AbortTx` through the partition
    /// Raft to converge; tolerate `Unknown` responses (the
    /// coordinator may not yet have written its decision; recovery
    /// runs on a loop and will pick it up later).
    ///
    /// Idempotent — re-running drives any new in-doubt PREPAREs to
    /// resolution and leaves resolved txes untouched. Callers
    /// typically schedule this once at startup post-leader-election
    /// plus periodically as a safety net.
    pub async fn recover_multi_raft_in_doubt(&self) -> std::result::Result<(), Status> {
        let Some(multi_raft) = self.multi_raft.clone() else {
            return Ok(());
        };
        #[cfg(any(test, feature = "fault-inject"))]
        if let Some(fp) = &self.fault_points {
            fp.recover_multi_raft_call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        for (partition, applier) in multi_raft.partition_appliers_snapshot() {
            // Run recovery on every replica that hosts this partition.
            // Non-leader proposals route through `forward_write` to
            // the current leader; the first successful CommitTx /
            // AbortTx wins, the rest are idempotent no-ops because
            // the applier's pending_txs entry is already gone.
            // Without this fan-out, a coordinator crash that re-
            // started on a non-leader peer would never converge —
            // followers don't run recovery on their own.
            let pending = applier.pending_tx_ids();
            for txid in pending {
                let decision = self.resolve_decision_across_peers(&multi_raft, &txid).await;
                let (resolution, outcome_label) = match decision {
                    Some(crate::TxDecision::Commit) => {
                        (GraphCommand::CommitTx { txid: txid.clone() }, "committed")
                    }
                    Some(crate::TxDecision::Abort) => {
                        (GraphCommand::AbortTx { txid: txid.clone() }, "aborted")
                    }
                    None => continue, // unknown — try again later
                };
                if let Err(e) = self
                    .propose_partition_command(&multi_raft, partition, resolution)
                    .await
                {
                    crate::metrics::MULTI_RAFT_INDOUBT_RESOLVED_TOTAL
                        .with_label_values(&["failed"])
                        .inc();
                    tracing::warn!(
                        partition = ?partition,
                        txid = %txid,
                        error = %e,
                        "in-doubt resolution propose failed; will retry"
                    );
                } else {
                    crate::metrics::MULTI_RAFT_INDOUBT_RESOLVED_TOTAL
                        .with_label_values(&[outcome_label])
                        .inc();
                }
            }
        }
        Ok(())
    }

    /// Poll every cluster peer's `ResolveTransaction` RPC for `txid`
    /// and return the coordinator's recorded decision. The first
    /// `Committed` / `Aborted` response wins (decisions are
    /// monotonic — once written, every other peer either agrees or
    /// returns `Unknown` because it isn't a coordinator). `None`
    /// when no peer has a recorded decision yet.
    async fn resolve_decision_across_peers(
        &self,
        multi_raft: &Arc<crate::MultiRaftCluster>,
        txid: &str,
    ) -> Option<crate::TxDecision> {
        let state = multi_raft.meta.current_state().await;
        let addrs: Vec<String> = state
            .membership
            .iter()
            .map(|(_, a)| a.to_string())
            .collect();
        drop(state);
        for addr in addrs {
            let mut client = match self.leader_write_client(&addr) {
                Ok(c) => c,
                Err(_) => continue,
            };
            let req = ResolveTransactionRequest {
                txid: txid.to_string(),
            };
            let resp = match client.resolve_transaction(req).await {
                Ok(r) => r.into_inner(),
                Err(_) => continue,
            };
            match resp.status() {
                crate::proto::TxResolutionStatus::Committed => {
                    return Some(crate::TxDecision::Commit);
                }
                crate::proto::TxResolutionStatus::Aborted => {
                    return Some(crate::TxDecision::Abort);
                }
                crate::proto::TxResolutionStatus::Unknown => {}
                crate::proto::TxResolutionStatus::Unspecified => {}
            }
        }
        None
    }

    /// Synchronous-DDL gate: wait for every other peer's meta
    /// replica to apply at least `target_index`. Polled via the
    /// `MeshWrite::MetaLastApplied` RPC every 20ms until every peer
    /// is caught up or the deadline expires.
    ///
    /// Skips this peer (its meta has already applied — we got the
    /// target_index from it in the first place). A peer that's
    /// transiently unreachable surfaces as a poll error and counts
    /// as "not caught up" — if it stays down, the deadline fires
    /// and the caller gets `Status::DeadlineExceeded`. The DDL is
    /// durably committed in that case; the operator can re-issue
    /// or accept the transient inconsistency.
    async fn await_cluster_meta_apply(
        &self,
        multi_raft: &Arc<crate::MultiRaftCluster>,
        target_index: u64,
        timeout: std::time::Duration,
    ) -> std::result::Result<(), Status> {
        if target_index == 0 {
            return Ok(());
        }
        let state = multi_raft.meta.current_state().await;
        let peers: Vec<(u64, String)> = state
            .membership
            .iter()
            .filter(|(id, _)| id.0 != multi_raft.self_id)
            .map(|(id, addr)| (id.0, addr.to_string()))
            .collect();
        drop(state);
        if peers.is_empty() {
            // Single-peer cluster — meta is local; we already applied.
            return Ok(());
        }
        let deadline = std::time::Instant::now() + timeout;
        let mut not_caught_up: std::collections::HashSet<u64> =
            peers.iter().map(|(id, _)| *id).collect();
        loop {
            let mut still_behind: std::collections::HashSet<u64> = std::collections::HashSet::new();
            for (id, addr) in &peers {
                if !not_caught_up.contains(id) {
                    continue;
                }
                let mut client = match self.leader_write_client(addr) {
                    Ok(c) => c,
                    Err(_) => {
                        still_behind.insert(*id);
                        continue;
                    }
                };
                match client
                    .meta_last_applied(crate::proto::MetaLastAppliedRequest {})
                    .await
                {
                    Ok(resp) => {
                        if resp.into_inner().last_applied < target_index {
                            still_behind.insert(*id);
                        }
                    }
                    Err(_) => {
                        still_behind.insert(*id);
                    }
                }
            }
            not_caught_up = still_behind;
            if not_caught_up.is_empty() {
                crate::metrics::MULTI_RAFT_DDL_GATE_TOTAL
                    .with_label_values(&["ok"])
                    .inc();
                return Ok(());
            }
            if std::time::Instant::now() > deadline {
                crate::metrics::MULTI_RAFT_DDL_GATE_TOTAL
                    .with_label_values(&["timeout"])
                    .inc();
                return Err(Status::deadline_exceeded(format!(
                    "ddl strict-apply gate: peers {not_caught_up:?} did not catch up to \
                     meta index {target_index} within {timeout:?} — DDL is durably \
                     committed; retry or query directly"
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// Spanner-style 2PC over partition leaders. Each touched
    /// partition's leader proposes a `PreparedTx` entry through its
    /// Raft group; once all PREPAREs ACK, the coordinator proposes
    /// `CommitTx` on each. A failed PREPARE rolls back via `AbortTx`
    /// on the partitions that prepared.
    async fn commit_multi_raft_cross_partition(
        &self,
        multi_raft: &Arc<crate::MultiRaftCluster>,
        touched: &std::collections::BTreeSet<meshdb_cluster::PartitionId>,
        commands: Vec<GraphCommand>,
    ) -> std::result::Result<(), Status> {
        let txid = uuid::Uuid::new_v4().to_string();

        // Group commands by partition (edges go to both endpoints).
        let partitioner = meshdb_cluster::Partitioner::new(multi_raft.replica_map.num_partitions());
        let mut by_partition: std::collections::BTreeMap<
            meshdb_cluster::PartitionId,
            Vec<GraphCommand>,
        > = std::collections::BTreeMap::new();
        for p in touched {
            by_partition.insert(*p, Vec::new());
        }
        flatten_commands_by_partition(&commands, &partitioner, &mut by_partition);

        // Coordinator log: record the intent before we touch any
        // partition. The log entry stores the per-partition command
        // groups keyed by partition leader's NodeId — sufficient for
        // recovery to identify which partitions need a follow-up
        // CommitTx / AbortTx after a coordinator crash.
        if let Some(log) = &self.coordinator_log {
            let groups: Vec<(PeerId, Vec<GraphCommand>)> = touched
                .iter()
                .map(|p| {
                    let leader = multi_raft.leader_of(*p).unwrap_or(0);
                    let cmds = by_partition.get(p).cloned().unwrap_or_default();
                    (PeerId(leader), cmds)
                })
                .collect();
            if let Err(e) = log.append(&crate::TxLogEntry::Prepared {
                txid: txid.clone(),
                groups,
            }) {
                return Err(Status::internal(format!("coordinator log Prepared: {e}")));
            }
        }

        // PREPARE phase. Each propose goes through the partition Raft;
        // ACKs only return after Raft quorum.
        let mut prepared: Vec<meshdb_cluster::PartitionId> = Vec::new();
        let mut prepare_err: Option<Status> = None;
        for partition in touched {
            let entry = GraphCommand::PreparedTx {
                txid: txid.clone(),
                commands: by_partition.remove(partition).unwrap_or_default(),
            };
            match self
                .propose_partition_command(multi_raft, *partition, entry)
                .await
            {
                Ok(()) => {
                    prepared.push(*partition);
                    // Fault injection: simulate a coordinator crash
                    // mid-PREPARE-fanout — return without sending the
                    // next PREPARE so recovery sees `Prepared` with
                    // no decision in the coordinator log.
                    #[cfg(any(test, feature = "fault-inject"))]
                    if let Some(fp) = &self.fault_points {
                        if fp
                            .multi_raft_crash_after_first_prepared_tx
                            .load(std::sync::atomic::Ordering::SeqCst)
                            && prepared.len() == 1
                            && touched.len() > 1
                        {
                            return Err(Status::internal(
                                "injected fault: multi_raft_crash_after_first_prepared_tx",
                            ));
                        }
                    }
                }
                Err(e) => {
                    prepare_err = Some(e);
                    break;
                }
            }
        }

        if let Some(err) = prepare_err {
            // Record the abort decision before fanning out so a
            // coordinator crash mid-rollback recovers correctly.
            if let Some(log) = &self.coordinator_log {
                let _ = log.append(&crate::TxLogEntry::AbortDecision { txid: txid.clone() });
            }
            // Best-effort abort on partitions that prepared.
            for partition in &prepared {
                let abort = GraphCommand::AbortTx { txid: txid.clone() };
                if let Err(e) = self
                    .propose_partition_command(multi_raft, *partition, abort)
                    .await
                {
                    tracing::warn!(
                        partition = ?partition,
                        txid = %txid,
                        error = %e,
                        "abort propose failed during multi-raft 2PC rollback"
                    );
                }
            }
            if let Some(log) = &self.coordinator_log {
                let _ = log.append(&crate::TxLogEntry::Completed { txid: txid.clone() });
            }
            crate::metrics::MULTI_RAFT_CROSS_PARTITION_TOTAL
                .with_label_values(&["aborted"])
                .inc();
            return Err(err);
        }

        // Record the commit decision before sending COMMITs — this is
        // the point of no return. A coordinator crash after this entry
        // means recovery rolls every prepared partition forward to
        // committed.
        if let Some(log) = &self.coordinator_log {
            if let Err(e) = log.append(&crate::TxLogEntry::CommitDecision { txid: txid.clone() }) {
                return Err(Status::internal(format!(
                    "coordinator log CommitDecision: {e}"
                )));
            }
        }

        // Fault injection: simulate a coordinator crash after the
        // commit decision is durable but before any CommitTx ships.
        // Recovery resolves every prepared partition forward to
        // committed via `ResolveTransaction` polling.
        #[cfg(any(test, feature = "fault-inject"))]
        if let Some(fp) = &self.fault_points {
            if fp
                .multi_raft_crash_after_commit_decision
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(Status::internal(
                    "injected fault: multi_raft_crash_after_commit_decision",
                ));
            }
        }

        // COMMIT phase.
        let mut commit_errs: Vec<Status> = Vec::new();
        for (commit_idx, partition) in prepared.iter().enumerate() {
            // Fault injection: simulate a coordinator crash after the
            // K-th CommitTx commits but before the (K+1)-th. Some
            // partitions are committed; the rest stay PREPAREd.
            // Recovery resolves the holdouts forward. `commit_idx` is
            // only read inside the cfg-gated block — release builds
            // get it through `enumerate()` without an unused-variable
            // warning.
            #[cfg(any(test, feature = "fault-inject"))]
            if let Some(fp) = &self.fault_points {
                let trigger = fp
                    .multi_raft_crash_after_kth_commit_tx
                    .load(std::sync::atomic::Ordering::SeqCst);
                if trigger >= 0 && (commit_idx as i32) == trigger {
                    return Err(Status::internal(
                        "injected fault: multi_raft_crash_after_kth_commit_tx",
                    ));
                }
            }
            #[cfg(not(any(test, feature = "fault-inject")))]
            let _ = commit_idx;
            let entry = GraphCommand::CommitTx { txid: txid.clone() };
            if let Err(e) = self
                .propose_partition_command(multi_raft, *partition, entry)
                .await
            {
                commit_errs.push(e);
            }
        }
        if !commit_errs.is_empty() {
            return Err(commit_errs.into_iter().next().unwrap());
        }
        if let Some(log) = &self.coordinator_log {
            let _ = log.append(&crate::TxLogEntry::Completed { txid });
        }
        crate::metrics::MULTI_RAFT_CROSS_PARTITION_TOTAL
            .with_label_values(&["committed"])
            .inc();
        Ok(())
    }

    /// Propose a DDL `GraphCommand` through the metadata Raft group.
    /// Local-leader proposes directly; non-leader forwards via
    /// `MeshWrite::ForwardDdl` to the meta leader's peer. Mirrors
    /// `propose_partition_command` for the per-partition case.
    /// On success, advances the local DDL barrier to the latest
    /// meta-applied index — subsequent partition writes through
    /// this peer await their leaders' meta replicas to catch up.
    async fn propose_meta_command(
        &self,
        multi_raft: &Arc<crate::MultiRaftCluster>,
        entry: GraphCommand,
    ) -> std::result::Result<(), Status> {
        // Try local propose first. If we're the meta leader the
        // entry commits immediately; if not, openraft returns
        // ForwardToLeader and we proxy.
        match multi_raft.meta.propose_graph(entry.clone()).await {
            Ok(_) => {
                // Local meta has applied. Snapshot the latest
                // applied index as the new barrier so partition
                // writes from this peer wait for it to propagate.
                let target = multi_raft.meta_last_applied();
                multi_raft.observe_meta_barrier(target);
                // Synchronous-DDL gate: wait for every peer's meta
                // replica to apply before returning to the user.
                // Once this clears, any subsequent write — anywhere
                // in the cluster — observes the DDL on its
                // partition leader's local meta replica.
                self.await_cluster_meta_apply(
                    multi_raft,
                    target,
                    crate::DEFAULT_DDL_STRICT_TIMEOUT,
                )
                .await?;
                Ok(())
            }
            Err(meshdb_cluster::Error::ForwardToLeader { leader_address, .. }) => {
                let leader_addr = leader_address.ok_or_else(|| {
                    Status::unavailable("meta-raft leader unknown; retry after election")
                })?;
                let mut client = self
                    .leader_write_client(&leader_addr)
                    .map_err(|e| Status::unavailable(format!("connecting to meta leader: {e}")))?;
                let commands_json = serde_json::to_vec(&vec![entry])
                    .map_err(|e| Status::internal(format!("encoding ddl: {e}")))?;
                let resp = client
                    .forward_ddl(crate::proto::ForwardDdlRequest { commands_json })
                    .await?
                    .into_inner();
                if resp.ok {
                    Ok(())
                } else {
                    Err(Status::unavailable(format!(
                        "forwarded ddl rejected: {}",
                        resp.error_message
                    )))
                }
            }
            Err(e) => Err(raft_propose_failed(e)),
        }
    }

    /// Propose a single `GraphCommand` through `partition`'s Raft
    /// group. Local-leader path proposes directly; non-leader path
    /// forwards via `MeshWrite::ForwardWrite`. Refreshes the leader
    /// cache on stale-leader hints. Awaits the DDL barrier on the
    /// local meta replica before any propose so a write doesn't
    /// race ahead of a CREATE INDEX visible on the proposing peer.
    async fn propose_partition_command(
        &self,
        multi_raft: &Arc<crate::MultiRaftCluster>,
        partition: meshdb_cluster::PartitionId,
        entry: GraphCommand,
    ) -> std::result::Result<(), Status> {
        // DDL barrier: wait up to 500ms for our meta replica to
        // catch up to whatever DDL the cluster has committed. The
        // tx-coordination markers (PreparedTx / CommitTx / AbortTx)
        // are out-of-band — they don't depend on schema state, so
        // skipping the wait keeps recovery latency-bounded.
        let skip_barrier = matches!(
            entry,
            GraphCommand::PreparedTx { .. }
                | GraphCommand::CommitTx { .. }
                | GraphCommand::AbortTx { .. }
        );
        if !skip_barrier {
            if let Err(e) = multi_raft
                .await_meta_barrier(std::time::Duration::from_millis(500))
                .await
            {
                return Err(Status::unavailable(format!("ddl barrier: {e}")));
            }
        }
        if multi_raft.is_local_leader(partition) {
            if let Some(raft) = multi_raft.partition(partition) {
                return match raft.propose_graph(entry).await {
                    Ok(_) => Ok(()),
                    Err(meshdb_cluster::Error::ForwardToLeader { leader_id, .. }) => {
                        if let Some(l) = leader_id {
                            multi_raft.leader_cache.set(partition, l.0);
                        } else {
                            multi_raft.leader_cache.invalidate(partition);
                        }
                        Err(Status::unavailable(
                            "partition leadership changed mid-propose; retry",
                        ))
                    }
                    Err(e) => Err(raft_propose_failed(e)),
                };
            }
        }

        let leader_id = multi_raft.leader_of(partition).ok_or_else(|| {
            Status::unavailable(format!("no leader for partition {}", partition.0))
        })?;
        let state = multi_raft.meta.current_state().await;
        let leader_addr = state
            .membership
            .iter()
            .find_map(|(id, addr)| {
                if id.0 == leader_id {
                    Some(addr.to_string())
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                Status::internal(format!("leader {leader_id} not in cluster membership"))
            })?;
        drop(state);

        let commands_json = serde_json::to_vec(&entry_to_vec(entry))
            .map_err(|e| Status::internal(format!("encoding commands: {e}")))?;
        let mut client = self
            .leader_write_client(&leader_addr)
            .map_err(|e| Status::unavailable(format!("connecting to partition leader: {e}")))?;
        let resp = client
            .forward_write(crate::proto::ForwardWriteRequest {
                partition: partition.0,
                commands_json,
                idempotency_key: Vec::new(),
                min_meta_index: multi_raft
                    .min_meta_index
                    .load(std::sync::atomic::Ordering::SeqCst),
            })
            .await?
            .into_inner();
        if resp.ok {
            Ok(())
        } else {
            if resp.leader_hint != 0 {
                multi_raft.leader_cache.set(partition, resp.leader_hint);
            } else {
                multi_raft.leader_cache.invalidate(partition);
            }
            Err(Status::unavailable(format!(
                "forwarded propose rejected: {}",
                resp.error_message
            )))
        }
    }

    /// Single-partition multi-raft commit. Local-leader path proposes
    /// directly; non-leader path proxies via `MeshWrite::ForwardWrite`
    /// to the partition leader's peer. The proxy hop reuses the
    /// shared `GrpcNetwork` channel pool — no new TCP connections.
    async fn commit_multi_raft_single_partition(
        &self,
        multi_raft: &Arc<crate::MultiRaftCluster>,
        partition: meshdb_cluster::PartitionId,
        commands: Vec<GraphCommand>,
    ) -> std::result::Result<(), Status> {
        let entry = if commands.len() == 1 {
            commands.into_iter().next().unwrap()
        } else {
            GraphCommand::Batch(commands.clone())
        };

        if multi_raft.is_local_leader(partition) {
            if let Some(raft) = multi_raft.partition(partition) {
                return match raft.propose_graph(entry).await {
                    Ok(_) => Ok(()),
                    Err(meshdb_cluster::Error::ForwardToLeader { leader_id, .. }) => {
                        // Lost leadership between cache check and propose —
                        // refresh and surface a retryable error.
                        if let Some(l) = leader_id {
                            multi_raft.leader_cache.set(partition, l.0);
                        } else {
                            multi_raft.leader_cache.invalidate(partition);
                        }
                        Err(Status::unavailable(
                            "partition leadership changed mid-propose; retry",
                        ))
                    }
                    Err(e) => Err(raft_propose_failed(e)),
                };
            }
        }

        // Encode the commands once. Both attempts of the forward
        // path reuse this buffer.
        let commands_json = serde_json::to_vec(&entry_to_vec(entry))
            .map_err(|e| Status::internal(format!("encoding commands: {e}")))?;

        // Non-leader path: forward to the partition leader's peer.
        // We make up to 2 attempts: if the first lands on a stale
        // cache entry (the proxy peer rejects with a leader_hint),
        // we refresh and retry once. A single leader change should
        // not surface as `Unavailable` to the caller.
        let mut last_err: Option<Status> = None;
        for attempt in 0..2 {
            let leader_id = match multi_raft.leader_of(partition) {
                Some(l) => l,
                None => {
                    last_err = Some(Status::unavailable(format!(
                        "no known leader for partition {} yet; retry shortly",
                        partition.0
                    )));
                    // Brief settle before retrying — election may be
                    // mid-flight.
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
            };

            // Resolve the leader's peer address from cluster
            // membership. We rely on the meta cluster state's
            // membership list (every peer is in the meta group).
            let state = multi_raft.meta.current_state().await;
            let leader_addr = state.membership.iter().find_map(|(id, addr)| {
                if id.0 == leader_id {
                    Some(addr.to_string())
                } else {
                    None
                }
            });
            drop(state);
            let leader_addr = match leader_addr {
                Some(a) => a,
                None => {
                    return Err(Status::internal(format!(
                        "leader {leader_id} not in cluster membership"
                    )))
                }
            };

            let mut client = match self.leader_write_client(&leader_addr) {
                Ok(c) => c,
                Err(e) => {
                    last_err = Some(Status::unavailable(format!(
                        "connecting to partition leader: {e}"
                    )));
                    continue;
                }
            };
            let resp = match client
                .forward_write(crate::proto::ForwardWriteRequest {
                    partition: partition.0,
                    commands_json: commands_json.clone(),
                    idempotency_key: Vec::new(),
                    min_meta_index: multi_raft
                        .min_meta_index
                        .load(std::sync::atomic::Ordering::SeqCst),
                })
                .await
            {
                Ok(r) => r.into_inner(),
                Err(e) => {
                    last_err = Some(e);
                    multi_raft.leader_cache.invalidate(partition);
                    continue;
                }
            };
            if resp.ok {
                let outcome = if attempt == 0 { "committed" } else { "retried" };
                crate::metrics::MULTI_RAFT_FORWARD_WRITES_TOTAL
                    .with_label_values(&[outcome])
                    .inc();
                return Ok(());
            }
            // Refresh the leader cache from the hint so the next
            // attempt (or the next request from the caller) routes
            // correctly.
            if resp.leader_hint != 0 {
                multi_raft.leader_cache.set(partition, resp.leader_hint);
            } else {
                multi_raft.leader_cache.invalidate(partition);
            }
            last_err = Some(Status::unavailable(format!(
                "forwarded write rejected by peer {leader_id}: {}",
                resp.error_message
            )));
            // Only loop again on the first attempt — second rejection
            // is surfaced to the caller.
            let _ = attempt;
        }
        crate::metrics::MULTI_RAFT_FORWARD_WRITES_TOTAL
            .with_label_values(&["exhausted"])
            .inc();
        Err(last_err.unwrap_or_else(|| Status::unavailable("forward_write retry exhausted")))
    }

    /// suppresses further trigger evaluation so a trigger's own
    /// writes don't infinitely re-fire. External callers always
    /// hit the public wrapper above with `from_trigger = false`.
    async fn commit_buffered_commands_inner(
        &self,
        #[cfg_attr(not(feature = "apoc-trigger"), allow(unused_mut))] mut commands: Vec<
            GraphCommand,
        >,
        from_trigger: bool,
    ) -> std::result::Result<(), Status> {
        let _ = from_trigger;
        if commands.is_empty() {
            return Ok(());
        }
        // Before-phase triggers fire here, ahead of any commit
        // path. Their writes get appended to the prepared batch
        // so they commit atomically with the user's writes; if
        // a before-trigger errors, the whole commit aborts —
        // and the rollback-phase triggers fire as part of the
        // abort path.
        #[cfg(feature = "apoc-trigger")]
        if !from_trigger {
            if let Some(diff) = self.snapshot_trigger_diff(&commands) {
                match self.fire_before_triggers_collect_writes(&diff) {
                    Ok(extra) => commands.extend(extra),
                    Err(e) => {
                        self.fire_rollback_triggers(Some(diff)).await;
                        return Err(Status::aborted(e.to_string()));
                    }
                }
            }
        }
        if let Some(routing) = &self.routing {
            // Schema DDL isn't keyed to a partition and doesn't fit
            // the 2PC coordinator model — it applies globally. Split
            // DDL out of the batch, fan it out to every peer with
            // rollback-on-failure, then hand any remaining graph
            // mutations to the coordinator as usual. A query that's
            // DDL-only (the common case) never touches the
            // coordinator at all.
            let (ddl, graph) = split_ddl(commands);
            if !ddl.is_empty() {
                self.replicate_index_ddl_routing(routing, &ddl).await?;
            }
            if graph.is_empty() {
                return Ok(());
            }
            let mut coordinator =
                TxCoordinator::new(self.store.as_ref(), &self.pending_batches, routing)
                    .with_timeouts(self.tx_timeouts);
            if let Some(log) = self.coordinator_log.as_deref() {
                coordinator = coordinator.with_log(log);
            }
            #[cfg(any(test, feature = "fault-inject"))]
            {
                coordinator = coordinator.with_fault_points(self.fault_points.clone());
            }
            #[cfg(feature = "apoc-trigger")]
            let routing_diff = if from_trigger {
                None
            } else {
                self.snapshot_trigger_diff(&graph)
            };
            match coordinator.run(graph).await {
                Ok(()) => {
                    crate::metrics::TWO_PHASE_COMMIT_TOTAL
                        .with_label_values(&["committed"])
                        .inc();
                    // Routing-mode trigger firing — only the
                    // coordinator (us) fires. Trigger-induced
                    // writes go through this same commit path
                    // recursively (with `from_trigger = true` so
                    // they don't re-fire), so they replicate
                    // through the same 2PC machinery the
                    // originator's writes did.
                    #[cfg(feature = "apoc-trigger")]
                    if !from_trigger {
                        self.fire_post_commit_triggers(routing_diff).await;
                    }
                    return Ok(());
                }
                Err(e) => {
                    crate::metrics::TWO_PHASE_COMMIT_TOTAL
                        .with_label_values(&["aborted"])
                        .inc();
                    #[cfg(feature = "apoc-trigger")]
                    if !from_trigger {
                        self.fire_rollback_triggers(routing_diff).await;
                    }
                    return Err(e);
                }
            }
        }
        if let Some(multi_raft) = self.multi_raft.clone() {
            #[cfg(feature = "apoc-trigger")]
            let mr_diff = if from_trigger {
                None
            } else {
                self.snapshot_trigger_diff(&commands)
            };
            // Snapshot whether the batch touches trigger DDL before
            // commands move into commit_multi_raft — same logic as
            // the single-node branch. Refresh the procedure
            // factory's TriggerRegistry post-commit so a follow-up
            // `apoc.trigger.list()` reflects the change.
            #[cfg(feature = "apoc-trigger")]
            let touched_triggers = commands.iter().any(|c| {
                matches!(
                    c,
                    GraphCommand::InstallTrigger { .. } | GraphCommand::DropTrigger { .. }
                )
            });
            let outcome = self.commit_multi_raft(&multi_raft, commands).await;
            return match outcome {
                Ok(()) => {
                    #[cfg(feature = "apoc-trigger")]
                    if touched_triggers {
                        if let Some(reg) = (self.procedure_registry_factory)().trigger_registry() {
                            let _ = reg.refresh();
                        }
                    }
                    #[cfg(feature = "apoc-trigger")]
                    if !from_trigger {
                        self.fire_post_commit_triggers(mr_diff).await;
                    }
                    Ok(())
                }
                Err(e) => {
                    #[cfg(feature = "apoc-trigger")]
                    if !from_trigger {
                        self.fire_rollback_triggers(mr_diff).await;
                    }
                    Err(e)
                }
            };
        }
        if let Some(raft) = &self.raft {
            #[cfg(feature = "apoc-trigger")]
            let raft_diff = if from_trigger {
                None
            } else {
                self.snapshot_trigger_diff(&commands)
            };
            let entry = if commands.len() == 1 {
                commands.into_iter().next().unwrap()
            } else {
                GraphCommand::Batch(commands)
            };
            return match raft.propose_graph(entry).await {
                Ok(_) => {
                    crate::metrics::RAFT_PROPOSALS_TOTAL
                        .with_label_values(&["committed"])
                        .inc();
                    // Raft-mode trigger firing — only the
                    // proposer (us; non-leaders forward + never
                    // reach this branch) fires. Followers see the
                    // commit through their own
                    // `StoreGraphApplier::apply`. Trigger writes
                    // recurse through propose_graph so every peer
                    // applies them through the Raft log.
                    #[cfg(feature = "apoc-trigger")]
                    if !from_trigger {
                        self.fire_post_commit_triggers(raft_diff).await;
                    }
                    Ok(())
                }
                Err(ClusterError::ForwardToLeader {
                    leader_address: Some(addr),
                    ..
                }) => {
                    crate::metrics::RAFT_PROPOSALS_TOTAL
                        .with_label_values(&["forwarded"])
                        .inc();
                    // Forwarding doesn't count as a rollback — the
                    // commit hasn't actually failed; the client
                    // will retry against the leader.
                    Err(Status::failed_precondition(format!(
                        "raft leader is at {addr}; reconnect and retry there"
                    )))
                }
                Err(e) => {
                    crate::metrics::RAFT_PROPOSALS_TOTAL
                        .with_label_values(&["failed"])
                        .inc();
                    #[cfg(feature = "apoc-trigger")]
                    if !from_trigger {
                        self.fire_rollback_triggers(raft_diff).await;
                    }
                    Err(raft_propose_failed(e))
                }
            };
        }
        // Single-node: apply the batch directly through the store's
        // atomic apply_batch helper (one rocksdb WriteBatch).
        // Triggers fire AFTER the commit succeeds — see
        // `fire_triggers_after_commit` for the pre-commit diff
        // snapshot + post-commit firing dance.
        #[cfg(feature = "apoc-trigger")]
        let trigger_diff = if from_trigger {
            None
        } else {
            self.snapshot_trigger_diff(&commands)
        };
        let apply_result = apply_prepared_batch(self.store.as_ref(), &commands);
        // Refresh the trigger registry first whether the apply
        // succeeded or failed — trigger DDL is idempotent and the
        // registry's view should match storage in either case.
        #[cfg(feature = "apoc-trigger")]
        {
            let touched_triggers = commands.iter().any(|c| {
                matches!(
                    c,
                    GraphCommand::InstallTrigger { .. } | GraphCommand::DropTrigger { .. }
                )
            });
            if touched_triggers {
                if let Some(reg) = (self.procedure_registry_factory)().trigger_registry() {
                    let _ = reg.refresh();
                }
            }
        }
        match apply_result {
            Ok(()) => {
                #[cfg(feature = "apoc-trigger")]
                if !from_trigger {
                    self.fire_post_commit_triggers(trigger_diff).await;
                }
                Ok(())
            }
            Err(e) => {
                #[cfg(feature = "apoc-trigger")]
                if !from_trigger {
                    self.fire_rollback_triggers(trigger_diff).await;
                }
                Err(internal(e))
            }
        }
    }

    /// Compute the trigger diff against the pre-commit store state.
    /// Returns `None` when no trigger registry is attached or the
    /// registry is empty, so the commit path skips the diff
    /// computation cost in the common case. Held as a snapshot
    /// so the firing path doesn't need to re-examine the commands
    /// (they've already been applied to the store).
    #[cfg(feature = "apoc-trigger")]
    fn snapshot_trigger_diff(
        &self,
        commands: &[GraphCommand],
    ) -> Option<meshdb_executor::apoc_trigger::TriggerDiff> {
        let factory = &self.procedure_registry_factory;
        let registry = factory();
        let trig = registry.trigger_registry()?;
        if trig.is_empty() {
            return None;
        }
        if meshdb_executor::apoc_trigger::is_suppressed() {
            return None;
        }
        Some(compute_trigger_diff(self.store.as_ref(), commands))
    }

    /// Fire every registered before-phase trigger against the
    /// pre-commit diff. Returns Ok with the trigger writes the
    /// caller should merge into the prepared batch, or Err if
    /// any trigger errors — aborting the commit.
    ///
    /// Sync — before-phase triggers run inline as part of the
    /// commit path, so the caller stays in `&self`.
    #[cfg(feature = "apoc-trigger")]
    fn fire_before_triggers_collect_writes(
        &self,
        diff: &meshdb_executor::apoc_trigger::TriggerDiff,
    ) -> std::result::Result<Vec<GraphCommand>, meshdb_executor::Error> {
        let factory = &self.procedure_registry_factory;
        let procedures = factory();
        let Some(trig) = procedures.trigger_registry().cloned() else {
            return Ok(Vec::new());
        };
        let storage_reader =
            meshdb_executor::StorageReaderAdapter(self.store.as_ref() as &dyn StorageEngine);
        let writer = BufferingGraphWriter::new();
        // Clone the diff so the original snapshot stays available
        // for the after-fire later.
        let diff_clone = diff.clone_diff();
        meshdb_executor::apoc_trigger::fire_before_triggers(
            &trig,
            diff_clone,
            &storage_reader as &dyn meshdb_executor::GraphReader,
            &writer as &dyn meshdb_executor::GraphWriter,
            &procedures,
        )?;
        Ok(writer.into_commands())
    }

    /// Combined after-phase + afterAsync trigger firing dispatch.
    /// Runs the synchronous after-phase (which awaits any
    /// trigger-induced cluster commits) and then spawns
    /// afterAsync in the background. Called from each commit
    /// branch's success arm.
    #[cfg(feature = "apoc-trigger")]
    async fn fire_post_commit_triggers(
        &self,
        diff: Option<meshdb_executor::apoc_trigger::TriggerDiff>,
    ) {
        let Some(diff) = diff else { return };
        // Clone fields for the spawned afterAsync arm; the sync
        // after-phase consumes the original diff.
        let async_diff = diff.clone_diff();
        self.fire_triggers_after_commit(Some(diff)).await;
        self.spawn_after_async_triggers(async_diff);
    }

    /// Fire registered `afterAsync`-phase triggers against a
    /// snapshot of the diff. Spawns a background task — does
    /// NOT block the originator's response. The clone of
    /// `MeshService` is cheap (every internal field is `Arc`).
    #[cfg(feature = "apoc-trigger")]
    fn spawn_after_async_triggers(&self, diff: meshdb_executor::apoc_trigger::TriggerDiff) {
        let factory = &self.procedure_registry_factory;
        let procedures = factory();
        let Some(trig) = procedures.trigger_registry().cloned() else {
            return;
        };
        // Quick check: nothing registered for this phase, skip
        // the spawn cost entirely.
        if !trig
            .list()
            .iter()
            .any(|t| !t.paused && t.phase == "afterAsync")
        {
            return;
        }
        let svc = self.clone();
        tokio::spawn(async move {
            let storage_reader =
                meshdb_executor::StorageReaderAdapter(svc.store.as_ref() as &dyn StorageEngine);
            let writer = BufferingGraphWriter::new();
            let factory = &svc.procedure_registry_factory;
            let procedures = factory();
            let trig = match procedures.trigger_registry().cloned() {
                Some(t) => t,
                None => return,
            };
            meshdb_executor::apoc_trigger::fire_phase_triggers(
                &trig,
                "afterAsync",
                diff,
                &storage_reader as &dyn meshdb_executor::GraphReader,
                &writer as &dyn meshdb_executor::GraphWriter,
                &procedures,
            );
            // Commit any writes via the cluster commit path with
            // from_trigger=true to suppress further firing.
            let cmds = writer.into_commands();
            if !cmds.is_empty() {
                if let Err(e) = svc.commit_buffered_commands_inner(cmds, true).await {
                    tracing::warn!(
                        error = %e,
                        "afterAsync trigger writes failed to commit"
                    );
                }
            }
        });
    }

    /// Fire registered `rollback`-phase triggers against the
    /// pre-commit diff snapshot. Synchronous — runs in the same
    /// task that handled the failing commit so the response
    /// surface stays predictable. Errors from the trigger body
    /// are logged and swallowed; we don't want a flaky rollback
    /// trigger to mask the real commit failure that's about to
    /// be returned.
    #[cfg(feature = "apoc-trigger")]
    async fn fire_rollback_triggers(
        &self,
        diff: Option<meshdb_executor::apoc_trigger::TriggerDiff>,
    ) {
        let Some(diff) = diff else { return };
        let factory = &self.procedure_registry_factory;
        let procedures = factory();
        let Some(trig) = procedures.trigger_registry().cloned() else {
            return;
        };
        let storage_reader =
            meshdb_executor::StorageReaderAdapter(self.store.as_ref() as &dyn StorageEngine);
        let writer = BufferingGraphWriter::new();
        meshdb_executor::apoc_trigger::fire_phase_triggers(
            &trig,
            "rollback",
            diff,
            &storage_reader as &dyn meshdb_executor::GraphReader,
            &writer as &dyn meshdb_executor::GraphWriter,
            &procedures,
        );
        // Rollback triggers may write (e.g., to an audit log).
        // Those writes commit through the normal cluster path so
        // they reach every peer.
        let cmds = writer.into_commands();
        if !cmds.is_empty() {
            let inner_fut = Box::pin(self.commit_buffered_commands_inner(cmds, true));
            if let Err(e) = inner_fut.await {
                tracing::warn!(
                    error = %e,
                    "rollback trigger writes failed to commit"
                );
            }
        }
    }

    /// Fire all registered after-phase triggers against a diff
    /// captured before the commit. Runs the trigger Cypher
    /// synchronously then commits any trigger-induced writes
    /// recursively through `commit_buffered_commands_inner`
    /// with `from_trigger = true`, so cluster-mode trigger
    /// writes ride through the same Raft / 2PC machinery the
    /// originating commit did.
    ///
    /// Failures during firing or the recursive commit are logged
    /// and swallowed — the originating transaction has already
    /// committed; an erroring trigger doesn't undo user work.
    /// The from_trigger flag prevents the inner commit from
    /// re-evaluating triggers, closing the obvious infinite-fire
    /// loop.
    #[cfg(feature = "apoc-trigger")]
    async fn fire_triggers_after_commit(
        &self,
        diff: Option<meshdb_executor::apoc_trigger::TriggerDiff>,
    ) {
        let Some(diff) = diff else { return };
        let factory = &self.procedure_registry_factory;
        let procedures = factory();
        let Some(trig) = procedures.trigger_registry().cloned() else {
            return;
        };
        let storage_reader =
            meshdb_executor::StorageReaderAdapter(self.store.as_ref() as &dyn StorageEngine);
        let writer = BufferingGraphWriter::new();
        meshdb_executor::apoc_trigger::fire_after_triggers(
            &trig,
            diff,
            &storage_reader as &dyn meshdb_executor::GraphReader,
            &writer as &dyn meshdb_executor::GraphWriter,
            &procedures,
        );
        // Trigger bodies that wrote now have buffered commands
        // in `writer`. Recurse through commit_buffered_commands
        // so cluster modes replicate the writes; the
        // `from_trigger = true` flag suppresses further firing.
        // Box::pin'd because async-fn recursion needs explicit
        // future indirection — we self-call.
        let cmds = writer.into_commands();
        if !cmds.is_empty() {
            let inner_fut = Box::pin(self.commit_buffered_commands_inner(cmds, true));
            if let Err(e) = inner_fut.await {
                tracing::warn!(
                    error = %e,
                    "committing trigger-induced writes failed"
                );
            }
        }
    }

    /// Routing-mode DDL fan-out with rollback. Apply every entry in
    /// `ddl` (a list of `CreateIndex` / `DropIndex` commands) to the
    /// local store first, then RPC every other peer in parallel. If
    /// any peer rejects the change, invert each successful
    /// application — including the local one — so the cluster
    /// doesn't end up with half the peers holding an index that the
    /// others don't.
    ///
    /// Rollback is best-effort: a compensating failure is logged but
    /// doesn't propagate, since there's no safer fallback. In
    /// practice this only matters for CREATE failures; DROP has to
    /// re-backfill from the live local graph on rollback and
    /// succeeds as long as the store is still reachable.
    #[tracing::instrument(skip_all, fields(ddl_count = ddl.len()))]
    async fn replicate_index_ddl_routing(
        &self,
        routing: &Arc<Routing>,
        ddl: &[GraphCommand],
    ) -> std::result::Result<(), Status> {
        // Local apply first — if this fails we haven't touched any
        // peer, so the caller's error is clean.
        for cmd in ddl {
            apply_ddl_to_store(cmd, self.store.as_ref()).map_err(internal)?;
        }
        // Refresh the local trigger registry if the batch
        // touched it; the receiver-side RPC handler does the
        // same on its end. Cheap (one CF iter) so we don't
        // bother gating on whether triggers were involved.
        #[cfg(feature = "apoc-trigger")]
        {
            let any_trigger = ddl.iter().any(|c| {
                matches!(
                    c,
                    GraphCommand::InstallTrigger { .. } | GraphCommand::DropTrigger { .. }
                )
            });
            if any_trigger {
                if let Some(reg) = (self.procedure_registry_factory)().trigger_registry() {
                    let _ = reg.refresh();
                }
            }
        }

        let self_id = routing.cluster().self_id();
        let remote_peers: Vec<PeerId> = routing
            .cluster()
            .membership()
            .peer_ids()
            .filter(|p| *p != self_id)
            .collect();

        // Parallel fan-out. `try_remote_ddl_on_peer` applies every
        // entry in `ddl` to the target peer sequentially; if the
        // peer succeeds on the first and fails on the second, the
        // partial state is recorded via the return value and the
        // top-level rollback undoes the successes.
        let mut handles = Vec::with_capacity(remote_peers.len());
        for peer_id in &remote_peers {
            let ddl = ddl.to_vec();
            let client = routing
                .write_client(*peer_id)
                .ok_or_else(|| no_client(*peer_id))?;
            let peer_id = *peer_id;
            handles.push(tokio::spawn(async move {
                let result = try_remote_ddl_on_peer(client, &ddl).await;
                (peer_id, result)
            }));
        }

        let mut succeeded: Vec<PeerId> = Vec::new();
        let mut first_error: Option<Status> = None;
        for h in handles {
            match h.await {
                Ok((peer_id, Ok(()))) => succeeded.push(peer_id),
                Ok((_, Err(status))) => {
                    if first_error.is_none() {
                        first_error = Some(status);
                    }
                }
                Err(join_err) => {
                    if first_error.is_none() {
                        first_error = Some(Status::internal(format!(
                            "ddl fan-out task panicked: {join_err}"
                        )));
                    }
                }
            }
        }

        if let Some(err) = first_error {
            // Compensate the local application and every successful
            // peer. Failures during rollback are logged and swallowed
            // — the user already sees the original error.
            for cmd in ddl.iter().rev() {
                let inverse = invert_ddl(cmd);
                if let Err(e) = apply_ddl_to_store(&inverse, self.store.as_ref()) {
                    tracing::error!(error = %e, "local DDL rollback failed");
                }
            }
            for peer_id in succeeded {
                if let Some(client) = routing.write_client(peer_id) {
                    if let Err(e) = try_remote_ddl_on_peer(
                        client,
                        &ddl.iter().rev().map(invert_ddl).collect::<Vec<_>>(),
                    )
                    .await
                    {
                        tracing::error!(error = %e, peer = peer_id.0, "remote DDL rollback failed");
                    }
                }
            }
            return Err(err);
        }

        Ok(())
    }

    /// Walk the coordinator log and push every unfinished transaction
    /// forward to its recorded decision — or, if no decision was ever
    /// recorded, roll it back. Call this once at startup, before the
    /// service begins accepting new traffic, so participants aren't
    /// left holding stuck staged batches from a previous run.
    ///
    /// Recovery decisions:
    ///
    /// - **Completed**: skip. Tx is already done; compaction will drop
    ///   the entries.
    /// - **CommitDecision present**: resend COMMIT to every peer in
    ///   the prepared groups. If a peer replies "not prepared" (its
    ///   own staging was lost to a crash), resend PREPARE with the
    ///   original commands and then COMMIT.
    /// - **AbortDecision present**: resend ABORT to every peer.
    ///   Idempotent on the participant side.
    /// - **No decision** (only Prepared): treat as abort. The original
    ///   client never saw a commit success, so rolling back is safe.
    ///
    /// After reconciling, append a `Completed` entry for each tx we
    /// touched and compact the log so future recovery runs stay
    /// short.
    ///
    /// Failures talking to individual peers are logged and swallowed
    /// — the tx is marked completed anyway, because a stuck staging
    /// entry on an unreachable peer is the participant's problem to
    /// resolve on *its* restart (the staging is in-memory and dies
    /// with the process).
    /// Rehydrate participant-side 2PC state from the durable log at
    /// startup. Synchronous complement to
    /// [`Self::recover_participant_decisions`]; call this one first
    /// so the staging map is populated before peer polling.
    ///
    /// Walks the participant log end-to-end:
    /// - Every `Prepared` whose last entry is still `Prepared` gets
    ///   its commands re-inserted into staging, so a late COMMIT
    ///   finds them.
    /// - Every `Committed` / `Aborted` populates the outcomes cache,
    ///   so a duplicate COMMIT / ABORT short-circuits with the right
    ///   answer instead of `failed_precondition`.
    ///
    /// No-op when the service has no participant log configured
    /// (single-node services, Raft mode).
    pub fn recover_participant_staging(&self) -> std::result::Result<(), Status> {
        let Some(log) = self.participant_log.clone() else {
            return Ok(());
        };
        let entries = log
            .read_all()
            .map_err(|e| Status::internal(format!("reading participant log: {e}")))?;
        let in_doubt = crate::replay_in_doubt_commands(&entries);
        let outcomes = crate::replay_outcomes(&entries);

        let in_doubt_count = in_doubt.len();
        for (txid, commands) in in_doubt {
            self.pending_batches.rehydrate(txid, commands);
        }
        let mut committed_count = 0usize;
        let mut aborted_count = 0usize;
        for (txid, outcome) in outcomes {
            match outcome {
                crate::ParticipantOutcome::Committed => {
                    self.pending_batches
                        .finalize(txid, crate::staging::TerminalOutcome::Committed);
                    committed_count += 1;
                }
                crate::ParticipantOutcome::Aborted => {
                    self.pending_batches
                        .finalize(txid, crate::staging::TerminalOutcome::Aborted);
                    aborted_count += 1;
                }
                crate::ParticipantOutcome::Prepared => {
                    // Already handled via the in_doubt rehydration above.
                }
            }
        }
        if in_doubt_count + committed_count + aborted_count > 0 {
            tracing::info!(
                in_doubt = in_doubt_count,
                committed = committed_count,
                aborted = aborted_count,
                "recovered participant 2PC state from log",
            );
        }
        Ok(())
    }

    /// After [`Self::recover_participant_staging`] has rehydrated the
    /// staging map, poll every peer's `ResolveTransaction` RPC for
    /// each in-doubt txid and apply the decision locally when a peer
    /// reports one. Closes the "coordinator alive, participant
    /// crashed" window without waiting out the staging TTL — the
    /// original coordinator's log carries the authoritative decision,
    /// and any cluster peer can look it up for us.
    ///
    /// UNKNOWN from every peer means no coordinator has the
    /// decision; the txid keeps its in-doubt state and eventually
    /// ages out via the staging sweeper. No-op when the service has
    /// no participant log or no routing (single-node / Raft).
    pub async fn recover_participant_decisions(&self) -> std::result::Result<(), Status> {
        let Some(log) = self.participant_log.clone() else {
            return Ok(());
        };
        let Some(routing) = self.routing.clone() else {
            return Ok(());
        };

        let entries = log
            .read_all()
            .map_err(|e| Status::internal(format!("reading participant log: {e}")))?;
        let in_doubt = crate::replay_in_doubt_commands(&entries);
        if in_doubt.is_empty() {
            return Ok(());
        }

        let self_id = routing.cluster().self_id();
        let remote_peers: Vec<meshdb_cluster::PeerId> = routing
            .cluster()
            .membership()
            .peer_ids()
            .filter(|p| *p != self_id)
            .collect();
        if remote_peers.is_empty() {
            return Ok(());
        }

        let mut resolved = 0usize;
        for (txid, _commands) in in_doubt {
            let mut decision: Option<crate::proto::TxResolutionStatus> = None;
            for peer in &remote_peers {
                let Some(mut client) = routing.write_client(*peer) else {
                    continue;
                };
                let req = ResolveTransactionRequest { txid: txid.clone() };
                match client.resolve_transaction(req).await {
                    Ok(resp) => {
                        let status =
                            crate::proto::TxResolutionStatus::try_from(resp.into_inner().status)
                                .unwrap_or(crate::proto::TxResolutionStatus::Unspecified);
                        match status {
                            crate::proto::TxResolutionStatus::Committed
                            | crate::proto::TxResolutionStatus::Aborted => {
                                decision = Some(status);
                                break;
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            peer = %peer,
                            txid = %txid,
                            error = %e,
                            "resolve_transaction RPC failed",
                        );
                    }
                }
            }

            match decision {
                Some(crate::proto::TxResolutionStatus::Committed) => {
                    if let Some(cmds) = self.pending_batches.take(&txid) {
                        if let Err(e) = apply_prepared_batch(self.store.as_ref(), &cmds) {
                            tracing::warn!(
                                txid = %txid,
                                error = %e,
                                "recovery apply failed; leaving in-doubt",
                            );
                            continue;
                        }
                    }
                    if let Err(e) =
                        log.append(&crate::ParticipantLogEntry::Committed { txid: txid.clone() })
                    {
                        tracing::warn!(error = %e, "participant log Committed append failed");
                    }
                    self.pending_batches
                        .finalize(txid, crate::staging::TerminalOutcome::Committed);
                    resolved += 1;
                }
                Some(crate::proto::TxResolutionStatus::Aborted) => {
                    let _ = self.pending_batches.take(&txid);
                    if let Err(e) =
                        log.append(&crate::ParticipantLogEntry::Aborted { txid: txid.clone() })
                    {
                        tracing::warn!(error = %e, "participant log Aborted append failed");
                    }
                    self.pending_batches
                        .finalize(txid, crate::staging::TerminalOutcome::Aborted);
                    resolved += 1;
                }
                _ => {
                    // No peer had a decision — leave it in-doubt for
                    // the coordinator to push or the TTL to clean up.
                }
            }
        }
        if resolved > 0 {
            tracing::info!(resolved, "resolved in-doubt transactions via peer polling");
        }
        Ok(())
    }

    pub async fn recover_pending_transactions(&self) -> std::result::Result<(), Status> {
        let Some(log) = self.coordinator_log.clone() else {
            return Ok(());
        };
        let Some(routing) = self.routing.clone() else {
            // Only routing mode uses the coordinator log.
            return Ok(());
        };

        let entries = log
            .read_all()
            .map_err(|e| Status::internal(format!("reading coordinator log: {e}")))?;
        let state = crate::coordinator_log::reconstruct_state(&entries);

        let unfinished: Vec<_> = state.values().filter(|s| !s.completed).cloned().collect();
        if unfinished.is_empty() {
            // Still compact: drop completed entries that accumulated
            // from previous runs.
            let keep: std::collections::HashSet<String> = std::collections::HashSet::new();
            let _ = log.compact(&keep);
            return Ok(());
        }

        tracing::info!(
            count = unfinished.len(),
            "recovering unfinished coordinator transactions",
        );

        for tx in &unfinished {
            match tx.decision {
                Some(crate::TxDecision::Commit) => {
                    self.recover_commit(&routing, &log, tx).await;
                }
                Some(crate::TxDecision::Abort) | None => {
                    // No decision → safe to abort. The original client
                    // hadn't received a commit ack, so nothing depends
                    // on this tx having committed.
                    self.recover_abort(&routing, &log, tx).await;
                }
            }
        }

        // All unfinished txids are now either committed or aborted.
        // Compact the log to drop every entry — the recovered txids
        // all have a Completed marker now.
        let keep: std::collections::HashSet<String> = std::collections::HashSet::new();
        if let Err(e) = log.compact(&keep) {
            tracing::warn!(error = %e, "compacting coordinator log after recovery");
        }
        Ok(())
    }

    /// Push a committed-but-unfinished tx forward on each peer. For
    /// each (peer, commands) pair: try COMMIT; if the peer lost its
    /// staging, resend PREPARE with the original commands and retry
    /// COMMIT. Errors on individual peers are logged and moved past.
    async fn recover_commit(
        &self,
        routing: &Arc<Routing>,
        log: &crate::CoordinatorLog,
        tx: &crate::TxState,
    ) {
        let txid = &tx.txid;
        let self_id = routing.cluster().self_id();
        for (peer_id, cmds) in &tx.groups {
            if *peer_id == self_id {
                // Local: the in-process pending_batches map was lost
                // to the crash, so just apply the commands directly.
                // This bypasses the prepare-commit dance but reaches
                // the same end state — an atomic apply_batch.
                if let Err(e) = apply_prepared_batch(self.store.as_ref(), cmds) {
                    tracing::warn!(
                        txid = %txid,
                        error = %e,
                        "local recovery apply_batch failed",
                    );
                }
                continue;
            }
            if let Err(e) = recover_commit_remote(routing, *peer_id, txid, cmds).await {
                tracing::warn!(
                    peer = %peer_id,
                    txid = %txid,
                    error = %e,
                    "remote recovery commit failed; skipping",
                );
            }
        }
        if let Err(e) = log.append(&crate::TxLogEntry::Completed { txid: txid.clone() }) {
            tracing::warn!(
                txid = %txid,
                error = %e,
                "coordinator log write (Completed during recovery) failed",
            );
        }
    }

    /// Push an aborted-or-undecided tx forward on each peer by
    /// resending ABORT. Idempotent on the participant side so this is
    /// safe to retry after crashes.
    async fn recover_abort(
        &self,
        routing: &Arc<Routing>,
        log: &crate::CoordinatorLog,
        tx: &crate::TxState,
    ) {
        let txid = &tx.txid;
        let self_id = routing.cluster().self_id();
        for peer_id in tx.groups.keys() {
            if *peer_id == self_id {
                // Local abort: drop any stale staging entry. The
                // pending_batches map is fresh after restart, so this
                // is a no-op in practice — kept for completeness and
                // in case we ever persist participant staging.
                let _ = self.pending_batches.take(txid);
                continue;
            }
            if let Err(e) = send_abort_remote(routing, *peer_id, txid).await {
                tracing::warn!(
                    peer = %peer_id,
                    txid = %txid,
                    error = %e,
                    "remote recovery abort failed; skipping",
                );
            }
        }
        if let Err(e) = log.append(&crate::TxLogEntry::Completed { txid: txid.clone() }) {
            tracing::warn!(
                txid = %txid,
                error = %e,
                "coordinator log write (Completed during recovery) failed",
            );
        }
    }

    /// Re-issue an `execute_cypher` gRPC call to the Raft leader and
    /// return the leader's rows. Used by [`execute_cypher_local`] when
    /// `commit_buffered_commands` reports a leader-redirect, and only
    /// reachable from the auto-commit path — Bolt explicit transactions
    /// surface the redirect to the driver via a FAILURE.
    async fn forward_execute_cypher_to_leader(
        &self,
        addr: &str,
        query: String,
        params: meshdb_executor::ParamMap,
    ) -> std::result::Result<Vec<meshdb_executor::Row>, Status> {
        let endpoint = self.peer_endpoint(addr)?;
        let mut client =
            crate::proto::mesh_query_client::MeshQueryClient::new(endpoint.connect_lazy());
        let params_json = serde_json::to_vec(&params)
            .map_err(|e| Status::internal(format!("encoding forwarded params: {e}")))?;
        let resp = client
            .execute_cypher(ExecuteCypherRequest { query, params_json })
            .await?;
        serde_json::from_slice(&resp.into_inner().rows_json)
            .map_err(|e| Status::internal(format!("decoding forwarded rows: {e}")))
    }
}

/// Recovery helper: try to COMMIT an in-flight tx on a remote peer.
/// If the peer's staging is gone (FailedPrecondition / "not
/// prepared"), resend PREPARE with the original commands and retry
/// COMMIT. Other error codes surface as-is so the caller can log them.
async fn recover_commit_remote(
    routing: &Arc<Routing>,
    peer: PeerId,
    txid: &str,
    cmds: &[GraphCommand],
) -> Result<(), Status> {
    let mut client = routing.write_client(peer).ok_or_else(|| no_client(peer))?;
    let commit_res = client
        .batch_write(crate::proto::BatchWriteRequest {
            txid: txid.to_string(),
            phase: crate::proto::BatchPhase::Commit as i32,
            commands_json: Vec::new(),
        })
        .await;
    match commit_res {
        Ok(_) => Ok(()),
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            // Peer forgot the staging — re-prepare and retry.
            let payload = serde_json::to_vec(&cmds.to_vec())
                .map_err(|e| Status::internal(format!("re-encoding PREPARE payload: {e}")))?;
            client
                .batch_write(crate::proto::BatchWriteRequest {
                    txid: txid.to_string(),
                    phase: crate::proto::BatchPhase::Prepare as i32,
                    commands_json: payload,
                })
                .await?;
            client
                .batch_write(crate::proto::BatchWriteRequest {
                    txid: txid.to_string(),
                    phase: crate::proto::BatchPhase::Commit as i32,
                    commands_json: Vec::new(),
                })
                .await?;
            Ok(())
        }
        Err(status) => Err(status),
    }
}

/// Recovery helper: send ABORT for `txid` to a remote peer.
/// Idempotent — the peer drops any staging entry whether or not one
/// exists.
async fn send_abort_remote(routing: &Arc<Routing>, peer: PeerId, txid: &str) -> Result<(), Status> {
    let mut client = routing.write_client(peer).ok_or_else(|| no_client(peer))?;
    client
        .batch_write(crate::proto::BatchWriteRequest {
            txid: txid.to_string(),
            phase: crate::proto::BatchPhase::Abort as i32,
            commands_json: Vec::new(),
        })
        .await?;
    Ok(())
}

/// Sniff a `Status` produced by `commit_buffered_commands` for the
/// "raft leader is at {addr}" leader-redirect message and return the
/// parsed address. Returns `None` for any other error so the caller
/// surfaces it verbatim.
fn leader_redirect_address(status: &Status) -> Option<String> {
    if status.code() != tonic::Code::FailedPrecondition {
        return None;
    }
    let msg = status.message();
    let prefix = "raft leader is at ";
    let rest = msg.strip_prefix(prefix)?;
    let addr = rest.split(';').next()?.trim();
    Some(addr.to_string())
}

/// Return the set of `PartitionId`s that `cmd`'s data writes touch.
/// `Batch` variants flatten into the same set; DDL is not partitioned
/// and contributes nothing. Used by the multi-raft commit path to
/// route a single-partition batch through one partition Raft and a
/// multi-partition batch through 2PC.
fn partitions_touched_by_command(
    cmd: &GraphCommand,
    partitioner: &meshdb_cluster::Partitioner,
    out: &mut std::collections::BTreeSet<meshdb_cluster::PartitionId>,
) {
    match cmd {
        GraphCommand::PutNode(n) => {
            out.insert(partitioner.partition_for(n.id));
        }
        GraphCommand::PutEdge(e) => {
            // Edges replicate to both endpoints' partitions for
            // forward + reverse adjacency.
            out.insert(partitioner.partition_for(e.source));
            out.insert(partitioner.partition_for(e.target));
        }
        GraphCommand::DetachDeleteNode(id) => {
            out.insert(partitioner.partition_for(*id));
        }
        GraphCommand::DeleteEdge(_) => {
            // We don't have the edge's endpoints from the id alone
            // here; the multi-raft commit path materializes
            // DeleteEdge as a broadcast across every partition that
            // could hold a copy. Adding every partition is the safe
            // fallback — future optimization could plumb through
            // the resolved endpoints.
            for p in 0..partitioner.num_partitions() {
                out.insert(meshdb_cluster::PartitionId(p));
            }
        }
        GraphCommand::Batch(inner) => {
            for c in inner {
                partitions_touched_by_command(c, partitioner, out);
            }
        }
        // DDL and tx markers don't contribute to data partition routing.
        _ => {}
    }
}

/// Wrap a single `GraphCommand` into a one-element vector or unwrap a
/// `Batch` into its constituent vector. Used by the multi-raft
/// forward path to round-trip the batched form across the wire.
fn entry_to_vec(entry: GraphCommand) -> Vec<GraphCommand> {
    match entry {
        GraphCommand::Batch(inner) => inner,
        other => vec![other],
    }
}

/// Walk `cmds`, distributing each leaf write into `by_partition`
/// according to the same routing the partition applier uses:
/// nodes / detach-deletes go to their owning partition only; edges
/// go to both endpoints' partitions; `Batch` recurses; DDL and tx
/// markers don't appear (caller has already split them out).
///
/// `DeleteEdge` is broadcast to every partition in the touched set
/// because the edge's endpoints aren't recoverable from its id alone
/// — we lean on the applier's idempotency for the no-op case.
fn flatten_commands_by_partition(
    cmds: &[GraphCommand],
    partitioner: &meshdb_cluster::Partitioner,
    by_partition: &mut std::collections::BTreeMap<meshdb_cluster::PartitionId, Vec<GraphCommand>>,
) {
    for cmd in cmds {
        match cmd {
            GraphCommand::PutNode(n) => {
                let p = partitioner.partition_for(n.id);
                by_partition
                    .entry(p)
                    .or_default()
                    .push(GraphCommand::PutNode(n.clone()));
            }
            GraphCommand::PutEdge(e) => {
                let src = partitioner.partition_for(e.source);
                let dst = partitioner.partition_for(e.target);
                by_partition
                    .entry(src)
                    .or_default()
                    .push(GraphCommand::PutEdge(e.clone()));
                if dst != src {
                    by_partition
                        .entry(dst)
                        .or_default()
                        .push(GraphCommand::PutEdge(e.clone()));
                }
            }
            GraphCommand::DetachDeleteNode(id) => {
                let p = partitioner.partition_for(*id);
                by_partition
                    .entry(p)
                    .or_default()
                    .push(GraphCommand::DetachDeleteNode(*id));
            }
            GraphCommand::DeleteEdge(id) => {
                // Broadcast to every partition we're already
                // visiting; the applier is idempotent on missing ids.
                let entries: Vec<meshdb_cluster::PartitionId> =
                    by_partition.keys().copied().collect();
                for p in entries {
                    by_partition
                        .entry(p)
                        .or_default()
                        .push(GraphCommand::DeleteEdge(*id));
                }
            }
            GraphCommand::Batch(inner) => {
                flatten_commands_by_partition(inner, partitioner, by_partition)
            }
            // DDL + tx markers don't reach this routing path.
            _ => {}
        }
    }
}

/// Flatten a tree of [`GraphCommand`] (which may nest `Batch` variants)
/// into a flat `Vec<GraphMutation>` so `StorageEngine::apply_batch` can
/// commit them atomically.
///
/// DDL commands (`CreateIndex` / `DropIndex`) are intentionally not
/// handled here — they can't be expressed as a `GraphMutation` because
/// the backfill step needs to read the live graph, and an uncommitted
/// batch isn't queryable. Callers going through [`apply_prepared_batch`]
/// get correct split semantics for free; direct callers of
/// `flatten_commands` must ensure the batch has already been stripped
/// of DDL entries.
pub(crate) fn flatten_commands(
    cmds: &[GraphCommand],
    out: &mut Vec<meshdb_storage::GraphMutation>,
) {
    use meshdb_storage::GraphMutation;
    for cmd in cmds {
        match cmd {
            GraphCommand::PutNode(n) => out.push(GraphMutation::PutNode(n.clone())),
            GraphCommand::PutEdge(e) => out.push(GraphMutation::PutEdge(e.clone())),
            GraphCommand::DeleteEdge(id) => out.push(GraphMutation::DeleteEdge(*id)),
            GraphCommand::DetachDeleteNode(id) => out.push(GraphMutation::DetachDeleteNode(*id)),
            GraphCommand::Batch(inner) => flatten_commands(inner, out),
            // Skip silently; the caller is responsible for applying
            // DDL through `StorageEngine::create_property_index` /
            // `drop_property_index` / `create_property_constraint` /
            // `drop_property_constraint` before (or alongside) this
            // batch.
            GraphCommand::CreateIndex { .. }
            | GraphCommand::DropIndex { .. }
            | GraphCommand::CreateEdgeIndex { .. }
            | GraphCommand::DropEdgeIndex { .. }
            | GraphCommand::CreatePointIndex { .. }
            | GraphCommand::DropPointIndex { .. }
            | GraphCommand::CreateEdgePointIndex { .. }
            | GraphCommand::DropEdgePointIndex { .. }
            | GraphCommand::CreateConstraint { .. }
            | GraphCommand::DropConstraint { .. }
            | GraphCommand::InstallTrigger { .. }
            | GraphCommand::DropTrigger { .. } => {}
            // Multi-raft tx-coordination markers — only the
            // PartitionGraphApplier consumes these; never reach the
            // routing-mode / single-Raft flatten path.
            GraphCommand::PreparedTx { .. }
            | GraphCommand::CommitTx { .. }
            | GraphCommand::AbortTx { .. } => {}
        }
    }
}

/// Apply a prepared batch to `store` atomically. Used by both the
/// `BatchWrite` commit phase and the in-process coordinator shortcut.
///
/// Index DDL in the batch is applied up-front via
/// `StorageEngine::create_property_index` / `drop_property_index`
/// (each in its own small batch), then the remaining graph mutations
/// commit as one atomic `apply_batch`. Cypher statements don't mix
/// DDL and graph writes so this ordering doesn't affect typical
/// workloads.
pub(crate) fn apply_prepared_batch(
    store: &dyn StorageEngine,
    cmds: &[GraphCommand],
) -> std::result::Result<(), meshdb_storage::Error> {
    apply_ddl_commands(cmds, store)?;
    let mut flat = Vec::with_capacity(cmds.len());
    flatten_commands(cmds, &mut flat);
    if flat.is_empty() {
        return Ok(());
    }
    store.apply_batch(&flat)
}

/// Find the (single) `ApocPeriodicIterate` node in `plan` and
/// return clones of its iterate plan, action plan, batch size,
/// and extra-params expressions. The dispatcher uses these to
/// drive the per-batch loop independently of any wrapping
/// clauses (Project / OrderBy / Limit / etc.) that surround
/// the procedure call site in the original plan tree.
fn find_apoc_periodic_iterate_node(
    plan: &meshdb_cypher::LogicalPlan,
) -> Option<(
    meshdb_cypher::LogicalPlan,
    meshdb_cypher::LogicalPlan,
    i64,
    std::collections::HashMap<String, meshdb_cypher::Expr>,
    i64,
    i64,
    bool,
    bool,
    usize,
)> {
    use meshdb_cypher::LogicalPlan as P;
    match plan {
        P::ApocPeriodicIterate {
            iterate,
            action,
            batch_size,
            extra_params,
            retries,
            failed_params_cap,
            iterate_list,
            parallel,
            concurrency,
        } => Some((
            (**iterate).clone(),
            (**action).clone(),
            *batch_size,
            extra_params.clone(),
            *retries,
            *failed_params_cap,
            *iterate_list,
            *parallel,
            *concurrency,
        )),
        P::Filter { input, .. }
        | P::Project { input, .. }
        | P::Aggregate { input, .. }
        | P::Distinct { input }
        | P::OrderBy { input, .. }
        | P::Skip { input, .. }
        | P::Limit { input, .. }
        | P::Identity { input }
        | P::CoalesceNullRow { input, .. }
        | P::UnwindChain { input, .. }
        | P::BindPath { input, .. }
        | P::ShortestPath { input, .. } => find_apoc_periodic_iterate_node(input),
        _ => None,
    }
}

/// Evaluate the `params` config map's expressions once, against
/// the calling query's existing param map. Returns a fresh
/// ParamMap suitable for merging with each input row's
/// bindings. Uses a lightweight execute path because the
/// expressions are typically literals/parameters — running them
/// via the full operator pipeline would be overkill, but reusing
/// `execute_with_reader_and_procs` keeps the eval consistent.
fn eval_extra_params(
    exprs: &std::collections::HashMap<String, meshdb_cypher::Expr>,
    base_params: &meshdb_executor::ParamMap,
    store: &std::sync::Arc<dyn StorageEngine>,
    routing: Option<&std::sync::Arc<Routing>>,
) -> std::result::Result<meshdb_executor::ParamMap, meshdb_executor::Error> {
    if exprs.is_empty() {
        return Ok(base_params.clone());
    }
    // Wrap each (name, expr) in a synthetic `RETURN <expr> AS <name>`
    // statement plan so the executor evaluates it correctly. We
    // build one synthetic Project + SeedRow per binding — small
    // overhead, never benchmarked because `params` lists are
    // typically tiny.
    let mut out = base_params.clone();
    for (name, expr) in exprs {
        let plan = meshdb_cypher::LogicalPlan::Project {
            input: Box::new(meshdb_cypher::LogicalPlan::SeedRow),
            items: vec![meshdb_cypher::ReturnItem {
                expr: expr.clone(),
                alias: Some(name.clone()),
                raw_text: None,
            }],
        };
        let storage_reader = StorageReaderAdapter(store.as_ref() as &dyn StorageEngine);
        let writer = BufferingGraphWriter::new();
        let mut procs = ProcedureRegistry::new();
        procs.register_defaults();
        let rows = if let Some(r) = routing {
            let partitioned = PartitionedGraphReader::new(store.clone(), r.clone());
            meshdb_executor::execute_with_reader_and_procs(
                &plan,
                &partitioned as &dyn GraphReader,
                &writer as &dyn GraphWriter,
                base_params,
                &procs,
            )?
        } else {
            meshdb_executor::execute_with_reader_and_procs(
                &plan,
                &storage_reader as &dyn GraphReader,
                &writer as &dyn GraphWriter,
                base_params,
                &procs,
            )?
        };
        if let Some(row) = rows.into_iter().next() {
            if let Some(value) = row.get(name).cloned() {
                out.insert(name.clone(), value);
            }
        }
    }
    Ok(out)
}

/// Build the standard 13-column apoc.periodic.iterate result
/// row from accumulated batch counters and update statistics.
#[allow(clippy::too_many_arguments)]
fn build_apoc_periodic_iterate_row(
    batches: i64,
    total: i64,
    committed_ops: i64,
    failed_ops: i64,
    failed_batches: i64,
    retries_count: i64,
    failed_params: &std::collections::HashMap<
        String,
        Vec<std::collections::HashMap<String, meshdb_core::Property>>,
    >,
    update_stats: &UpdateStatsAccumulator,
    time_taken_ms: i64,
    error_messages: &std::collections::HashMap<String, i64>,
) -> meshdb_executor::Row {
    use meshdb_core::Property;
    let mut row = meshdb_executor::Row::new();
    let v = |p: Property| meshdb_executor::Value::Property(p);
    row.insert("batches".into(), v(Property::Int64(batches)));
    row.insert("total".into(), v(Property::Int64(total)));
    row.insert(
        "committedOperations".into(),
        v(Property::Int64(committed_ops)),
    );
    row.insert("failedOperations".into(), v(Property::Int64(failed_ops)));
    row.insert("failedBatches".into(), v(Property::Int64(failed_batches)));
    row.insert("timeTaken".into(), v(Property::Int64(time_taken_ms)));
    row.insert("wasTerminated".into(), v(Property::Bool(false)));
    row.insert("retries".into(), v(Property::Int64(retries_count)));
    let err_map: std::collections::HashMap<String, Property> = error_messages
        .iter()
        .map(|(k, n)| (k.clone(), Property::Int64(*n)))
        .collect();
    row.insert("errorMessages".into(), v(Property::Map(err_map)));
    // failedParams: { errorMessage: [paramMap, paramMap, ...] }
    let mut fp_map: std::collections::HashMap<String, Property> =
        std::collections::HashMap::with_capacity(failed_params.len());
    for (err, samples) in failed_params {
        let list: Vec<Property> = samples.iter().map(|m| Property::Map(m.clone())).collect();
        fp_map.insert(err.clone(), Property::List(list));
    }
    row.insert("failedParams".into(), v(Property::Map(fp_map)));
    // batch + operations: per-batch and per-operation counters.
    // For row-by-row execution they mirror the top-level
    // counts (each row is one operation, each chunk is one
    // batch); the structured maps match the Neo4j shape so
    // dashboards parsing them keep working.
    let mut batch_map: std::collections::HashMap<String, Property> =
        std::collections::HashMap::with_capacity(4);
    batch_map.insert("total".into(), Property::Int64(batches));
    batch_map.insert(
        "committed".into(),
        Property::Int64(batches - failed_batches),
    );
    batch_map.insert("failed".into(), Property::Int64(failed_batches));
    batch_map.insert("errors".into(), Property::Int64(failed_batches));
    row.insert("batch".into(), v(Property::Map(batch_map)));
    let mut ops_map: std::collections::HashMap<String, Property> =
        std::collections::HashMap::with_capacity(4);
    ops_map.insert("total".into(), Property::Int64(total));
    ops_map.insert("committed".into(), Property::Int64(committed_ops));
    ops_map.insert("failed".into(), Property::Int64(failed_ops));
    ops_map.insert("errors".into(), Property::Int64(failed_ops));
    row.insert("operations".into(), v(Property::Map(ops_map)));
    row.insert(
        "updateStatistics".into(),
        v(Property::Map(update_stats.to_map())),
    );
    row
}

/// Convert a Row's bindings into a flat property map suitable
/// for storing as a `failedParams` sample. Skips Node / Edge /
/// Path bindings (they don't round-trip as properties) — keeps
/// only the scalar/list/map property bindings the action would
/// have referenced via `$param` substitution.
fn row_to_property_map(
    row: &meshdb_executor::Row,
) -> std::collections::HashMap<String, meshdb_core::Property> {
    use meshdb_core::Property;
    let mut out = std::collections::HashMap::with_capacity(row.len());
    for (k, val) in row {
        if let meshdb_executor::Value::Property(p) = val {
            out.insert(k.clone(), p.clone());
        } else {
            // Coarse fallback: stringify graph elements so the
            // sample is at least diagnostic for the user.
            out.insert(k.clone(), Property::String(format!("{val:?}")));
        }
    }
    out
}

/// Accumulator for the update-statistics counters that the
/// apoc.periodic.iterate `updateStatistics` column reports —
/// nodesCreated, nodesDeleted, relationshipsCreated,
/// relationshipsDeleted, propertiesSet, labelsAdded. Walks the
/// post-batch GraphCommand list and counts the structural
/// changes; only invoked on successful batches so failed
/// attempts don't leak into the stats.
#[derive(Debug, Default, Clone)]
struct UpdateStatsAccumulator {
    nodes_created: i64,
    nodes_deleted: i64,
    relationships_created: i64,
    relationships_deleted: i64,
    properties_set: i64,
    labels_added: i64,
}

impl UpdateStatsAccumulator {
    fn absorb(&mut self, cmds: &[GraphCommand]) {
        for cmd in cmds {
            self.absorb_one(cmd);
        }
    }

    fn absorb_one(&mut self, cmd: &GraphCommand) {
        match cmd {
            GraphCommand::PutNode(n) => {
                // PutNode is upsert — without store-side diff
                // info we can't tell create vs update. Count
                // every PutNode as one creation (matching the
                // common-case shape for IN TRANSACTIONS-style
                // bulk loads where nodes are new). Properties
                // and labels add their own counters.
                self.nodes_created += 1;
                self.properties_set += n.properties.len() as i64;
                self.labels_added += n.labels.len() as i64;
            }
            GraphCommand::PutEdge(e) => {
                self.relationships_created += 1;
                self.properties_set += e.properties.len() as i64;
            }
            GraphCommand::DeleteEdge(_) => {
                self.relationships_deleted += 1;
            }
            GraphCommand::DetachDeleteNode(_) => {
                self.nodes_deleted += 1;
            }
            GraphCommand::Batch(inner) => self.absorb(inner),
            // DDL and other variants don't contribute to
            // `updateStatistics` — they're schema operations,
            // not data mutations.
            _ => {}
        }
    }

    fn to_map(&self) -> std::collections::HashMap<String, meshdb_core::Property> {
        use meshdb_core::Property;
        let mut m = std::collections::HashMap::with_capacity(6);
        m.insert("nodesCreated".into(), Property::Int64(self.nodes_created));
        m.insert("nodesDeleted".into(), Property::Int64(self.nodes_deleted));
        m.insert(
            "relationshipsCreated".into(),
            Property::Int64(self.relationships_created),
        );
        m.insert(
            "relationshipsDeleted".into(),
            Property::Int64(self.relationships_deleted),
        );
        m.insert("propertiesSet".into(), Property::Int64(self.properties_set));
        m.insert("labelsAdded".into(), Property::Int64(self.labels_added));
        m
    }
}

/// Cross-batch counters that add up to the top-level
/// `batches`, `total`, `committedOperations`, `failedOperations`,
/// `failedBatches`, and `retries` columns in the final apoc
/// result row. Lives inside [`SharedApocStats`] behind a mutex
/// so parallel batch tasks can fold their outcomes in safely.
#[derive(Debug, Default, Clone)]
struct ApocBatchCounters {
    batches: i64,
    total: i64,
    committed_ops: i64,
    failed_ops: i64,
    failed_batches: i64,
    retries: i64,
}

/// Shared per-run state for one `apoc.periodic.iterate`
/// dispatch. Each batch — whether the default sequential loop
/// or a tokio-spawned parallel task — folds its outcome into
/// these mutex-guarded fields, and the final result row reads
/// them once after every batch has completed.
///
/// `std::sync::Mutex` is the right choice here: every locked
/// section is short and non-async, so we never hold a guard
/// across an `.await`. `parking_lot::Mutex` would save a few
/// nanoseconds per lock but `apoc.periodic.iterate` is
/// bottlenecked on per-batch commit latency, not lock
/// overhead.
struct SharedApocStats {
    counters: std::sync::Mutex<ApocBatchCounters>,
    errors: std::sync::Mutex<std::collections::HashMap<String, i64>>,
    failed_params: std::sync::Mutex<
        std::collections::HashMap<
            String,
            Vec<std::collections::HashMap<String, meshdb_core::Property>>,
        >,
    >,
    update_stats: std::sync::Mutex<UpdateStatsAccumulator>,
}

impl SharedApocStats {
    fn new() -> Self {
        Self {
            counters: std::sync::Mutex::new(ApocBatchCounters::default()),
            errors: std::sync::Mutex::new(std::collections::HashMap::new()),
            failed_params: std::sync::Mutex::new(std::collections::HashMap::new()),
            update_stats: std::sync::Mutex::new(UpdateStatsAccumulator::default()),
        }
    }
}

/// Build the `REPORT STATUS AS <var>` row for a single batch:
/// one Row with `var → Map { started, committed, errorMessage,
/// transactionId }`. Mirrors Neo4j 5's status surface.
/// `started` is always true since we only invoke this once the
/// batch is dispatched; `committed` reflects the actual commit
/// outcome; `errorMessage` is null on success.
fn make_status_row(
    var: &str,
    tx_id: &str,
    committed: bool,
    error_message: Option<&str>,
) -> meshdb_executor::Row {
    use meshdb_core::Property;
    let mut status: std::collections::HashMap<String, Property> =
        std::collections::HashMap::with_capacity(4);
    status.insert("started".into(), Property::Bool(true));
    status.insert("committed".into(), Property::Bool(committed));
    status.insert("transactionId".into(), Property::String(tx_id.into()));
    let err_prop = match error_message {
        Some(s) => Property::String(s.into()),
        None => Property::Null,
    };
    status.insert("errorMessage".into(), err_prop);
    let mut row = meshdb_executor::Row::new();
    row.insert(
        var.to_string(),
        meshdb_executor::Value::Property(Property::Map(status)),
    );
    row
}

/// Find the (single) `CallSubqueryInTransactions` node in
/// `plan` and return clones of its input plan, body plan,
/// batch size, and error mode. The dispatcher uses these to
/// drive the per-batch loop independently of the wrapping
/// clauses (Project / OrderBy / etc.) that surround the IN
/// TRANSACTIONS node in the original plan tree.
fn find_in_transactions_node(
    plan: &meshdb_cypher::LogicalPlan,
) -> Option<(
    meshdb_cypher::LogicalPlan,
    meshdb_cypher::LogicalPlan,
    i64,
    meshdb_cypher::OnErrorMode,
    Option<String>,
)> {
    use meshdb_cypher::LogicalPlan as P;
    match plan {
        P::CallSubqueryInTransactions {
            input,
            body,
            batch_size,
            error_mode,
            report_status_as,
        } => Some((
            (**input).clone(),
            (**body).clone(),
            *batch_size,
            *error_mode,
            report_status_as.clone(),
        )),
        P::Filter { input, .. }
        | P::Project { input, .. }
        | P::Aggregate { input, .. }
        | P::Distinct { input }
        | P::OrderBy { input, .. }
        | P::Skip { input, .. }
        | P::Limit { input, .. }
        | P::Identity { input }
        | P::CoalesceNullRow { input, .. }
        | P::UnwindChain { input, .. }
        | P::BindPath { input, .. }
        | P::ShortestPath { input, .. } => find_in_transactions_node(input),
        // Other variants don't normally wrap an IN TRANSACTIONS
        // node in practice — and even if they did, the planner
        // would reject the resulting structure as ill-formed.
        _ => None,
    }
}

/// True when `plan` carries an `ApocPeriodicIterate` node
/// anywhere in its tree. Used by `execute_cypher_in_tx` to
/// route `apoc.periodic.iterate` calls to a custom batched
/// dispatcher rather than the regular operator pipeline (which
/// would panic at `build_op` because the variant requires a
/// row substitute).
fn plan_contains_apoc_periodic_iterate(plan: &meshdb_cypher::LogicalPlan) -> bool {
    use meshdb_cypher::LogicalPlan as P;
    match plan {
        P::ApocPeriodicIterate { .. } => true,
        P::Filter { input, .. }
        | P::Project { input, .. }
        | P::Aggregate { input, .. }
        | P::Distinct { input }
        | P::OrderBy { input, .. }
        | P::Skip { input, .. }
        | P::Limit { input, .. }
        | P::Identity { input }
        | P::CoalesceNullRow { input, .. }
        | P::EdgeExpand { input, .. }
        | P::OptionalEdgeExpand { input, .. }
        | P::VarLengthExpand { input, .. }
        | P::MergeEdge { input, .. }
        | P::UnwindChain { input, .. }
        | P::BindPath { input, .. }
        | P::ShortestPath { input, .. } => plan_contains_apoc_periodic_iterate(input),
        _ => false,
    }
}

/// True when `plan` carries a `CallSubqueryInTransactions`
/// node anywhere in its tree. Used by `execute_cypher_in_tx`
/// to detect the IN TRANSACTIONS form and route to
/// [`MeshService::execute_call_in_transactions`].
fn plan_contains_in_transactions(plan: &meshdb_cypher::LogicalPlan) -> bool {
    use meshdb_cypher::LogicalPlan as P;
    match plan {
        P::CallSubqueryInTransactions { .. } => true,
        P::Filter { input, .. }
        | P::Project { input, .. }
        | P::Aggregate { input, .. }
        | P::Distinct { input }
        | P::OrderBy { input, .. }
        | P::Skip { input, .. }
        | P::Limit { input, .. }
        | P::Delete { input, .. }
        | P::SetProperty { input, .. }
        | P::EdgeExpand { input, .. }
        | P::OptionalEdgeExpand { input, .. }
        | P::VarLengthExpand { input, .. }
        | P::MergeEdge { input, .. }
        | P::UnwindChain { input, .. }
        | P::Remove { input, .. }
        | P::Foreach { input, .. }
        | P::CallSubquery { input, .. }
        | P::Identity { input }
        | P::CoalesceNullRow { input, .. }
        | P::BindPath { input, .. }
        | P::ShortestPath { input, .. } => plan_contains_in_transactions(input),
        P::CartesianProduct { left, right } => {
            plan_contains_in_transactions(left) || plan_contains_in_transactions(right)
        }
        P::Union { branches, .. } => branches.iter().any(plan_contains_in_transactions),
        P::OptionalApply { input, body, .. } => {
            plan_contains_in_transactions(input) || plan_contains_in_transactions(body)
        }
        P::CreatePath { input, .. }
        | P::MergeNode { input, .. }
        | P::ProcedureCall { input, .. }
        | P::LoadCsv { input, .. } => input
            .as_deref()
            .map(plan_contains_in_transactions)
            .unwrap_or(false),
        _ => false,
    }
}

/// Count the number of `IndexSeek` plan nodes in `plan`,
/// recursively. Used by `execute_cypher_in_tx` to bump the
/// `meshdb_cypher_index_seeks_total` counter once per query — cheap
/// because plans are tiny, and accurate enough for usage tracking
/// without instrumenting the executor itself.
fn count_index_seeks(plan: &meshdb_cypher::LogicalPlan) -> u64 {
    use meshdb_cypher::LogicalPlan as P;
    match plan {
        P::IndexSeek { .. }
        | P::EdgeSeek { .. }
        | P::PointIndexSeek { .. }
        | P::EdgePointIndexSeek { .. } => 1,
        P::Filter { input, .. }
        | P::Project { input, .. }
        | P::Aggregate { input, .. }
        | P::Distinct { input }
        | P::OrderBy { input, .. }
        | P::Skip { input, .. }
        | P::Limit { input, .. }
        | P::Delete { input, .. }
        | P::SetProperty { input, .. }
        | P::EdgeExpand { input, .. }
        | P::OptionalEdgeExpand { input, .. }
        | P::VarLengthExpand { input, .. }
        | P::MergeEdge { input, .. }
        | P::UnwindChain { input, .. }
        | P::Remove { input, .. }
        | P::Foreach { input, .. }
        | P::CallSubquery { input, .. }
        | P::CallSubqueryInTransactions { input, .. }
        | P::Identity { input }
        | P::CoalesceNullRow { input, .. }
        | P::LoadCsv {
            input: Some(input), ..
        } => count_index_seeks(input),
        P::CartesianProduct { left, right } => count_index_seeks(left) + count_index_seeks(right),
        P::Union { branches, .. } => branches.iter().map(count_index_seeks).sum(),
        P::BindPath { input, .. } | P::ShortestPath { input, .. } => count_index_seeks(input),
        P::CreatePath { input, .. } => input.as_deref().map(count_index_seeks).unwrap_or(0),
        P::MergeNode { input, .. } => input.as_deref().map(count_index_seeks).unwrap_or(0),
        P::ProcedureCall { input, .. } => input.as_deref().map(count_index_seeks).unwrap_or(0),
        P::OptionalApply { input, body, .. } => count_index_seeks(input) + count_index_seeks(body),
        P::ApocPeriodicIterate {
            iterate, action, ..
        } => count_index_seeks(iterate) + count_index_seeks(action),
        P::NodeScanAll { .. }
        | P::NodeScanByLabels { .. }
        | P::Unwind { .. }
        | P::SeedRow
        | P::LoadCsv { input: None, .. }
        | P::CreatePropertyIndex { .. }
        | P::DropPropertyIndex { .. }
        | P::CreateEdgePropertyIndex { .. }
        | P::DropEdgePropertyIndex { .. }
        | P::ShowPropertyIndexes
        | P::CreatePointIndex { .. }
        | P::DropPointIndex { .. }
        | P::CreateEdgePointIndex { .. }
        | P::DropEdgePointIndex { .. }
        | P::ShowPointIndexes
        | P::CreatePropertyConstraint { .. }
        | P::DropPropertyConstraint { .. }
        | P::ShowPropertyConstraints => 0,
    }
}

/// Apply a single DDL [`GraphCommand`] directly to `store`. Non-DDL
/// variants are a no-op — the caller filters them out beforehand.
/// Resolve the `(property, properties)` wire pair in a *PropertyIndex
/// request to a single canonical property list. New clients populate
/// `properties` and leave `property` empty; older clients fall back to
/// the single-property slot. An empty list from both sides is rejected
/// — the storage layer requires at least one property.
fn decoded_index_properties(
    repeated: &[String],
    legacy_single: &str,
) -> std::result::Result<Vec<String>, Status> {
    if !repeated.is_empty() {
        return Ok(repeated.to_vec());
    }
    if !legacy_single.is_empty() {
        return Ok(vec![legacy_single.to_string()]);
    }
    Err(Status::invalid_argument(
        "property index request must specify at least one property",
    ))
}

fn apply_ddl_to_store(
    cmd: &GraphCommand,
    store: &dyn StorageEngine,
) -> std::result::Result<(), meshdb_storage::Error> {
    match cmd {
        GraphCommand::CreateIndex { label, properties } => {
            store.create_property_index_composite(label, properties)
        }
        GraphCommand::DropIndex { label, properties } => {
            store.drop_property_index_composite(label, properties)
        }
        GraphCommand::CreateEdgeIndex {
            edge_type,
            properties,
        } => store.create_edge_property_index_composite(edge_type, properties),
        GraphCommand::DropEdgeIndex {
            edge_type,
            properties,
        } => store.drop_edge_property_index_composite(edge_type, properties),
        GraphCommand::CreatePointIndex { label, property } => {
            store.create_point_index(label, property)
        }
        GraphCommand::DropPointIndex { label, property } => store.drop_point_index(label, property),
        GraphCommand::CreateEdgePointIndex {
            edge_type,
            property,
        } => store.create_edge_point_index(edge_type, property),
        GraphCommand::DropEdgePointIndex {
            edge_type,
            property,
        } => store.drop_edge_point_index(edge_type, property),
        GraphCommand::CreateConstraint {
            name,
            scope,
            properties,
            kind,
            if_not_exists,
        } => {
            store.create_property_constraint(
                name.as_deref(),
                &storage_scope(scope),
                properties,
                storage_kind(*kind),
                *if_not_exists,
            )?;
            Ok(())
        }
        GraphCommand::DropConstraint { name, if_exists } => {
            store.drop_property_constraint(name, *if_exists)
        }
        GraphCommand::InstallTrigger { name, spec_blob } => store.put_trigger(name, spec_blob),
        GraphCommand::DropTrigger { name } => store.delete_trigger(name),
        _ => Ok(()),
    }
}

/// Compute the inverse of a DDL command so
/// [`MeshService::replicate_index_ddl_routing`] can roll back
/// successful applications on a partial failure. `CreateIndex`
/// inverts to `DropIndex` and vice versa. Non-DDL variants aren't
/// expected here — callers only pass DDL.
fn invert_ddl(cmd: &GraphCommand) -> GraphCommand {
    match cmd {
        GraphCommand::CreateIndex { label, properties } => GraphCommand::DropIndex {
            label: label.clone(),
            properties: properties.clone(),
        },
        GraphCommand::DropIndex { label, properties } => GraphCommand::CreateIndex {
            label: label.clone(),
            properties: properties.clone(),
        },
        GraphCommand::CreateEdgeIndex {
            edge_type,
            properties,
        } => GraphCommand::DropEdgeIndex {
            edge_type: edge_type.clone(),
            properties: properties.clone(),
        },
        GraphCommand::DropEdgeIndex {
            edge_type,
            properties,
        } => GraphCommand::CreateEdgeIndex {
            edge_type: edge_type.clone(),
            properties: properties.clone(),
        },
        GraphCommand::CreatePointIndex { label, property } => GraphCommand::DropPointIndex {
            label: label.clone(),
            property: property.clone(),
        },
        GraphCommand::DropPointIndex { label, property } => GraphCommand::CreatePointIndex {
            label: label.clone(),
            property: property.clone(),
        },
        GraphCommand::CreateEdgePointIndex {
            edge_type,
            property,
        } => GraphCommand::DropEdgePointIndex {
            edge_type: edge_type.clone(),
            property: property.clone(),
        },
        GraphCommand::DropEdgePointIndex {
            edge_type,
            property,
        } => GraphCommand::CreateEdgePointIndex {
            edge_type: edge_type.clone(),
            property: property.clone(),
        },
        GraphCommand::CreateConstraint {
            name,
            scope,
            properties,
            kind,
            ..
        } => {
            // Rollback of a CREATE is a DROP on the resolved name. The
            // resolved name is the user's name if supplied, otherwise
            // the deterministic auto-generated one — both sides of
            // the fan-out compute it identically. `if_exists: true`
            // because the forward op may have been a no-op
            // (`IF NOT EXISTS` or idempotent re-declaration), in
            // which case the inverse must also succeed as a no-op.
            GraphCommand::DropConstraint {
                name: resolved_constraint_name(name, scope, properties, *kind),
                if_exists: true,
            }
        }
        GraphCommand::DropConstraint { .. } => {
            // Inverting a DROP would require the original spec
            // (label / property / kind) which the DROP command
            // doesn't carry. The routing fan-out captures
            // specs-before-drop separately when it needs symmetric
            // rollback; for unknown callers we return the clone so
            // the inverse is a no-op (equivalent to DROP IF EXISTS
            // on an already-absent constraint).
            cmd.clone()
        }
        GraphCommand::InstallTrigger { name, .. } => {
            GraphCommand::DropTrigger { name: name.clone() }
        }
        GraphCommand::DropTrigger { .. } => {
            // Symmetric to DropConstraint: undoing a DROP needs the
            // original spec, which the command doesn't carry.
            // Returning the clone makes rollback a no-op (drop is
            // idempotent on missing names) — operators who need
            // exact rollback should snapshot the registry before
            // issuing the DROP.
            cmd.clone()
        }
        other => other.clone(),
    }
}

/// Send every `ddl` command to a single peer via the write client,
/// sequentially. Returns `Ok(())` only when every entry succeeds.
async fn try_remote_ddl_on_peer(
    mut client: crate::proto::mesh_write_client::MeshWriteClient<tonic::transport::Channel>,
    ddl: &[GraphCommand],
) -> std::result::Result<(), Status> {
    for cmd in ddl {
        match cmd {
            GraphCommand::CreateIndex { label, properties } => {
                client
                    .create_property_index(CreatePropertyIndexRequest {
                        label: label.clone(),
                        property: String::new(),
                        properties: properties.clone(),
                    })
                    .await?;
            }
            GraphCommand::DropIndex { label, properties } => {
                client
                    .drop_property_index(DropPropertyIndexRequest {
                        label: label.clone(),
                        property: String::new(),
                        properties: properties.clone(),
                    })
                    .await?;
            }
            GraphCommand::CreateEdgeIndex {
                edge_type,
                properties,
            } => {
                client
                    .create_edge_property_index(CreateEdgePropertyIndexRequest {
                        edge_type: edge_type.clone(),
                        property: String::new(),
                        properties: properties.clone(),
                    })
                    .await?;
            }
            GraphCommand::DropEdgeIndex {
                edge_type,
                properties,
            } => {
                client
                    .drop_edge_property_index(DropEdgePropertyIndexRequest {
                        edge_type: edge_type.clone(),
                        property: String::new(),
                        properties: properties.clone(),
                    })
                    .await?;
            }
            GraphCommand::CreatePointIndex { label, property } => {
                client
                    .create_point_index(CreatePointIndexRequest {
                        label: label.clone(),
                        property: property.clone(),
                    })
                    .await?;
            }
            GraphCommand::DropPointIndex { label, property } => {
                client
                    .drop_point_index(DropPointIndexRequest {
                        label: label.clone(),
                        property: property.clone(),
                    })
                    .await?;
            }
            GraphCommand::CreateEdgePointIndex {
                edge_type,
                property,
            } => {
                client
                    .create_edge_point_index(CreateEdgePointIndexRequest {
                        edge_type: edge_type.clone(),
                        property: property.clone(),
                    })
                    .await?;
            }
            GraphCommand::DropEdgePointIndex {
                edge_type,
                property,
            } => {
                client
                    .drop_edge_point_index(DropEdgePointIndexRequest {
                        edge_type: edge_type.clone(),
                        property: property.clone(),
                    })
                    .await?;
            }
            GraphCommand::CreateConstraint {
                name,
                scope,
                properties,
                kind,
                if_not_exists,
            } => {
                let (scope_kind, scope_target) = proto_scope(scope);
                client
                    .create_property_constraint(CreatePropertyConstraintRequest {
                        // Empty string on the wire stands for `None`
                        // (no user-supplied name). Matches the proto's
                        // optional-by-convention encoding.
                        name: name.clone().unwrap_or_default(),
                        scope_kind: scope_kind as i32,
                        scope_target,
                        properties: properties.clone(),
                        kind: proto_kind(*kind) as i32,
                        if_not_exists: *if_not_exists,
                        property_type: proto_property_type_from_kind(*kind) as i32,
                    })
                    .await?;
            }
            GraphCommand::DropConstraint { name, if_exists } => {
                client
                    .drop_property_constraint(DropPropertyConstraintRequest {
                        name: name.clone(),
                        if_exists: *if_exists,
                    })
                    .await?;
            }
            GraphCommand::InstallTrigger { name, spec_blob } => {
                client
                    .install_trigger(crate::proto::InstallTriggerRequest {
                        name: name.clone(),
                        spec_blob: spec_blob.clone(),
                    })
                    .await?;
            }
            GraphCommand::DropTrigger { name } => {
                client
                    .drop_trigger(crate::proto::DropTriggerRequest { name: name.clone() })
                    .await?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Map the cluster-crate `ConstraintScope` into proto wire form:
/// a scope-kind enum plus the target string (label or edge type).
fn proto_scope(scope: &ClusterConstraintScope) -> (ProtoConstraintScopeKind, String) {
    match scope {
        ClusterConstraintScope::Node(l) => (ProtoConstraintScopeKind::Node, l.clone()),
        ClusterConstraintScope::Relationship(t) => {
            (ProtoConstraintScopeKind::Relationship, t.clone())
        }
    }
}

/// Inverse of [`proto_scope`] for inbound RPCs. Returns an error on
/// unspecified scope so the storage layer never sees an ambiguous
/// target.
fn cluster_scope_from_proto(
    scope_kind: i32,
    scope_target: String,
) -> Result<ClusterConstraintScope, Status> {
    match ProtoConstraintScopeKind::try_from(scope_kind)
        .unwrap_or(ProtoConstraintScopeKind::Unspecified)
    {
        ProtoConstraintScopeKind::Node => Ok(ClusterConstraintScope::Node(scope_target)),
        ProtoConstraintScopeKind::Relationship => {
            Ok(ClusterConstraintScope::Relationship(scope_target))
        }
        ProtoConstraintScopeKind::Unspecified => {
            Err(Status::invalid_argument("constraint scope is unspecified"))
        }
    }
}

/// Map the cluster-crate `ConstraintKind` to the proto enum. Kept
/// next to `try_remote_ddl_on_peer` because that's the only place
/// we need to flip the outbound direction; the inbound mapping
/// (proto → cluster / storage) lives on the RPC handler.
fn proto_kind(kind: ClusterConstraintKind) -> ProtoConstraintKind {
    match kind {
        ClusterConstraintKind::Unique => ProtoConstraintKind::Unique,
        ClusterConstraintKind::NotNull => ProtoConstraintKind::NotNull,
        ClusterConstraintKind::PropertyType(_) => ProtoConstraintKind::PropertyType,
        ClusterConstraintKind::NodeKey => ProtoConstraintKind::NodeKey,
    }
}

/// Derive the proto `property_type` field from a constraint kind.
/// Returns `Unspecified` for non-`PropertyType` kinds — the server
/// ignores the field in those cases.
fn proto_property_type_from_kind(kind: ClusterConstraintKind) -> ProtoPropertyTypeKind {
    match kind {
        ClusterConstraintKind::PropertyType(t) => match t {
            ClusterPropertyType::String => ProtoPropertyTypeKind::String,
            ClusterPropertyType::Integer => ProtoPropertyTypeKind::Integer,
            ClusterPropertyType::Float => ProtoPropertyTypeKind::Float,
            ClusterPropertyType::Boolean => ProtoPropertyTypeKind::Boolean,
        },
        _ => ProtoPropertyTypeKind::Unspecified,
    }
}

/// Inverse mapping for inbound proto requests. For `PropertyType`
/// kind, the caller also supplies a `property_type` tag which we
/// carry here as a second argument.
fn cluster_kind_from_proto(kind: i32, property_type: i32) -> Result<ClusterConstraintKind, Status> {
    match ProtoConstraintKind::try_from(kind).unwrap_or(ProtoConstraintKind::Unspecified) {
        ProtoConstraintKind::Unique => Ok(ClusterConstraintKind::Unique),
        ProtoConstraintKind::NotNull => Ok(ClusterConstraintKind::NotNull),
        ProtoConstraintKind::NodeKey => Ok(ClusterConstraintKind::NodeKey),
        ProtoConstraintKind::PropertyType => {
            let t = match ProtoPropertyTypeKind::try_from(property_type)
                .unwrap_or(ProtoPropertyTypeKind::Unspecified)
            {
                ProtoPropertyTypeKind::String => ClusterPropertyType::String,
                ProtoPropertyTypeKind::Integer => ClusterPropertyType::Integer,
                ProtoPropertyTypeKind::Float => ClusterPropertyType::Float,
                ProtoPropertyTypeKind::Boolean => ClusterPropertyType::Boolean,
                ProtoPropertyTypeKind::Unspecified => {
                    return Err(Status::invalid_argument(
                        "property_type is unspecified for PROPERTY_TYPE constraint",
                    ))
                }
            };
            Ok(ClusterConstraintKind::PropertyType(t))
        }
        ProtoConstraintKind::Unspecified => {
            Err(Status::invalid_argument("constraint kind is unspecified"))
        }
    }
}

/// Partition a flat command list into `(ddl, graph)` where `ddl`
/// holds `CreateIndex` / `DropIndex` entries and `graph` holds
/// everything else. Nested `Batch` variants are recursed into and
/// re-wrapped: the non-DDL children of a batch stay together inside
/// a new `Batch` so the coordinator still commits them atomically.
///
/// Used by routing-mode [`MeshService::commit_buffered_commands`] so
/// DDL can take its own fan-out path instead of going through the
/// 2PC coordinator (which is keyed to per-partition mutations).
pub(crate) fn split_ddl(cmds: Vec<GraphCommand>) -> (Vec<GraphCommand>, Vec<GraphCommand>) {
    let mut ddl = Vec::new();
    let mut graph = Vec::new();
    for cmd in cmds {
        match cmd {
            GraphCommand::CreateIndex { .. }
            | GraphCommand::DropIndex { .. }
            | GraphCommand::CreateEdgeIndex { .. }
            | GraphCommand::DropEdgeIndex { .. }
            | GraphCommand::CreateConstraint { .. }
            | GraphCommand::DropConstraint { .. }
            | GraphCommand::InstallTrigger { .. }
            | GraphCommand::DropTrigger { .. } => ddl.push(cmd),
            GraphCommand::Batch(inner) => {
                let (nested_ddl, nested_graph) = split_ddl(inner);
                ddl.extend(nested_ddl);
                if !nested_graph.is_empty() {
                    graph.push(GraphCommand::Batch(nested_graph));
                }
            }
            other => graph.push(other),
        }
    }
    (ddl, graph)
}

/// Walk the command tree and invoke DDL side-effects directly on the
/// store. Called ahead of `flatten_commands` by `apply_prepared_batch`
/// so index backfill reads see the pre-batch graph state.
fn apply_ddl_commands(
    cmds: &[GraphCommand],
    store: &dyn StorageEngine,
) -> std::result::Result<(), meshdb_storage::Error> {
    for cmd in cmds {
        match cmd {
            GraphCommand::CreateIndex { label, properties } => {
                store.create_property_index_composite(label, properties)?;
            }
            GraphCommand::DropIndex { label, properties } => {
                store.drop_property_index_composite(label, properties)?;
            }
            GraphCommand::CreateEdgeIndex {
                edge_type,
                properties,
            } => {
                store.create_edge_property_index_composite(edge_type, properties)?;
            }
            GraphCommand::DropEdgeIndex {
                edge_type,
                properties,
            } => {
                store.drop_edge_property_index_composite(edge_type, properties)?;
            }
            GraphCommand::CreateConstraint {
                name,
                scope,
                properties,
                kind,
                if_not_exists,
            } => {
                store.create_property_constraint(
                    name.as_deref(),
                    &storage_scope(scope),
                    properties,
                    storage_kind(*kind),
                    *if_not_exists,
                )?;
            }
            GraphCommand::DropConstraint { name, if_exists } => {
                store.drop_property_constraint(name, *if_exists)?;
            }
            GraphCommand::InstallTrigger { name, spec_blob } => {
                store.put_trigger(name, spec_blob)?;
            }
            GraphCommand::DropTrigger { name } => {
                store.delete_trigger(name)?;
            }
            GraphCommand::Batch(inner) => apply_ddl_commands(inner, store)?,
            _ => {}
        }
    }
    Ok(())
}

fn raft_propose_failed<E: std::fmt::Display>(e: E) -> Status {
    Status::internal(format!("raft propose failed: {e}"))
}

impl MeshService {
    fn leader_write_client(&self, addr: &str) -> Result<MeshWriteClient<Channel>, Status> {
        let endpoint = self.peer_endpoint(addr)?;
        Ok(MeshWriteClient::new(endpoint.connect_lazy()))
    }
}

fn internal<E: std::fmt::Display>(e: E) -> Status {
    Status::internal(e.to_string())
}

fn bad_request<E: std::fmt::Display>(e: E) -> Status {
    Status::invalid_argument(e.to_string())
}

fn no_client(peer: PeerId) -> Status {
    Status::internal(format!("no client registered for peer {}", peer))
}

#[tonic::async_trait]
impl MeshQuery for MeshService {
    #[tracing::instrument(skip_all, fields(rpc = "get_node"))]
    async fn get_node(
        &self,
        request: Request<GetNodeRequest>,
    ) -> Result<Response<GetNodeResponse>, Status> {
        let req = request.into_inner();
        let local_only = req.local_only;
        let id_proto = req
            .id
            .ok_or_else(|| Status::invalid_argument("missing id"))?;
        let id = node_id_from_proto(&id_proto).map_err(bad_request)?;

        // Forward to the partition owner if this node doesn't live locally.
        // `local_only` short-circuits forwarding so the partitioned reader
        // can issue direct point reads against a specific peer.
        if !local_only {
            if let Some(routing) = &self.routing {
                if !routing.cluster().is_local(id) {
                    let owner = routing.cluster().owner_of(id);
                    let mut client = routing
                        .query_client(owner)
                        .ok_or_else(|| no_client(owner))?;
                    return client
                        .get_node(GetNodeRequest {
                            id: Some(id_proto),
                            local_only: false,
                        })
                        .await;
                }
            }
        }

        let node = self.store.get_node(id).map_err(internal)?;
        let (found, node) = match node {
            Some(n) => (true, Some(node_to_proto(&n).map_err(internal)?)),
            None => (false, None),
        };
        Ok(Response::new(GetNodeResponse { found, node }))
    }

    #[tracing::instrument(skip_all, fields(rpc = "get_edge"))]
    async fn get_edge(
        &self,
        request: Request<GetEdgeRequest>,
    ) -> Result<Response<GetEdgeResponse>, Status> {
        let req = request.into_inner();
        let id_proto = req
            .id
            .ok_or_else(|| Status::invalid_argument("missing id"))?;
        let local_only = req.local_only;
        let id = edge_id_from_proto(&id_proto).map_err(bad_request)?;

        // Always check local first — if the edge lives here, we're done.
        if let Some(edge) = self.store.get_edge(id).map_err(internal)? {
            let proto_edge = edge_to_proto(&edge).map_err(internal)?;
            return Ok(Response::new(GetEdgeResponse {
                found: true,
                edge: Some(proto_edge),
            }));
        }

        // Otherwise scatter-gather to each remote peer until one returns a hit.
        // `local_only` on the forwarded request prevents infinite recursion.
        if !local_only {
            if let Some(routing) = &self.routing {
                let self_id = routing.cluster().self_id();
                for peer_id in routing.cluster().membership().peer_ids() {
                    if peer_id == self_id {
                        continue;
                    }
                    let mut client = routing
                        .query_client(peer_id)
                        .ok_or_else(|| no_client(peer_id))?;
                    let resp = client
                        .get_edge(GetEdgeRequest {
                            id: Some(id_proto.clone()),
                            local_only: true,
                        })
                        .await?;
                    let inner = resp.into_inner();
                    if inner.found {
                        return Ok(Response::new(inner));
                    }
                }
            }
        }

        Ok(Response::new(GetEdgeResponse {
            found: false,
            edge: None,
        }))
    }

    #[tracing::instrument(skip_all, fields(rpc = "nodes_by_label"))]
    async fn nodes_by_label(
        &self,
        request: Request<NodesByLabelRequest>,
    ) -> Result<Response<NodesByLabelResponse>, Status> {
        let req = request.into_inner();
        let label = req.label;
        let local_only = req.local_only;

        let mut ids: Vec<_> = self
            .store
            .nodes_by_label(&label)
            .map_err(internal)?
            .into_iter()
            .map(|id| uuid_to_proto(id.as_uuid()))
            .collect();

        if !local_only {
            if let Some(routing) = &self.routing {
                let self_id = routing.cluster().self_id();
                for peer_id in routing.cluster().membership().peer_ids() {
                    if peer_id == self_id {
                        continue;
                    }
                    let mut client = routing
                        .query_client(peer_id)
                        .ok_or_else(|| no_client(peer_id))?;
                    let resp = client
                        .nodes_by_label(NodesByLabelRequest {
                            label: label.clone(),
                            local_only: true,
                        })
                        .await?;
                    ids.extend(resp.into_inner().ids);
                }
            }
        }

        Ok(Response::new(NodesByLabelResponse { ids }))
    }

    #[tracing::instrument(skip_all, fields(rpc = "nodes_by_property"))]
    async fn nodes_by_property(
        &self,
        request: Request<NodesByPropertyRequest>,
    ) -> Result<Response<NodesByPropertyResponse>, Status> {
        let req = request.into_inner();
        let label = req.label;
        let property = req.property;
        let local_only = req.local_only;
        // Decode the JSON-carried Property value. A malformed blob
        // is a client bug, surface as InvalidArgument.
        let value: meshdb_core::Property = serde_json::from_slice(&req.value_json)
            .map_err(|e| Status::invalid_argument(format!("value_json: {e}")))?;

        let mut ids: Vec<_> = self
            .store
            .nodes_by_property(&label, &property, &value)
            .map_err(internal)?
            .into_iter()
            .map(|id| uuid_to_proto(id.as_uuid()))
            .collect();

        if !local_only {
            if let Some(routing) = &self.routing {
                let self_id = routing.cluster().self_id();
                for peer_id in routing.cluster().membership().peer_ids() {
                    if peer_id == self_id {
                        continue;
                    }
                    let mut client = routing
                        .query_client(peer_id)
                        .ok_or_else(|| no_client(peer_id))?;
                    let resp = client
                        .nodes_by_property(NodesByPropertyRequest {
                            label: label.clone(),
                            property: property.clone(),
                            value_json: req.value_json.clone(),
                            local_only: true,
                        })
                        .await?;
                    ids.extend(resp.into_inner().ids);
                }
            }
        }

        Ok(Response::new(NodesByPropertyResponse { ids }))
    }

    #[tracing::instrument(skip_all, fields(rpc = "edges_by_property"))]
    async fn edges_by_property(
        &self,
        request: Request<EdgesByPropertyRequest>,
    ) -> Result<Response<EdgesByPropertyResponse>, Status> {
        let req = request.into_inner();
        let edge_type = req.edge_type;
        let property = req.property;
        let local_only = req.local_only;
        let value: meshdb_core::Property = serde_json::from_slice(&req.value_json)
            .map_err(|e| Status::invalid_argument(format!("value_json: {e}")))?;

        let mut ids: Vec<_> = self
            .store
            .edges_by_property(&edge_type, &property, &value)
            .map_err(internal)?
            .into_iter()
            .map(|id| uuid_to_proto(id.as_uuid()))
            .collect();

        if !local_only {
            if let Some(routing) = &self.routing {
                let self_id = routing.cluster().self_id();
                for peer_id in routing.cluster().membership().peer_ids() {
                    if peer_id == self_id {
                        continue;
                    }
                    let mut client = routing
                        .query_client(peer_id)
                        .ok_or_else(|| no_client(peer_id))?;
                    let resp = client
                        .edges_by_property(EdgesByPropertyRequest {
                            edge_type: edge_type.clone(),
                            property: property.clone(),
                            value_json: req.value_json.clone(),
                            local_only: true,
                        })
                        .await?;
                    ids.extend(resp.into_inner().ids);
                }
            }
        }

        Ok(Response::new(EdgesByPropertyResponse { ids }))
    }

    #[tracing::instrument(skip_all, fields(rpc = "outgoing"))]
    async fn outgoing(
        &self,
        request: Request<NeighborRequest>,
    ) -> Result<Response<NeighborResponse>, Status> {
        let req = request.into_inner();
        let local_only = req.local_only;
        let id_proto = req
            .node_id
            .ok_or_else(|| Status::invalid_argument("missing node_id"))?;
        let id = node_id_from_proto(&id_proto).map_err(bad_request)?;

        if !local_only {
            if let Some(routing) = &self.routing {
                if !routing.cluster().is_local(id) {
                    let owner = routing.cluster().owner_of(id);
                    let mut client = routing
                        .query_client(owner)
                        .ok_or_else(|| no_client(owner))?;
                    return client
                        .outgoing(NeighborRequest {
                            node_id: Some(id_proto),
                            local_only: false,
                        })
                        .await;
                }
            }
        }

        let out = self.store.outgoing(id).map_err(internal)?;
        let neighbors = out
            .into_iter()
            .map(|(eid, nid)| NeighborInfo {
                edge_id: Some(uuid_to_proto(eid.as_uuid())),
                neighbor_id: Some(uuid_to_proto(nid.as_uuid())),
            })
            .collect();
        Ok(Response::new(NeighborResponse { neighbors }))
    }

    #[tracing::instrument(skip_all, fields(rpc = "incoming"))]
    async fn incoming(
        &self,
        request: Request<NeighborRequest>,
    ) -> Result<Response<NeighborResponse>, Status> {
        let req = request.into_inner();
        let local_only = req.local_only;
        let id_proto = req
            .node_id
            .ok_or_else(|| Status::invalid_argument("missing node_id"))?;
        let id = node_id_from_proto(&id_proto).map_err(bad_request)?;

        if !local_only {
            if let Some(routing) = &self.routing {
                if !routing.cluster().is_local(id) {
                    let owner = routing.cluster().owner_of(id);
                    let mut client = routing
                        .query_client(owner)
                        .ok_or_else(|| no_client(owner))?;
                    return client
                        .incoming(NeighborRequest {
                            node_id: Some(id_proto),
                            local_only: false,
                        })
                        .await;
                }
            }
        }

        let inc = self.store.incoming(id).map_err(internal)?;
        let neighbors = inc
            .into_iter()
            .map(|(eid, nid)| NeighborInfo {
                edge_id: Some(uuid_to_proto(eid.as_uuid())),
                neighbor_id: Some(uuid_to_proto(nid.as_uuid())),
            })
            .collect();
        Ok(Response::new(NeighborResponse { neighbors }))
    }

    #[tracing::instrument(skip_all, fields(rpc = "all_node_ids"))]
    async fn all_node_ids(
        &self,
        request: Request<AllNodeIdsRequest>,
    ) -> Result<Response<AllNodeIdsResponse>, Status> {
        let local_only = request.into_inner().local_only;

        let mut ids: Vec<_> = self
            .store
            .all_node_ids()
            .map_err(internal)?
            .into_iter()
            .map(|id| uuid_to_proto(id.as_uuid()))
            .collect();

        if !local_only {
            if let Some(routing) = &self.routing {
                let self_id = routing.cluster().self_id();
                for peer_id in routing.cluster().membership().peer_ids() {
                    if peer_id == self_id {
                        continue;
                    }
                    let mut client = routing
                        .query_client(peer_id)
                        .ok_or_else(|| no_client(peer_id))?;
                    let resp = client
                        .all_node_ids(AllNodeIdsRequest { local_only: true })
                        .await?;
                    ids.extend(resp.into_inner().ids);
                }
            }
        }

        Ok(Response::new(AllNodeIdsResponse { ids }))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse { serving: true }))
    }

    #[tracing::instrument(skip_all, fields(rpc = "execute_cypher", query_len))]
    async fn execute_cypher(
        &self,
        request: Request<ExecuteCypherRequest>,
    ) -> Result<Response<ExecuteCypherResponse>, Status> {
        let req = request.into_inner();
        let query = req.query;
        tracing::Span::current().record("query_len", query.len());
        // Decode the params blob — empty / missing means no params.
        // Anything else is the serde_json-encoded HashMap that the
        // forwarding leader path produced via `serde_json::to_vec`.
        let params: meshdb_executor::ParamMap = if req.params_json.is_empty() {
            std::collections::HashMap::new()
        } else {
            serde_json::from_slice(&req.params_json)
                .map_err(|e| Status::invalid_argument(format!("decoding params: {e}")))?
        };
        let rows = self.execute_cypher_local(query.clone(), params).await?;
        let rows_json = serde_json::to_vec(&rows)
            .map_err(|e| Status::internal(format!("encoding rows: {e}")))?;
        // Field order derives from re-parsing the query string. The
        // double parse (once here, once in execute_cypher_local) is
        // cheap compared to the actual execute cost and keeps the
        // rows-only return shape backwards-compat for every caller.
        // Empty fields means "no RETURN clause" — Bolt falls back to
        // alphabetical row-key ordering for that case.
        let fields = match meshdb_cypher::parse(&query) {
            Ok(stmt) => meshdb_cypher::output_columns(&stmt),
            Err(_) => Vec::new(),
        };
        Ok(Response::new(ExecuteCypherResponse { rows_json, fields }))
    }
}

#[tonic::async_trait]
impl MeshWrite for MeshService {
    #[tracing::instrument(skip_all, fields(rpc = "put_node"))]
    async fn put_node(
        &self,
        request: Request<PutNodeRequest>,
    ) -> Result<Response<PutNodeResponse>, Status> {
        let req = request.into_inner();
        let local_only = req.local_only;
        let proto_node = req
            .node
            .ok_or_else(|| Status::invalid_argument("missing node"))?;
        let node = node_from_proto(proto_node.clone()).map_err(bad_request)?;
        let id = node.id;

        // Raft mode: propose through consensus. Every replica's state
        // machine applies the same write via StoreGraphApplier. If we are
        // not the leader, openraft surfaces a ForwardToLeader error with
        // the leader's address; we transparently forward the original RPC
        // there so clients can write to any peer.
        if let Some(raft) = &self.raft {
            match raft.propose_graph(GraphCommand::PutNode(node)).await {
                Ok(_) => return Ok(Response::new(PutNodeResponse {})),
                Err(ClusterError::ForwardToLeader {
                    leader_address: Some(addr),
                    ..
                }) => {
                    let mut client = self.leader_write_client(&addr)?;
                    return client
                        .put_node(PutNodeRequest {
                            node: Some(proto_node),
                            local_only,
                        })
                        .await;
                }
                Err(e) => return Err(raft_propose_failed(e)),
            }
        }

        // Route to the owner when we aren't it.
        if let Some(routing) = &self.routing {
            if !routing.cluster().is_local(id) && !local_only {
                let owner = routing.cluster().owner_of(id);
                let mut client = routing
                    .write_client(owner)
                    .ok_or_else(|| no_client(owner))?;
                client
                    .put_node(PutNodeRequest {
                        node: Some(proto_node),
                        local_only: true,
                    })
                    .await?;
                return Ok(Response::new(PutNodeResponse {}));
            }
        }

        self.store.put_node(&node).map_err(internal)?;
        Ok(Response::new(PutNodeResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "put_edge"))]
    async fn put_edge(
        &self,
        request: Request<PutEdgeRequest>,
    ) -> Result<Response<PutEdgeResponse>, Status> {
        let req = request.into_inner();
        let local_only = req.local_only;
        let proto_edge = req
            .edge
            .ok_or_else(|| Status::invalid_argument("missing edge"))?;
        let edge = edge_from_proto(proto_edge.clone()).map_err(bad_request)?;

        if let Some(raft) = &self.raft {
            match raft.propose_graph(GraphCommand::PutEdge(edge)).await {
                Ok(_) => return Ok(Response::new(PutEdgeResponse {})),
                Err(ClusterError::ForwardToLeader {
                    leader_address: Some(addr),
                    ..
                }) => {
                    let mut client = self.leader_write_client(&addr)?;
                    return client
                        .put_edge(PutEdgeRequest {
                            edge: Some(proto_edge),
                            local_only,
                        })
                        .await;
                }
                Err(e) => return Err(raft_propose_failed(e)),
            }
        }

        if let Some(routing) = &self.routing {
            let cluster = routing.cluster();
            let self_id = cluster.self_id();
            let mut targets: HashSet<PeerId> = HashSet::new();
            targets.insert(cluster.owner_of(edge.source));
            targets.insert(cluster.owner_of(edge.target));
            let self_is_target = targets.remove(&self_id);

            if self_is_target {
                self.store.put_edge(&edge).map_err(internal)?;
            }

            if !local_only {
                for owner in targets {
                    let mut client = routing
                        .write_client(owner)
                        .ok_or_else(|| no_client(owner))?;
                    client
                        .put_edge(PutEdgeRequest {
                            edge: Some(proto_edge.clone()),
                            local_only: true,
                        })
                        .await?;
                }
            }

            return Ok(Response::new(PutEdgeResponse {}));
        }

        // No routing — local-only behavior.
        self.store.put_edge(&edge).map_err(internal)?;
        Ok(Response::new(PutEdgeResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "delete_edge"))]
    async fn delete_edge(
        &self,
        request: Request<DeleteEdgeRequest>,
    ) -> Result<Response<DeleteEdgeResponse>, Status> {
        let req = request.into_inner();
        let local_only = req.local_only;
        let id_proto = req
            .edge_id
            .ok_or_else(|| Status::invalid_argument("missing edge_id"))?;
        let id = edge_id_from_proto(&id_proto).map_err(bad_request)?;

        if let Some(raft) = &self.raft {
            match raft.propose_graph(GraphCommand::DeleteEdge(id)).await {
                Ok(_) => return Ok(Response::new(DeleteEdgeResponse {})),
                Err(ClusterError::ForwardToLeader {
                    leader_address: Some(addr),
                    ..
                }) => {
                    let mut client = self.leader_write_client(&addr)?;
                    return client
                        .delete_edge(DeleteEdgeRequest {
                            edge_id: Some(id_proto),
                            local_only,
                        })
                        .await;
                }
                Err(e) => return Err(raft_propose_failed(e)),
            }
        }

        // Local delete is idempotent (check-then-delete).
        if self.store.get_edge(id).map_err(internal)?.is_some() {
            self.store.delete_edge(id).map_err(internal)?;
        }

        // Scatter-gather to all other peers.
        if !local_only {
            if let Some(routing) = &self.routing {
                let self_id = routing.cluster().self_id();
                for peer_id in routing.cluster().membership().peer_ids() {
                    if peer_id == self_id {
                        continue;
                    }
                    let mut client = routing
                        .write_client(peer_id)
                        .ok_or_else(|| no_client(peer_id))?;
                    client
                        .delete_edge(DeleteEdgeRequest {
                            edge_id: Some(id_proto.clone()),
                            local_only: true,
                        })
                        .await?;
                }
            }
        }

        Ok(Response::new(DeleteEdgeResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "detach_delete_node"))]
    async fn detach_delete_node(
        &self,
        request: Request<DetachDeleteNodeRequest>,
    ) -> Result<Response<DetachDeleteNodeResponse>, Status> {
        let req = request.into_inner();
        let local_only = req.local_only;
        let id_proto = req
            .node_id
            .ok_or_else(|| Status::invalid_argument("missing node_id"))?;
        let id = node_id_from_proto(&id_proto).map_err(bad_request)?;

        if let Some(raft) = &self.raft {
            match raft.propose_graph(GraphCommand::DetachDeleteNode(id)).await {
                Ok(_) => return Ok(Response::new(DetachDeleteNodeResponse {})),
                Err(ClusterError::ForwardToLeader {
                    leader_address: Some(addr),
                    ..
                }) => {
                    let mut client = self.leader_write_client(&addr)?;
                    return client
                        .detach_delete_node(DetachDeleteNodeRequest {
                            node_id: Some(id_proto),
                            local_only,
                        })
                        .await;
                }
                Err(e) => return Err(raft_propose_failed(e)),
            }
        }

        // Local detach-delete is idempotent — no-op if the node isn't here.
        self.store.detach_delete_node(id).map_err(internal)?;

        // Scatter-gather so every peer cleans up any incident edges it owns.
        if !local_only {
            if let Some(routing) = &self.routing {
                let self_id = routing.cluster().self_id();
                for peer_id in routing.cluster().membership().peer_ids() {
                    if peer_id == self_id {
                        continue;
                    }
                    let mut client = routing
                        .write_client(peer_id)
                        .ok_or_else(|| no_client(peer_id))?;
                    client
                        .detach_delete_node(DetachDeleteNodeRequest {
                            node_id: Some(id_proto.clone()),
                            local_only: true,
                        })
                        .await?;
                }
            }
        }

        Ok(Response::new(DetachDeleteNodeResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "batch_write", txid, phase))]
    async fn batch_write(
        &self,
        request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>, Status> {
        let req = request.into_inner();
        let txid = req.txid;
        let phase = BatchPhase::try_from(req.phase)
            .map_err(|_| Status::invalid_argument(format!("unknown phase {}", req.phase)))?;
        tracing::Span::current().record("txid", txid.as_str());
        tracing::Span::current().record("phase", format!("{:?}", phase).as_str());

        if txid.is_empty() {
            return Err(Status::invalid_argument("empty txid"));
        }

        match phase {
            BatchPhase::Unspecified => {
                Err(Status::invalid_argument("phase UNSPECIFIED is not valid"))
            }
            BatchPhase::Prepare => {
                #[cfg(any(test, feature = "fault-inject"))]
                if let Some(fp) = &self.fault_points {
                    if fp.reject_prepare.load(std::sync::atomic::Ordering::SeqCst) {
                        return Err(Status::internal("injected fault: reject_prepare"));
                    }
                }
                let commands: Vec<GraphCommand> =
                    serde_json::from_slice(&req.commands_json).map_err(bad_request)?;
                // Terminal outcome takes precedence over staging
                // state: a PREPARE arriving after the coordinator's
                // COMMIT (or ABORT) already reached this peer is a
                // coordinator bug. Return `already_exists` with an
                // error message that names the prior decision so the
                // coordinator's logs show the causal order.
                if let Some(outcome) = self.pending_batches.terminal_outcome(&txid) {
                    let label = match outcome {
                        crate::staging::TerminalOutcome::Committed => "committed",
                        crate::staging::TerminalOutcome::Aborted => "aborted",
                    };
                    return Err(Status::already_exists(format!(
                        "txid {} was already {} on this peer",
                        txid, label
                    )));
                }
                match self
                    .pending_batches
                    .try_insert_or_match(txid.clone(), commands.clone())
                {
                    crate::staging::InsertOutcome::Inserted => {
                        // Fresh PREPARE — fsync before ACKing so a
                        // crash between here and the return doesn't
                        // orphan the staged batch. Log AFTER the
                        // insert so a retry that finds the staged
                        // entry via `Duplicate` doesn't double-log.
                        if let Some(log) = &self.participant_log {
                            if let Err(e) = log.append(&crate::ParticipantLogEntry::Prepared {
                                txid: txid.clone(),
                                commands,
                            }) {
                                // Rolling back the staging entry keeps
                                // the invariant "staged implies logged"
                                // after a fsync failure.
                                let _ = self.pending_batches.take(&txid);
                                return Err(internal(e));
                            }
                        }
                        Ok(Response::new(BatchWriteResponse {}))
                    }
                    crate::staging::InsertOutcome::Duplicate => {
                        // Identical retry — the first PREPARE
                        // already fsync'd its log entry and staged
                        // the commands. Nothing to do; return OK so
                        // a coordinator retry after a transient
                        // network glitch proceeds to COMMIT cleanly.
                        Ok(Response::new(BatchWriteResponse {}))
                    }
                    crate::staging::InsertOutcome::Conflict => Err(Status::already_exists(
                        format!("txid {} already prepared with different commands", txid),
                    )),
                }
            }
            BatchPhase::Commit => {
                // Three cases on receiving COMMIT:
                // 1. Staging has the entry: normal path — apply, log,
                //    mark outcome.
                // 2. Staging is empty but the outcomes cache says we
                //    already committed: duplicate RPC, return OK.
                // 3. Staging is empty and no cached outcome (or the
                //    cached outcome is Aborted): the peer never
                //    received PREPARE, or its staging expired, or the
                //    coordinator is sending COMMIT against the wrong
                //    decision. Fail the RPC so the coordinator's
                //    recovery path can re-PREPARE.
                match self.pending_batches.take(&txid) {
                    Some(cmds) => {
                        #[cfg(any(test, feature = "fault-inject"))]
                        if let Some(fp) = &self.fault_points {
                            if fp
                                .reject_commit_before_apply
                                .load(std::sync::atomic::Ordering::SeqCst)
                            {
                                // Re-stage the taken batch so recovery
                                // sees the same `Prepared`-only state
                                // it would after a real crash between
                                // the `take()` and the apply. Without
                                // this, the staging map drops the
                                // entry and the restart path treats
                                // the txid as never-staged.
                                self.pending_batches.rehydrate(txid.clone(), cmds);
                                return Err(Status::internal(
                                    "injected fault: reject_commit_before_apply",
                                ));
                            }
                        }
                        apply_prepared_batch(self.store.as_ref(), &cmds).map_err(internal)?;
                        if let Some(log) = &self.participant_log {
                            log.append(&crate::ParticipantLogEntry::Committed {
                                txid: txid.clone(),
                            })
                            .map_err(internal)?;
                        }
                        self.pending_batches
                            .finalize(txid, crate::staging::TerminalOutcome::Committed);
                        Ok(Response::new(BatchWriteResponse {}))
                    }
                    None => match self.pending_batches.terminal_outcome(&txid) {
                        Some(crate::staging::TerminalOutcome::Committed) => {
                            Ok(Response::new(BatchWriteResponse {}))
                        }
                        Some(crate::staging::TerminalOutcome::Aborted) => {
                            Err(Status::failed_precondition(format!(
                                "txid {} was aborted on this peer",
                                txid
                            )))
                        }
                        None => Err(Status::failed_precondition(format!(
                            "txid {} not prepared on this peer",
                            txid
                        ))),
                    },
                }
            }
            BatchPhase::Abort => {
                // ABORT is idempotent by design. Drop the staged
                // entry if present; log the outcome so a later COMMIT
                // for the same txid can be rejected with a meaningful
                // error rather than the generic "not prepared".
                let _ = self.pending_batches.take(&txid);
                if let Some(log) = &self.participant_log {
                    log.append(&crate::ParticipantLogEntry::Aborted { txid: txid.clone() })
                        .map_err(internal)?;
                }
                self.pending_batches
                    .finalize(txid, crate::staging::TerminalOutcome::Aborted);
                Ok(Response::new(BatchWriteResponse {}))
            }
        }
    }

    #[tracing::instrument(skip_all, fields(rpc = "create_property_index"))]
    async fn create_property_index(
        &self,
        request: Request<CreatePropertyIndexRequest>,
    ) -> Result<Response<CreatePropertyIndexResponse>, Status> {
        // Local-only: the routing-mode fan-out caller is responsible
        // for calling this RPC on every peer. In Raft mode it's
        // unused — DDL replicates via `propose_graph` instead.
        let req = request.into_inner();
        let props = decoded_index_properties(&req.properties, &req.property)?;
        self.store
            .create_property_index_composite(&req.label, &props)
            .map_err(internal)?;
        Ok(Response::new(CreatePropertyIndexResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "drop_property_index"))]
    async fn drop_property_index(
        &self,
        request: Request<DropPropertyIndexRequest>,
    ) -> Result<Response<DropPropertyIndexResponse>, Status> {
        let req = request.into_inner();
        let props = decoded_index_properties(&req.properties, &req.property)?;
        self.store
            .drop_property_index_composite(&req.label, &props)
            .map_err(internal)?;
        Ok(Response::new(DropPropertyIndexResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "create_edge_property_index"))]
    async fn create_edge_property_index(
        &self,
        request: Request<CreateEdgePropertyIndexRequest>,
    ) -> Result<Response<CreateEdgePropertyIndexResponse>, Status> {
        let req = request.into_inner();
        let props = decoded_index_properties(&req.properties, &req.property)?;
        self.store
            .create_edge_property_index_composite(&req.edge_type, &props)
            .map_err(internal)?;
        Ok(Response::new(CreateEdgePropertyIndexResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "drop_edge_property_index"))]
    async fn drop_edge_property_index(
        &self,
        request: Request<DropEdgePropertyIndexRequest>,
    ) -> Result<Response<DropEdgePropertyIndexResponse>, Status> {
        let req = request.into_inner();
        let props = decoded_index_properties(&req.properties, &req.property)?;
        self.store
            .drop_edge_property_index_composite(&req.edge_type, &props)
            .map_err(internal)?;
        Ok(Response::new(DropEdgePropertyIndexResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "create_point_index"))]
    async fn create_point_index(
        &self,
        request: Request<CreatePointIndexRequest>,
    ) -> Result<Response<CreatePointIndexResponse>, Status> {
        let req = request.into_inner();
        self.store
            .create_point_index(&req.label, &req.property)
            .map_err(internal)?;
        Ok(Response::new(CreatePointIndexResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "drop_point_index"))]
    async fn drop_point_index(
        &self,
        request: Request<DropPointIndexRequest>,
    ) -> Result<Response<DropPointIndexResponse>, Status> {
        let req = request.into_inner();
        self.store
            .drop_point_index(&req.label, &req.property)
            .map_err(internal)?;
        Ok(Response::new(DropPointIndexResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "create_edge_point_index"))]
    async fn create_edge_point_index(
        &self,
        request: Request<CreateEdgePointIndexRequest>,
    ) -> Result<Response<CreateEdgePointIndexResponse>, Status> {
        let req = request.into_inner();
        self.store
            .create_edge_point_index(&req.edge_type, &req.property)
            .map_err(internal)?;
        Ok(Response::new(CreateEdgePointIndexResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "drop_edge_point_index"))]
    async fn drop_edge_point_index(
        &self,
        request: Request<DropEdgePointIndexRequest>,
    ) -> Result<Response<DropEdgePointIndexResponse>, Status> {
        let req = request.into_inner();
        self.store
            .drop_edge_point_index(&req.edge_type, &req.property)
            .map_err(internal)?;
        Ok(Response::new(DropEdgePointIndexResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "create_property_constraint"))]
    async fn create_property_constraint(
        &self,
        request: Request<CreatePropertyConstraintRequest>,
    ) -> Result<Response<CreatePropertyConstraintResponse>, Status> {
        // Local-only: the routing-mode fan-out caller runs its own
        // local apply, then calls this RPC on every other peer. In
        // Raft mode it's unused — DDL flows through `propose_graph`
        // as a `GraphCommand::CreateConstraint` entry.
        let req = request.into_inner();
        let kind = cluster_kind_from_proto(req.kind, req.property_type)?;
        let cluster_scope = cluster_scope_from_proto(req.scope_kind, req.scope_target)?;
        let name = if req.name.is_empty() {
            None
        } else {
            Some(req.name.as_str())
        };
        self.store
            .create_property_constraint(
                name,
                &storage_scope(&cluster_scope),
                &req.properties,
                storage_kind(kind),
                req.if_not_exists,
            )
            .map_err(internal)?;
        Ok(Response::new(CreatePropertyConstraintResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "drop_property_constraint"))]
    async fn drop_property_constraint(
        &self,
        request: Request<DropPropertyConstraintRequest>,
    ) -> Result<Response<DropPropertyConstraintResponse>, Status> {
        let req = request.into_inner();
        self.store
            .drop_property_constraint(&req.name, req.if_exists)
            .map_err(internal)?;
        Ok(Response::new(DropPropertyConstraintResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "install_trigger"))]
    async fn install_trigger(
        &self,
        request: Request<crate::proto::InstallTriggerRequest>,
    ) -> Result<Response<crate::proto::InstallTriggerResponse>, Status> {
        let req = request.into_inner();
        self.store
            .put_trigger(&req.name, &req.spec_blob)
            .map_err(internal)?;
        // Fire-and-forget refresh of the local trigger registry —
        // the apply succeeded, so a stale cache here would be
        // user-visible inconsistency. The factory closure holds
        // the registry; we re-build a registry instance to read
        // the field. Failures log and swallow; the next firing
        // path will pick up the change too.
        #[cfg(feature = "apoc-trigger")]
        if let Some(reg) = (self.procedure_registry_factory)().trigger_registry() {
            let _ = reg.refresh();
        }
        Ok(Response::new(crate::proto::InstallTriggerResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "drop_trigger"))]
    async fn drop_trigger(
        &self,
        request: Request<crate::proto::DropTriggerRequest>,
    ) -> Result<Response<crate::proto::DropTriggerResponse>, Status> {
        let req = request.into_inner();
        self.store.delete_trigger(&req.name).map_err(internal)?;
        #[cfg(feature = "apoc-trigger")]
        if let Some(reg) = (self.procedure_registry_factory)().trigger_registry() {
            let _ = reg.refresh();
        }
        Ok(Response::new(crate::proto::DropTriggerResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "resolve_transaction", txid))]
    async fn resolve_transaction(
        &self,
        request: Request<ResolveTransactionRequest>,
    ) -> Result<Response<ResolveTransactionResponse>, Status> {
        let req = request.into_inner();
        let txid = req.txid;
        tracing::Span::current().record("txid", txid.as_str());

        #[cfg(any(test, feature = "fault-inject"))]
        if let Some(fp) = &self.fault_points {
            fp.resolve_transaction_call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }

        let Some(log) = self.coordinator_log.as_ref() else {
            // This peer isn't a coordinator (no log) — can't possibly
            // know the decision for anyone's txid. Return UNKNOWN so
            // the caller keeps polling other peers.
            return Ok(Response::new(ResolveTransactionResponse {
                status: crate::proto::TxResolutionStatus::Unknown as i32,
            }));
        };

        let entries = log
            .read_all()
            .map_err(|e| Status::internal(format!("reading coordinator log: {e}")))?;
        let state = crate::coordinator_log::reconstruct_state(&entries);
        let status = match state.get(&txid) {
            Some(s) => match s.decision {
                Some(crate::TxDecision::Commit) => crate::proto::TxResolutionStatus::Committed,
                Some(crate::TxDecision::Abort) => crate::proto::TxResolutionStatus::Aborted,
                None => crate::proto::TxResolutionStatus::Unknown,
            },
            None => crate::proto::TxResolutionStatus::Unknown,
        };
        Ok(Response::new(ResolveTransactionResponse {
            status: status as i32,
        }))
    }

    #[tracing::instrument(skip_all, fields(rpc = "forward_write", partition))]
    async fn forward_write(
        &self,
        request: Request<crate::proto::ForwardWriteRequest>,
    ) -> Result<Response<crate::proto::ForwardWriteResponse>, Status> {
        let req = request.into_inner();
        tracing::Span::current().record("partition", req.partition);

        #[cfg(any(test, feature = "fault-inject"))]
        if let Some(fp) = &self.fault_points {
            fp.forward_write_call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if fp
                .multi_raft_reject_forward_write
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(Status::internal(
                    "injected fault: multi_raft_reject_forward_write",
                ));
            }
        }

        let Some(multi_raft) = self.multi_raft.as_ref() else {
            return Err(Status::failed_precondition(
                "ForwardWrite called on a peer not running mode = \"multi-raft\"",
            ));
        };

        // Absorb the caller's DDL barrier into our cluster's
        // `min_meta_index`. The propose path's `await_meta_barrier`
        // will block until our meta replica has applied at least
        // this much DDL — guarantees a CREATE INDEX issued through
        // a different peer right before this write is visible by
        // the time the partition Raft applies it.
        if req.min_meta_index > 0 {
            multi_raft.observe_meta_barrier(req.min_meta_index);
        }

        let partition_id = meshdb_cluster::PartitionId(req.partition);
        let partition_raft = match multi_raft.partition(partition_id) {
            Some(r) => r,
            None => {
                // We don't host this partition's Raft replica; the
                // caller's leader cache is stale.
                return Ok(Response::new(crate::proto::ForwardWriteResponse {
                    ok: false,
                    leader_hint: 0,
                    error_message: format!("partition {} not hosted on this peer", partition_id.0),
                }));
            }
        };

        if !multi_raft.is_local_leader(partition_id) {
            // Best-effort leader hint — None falls through as 0
            // ("unknown"), which the caller treats as a retry-after-poll
            // signal rather than a fixed redirect.
            let leader_hint = multi_raft.leader_of(partition_id).unwrap_or(0);
            return Ok(Response::new(crate::proto::ForwardWriteResponse {
                ok: false,
                leader_hint,
                error_message: format!(
                    "this peer is not the leader of partition {}",
                    partition_id.0
                ),
            }));
        }

        // Decode the carried command list and propose as one Raft
        // entry. The applier on every replica of this partition will
        // converge through the partition Raft log.
        let commands: Vec<meshdb_cluster::GraphCommand> =
            serde_json::from_slice(&req.commands_json)
                .map_err(|e| Status::invalid_argument(format!("decoding commands: {e}")))?;
        let entry = if commands.len() == 1 {
            commands.into_iter().next().unwrap()
        } else {
            meshdb_cluster::GraphCommand::Batch(commands)
        };
        match partition_raft.propose_graph(entry).await {
            Ok(_) => Ok(Response::new(crate::proto::ForwardWriteResponse {
                ok: true,
                leader_hint: 0,
                error_message: String::new(),
            })),
            Err(meshdb_cluster::Error::ForwardToLeader { leader_id, .. }) => {
                // Lost leadership between is_local_leader check and
                // propose. Surface the new hint so the caller retries.
                let hint = leader_id.map(|id| id.0).unwrap_or(0);
                if let Some(l) = leader_id {
                    multi_raft.leader_cache.set(partition_id, l.0);
                    let _ = l;
                }
                Ok(Response::new(crate::proto::ForwardWriteResponse {
                    ok: false,
                    leader_hint: hint,
                    error_message: "leadership changed mid-propose".into(),
                }))
            }
            Err(e) => Err(Status::internal(format!("propose_graph: {e}"))),
        }
    }

    #[tracing::instrument(skip_all, fields(rpc = "forward_ddl"))]
    async fn forward_ddl(
        &self,
        request: Request<crate::proto::ForwardDdlRequest>,
    ) -> Result<Response<crate::proto::ForwardDdlResponse>, Status> {
        let req = request.into_inner();
        let Some(multi_raft) = self.multi_raft.as_ref() else {
            return Err(Status::failed_precondition(
                "ForwardDdl called on a peer not running mode = \"multi-raft\"",
            ));
        };
        let commands: Vec<GraphCommand> = serde_json::from_slice(&req.commands_json)
            .map_err(|e| Status::invalid_argument(format!("decoding ddl commands: {e}")))?;
        for entry in commands {
            match multi_raft.meta.propose_graph(entry).await {
                Ok(_) => {}
                Err(meshdb_cluster::Error::ForwardToLeader { leader_id, .. }) => {
                    let hint = leader_id.map(|id| id.0).unwrap_or(0);
                    return Ok(Response::new(crate::proto::ForwardDdlResponse {
                        ok: false,
                        leader_hint: hint,
                        error_message: "this peer is not the meta leader".into(),
                    }));
                }
                Err(e) => {
                    return Ok(Response::new(crate::proto::ForwardDdlResponse {
                        ok: false,
                        leader_hint: 0,
                        error_message: format!("meta propose failed: {e}"),
                    }));
                }
            }
        }
        // Synchronous-DDL gate: don't ACK the forwarded DDL until
        // every peer's meta replica has applied. The originating
        // user-visible call only completes once we return here, so
        // the strict-apply guarantee holds end-to-end across the
        // forwarding hop.
        let target = multi_raft.meta_last_applied();
        if let Err(e) = self
            .await_cluster_meta_apply(multi_raft, target, crate::DEFAULT_DDL_STRICT_TIMEOUT)
            .await
        {
            return Ok(Response::new(crate::proto::ForwardDdlResponse {
                ok: false,
                leader_hint: 0,
                error_message: format!("strict-apply gate: {}", e.message()),
            }));
        }
        Ok(Response::new(crate::proto::ForwardDdlResponse {
            ok: true,
            leader_hint: 0,
            error_message: String::new(),
        }))
    }

    #[tracing::instrument(skip_all, fields(rpc = "meta_last_applied"))]
    async fn meta_last_applied(
        &self,
        _request: Request<crate::proto::MetaLastAppliedRequest>,
    ) -> Result<Response<crate::proto::MetaLastAppliedResponse>, Status> {
        let last_applied = match &self.multi_raft {
            Some(mr) => mr.meta_last_applied(),
            None => 0,
        };
        Ok(Response::new(crate::proto::MetaLastAppliedResponse {
            last_applied,
        }))
    }
}
