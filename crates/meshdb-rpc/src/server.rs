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
}

impl MeshService {
    /// Local-only service: every request is answered from the local store.
    pub fn new(store: Arc<dyn StorageEngine>) -> Self {
        Self {
            store,
            routing: None,
            raft: None,
            pending_batches: crate::ParticipantStaging::with_default_ttl(),
            coordinator_log: None,
            participant_log: None,
            tx_timeouts: crate::TxCoordinatorTimeouts::default(),
            client_tls: None,
            procedure_registry_factory: default_registry_factory(),
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
            raft: None,
            pending_batches: crate::ParticipantStaging::with_default_ttl(),
            coordinator_log,
            participant_log: None,
            tx_timeouts: crate::TxCoordinatorTimeouts::default(),
            client_tls: None,
            procedure_registry_factory: default_registry_factory(),
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
            raft: Some(raft),
            pending_batches: crate::ParticipantStaging::with_default_ttl(),
            coordinator_log: None,
            participant_log: None,
            tx_timeouts: crate::TxCoordinatorTimeouts::default(),
            client_tls: None,
            procedure_registry_factory: default_registry_factory(),
        }
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
        let mode_label = if self.routing.is_some() {
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
        let mut batches_count: i64 = 0;
        let mut total: i64 = 0;
        let mut committed_ops: i64 = 0;
        let mut failed_ops: i64 = 0;
        let mut failed_batches: i64 = 0;
        let mut retries_count: i64 = 0;
        let mut error_messages: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        let mut failed_params: std::collections::HashMap<
            String,
            Vec<std::collections::HashMap<String, meshdb_core::Property>>,
        > = std::collections::HashMap::new();
        let mut update_stats = UpdateStatsAccumulator::default();
        let bs = batch_size.max(1) as usize;

        for chunk in input_rows.chunks(bs) {
            batches_count += 1;
            let chunk_size = chunk.len() as i64;
            total += chunk_size;
            let chunk_rows: Vec<meshdb_executor::Row> = chunk.to_vec();

            // Retry loop: try the batch up to max_retries + 1
            // times before counting it as a definitive failure.
            // Each attempt rebuilds the action's writes from
            // scratch — no partial-batch state from a previous
            // failed attempt leaks in.
            let max_attempts = max_retries.max(0) + 1;
            let mut last_err: Option<String> = None;
            let mut succeeded = false;
            let mut succeeded_commands: Option<Vec<GraphCommand>> = None;
            for attempt in 0..max_attempts {
                if attempt > 0 {
                    retries_count += 1;
                }
                let chunk_rows = chunk_rows.clone();
                let action_plan = action_plan.clone();
                let store = self.store.clone();
                let routing = self.routing.clone();
                let extra_params = extra_params.clone();
                let registry_factory = self.procedure_registry_factory.clone();
                let outcome_result = tokio::task::spawn_blocking(
                    move || -> std::result::Result<Vec<GraphCommand>, meshdb_executor::Error> {
                        let storage_reader =
                            StorageReaderAdapter(store.as_ref() as &dyn StorageEngine);
                        let writer = BufferingGraphWriter::new();
                        let procs = registry_factory();
                        if iterate_list {
                            // Batch-as-list mode: bind the whole
                            // chunk to `$_batch` (a list of
                            // row-Maps) and run the action ONCE.
                            // The action is responsible for
                            // `UNWIND $_batch AS row` to process
                            // its elements.
                            let mut params = extra_params.clone();
                            let batch_list: Vec<meshdb_core::Property> = chunk_rows
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
                                let partitioned =
                                    PartitionedGraphReader::new(store.clone(), r.clone());
                                meshdb_executor::execute_with_reader_and_procs(
                                    &action_plan,
                                    &partitioned as &dyn GraphReader,
                                    &writer as &dyn GraphWriter,
                                    &params,
                                    &procs,
                                )?;
                            } else {
                                meshdb_executor::execute_with_reader_and_procs(
                                    &action_plan,
                                    &storage_reader as &dyn GraphReader,
                                    &writer as &dyn GraphWriter,
                                    &params,
                                    &procs,
                                )?;
                            }
                        } else {
                            for row in chunk_rows {
                                let mut row_params = extra_params.clone();
                                for (k, v) in row {
                                    row_params.insert(k, v);
                                }
                                if let Some(r) = routing.as_ref() {
                                    let partitioned =
                                        PartitionedGraphReader::new(store.clone(), r.clone());
                                    meshdb_executor::execute_with_reader_and_procs(
                                        &action_plan,
                                        &partitioned as &dyn GraphReader,
                                        &writer as &dyn GraphWriter,
                                        &row_params,
                                        &procs,
                                    )?;
                                } else {
                                    meshdb_executor::execute_with_reader_and_procs(
                                        &action_plan,
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
                .map_err(|e| {
                    Status::internal(format!("batch {batches_count} executor panicked: {e}"))
                })?;
                match outcome_result {
                    Ok(commands) => match self.commit_buffered_commands(commands.clone()).await {
                        Ok(()) => {
                            update_stats.absorb(&commands);
                            succeeded = true;
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
            if succeeded {
                let _ = succeeded_commands;
                committed_ops += chunk_size;
            } else {
                failed_ops += chunk_size;
                failed_batches += 1;
                let err_str = last_err.unwrap_or_else(|| "<no error captured>".into());
                *error_messages.entry(err_str.clone()).or_insert(0) += 1;
                if failed_params_cap != 0 {
                    let bucket = failed_params.entry(err_str.clone()).or_default();
                    let cap = if failed_params_cap < 0 {
                        usize::MAX
                    } else {
                        failed_params_cap as usize
                    };
                    if bucket.len() < cap {
                        for row in &chunk_rows {
                            if bucket.len() >= cap {
                                break;
                            }
                            bucket.push(row_to_property_map(row));
                        }
                    }
                }
                tracing::warn!(
                    batch = batches_count,
                    error = %err_str,
                    attempts = max_attempts,
                    "apoc.periodic.iterate — batch failed after retries"
                );
            }
        }
        let time_taken = started_at.elapsed().as_millis() as i64;

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
        } => Some((
            (**iterate).clone(),
            (**action).clone(),
            *batch_size,
            extra_params.clone(),
            *retries,
            *failed_params_cap,
            *iterate_list,
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
#[derive(Debug, Default)]
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
        let rows = self.execute_cypher_local(query, params).await?;
        let rows_json = serde_json::to_vec(&rows)
            .map_err(|e| Status::internal(format!("encoding rows: {e}")))?;
        Ok(Response::new(ExecuteCypherResponse { rows_json }))
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
}
