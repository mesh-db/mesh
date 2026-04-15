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
    CreatePropertyIndexRequest, CreatePropertyIndexResponse, DeleteEdgeRequest, DeleteEdgeResponse,
    DetachDeleteNodeRequest, DetachDeleteNodeResponse, DropPropertyIndexRequest,
    DropPropertyIndexResponse, ExecuteCypherRequest, ExecuteCypherResponse, GetEdgeRequest,
    GetEdgeResponse, GetNodeRequest, GetNodeResponse, HealthRequest, HealthResponse, NeighborInfo,
    NeighborRequest, NeighborResponse, NodesByLabelRequest, NodesByLabelResponse,
    NodesByPropertyRequest, NodesByPropertyResponse, PutEdgeRequest, PutEdgeResponse,
    PutNodeRequest, PutNodeResponse,
};
use crate::routing::Routing;
use crate::tx_coordinator::TxCoordinator;
use mesh_cluster::raft::RaftCluster;
use mesh_cluster::{Error as ClusterError, GraphCommand, PeerId};
use mesh_executor::{execute_with_reader, GraphReader, GraphWriter};
use mesh_storage::Store;
use std::collections::HashSet;
use std::sync::Arc;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct MeshService {
    store: Arc<Store>,
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
}

impl MeshService {
    /// Local-only service: every request is answered from the local store.
    pub fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            routing: None,
            raft: None,
            pending_batches: crate::ParticipantStaging::with_default_ttl(),
            coordinator_log: None,
        }
    }

    /// Routed service without a durable coordinator log. Equivalent
    /// to [`with_routing_and_log`] passing `None`.
    pub fn with_routing(store: Arc<Store>, routing: Arc<Routing>) -> Self {
        Self::with_routing_and_log(store, routing, None)
    }

    /// Routed service with an optional durable coordinator log. The
    /// log records 2PC progress so a coordinator crash between PREPARE
    /// and COMMIT can be recovered via
    /// [`Self::recover_pending_transactions`] at startup.
    pub fn with_routing_and_log(
        store: Arc<Store>,
        routing: Arc<Routing>,
        coordinator_log: Option<Arc<crate::CoordinatorLog>>,
    ) -> Self {
        Self {
            store,
            routing: Some(routing),
            raft: None,
            pending_batches: crate::ParticipantStaging::with_default_ttl(),
            coordinator_log,
        }
    }

    /// Raft-backed service: writes go through `RaftCluster::propose_graph`
    /// so every replica's local store ends up with the same data via the
    /// state machine's apply path. Reads go straight to the local store
    /// (every peer holds the full graph in this single-Raft-group model).
    pub fn with_raft(store: Arc<Store>, raft: Arc<RaftCluster>) -> Self {
        Self {
            store,
            routing: None,
            raft: Some(raft),
            pending_batches: crate::ParticipantStaging::with_default_ttl(),
            coordinator_log: None,
        }
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
    /// raw [`mesh_executor::Row`]s. Shared between the gRPC
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
        params: mesh_executor::ParamMap,
    ) -> std::result::Result<Vec<mesh_executor::Row>, Status> {
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
        params: mesh_executor::ParamMap,
    ) -> std::result::Result<(Vec<mesh_executor::Row>, Vec<GraphCommand>), Status> {
        // Equivalent to running an explicit tx with an empty
        // accumulator — no previously-buffered writes to overlay.
        self.execute_cypher_in_tx(query, params, Vec::new()).await
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
        params: mesh_executor::ParamMap,
        prev_commands: Vec<GraphCommand>,
    ) -> std::result::Result<(Vec<mesh_executor::Row>, Vec<GraphCommand>), Status> {
        let statement = mesh_cypher::parse(&query).map_err(bad_request)?;

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
        let planner_ctx = mesh_cypher::PlannerContext {
            indexes: self
                .store
                .list_property_indexes()
                .into_iter()
                .map(|s| (s.label, s.property))
                .collect(),
        };
        let plan = mesh_cypher::plan_with_context(&statement, &planner_ctx).map_err(bad_request)?;

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

        let (rows, commands) = tokio::task::spawn_blocking(
            move || -> std::result::Result<
                (Vec<mesh_executor::Row>, Vec<GraphCommand>),
                mesh_executor::Error,
            > {
                let store_ref: &Store = &store;
                let writer = BufferingGraphWriter::new();
                // Fold the previously-buffered tx writes into an
                // overlay state. Empty for auto-commit RUNs; populated
                // for in-tx RUNs so the executor sees the prior writes.
                let overlay = crate::TxOverlayState::from_commands(&prev_commands);

                let rows = if let Some(r) = routing.as_ref() {
                    // Routing mode: reads go through a partitioned
                    // reader. Single-node and Raft modes use the local
                    // store directly (Raft replicates the full graph).
                    let partitioned =
                        PartitionedGraphReader::new(store.clone(), r.clone());
                    let base: &dyn GraphReader = &partitioned;
                    let reader = crate::OverlayGraphReader::new(base, &overlay);
                    execute_with_reader(
                        &plan,
                        &reader as &dyn GraphReader,
                        &writer as &dyn GraphWriter,
                        &exec_params,
                    )?
                } else {
                    let base: &dyn GraphReader = store_ref as &dyn GraphReader;
                    let reader = crate::OverlayGraphReader::new(base, &overlay);
                    execute_with_reader(
                        &plan,
                        &reader as &dyn GraphReader,
                        &writer as &dyn GraphWriter,
                        &exec_params,
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
        if commands.is_empty() {
            return Ok(());
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
            let mut coordinator = TxCoordinator::new(&self.store, &self.pending_batches, routing);
            if let Some(log) = self.coordinator_log.as_deref() {
                coordinator = coordinator.with_log(log);
            }
            match coordinator.run(graph).await {
                Ok(()) => {
                    crate::metrics::TWO_PHASE_COMMIT_TOTAL
                        .with_label_values(&["committed"])
                        .inc();
                    return Ok(());
                }
                Err(e) => {
                    crate::metrics::TWO_PHASE_COMMIT_TOTAL
                        .with_label_values(&["aborted"])
                        .inc();
                    return Err(e);
                }
            }
        }
        if let Some(raft) = &self.raft {
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
                    Ok(())
                }
                Err(ClusterError::ForwardToLeader {
                    leader_address: Some(addr),
                    ..
                }) => {
                    crate::metrics::RAFT_PROPOSALS_TOTAL
                        .with_label_values(&["forwarded"])
                        .inc();
                    Err(Status::failed_precondition(format!(
                        "raft leader is at {addr}; reconnect and retry there"
                    )))
                }
                Err(e) => {
                    crate::metrics::RAFT_PROPOSALS_TOTAL
                        .with_label_values(&["failed"])
                        .inc();
                    Err(raft_propose_failed(e))
                }
            };
        }
        // Single-node: apply the batch directly through the store's
        // atomic apply_batch helper (one rocksdb WriteBatch).
        apply_prepared_batch(&self.store, &commands).map_err(internal)?;
        Ok(())
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
            apply_ddl_to_store(cmd, &self.store).map_err(internal)?;
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
                if let Err(e) = apply_ddl_to_store(&inverse, &self.store) {
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
                if let Err(e) = apply_prepared_batch(&self.store, cmds) {
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
        params: mesh_executor::ParamMap,
    ) -> std::result::Result<Vec<mesh_executor::Row>, Status> {
        let endpoint = Endpoint::from_shared(format!("http://{}", addr))
            .map_err(|e| Status::internal(format!("invalid leader address {addr}: {e}")))?;
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
/// into a flat `Vec<StoreMutation>` so `Store::apply_batch` can commit
/// them as one rocksdb `WriteBatch`.
///
/// DDL commands (`CreateIndex` / `DropIndex`) are intentionally not
/// handled here — they can't be expressed as a `StoreMutation` because
/// the backfill step needs to read the live graph, and an uncommitted
/// `WriteBatch` isn't queryable. Callers going through
/// [`apply_prepared_batch`] get correct split semantics for free;
/// direct callers of `flatten_commands` must ensure the batch has
/// already been stripped of DDL entries.
pub(crate) fn flatten_commands(cmds: &[GraphCommand], out: &mut Vec<mesh_storage::StoreMutation>) {
    use mesh_storage::StoreMutation;
    for cmd in cmds {
        match cmd {
            GraphCommand::PutNode(n) => out.push(StoreMutation::PutNode(n.clone())),
            GraphCommand::PutEdge(e) => out.push(StoreMutation::PutEdge(e.clone())),
            GraphCommand::DeleteEdge(id) => out.push(StoreMutation::DeleteEdge(*id)),
            GraphCommand::DetachDeleteNode(id) => out.push(StoreMutation::DetachDeleteNode(*id)),
            GraphCommand::Batch(inner) => flatten_commands(inner, out),
            // Skip silently; the caller is responsible for applying
            // DDL through `Store::create_property_index` /
            // `Store::drop_property_index` before (or alongside)
            // this batch.
            GraphCommand::CreateIndex { .. } | GraphCommand::DropIndex { .. } => {}
        }
    }
}

/// Apply a prepared batch to `store` atomically. Used by both the
/// `BatchWrite` commit phase and the in-process coordinator shortcut.
///
/// Index DDL in the batch is applied up-front via
/// `Store::create_property_index` / `drop_property_index` (each in
/// its own rocksdb batch), then the remaining graph mutations commit
/// as one atomic `apply_batch`. Cypher statements don't mix DDL and
/// graph writes so this ordering doesn't affect typical workloads.
pub(crate) fn apply_prepared_batch(
    store: &Store,
    cmds: &[GraphCommand],
) -> std::result::Result<(), mesh_storage::Error> {
    apply_ddl_commands(cmds, store)?;
    let mut flat = Vec::with_capacity(cmds.len());
    flatten_commands(cmds, &mut flat);
    if flat.is_empty() {
        return Ok(());
    }
    store.apply_batch(&flat)
}

/// Count the number of `IndexSeek` plan nodes in `plan`,
/// recursively. Used by `execute_cypher_in_tx` to bump the
/// `mesh_cypher_index_seeks_total` counter once per query — cheap
/// because plans are tiny, and accurate enough for usage tracking
/// without instrumenting the executor itself.
fn count_index_seeks(plan: &mesh_cypher::LogicalPlan) -> u64 {
    use mesh_cypher::LogicalPlan as P;
    match plan {
        P::IndexSeek { .. } => 1,
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
        | P::VarLengthExpand { input, .. } => count_index_seeks(input),
        P::CartesianProduct { left, right } => count_index_seeks(left) + count_index_seeks(right),
        P::CreatePath { input, .. } => input.as_deref().map(count_index_seeks).unwrap_or(0),
        P::NodeScanAll { .. }
        | P::NodeScanByLabels { .. }
        | P::MergeNode { .. }
        | P::Unwind { .. }
        | P::CreatePropertyIndex { .. }
        | P::DropPropertyIndex { .. }
        | P::ShowPropertyIndexes => 0,
    }
}

/// Apply a single DDL [`GraphCommand`] directly to `store`. Non-DDL
/// variants are a no-op — the caller filters them out beforehand.
fn apply_ddl_to_store(
    cmd: &GraphCommand,
    store: &Store,
) -> std::result::Result<(), mesh_storage::Error> {
    match cmd {
        GraphCommand::CreateIndex { label, property } => {
            store.create_property_index(label, property)
        }
        GraphCommand::DropIndex { label, property } => store.drop_property_index(label, property),
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
        GraphCommand::CreateIndex { label, property } => GraphCommand::DropIndex {
            label: label.clone(),
            property: property.clone(),
        },
        GraphCommand::DropIndex { label, property } => GraphCommand::CreateIndex {
            label: label.clone(),
            property: property.clone(),
        },
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
            GraphCommand::CreateIndex { label, property } => {
                client
                    .create_property_index(CreatePropertyIndexRequest {
                        label: label.clone(),
                        property: property.clone(),
                    })
                    .await?;
            }
            GraphCommand::DropIndex { label, property } => {
                client
                    .drop_property_index(DropPropertyIndexRequest {
                        label: label.clone(),
                        property: property.clone(),
                    })
                    .await?;
            }
            _ => {}
        }
    }
    Ok(())
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
            GraphCommand::CreateIndex { .. } | GraphCommand::DropIndex { .. } => ddl.push(cmd),
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
    store: &Store,
) -> std::result::Result<(), mesh_storage::Error> {
    for cmd in cmds {
        match cmd {
            GraphCommand::CreateIndex { label, property } => {
                store.create_property_index(label, property)?;
            }
            GraphCommand::DropIndex { label, property } => {
                store.drop_property_index(label, property)?;
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

fn leader_write_client(addr: &str) -> Result<MeshWriteClient<Channel>, Status> {
    let endpoint = Endpoint::from_shared(format!("http://{}", addr))
        .map_err(|e| Status::internal(format!("invalid leader address {addr}: {e}")))?;
    Ok(MeshWriteClient::new(endpoint.connect_lazy()))
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
        let value: mesh_core::Property = serde_json::from_slice(&req.value_json)
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
        let params: mesh_executor::ParamMap = if req.params_json.is_empty() {
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
                    let mut client = leader_write_client(&addr)?;
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
                    let mut client = leader_write_client(&addr)?;
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
                    let mut client = leader_write_client(&addr)?;
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
                    let mut client = leader_write_client(&addr)?;
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
                self.pending_batches
                    .try_insert(txid.clone(), commands)
                    .map_err(|()| {
                        Status::already_exists(format!("txid {} already prepared", txid))
                    })?;
                Ok(Response::new(BatchWriteResponse {}))
            }
            BatchPhase::Commit => {
                let cmds = self.pending_batches.take(&txid).ok_or_else(|| {
                    Status::failed_precondition(format!("txid {} not prepared on this peer", txid))
                })?;
                apply_prepared_batch(&self.store, &cmds).map_err(internal)?;
                Ok(Response::new(BatchWriteResponse {}))
            }
            BatchPhase::Abort => {
                // Abort is always idempotent: dropping the staged entry is
                // safe even if PREPARE never arrived (e.g., coordinator
                // sends ABORT defensively on a partial failure).
                let _ = self.pending_batches.take(&txid);
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
        self.store
            .create_property_index(&req.label, &req.property)
            .map_err(internal)?;
        Ok(Response::new(CreatePropertyIndexResponse {}))
    }

    #[tracing::instrument(skip_all, fields(rpc = "drop_property_index"))]
    async fn drop_property_index(
        &self,
        request: Request<DropPropertyIndexRequest>,
    ) -> Result<Response<DropPropertyIndexResponse>, Status> {
        let req = request.into_inner();
        self.store
            .drop_property_index(&req.label, &req.property)
            .map_err(internal)?;
        Ok(Response::new(DropPropertyIndexResponse {}))
    }
}
