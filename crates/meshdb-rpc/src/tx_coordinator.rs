//! Two-phase commit coordinator for multi-peer Cypher write transactions.
//!
//! Groups a buffered sequence of [`GraphCommand`]s by destination peer,
//! sends `BatchWrite(PREPARE)` to every peer, then issues `COMMIT` iff all
//! PREPAREs succeeded — otherwise `ABORT`. The local peer is handled via a
//! direct pointer into `MeshService`'s in-memory staging map so the
//! coordinator doesn't loop back through gRPC for self.
//!
//! Consistency caveats:
//! - Participant staging is in memory only. A participant crash between
//!   PREPARE and COMMIT implicitly aborts the tx (PREPARE is lost, COMMIT
//!   returns `FAILED_PRECONDITION`). That matches standard 2PC semantics.
//! - Coordinator crash between PREPARE and COMMIT leaves participants with
//!   stuck staged batches until the process restarts. A future step can
//!   add a staging TTL or a recovery log.

use crate::proto::mesh_write_client::MeshWriteClient;
use crate::proto::{BatchPhase, BatchWriteRequest};
use crate::routing::Routing;
use meshdb_cluster::{GraphCommand, PeerId};
use meshdb_storage::StorageEngine;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;
use tonic::Status;
use uuid::Uuid;

/// Per-phase deadlines for the 2PC round. Each peer RPC is wrapped
/// in the matching timeout; without these, a slow or hung peer would
/// stall the entire transaction under tonic's default client timeout
/// (effectively unbounded for in-cluster traffic).
///
/// Defaults: 10s on PREPARE and ABORT, 30s on COMMIT. The asymmetry
/// reflects what each phase actually does — PREPARE should complete
/// quickly because the work is only staging the commands, ABORT is a
/// single `take()` on the staging map, but COMMIT applies the batch
/// against RocksDB and may block on fsync under heavy write load.
#[derive(Debug, Clone, Copy)]
pub struct TxCoordinatorTimeouts {
    pub prepare: Duration,
    pub commit: Duration,
    pub abort: Duration,
}

impl Default for TxCoordinatorTimeouts {
    fn default() -> Self {
        Self {
            prepare: Duration::from_secs(10),
            commit: Duration::from_secs(30),
            abort: Duration::from_secs(10),
        }
    }
}

pub(crate) struct TxCoordinator<'a> {
    local_store: &'a dyn StorageEngine,
    local_pending: &'a Arc<crate::ParticipantStaging>,
    routing: &'a Routing,
    /// Optional durable log used to recover from a coordinator crash
    /// between PREPARE and COMMIT. When `None`, the coordinator runs
    /// without crash recovery — useful for tests that don't care, but
    /// production paths should always pass a real log.
    log: Option<&'a crate::CoordinatorLog>,
    /// Per-phase RPC deadlines. Defaulted at construction; override
    /// via [`TxCoordinator::with_timeouts`] in tests that need
    /// millisecond-scale deadlines to exercise the timeout paths.
    timeouts: TxCoordinatorTimeouts,
    /// Test-only deterministic crash points. Consulted at three
    /// checkpoints inside [`Self::run`] so an integration test can
    /// drive the coordinator into each of its recovery branches
    /// without timing races.
    #[cfg(any(test, feature = "fault-inject"))]
    fault_points: Option<Arc<crate::FaultPoints>>,
}

impl<'a> TxCoordinator<'a> {
    pub fn new(
        local_store: &'a dyn StorageEngine,
        local_pending: &'a Arc<crate::ParticipantStaging>,
        routing: &'a Routing,
    ) -> Self {
        Self {
            local_store,
            local_pending,
            routing,
            log: None,
            timeouts: TxCoordinatorTimeouts::default(),
            #[cfg(any(test, feature = "fault-inject"))]
            fault_points: None,
        }
    }

    /// Attach a [`FaultPoints`](crate::FaultPoints) handle so the
    /// integration-test harness can fire deterministic crashes at
    /// the three checkpoints inside [`Self::run`]. Zero cost in
    /// release builds — the field and checkpoint checks are cfg-gated.
    #[cfg(any(test, feature = "fault-inject"))]
    pub fn with_fault_points(mut self, fp: Option<Arc<crate::FaultPoints>>) -> Self {
        self.fault_points = fp;
        self
    }

    /// Attach a durable coordinator log to this coordinator instance.
    /// Each 2PC round will append `Prepared` / decision / `Completed`
    /// entries at the protocol boundaries; on a restart, the recovery
    /// routine in `MeshService` walks the log and pushes any
    /// unfinished transactions forward.
    pub fn with_log(mut self, log: &'a crate::CoordinatorLog) -> Self {
        self.log = Some(log);
        self
    }

    /// Override the per-phase RPC timeouts. Call sites pass
    /// `MeshService`'s configured value through; tests override with
    /// millisecond-scale deadlines to exercise the timeout branch.
    pub fn with_timeouts(mut self, timeouts: TxCoordinatorTimeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Run a 2PC round over the buffered commands. The commands get
    /// grouped by destination peer, PREPAREd on every peer in one pass,
    /// and then either COMMITted on every peer or ABORTed on every peer
    /// that saw the PREPARE.
    pub async fn run(&self, commands: Vec<GraphCommand>) -> Result<(), Status> {
        if commands.is_empty() {
            return Ok(());
        }

        let groups = self.group_by_peer(commands);
        if groups.is_empty() {
            return Ok(());
        }

        let txid = Uuid::now_v7().to_string();

        // Durable intent: record the full tx (groups + commands) in
        // the coordinator log before we touch any participant. If we
        // crash after this and before writing a decision, recovery
        // will find the Prepared entry and abort the tx.
        if let Some(log) = self.log {
            let groups_vec: Vec<(PeerId, Vec<GraphCommand>)> =
                groups.iter().map(|(p, c)| (*p, c.clone())).collect();
            log.append(&crate::TxLogEntry::Prepared {
                txid: txid.clone(),
                groups: groups_vec,
            })
            .map_err(|e| {
                Status::internal(format!("coordinator log write (Prepared) failed: {e}"))
            })?;
        }

        #[cfg(any(test, feature = "fault-inject"))]
        if let Some(fp) = &self.fault_points {
            if fp
                .crash_after_prepare_log
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(Status::internal("injected fault: crash_after_prepare_log"));
            }
        }

        // PREPARE phase. Track which peers ack'd so we can target ABORT
        // precisely if one of the later PREPAREs fails.
        let mut prepared: Vec<PeerId> = Vec::new();
        for (peer_id, cmds) in &groups {
            match self.prepare(*peer_id, &txid, cmds).await {
                Ok(()) => prepared.push(*peer_id),
                Err(prepare_err) => {
                    // Record the abort decision *before* sending any
                    // ABORT so a crash mid-rollback still leaves
                    // recovery with a consistent picture: "this tx was
                    // decided to abort; finish the rollback."
                    if let Some(log) = self.log {
                        if let Err(e) =
                            log.append(&crate::TxLogEntry::AbortDecision { txid: txid.clone() })
                        {
                            tracing::warn!(
                                txid = %txid,
                                error = %e,
                                "coordinator log write (AbortDecision) failed",
                            );
                        }
                    }
                    // Best-effort ABORT of the peers that did prepare.
                    for p in &prepared {
                        if let Err(abort_err) = self.abort(*p, &txid).await {
                            tracing::warn!(
                                peer = %p,
                                txid = %txid,
                                error = %abort_err,
                                "abort during rollback failed"
                            );
                        }
                    }
                    // Mark the tx done so compaction can drop it.
                    if let Some(log) = self.log {
                        let _ = log.append(&crate::TxLogEntry::Completed { txid: txid.clone() });
                    }
                    return Err(prepare_err);
                }
            }
        }

        // Point of no return: every PREPARE ack'd. Write the commit
        // decision before we start sending COMMIT RPCs so recovery can
        // tell "we intended to commit" from "we intended to abort."
        if let Some(log) = self.log {
            log.append(&crate::TxLogEntry::CommitDecision { txid: txid.clone() })
                .map_err(|e| {
                    Status::internal(format!(
                        "coordinator log write (CommitDecision) failed: {e}"
                    ))
                })?;
        }

        #[cfg(any(test, feature = "fault-inject"))]
        if let Some(fp) = &self.fault_points {
            if fp
                .crash_after_commit_decision_log
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(Status::internal(
                    "injected fault: crash_after_commit_decision_log",
                ));
            }
        }

        // COMMIT phase. A COMMIT failure is harder to recover from
        // (some peers may already have applied), but we still try to
        // commit every peer so the transaction makes maximum progress.
        // The first commit error is returned to the caller.
        let mut first_commit_err: Option<Status> = None;
        for peer_id in &prepared {
            #[cfg(any(test, feature = "fault-inject"))]
            if let Some(fp) = &self.fault_points {
                // `crash_after_kth_commit_rpc` is a countdown: fire
                // when the remaining-allowed-sends counter has
                // decremented to zero, so setting it to `N` crashes
                // after the Nth COMMIT has been attempted.
                let remaining = fp
                    .crash_after_kth_commit_rpc
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                if remaining == 0 {
                    return Err(Status::internal(
                        "injected fault: crash_after_kth_commit_rpc",
                    ));
                }
            }
            if let Err(e) = self.commit(*peer_id, &txid).await {
                if first_commit_err.is_none() {
                    first_commit_err = Some(e);
                }
            }
        }

        // Append Completed regardless of whether every commit
        // succeeded — we've done everything we intend to do, and
        // recovery resend-on-restart handles any peer that actually
        // missed the COMMIT.
        if let Some(log) = self.log {
            if let Err(e) = log.append(&crate::TxLogEntry::Completed { txid: txid.clone() }) {
                tracing::warn!(
                    txid = %txid,
                    error = %e,
                    "coordinator log write (Completed) failed",
                );
            }
        }

        match first_commit_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Route each command to the peer(s) that should own a copy.
    ///
    /// * `PutNode(n)` → owner_of(n.id) only.
    /// * `PutEdge(e)` → both owner_of(source) and owner_of(target), so
    ///   each endpoint's local adjacency is complete for its side of
    ///   the edge.
    /// * `DeleteEdge(id)` and `DetachDeleteNode(id)` fan out to every
    ///   peer — we don't know which peers hold ghost copies, and local
    ///   apply is idempotent for missing targets.
    /// * `Batch(inner)` flattens recursively.
    fn group_by_peer(&self, commands: Vec<GraphCommand>) -> HashMap<PeerId, Vec<GraphCommand>> {
        let cluster = self.routing.cluster();
        let all_peers: Vec<PeerId> = cluster.membership().peer_ids().collect();
        let mut groups: HashMap<PeerId, Vec<GraphCommand>> = HashMap::new();
        self.group_into(&mut groups, &all_peers, commands);
        groups
    }

    fn group_into(
        &self,
        groups: &mut HashMap<PeerId, Vec<GraphCommand>>,
        all_peers: &[PeerId],
        commands: Vec<GraphCommand>,
    ) {
        let cluster = self.routing.cluster();
        for cmd in commands {
            match cmd {
                GraphCommand::PutNode(n) => {
                    let owner = cluster.owner_of(n.id);
                    groups
                        .entry(owner)
                        .or_default()
                        .push(GraphCommand::PutNode(n));
                }
                GraphCommand::PutEdge(e) => {
                    let src_owner = cluster.owner_of(e.source);
                    let dst_owner = cluster.owner_of(e.target);
                    // Same-owner edge: one copy. Cross-partition edge:
                    // copy to both owners so reverse traversal works.
                    if src_owner == dst_owner {
                        groups
                            .entry(src_owner)
                            .or_default()
                            .push(GraphCommand::PutEdge(e));
                    } else {
                        groups
                            .entry(src_owner)
                            .or_default()
                            .push(GraphCommand::PutEdge(e.clone()));
                        groups
                            .entry(dst_owner)
                            .or_default()
                            .push(GraphCommand::PutEdge(e));
                    }
                }
                GraphCommand::DeleteEdge(id) => {
                    for p in all_peers {
                        groups
                            .entry(*p)
                            .or_default()
                            .push(GraphCommand::DeleteEdge(id));
                    }
                }
                GraphCommand::DetachDeleteNode(id) => {
                    for p in all_peers {
                        groups
                            .entry(*p)
                            .or_default()
                            .push(GraphCommand::DetachDeleteNode(id));
                    }
                }
                GraphCommand::Batch(inner) => {
                    self.group_into(groups, all_peers, inner);
                }
                // DDL is split out of the command list by
                // `split_ddl` in `commit_buffered_commands` before
                // it reaches the coordinator, so seeing one here is
                // an internal bug rather than user-visible state.
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
                | GraphCommand::DropTrigger { .. } => {
                    debug_assert!(
                        false,
                        "DDL command reached the routing coordinator; \
                         it should have been split out by split_ddl"
                    );
                }
            }
        }
    }

    async fn prepare(&self, peer: PeerId, txid: &str, cmds: &[GraphCommand]) -> Result<(), Status> {
        if peer == self.routing.cluster().self_id() {
            // Local shortcut: stage directly into the service's
            // participant staging. Matches the remote
            // BatchWrite(PREPARE) semantics, including the
            // already-exists error on duplicate txid. Purely synchronous,
            // so no timeout wrapper.
            self.local_pending
                .try_insert(txid.to_string(), cmds.to_vec())
                .map_err(|()| {
                    Status::already_exists(format!("txid {} already prepared locally", txid))
                })?;
            return Ok(());
        }

        let mut client = self.write_client(peer)?;
        let payload = serde_json::to_vec(&cmds.to_vec())
            .map_err(|e| Status::internal(format!("encode PREPARE payload: {e}")))?;
        let call = client.batch_write(BatchWriteRequest {
            txid: txid.to_string(),
            phase: BatchPhase::Prepare as i32,
            commands_json: payload,
        });
        match tokio::time::timeout(self.timeouts.prepare, call).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Status::deadline_exceeded(format!(
                "PREPARE on peer {peer} timed out after {:?}",
                self.timeouts.prepare
            ))),
        }
    }

    async fn commit(&self, peer: PeerId, txid: &str) -> Result<(), Status> {
        if peer == self.routing.cluster().self_id() {
            let cmds = self.local_pending.take(txid).ok_or_else(|| {
                Status::failed_precondition(format!("txid {} not prepared locally", txid))
            })?;
            crate::server::apply_prepared_batch(self.local_store, &cmds)
                .map_err(|e| Status::internal(e.to_string()))?;
            return Ok(());
        }

        let mut client = self.write_client(peer)?;
        let call = client.batch_write(BatchWriteRequest {
            txid: txid.to_string(),
            phase: BatchPhase::Commit as i32,
            commands_json: Vec::new(),
        });
        match tokio::time::timeout(self.timeouts.commit, call).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Status::deadline_exceeded(format!(
                "COMMIT on peer {peer} timed out after {:?}",
                self.timeouts.commit
            ))),
        }
    }

    async fn abort(&self, peer: PeerId, txid: &str) -> Result<(), Status> {
        if peer == self.routing.cluster().self_id() {
            let _ = self.local_pending.take(txid);
            return Ok(());
        }
        let mut client = self.write_client(peer)?;
        let call = client.batch_write(BatchWriteRequest {
            txid: txid.to_string(),
            phase: BatchPhase::Abort as i32,
            commands_json: Vec::new(),
        });
        match tokio::time::timeout(self.timeouts.abort, call).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Status::deadline_exceeded(format!(
                "ABORT on peer {peer} timed out after {:?}",
                self.timeouts.abort
            ))),
        }
    }

    fn write_client(&self, peer: PeerId) -> Result<MeshWriteClient<Channel>, Status> {
        self.routing
            .write_client(peer)
            .ok_or_else(|| Status::internal(format!("no client registered for peer {}", peer)))
    }
}
