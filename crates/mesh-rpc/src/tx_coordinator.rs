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
use mesh_cluster::{GraphCommand, PeerId};
use mesh_storage::Store;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::transport::Channel;
use tonic::Status;
use uuid::Uuid;

type PendingBatches = Arc<Mutex<HashMap<String, Vec<GraphCommand>>>>;

pub(crate) struct TxCoordinator<'a> {
    local_store: &'a Store,
    local_pending: &'a PendingBatches,
    routing: &'a Routing,
}

impl<'a> TxCoordinator<'a> {
    pub fn new(
        local_store: &'a Store,
        local_pending: &'a PendingBatches,
        routing: &'a Routing,
    ) -> Self {
        Self {
            local_store,
            local_pending,
            routing,
        }
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

        // PREPARE phase. Track which peers ack'd so we can target ABORT
        // precisely if one of the later PREPAREs fails.
        let mut prepared: Vec<PeerId> = Vec::new();
        for (peer_id, cmds) in &groups {
            match self.prepare(*peer_id, &txid, cmds).await {
                Ok(()) => prepared.push(*peer_id),
                Err(prepare_err) => {
                    // Best-effort ABORT of the peers that did prepare.
                    // Abort failures are logged and swallowed — the
                    // original PREPARE error is what we surface to the
                    // caller, since it's the root cause.
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
                    return Err(prepare_err);
                }
            }
        }

        // COMMIT phase. A COMMIT failure is harder to recover from
        // (some peers may already have applied), but we still try to
        // commit every peer so the transaction makes maximum progress.
        // The first commit error is returned to the caller.
        let mut first_commit_err: Option<Status> = None;
        for peer_id in &prepared {
            if let Err(e) = self.commit(*peer_id, &txid).await {
                if first_commit_err.is_none() {
                    first_commit_err = Some(e);
                }
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
            }
        }
    }

    async fn prepare(&self, peer: PeerId, txid: &str, cmds: &[GraphCommand]) -> Result<(), Status> {
        if peer == self.routing.cluster().self_id() {
            // Local shortcut: stage directly into the service's pending
            // map. Matches the remote BatchWrite(PREPARE) semantics.
            let mut pending = self.local_pending.lock().unwrap();
            if pending.contains_key(txid) {
                return Err(Status::already_exists(format!(
                    "txid {} already prepared locally",
                    txid
                )));
            }
            pending.insert(txid.to_string(), cmds.to_vec());
            return Ok(());
        }

        let mut client = self.write_client(peer)?;
        let payload = serde_json::to_vec(&cmds.to_vec())
            .map_err(|e| Status::internal(format!("encode PREPARE payload: {e}")))?;
        client
            .batch_write(BatchWriteRequest {
                txid: txid.to_string(),
                phase: BatchPhase::Prepare as i32,
                commands_json: payload,
            })
            .await?;
        Ok(())
    }

    async fn commit(&self, peer: PeerId, txid: &str) -> Result<(), Status> {
        if peer == self.routing.cluster().self_id() {
            let cmds = {
                let mut pending = self.local_pending.lock().unwrap();
                pending.remove(txid).ok_or_else(|| {
                    Status::failed_precondition(format!("txid {} not prepared locally", txid))
                })?
            };
            crate::server::apply_prepared_batch(self.local_store, &cmds)
                .map_err(|e| Status::internal(e.to_string()))?;
            return Ok(());
        }

        let mut client = self.write_client(peer)?;
        client
            .batch_write(BatchWriteRequest {
                txid: txid.to_string(),
                phase: BatchPhase::Commit as i32,
                commands_json: Vec::new(),
            })
            .await?;
        Ok(())
    }

    async fn abort(&self, peer: PeerId, txid: &str) -> Result<(), Status> {
        if peer == self.routing.cluster().self_id() {
            let _ = self.local_pending.lock().unwrap().remove(txid);
            return Ok(());
        }
        let mut client = self.write_client(peer)?;
        client
            .batch_write(BatchWriteRequest {
                txid: txid.to_string(),
                phase: BatchPhase::Abort as i32,
                commands_json: Vec::new(),
            })
            .await?;
        Ok(())
    }

    fn write_client(&self, peer: PeerId) -> Result<MeshWriteClient<Channel>, Status> {
        self.routing
            .write_client(peer)
            .ok_or_else(|| Status::internal(format!("no client registered for peer {}", peer)))
    }
}
