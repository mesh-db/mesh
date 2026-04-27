//! Failure-injection integration tests for the routing-mode 2PC path.
//!
//! Each test exercises a distinct failure branch:
//! - coordinator restart after a crash pre-decision
//!   (`recover_pending_transactions`, abort branch)
//! - coordinator restart after a crash post-`CommitDecision`
//!   (`recover_pending_transactions`, resume-commit branch)
//! - participant restart after a rejected COMMIT
//!   (`recover_participant_staging` +
//!   `recover_participant_decisions` via the `ResolveTransaction` RPC)
//! - synchronous rollback when a peer rejects PREPARE outright
//!   (the in-`run` rollback path — no restart involved)
//!
//! Deterministic injection is done via the cfg-gated
//! [`meshdb_rpc::FaultPoints`] struct — a handle the test harness hangs
//! off each peer's [`MeshService`] and flips at precise points in the
//! write path. Zero code runs in release builds; the whole surface
//! compiles away unless the `meshdb-rpc/fault-inject` feature is on,
//! which this crate's dev-dependency row activates automatically.

use meshdb_rpc::proto::mesh_query_client::MeshQueryClient;
use meshdb_rpc::proto::ExecuteCypherRequest;
use meshdb_rpc::proto::NodesByLabelRequest;
use meshdb_rpc::{FaultPoints, ParticipantLog, ParticipantLogEntry};
use meshdb_server::config::{ClusterMode, PeerConfig, ServerConfig};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// One peer under test. Owns the temp dir, the live gRPC listener task,
/// and the shared `FaultPoints` handle the test flips to drive recovery
/// branches. Restart flows shut down the inner `shutdown` oneshot, wait
/// for `join`, then rebind the same port + reopen the same data dir.
struct TestPeer {
    addr: SocketAddr,
    config: ServerConfig,
    fault_points: Arc<FaultPoints>,
    _dir: Arc<TempDir>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl TestPeer {
    fn grpc_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn shutdown_and_wait(mut self) -> (SocketAddr, ServerConfig, Arc<TempDir>) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
        // Give the runtime a moment to drop the service / store Arcs
        // so the data_dir lock is freed before a same-dir rebind.
        tokio::time::sleep(Duration::from_millis(200)).await;
        (self.addr, self.config, self._dir)
    }
}

fn make_peer_configs(addr_a: SocketAddr, addr_b: SocketAddr) -> Vec<PeerConfig> {
    vec![
        PeerConfig {
            id: 1,
            address: addr_a.to_string(),
            bolt_address: None,
            weight: None,
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
            bolt_address: None,
            weight: None,
        },
    ]
}

fn make_server_config(
    self_id: u64,
    addr: SocketAddr,
    data_dir: &Path,
    peers: Vec<PeerConfig>,
) -> ServerConfig {
    ServerConfig {
        self_id,
        listen_address: addr.to_string(),
        data_dir: data_dir.to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
        bolt_address: None,
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: Some(ClusterMode::Routing),
        replication_factor: None,
        read_consistency: None,
        cluster_auth: None,
        routing_ttl_seconds: None,
        shutdown_drain_timeout_seconds: None,
        query_timeout_seconds: None,
        query_max_rows: None,
        max_concurrent_queries: None,
        tracing: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
    }
}

/// Cypher statement that creates 20 `:Probe` nodes in one tx. 2^20 =
/// ~1M possible hash outcomes, so the probability of all 20 id-UUIDs
/// landing on the same partition owner is ≈ 2e-6 — effectively
/// deterministic for the cross-partition fanout these tests need.
fn twenty_probe_nodes_cypher() -> String {
    let parts: Vec<String> = (0..20)
        .map(|i| format!("(n{i}:Probe {{idx: {i}}})"))
        .collect();
    format!("CREATE {}", parts.join(", "))
}

/// Local-only Probe-node count against `addr`. Hits the direct
/// `nodes_by_label` gRPC with `local_only = true`, bypassing the
/// partitioned reader's scatter-gather and dedup so per-peer counts
/// reflect exactly what that peer's store holds. Summing across both
/// peers gives the cluster total.
async fn count_probes_local(addr: String) -> usize {
    let mut q = MeshQueryClient::connect(addr).await.unwrap();
    let resp = q
        .nodes_by_label(NodesByLabelRequest {
            label: "Probe".into(),
            local_only: true,
            linearizable: false,
        })
        .await
        .unwrap();
    resp.into_inner().ids.len()
}

async fn spawn_peer(
    config: ServerConfig,
    listener: TcpListener,
    fault_points: Arc<FaultPoints>,
    data_dir: Arc<TempDir>,
) -> TestPeer {
    let addr = listener.local_addr().unwrap();
    // Routing mode is synchronous to build — no Raft bootstrap — so we
    // can lean on the sync `build_service` entry point and just plug
    // `FaultPoints` in before handing the service to tonic.
    let service = meshdb_server::build_service(&config)
        .unwrap()
        .with_fault_points(Some(fault_points.clone()));

    // Rehydrate 2PC participant + coordinator state BEFORE the
    // listener accepts traffic — same ordering as `meshdb_server::serve`
    // in the real binary. Without this, recovery races with inbound
    // RPCs on the restart path of Tests A and B.
    service
        .recover_participant_staging()
        .expect("recover participant staging");
    service
        .recover_participant_decisions()
        .await
        .expect("recover participant decisions");
    service
        .recover_pending_transactions()
        .await
        .expect("recover pending transactions");

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        let shutdown = async {
            let _ = shutdown_rx.await;
        };
        Server::builder()
            .add_service(service.clone().into_query_server())
            .add_service(service.into_write_server())
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
            .await
            .unwrap();
    });

    // Let the server bind before the caller starts issuing RPCs.
    tokio::time::sleep(Duration::from_millis(50)).await;

    TestPeer {
        addr,
        config,
        fault_points,
        _dir: data_dir,
        shutdown: Some(shutdown_tx),
        join: Some(join),
    }
}

/// Rebind the configured address with a bounded retry. Matches the
/// pattern in `integration.rs`'s peer-restart test — the previous
/// listener's socket sits in the kernel's release path for a moment
/// after the server task stops.
async fn rebind(addr: SocketAddr) -> TcpListener {
    let mut last_err: Option<std::io::Error> = None;
    for _ in 0..20 {
        match TcpListener::bind(addr).await {
            Ok(l) => return l,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    panic!("could not rebind {addr} after 20 retries: {:?}", last_err);
}

fn read_participant_log(data_dir: &Path) -> Vec<ParticipantLogEntry> {
    let path = meshdb_server::participant_log_path(data_dir);
    if !path.exists() {
        return Vec::new();
    }
    let log = ParticipantLog::open(path).expect("open participant log");
    log.read_all().expect("read participant log")
}

async fn spawn_two_peer_cluster() -> (TestPeer, TestPeer) {
    let dir_a = Arc::new(TempDir::new().unwrap());
    let dir_b = Arc::new(TempDir::new().unwrap());
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let peers = make_peer_configs(addr_a, addr_b);
    let config_a = make_server_config(1, addr_a, dir_a.path(), peers.clone());
    let config_b = make_server_config(2, addr_b, dir_b.path(), peers);

    let fp_a = Arc::new(FaultPoints::new());
    let fp_b = Arc::new(FaultPoints::new());

    let peer_a = spawn_peer(config_a, listener_a, fp_a, dir_a).await;
    let peer_b = spawn_peer(config_b, listener_b, fp_b, dir_b).await;

    // Give both peers a beat for their routing tables to connect.
    tokio::time::sleep(Duration::from_millis(80)).await;

    (peer_a, peer_b)
}

#[tokio::test]
async fn two_pc_coordinator_crash_before_decision_aborts_on_restart() {
    // Scenario: coordinator (peer A) fsyncs the `Prepared` log entry,
    // the injected fault fires before a single PREPARE RPC goes out,
    // so peer B never hears about the tx. On A's restart,
    // `recover_pending_transactions` sees the `Prepared` entry with no
    // decision and fans out ABORT — which peer B logs as `Aborted`
    // per the participant handler's idempotency rule.
    //
    // Verifies the "no decision → abort by default" recovery branch.

    let (peer_a, peer_b) = spawn_two_peer_cluster().await;

    peer_a
        .fault_points
        .crash_after_prepare_log
        .store(true, Ordering::SeqCst);

    // 20 Probe nodes in one Cypher CREATE — essentially guaranteed to
    // include at least one node whose partition owner is peer B, so
    // `group_by_peer` puts B into the log's `Prepared { groups: ... }`
    // record. Without B in the groups, recovery has no one to send
    // ABORT to.
    let mut q_a = MeshQueryClient::connect(peer_a.grpc_url()).await.unwrap();
    let err = q_a
        .execute_cypher(ExecuteCypherRequest {
            query: twenty_probe_nodes_cypher(),
            params_json: vec![],
        })
        .await
        .expect_err("fault should surface as an error");
    assert!(
        err.message().contains("crash_after_prepare_log"),
        "unexpected error message: {}",
        err.message()
    );

    // Fault fired between the Prepared log append and any PREPARE RPC,
    // so peer B never staged anything — its participant log is empty.
    assert!(
        read_participant_log(peer_b.config.data_dir.as_path()).is_empty(),
        "peer B has unexpected participant-log entries after a crash-before-PREPARE fault",
    );
    // And nothing was applied on either peer.
    assert_eq!(count_probes_local(peer_a.grpc_url()).await, 0);
    assert_eq!(count_probes_local(peer_b.grpc_url()).await, 0);

    // Clear the flag so the restarted A can actually complete recovery.
    peer_a
        .fault_points
        .crash_after_prepare_log
        .store(false, Ordering::SeqCst);

    // Shutdown A, restart against the same data_dir + address.
    let (addr_a, config_a, dir_a) = peer_a.shutdown_and_wait().await;
    let listener_a2 = rebind(addr_a).await;
    let fp_a2 = Arc::new(FaultPoints::new());
    let peer_a2 = spawn_peer(config_a, listener_a2, fp_a2, dir_a).await;

    // Recovery ran synchronously inside `spawn_peer` before the
    // listener accepted traffic, so by now B should have received an
    // ABORT and logged it.
    let b_entries = read_participant_log(peer_b.config.data_dir.as_path());
    assert!(
        b_entries
            .iter()
            .any(|e| matches!(e, ParticipantLogEntry::Aborted { .. })),
        "peer B didn't log Aborted after coordinator recovery: {:?}",
        b_entries,
    );

    // Post-recovery the cluster is still empty — we verified the
    // abort path, nothing got committed along the way.
    assert_eq!(count_probes_local(peer_a2.grpc_url()).await, 0);
    assert_eq!(count_probes_local(peer_b.grpc_url()).await, 0);
}

#[tokio::test]
async fn two_pc_coordinator_crash_after_commit_decision_resumes_commit_idempotently() {
    // Scenario: both peers successfully PREPARE, coordinator logs
    // `CommitDecision`, sends COMMIT to the first peer (which
    // applies), then crashes before the second COMMIT. On restart,
    // `recover_pending_transactions` sees `CommitDecision` with no
    // `Completed` and drives both commits forward — the already-
    // committed peer short-circuits idempotently via its terminal-
    // outcome cache, the missing peer applies fresh.
    //
    // Verifies the "decision = Commit, not completed → resume commit"
    // recovery branch plus COMMIT idempotency.

    let (peer_a, peer_b) = spawn_two_peer_cluster().await;

    // Crash after the first COMMIT send. HashMap iteration order for
    // the per-peer fanout isn't deterministic, so either A or B may
    // be the "first" peer — the test tolerates both.
    peer_a
        .fault_points
        .crash_after_kth_commit_rpc
        .store(1, Ordering::SeqCst);

    // A 20-node Cypher CREATE is essentially guaranteed to span both
    // partitions (p(all-same-peer) ≈ 2 * (1/2)^20 ≈ 2e-6), which is
    // what we need to put at least one command in each peer's group
    // so the COMMIT fanout actually has two iterations.
    let mut q_a = MeshQueryClient::connect(peer_a.grpc_url()).await.unwrap();
    let err = q_a
        .execute_cypher(ExecuteCypherRequest {
            query: twenty_probe_nodes_cypher(),
            params_json: vec![],
        })
        .await
        .expect_err("fault should surface as an error");
    assert!(
        err.message().contains("crash_after_kth_commit_rpc")
            || err.message().contains("injected fault"),
        "unexpected error: {}",
        err.message()
    );

    // Pre-restart: exactly one of the two peers has applied its
    // portion (crash_after_kth_commit_rpc = 1 means the 2nd COMMIT
    // never fired). Sum across peers since a label scan doesn't
    // scatter-gather; total should be non-zero (one peer applied)
    // and less than 20 (the other didn't).
    let pre_a = count_probes_local(peer_a.grpc_url()).await;
    let pre_b = count_probes_local(peer_b.grpc_url()).await;
    let pre_total = pre_a + pre_b;
    assert!(
        pre_total > 0 && pre_total < 20,
        "expected partial commit (1..20), got {pre_total} (a={pre_a}, b={pre_b})",
    );

    // Clear the crash flag so restart recovery can actually finish.
    peer_a
        .fault_points
        .crash_after_kth_commit_rpc
        .store(-1, Ordering::SeqCst);

    let (addr_a, config_a, dir_a) = peer_a.shutdown_and_wait().await;
    let listener_a2 = rebind(addr_a).await;
    let fp_a2 = Arc::new(FaultPoints::new());
    let peer_a2 = spawn_peer(config_a, listener_a2, fp_a2, dir_a).await;

    // After recovery, the cluster's total must be 20 — both peers
    // got their COMMIT, whether freshly or idempotently.
    let post_a = count_probes_local(peer_a2.grpc_url()).await;
    let post_b = count_probes_local(peer_b.grpc_url()).await;
    assert_eq!(
        post_a + post_b,
        20,
        "post-recovery cluster missing nodes (a={post_a}, b={post_b})",
    );

    // Idempotency check: neither participant log should have more
    // than one Committed entry for any given txid.
    for dir in [
        peer_a2.config.data_dir.as_path(),
        peer_b.config.data_dir.as_path(),
    ] {
        let entries = read_participant_log(dir);
        let mut per_txid: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
        for e in &entries {
            if let ParticipantLogEntry::Committed { txid } = e {
                *per_txid.entry(txid.as_str()).or_default() += 1;
            }
        }
        for (txid, n) in &per_txid {
            assert_eq!(
                *n, 1,
                "participant log at {:?} has {n} Committed entries for txid {txid} — not idempotent",
                dir,
            );
        }
    }
}

#[tokio::test]
async fn two_pc_participant_crash_after_prepare_ack_recovers_via_resolve_transaction() {
    // Scenario: participant (peer B) fsyncs `Prepared`, ACKs PREPARE,
    // then rejects the inbound COMMIT via `reject_commit_before_apply`
    // and is shut down. The coordinator (A) keeps running — its log
    // ends with `CommitDecision` + `Completed`, so A's
    // `resolve_transaction` RPC can report the decision to B. On B's
    // restart, `recover_participant_staging` rehydrates the Prepared
    // batch and `recover_participant_decisions` polls A, learns the
    // commit, and applies B's portion of the tx.
    //
    // Verifies the `ResolveTransaction` cross-peer recovery path —
    // this is the only test that exercises that RPC.

    let (peer_a, peer_b) = spawn_two_peer_cluster().await;

    peer_b
        .fault_points
        .reject_commit_before_apply
        .store(true, Ordering::SeqCst);

    // Same 20-node CREATE pattern as Tests A and B — guarantees at
    // least one node lands on peer B, so the tx hits B's COMMIT
    // handler (where the fault lives).
    let mut q_a = MeshQueryClient::connect(peer_a.grpc_url()).await.unwrap();
    let err = q_a
        .execute_cypher(ExecuteCypherRequest {
            query: twenty_probe_nodes_cypher(),
            params_json: vec![],
        })
        .await
        .expect_err("fault on B should surface as a coordinator-side error");
    assert!(
        err.message().contains("reject_commit_before_apply")
            || err.message().contains("injected fault"),
        "unexpected error: {}",
        err.message()
    );

    // B should have a Prepared entry but no Committed / Aborted yet.
    let pre_entries = read_participant_log(peer_b.config.data_dir.as_path());
    assert!(
        pre_entries
            .iter()
            .any(|e| matches!(e, ParticipantLogEntry::Prepared { .. })),
        "peer B didn't log Prepared before the reject_commit fault: {:?}",
        pre_entries,
    );
    assert!(
        !pre_entries
            .iter()
            .any(|e| matches!(e, ParticipantLogEntry::Committed { .. })),
        "peer B logged Committed despite the reject_commit fault: {:?}",
        pre_entries,
    );

    // Clear B's flag so the restarted B can finish the apply.
    peer_b
        .fault_points
        .reject_commit_before_apply
        .store(false, Ordering::SeqCst);

    // Shutdown B, restart against the same data_dir + address.
    let pre_resolve_count = peer_a
        .fault_points
        .resolve_transaction_call_count
        .load(Ordering::SeqCst);
    let (addr_b, config_b, dir_b) = peer_b.shutdown_and_wait().await;
    let listener_b2 = rebind(addr_b).await;
    let fp_b2 = Arc::new(FaultPoints::new());
    let peer_b2 = spawn_peer(config_b, listener_b2, fp_b2, dir_b).await;

    // After recovery, the cluster's total should be 20 — peer A
    // committed its portion synchronously (the commit fault was on B
    // only), and peer B's portion comes in through the
    // ResolveTransaction-driven recovery apply.
    let post_a = count_probes_local(peer_a.grpc_url()).await;
    let post_b = count_probes_local(peer_b2.grpc_url()).await;
    assert!(
        post_b > 0,
        "peer B applied nothing during recovery (a={post_a}, b={post_b})",
    );
    assert_eq!(
        post_a + post_b,
        20,
        "post-recovery cluster total mismatch (a={post_a}, b={post_b})",
    );

    let post_resolve_count = peer_a
        .fault_points
        .resolve_transaction_call_count
        .load(Ordering::SeqCst);

    // B's participant log should now end with Committed for the txid
    // that was stuck in-doubt.
    let post_entries = read_participant_log(peer_b2.config.data_dir.as_path());
    assert!(
        post_entries
            .iter()
            .any(|e| matches!(e, ParticipantLogEntry::Committed { .. })),
        "peer B didn't log Committed after recovery: {:?}",
        post_entries,
    );

    // The ResolveTransaction RPC on peer A must have been bumped at
    // least once by B's `recover_participant_decisions` loop — that
    // is the whole point of this test.
    assert!(
        post_resolve_count > pre_resolve_count,
        "ResolveTransaction was never called on peer A during B's recovery (before: {pre_resolve_count}, after: {post_resolve_count})",
    );
}

#[tokio::test]
async fn two_pc_prepare_reject_rolls_back_prepared_peers_cleanly() {
    // Scenario: peer B refuses PREPARE outright. The coordinator
    // (A) must:
    //   1. Stop the PREPARE loop on the first failure.
    //   2. Log `AbortDecision` before attempting rollback.
    //   3. Send ABORT to every peer already in the `prepared` set
    //      (i.e. whichever peers' PREPARE the coordinator had
    //      already collected — at most one in a two-peer cluster).
    //   4. Log `Completed` and surface B's PREPARE error to the
    //      client.
    //
    // Verifies the in-`run` rollback path — distinct from the
    // restart-driven recovery paths exercised by Tests A / B / C.
    // No restart is needed here: the rollback is synchronous and
    // must leave the cluster clean.

    let (peer_a, peer_b) = spawn_two_peer_cluster().await;

    peer_b
        .fault_points
        .reject_prepare
        .store(true, Ordering::SeqCst);

    let mut q_a = MeshQueryClient::connect(peer_a.grpc_url()).await.unwrap();
    let err = q_a
        .execute_cypher(ExecuteCypherRequest {
            query: twenty_probe_nodes_cypher(),
            params_json: vec![],
        })
        .await
        .expect_err("B's PREPARE rejection must surface as a client error");
    assert!(
        err.message().contains("reject_prepare") || err.message().contains("injected fault"),
        "unexpected error: {}",
        err.message()
    );

    // Rollback drained any per-peer staging — nothing should have
    // been applied on either peer.
    assert_eq!(count_probes_local(peer_a.grpc_url()).await, 0);
    assert_eq!(count_probes_local(peer_b.grpc_url()).await, 0);

    // B refused PREPARE before staging or logging, so its
    // participant log must not contain a `Prepared` entry for the
    // rolled-back txid. (It may pick up an `Aborted` entry if the
    // coordinator fanned ABORT to B too, which is fine — the
    // invariant is "never Prepared without a matching Committed
    // or Aborted later.")
    let b_entries = read_participant_log(peer_b.config.data_dir.as_path());
    assert!(
        !b_entries
            .iter()
            .any(|e| matches!(e, ParticipantLogEntry::Prepared { .. })),
        "peer B logged Prepared despite rejecting PREPARE: {:?}",
        b_entries,
    );

    // After the rollback, a fresh write must succeed — the service
    // didn't get wedged by the abort path. Clear the fault, issue
    // a second CREATE, and confirm it lands on both peers.
    peer_b
        .fault_points
        .reject_prepare
        .store(false, Ordering::SeqCst);
    q_a.execute_cypher(ExecuteCypherRequest {
        query: twenty_probe_nodes_cypher(),
        params_json: vec![],
    })
    .await
    .expect("post-rollback write should succeed");
    let post_a = count_probes_local(peer_a.grpc_url()).await;
    let post_b = count_probes_local(peer_b.grpc_url()).await;
    assert_eq!(
        post_a + post_b,
        20,
        "post-rollback write didn't fully land (a={post_a}, b={post_b})",
    );
}
