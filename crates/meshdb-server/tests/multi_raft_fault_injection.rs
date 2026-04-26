//! Failure-injection integration tests for the multi-raft path.
//!
//! Each test exercises a distinct recovery branch:
//! - Coordinator crash between PREPARE-acks and CommitDecision log
//!   (`multi_raft_crash_after_first_prepared_tx`). On restart,
//!   `recover_multi_raft_in_doubt` reads `Prepared` with no decision
//!   in the coordinator log and aborts every prepared partition via
//!   `AbortTx`.
//! - Coordinator crash post-CommitDecision, before any CommitTx
//!   propose (`multi_raft_crash_after_commit_decision`). On restart,
//!   recovery polls `ResolveTransaction` across peers, finds the
//!   CommitDecision in this peer's own log, and proposes `CommitTx`
//!   through every prepared partition's Raft to converge.
//! - Forward-write proxy rejection
//!   (`multi_raft_reject_forward_write`). The caller refreshes its
//!   leader cache and retries; the second attempt succeeds against
//!   the actual leader.
//!
//! These tests require multi-raft mode plus the `fault-inject`
//! feature on `meshdb-rpc`. The dev-dependency row in this crate's
//! Cargo.toml already activates it.

use meshdb_cluster::PartitionId;
use meshdb_rpc::{FaultPoints, MeshService};
use meshdb_server::config::{ClusterMode, PeerConfig, ServerConfig};
use meshdb_server::{build_components, initialize_multi_raft_if_seed};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Multi-raft test peer. Each peer carries its own `FaultPoints`
/// handle that the test flips between writes to drive specific
/// recovery branches.
struct McPeer {
    addr: SocketAddr,
    config: ServerConfig,
    multi_raft: Arc<meshdb_rpc::MultiRaftCluster>,
    service: MeshService,
    fault_points: Arc<FaultPoints>,
    _dir: Arc<TempDir>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl McPeer {
    async fn shutdown_and_wait(mut self) -> SuspendedPeer {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
        // Drop the service / multi-raft / fault-point Arcs BEFORE
        // the settle sleep — they hold the only remaining references
        // to the rocksdb instances at `data_dir`. Without this
        // ordering, the rebind below races against the LOCK file.
        let McPeer {
            addr,
            config,
            _dir,
            multi_raft,
            service,
            fault_points,
            shutdown: _,
            join: _,
        } = self;
        drop(multi_raft);
        drop(service);
        drop(fault_points);
        tokio::time::sleep(Duration::from_millis(200)).await;
        SuspendedPeer {
            addr,
            config,
            dir: _dir,
        }
    }
}

/// State carried across a peer restart — preserves the data_dir and
/// listen address so the rebind picks up the same on-disk state.
struct SuspendedPeer {
    addr: SocketAddr,
    config: ServerConfig,
    dir: Arc<TempDir>,
}

async fn rebind(addr: SocketAddr) -> TcpListener {
    let mut last_err: Option<std::io::Error> = None;
    for _ in 0..40 {
        match TcpListener::bind(addr).await {
            Ok(l) => return l,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    panic!("could not rebind {addr} after 40 retries: {last_err:?}");
}

/// Build a single peer's components, attach a `FaultPoints` handle,
/// run multi-raft initialize + in-doubt recovery, then spawn the
/// gRPC listener. Returns once the server has bound the address.
async fn spawn_peer(
    config: ServerConfig,
    listener: TcpListener,
    fault_points: Arc<FaultPoints>,
    dir: Arc<TempDir>,
) -> McPeer {
    let addr = listener.local_addr().unwrap();
    let components = build_components(&config).await.unwrap();
    let multi_raft = components.multi_raft.clone().expect("multi-raft built");
    let service = components
        .service
        .clone()
        .with_fault_points(Some(fault_points.clone()));

    // Run multi-raft init (no-op on a non-fresh data_dir; openraft
    // returns NotAllowed which initialize_multi_raft_if_seed
    // tolerates).
    initialize_multi_raft_if_seed(&config, &multi_raft)
        .await
        .unwrap();

    // Build a fresh MeshRaftService from the multi-raft registry —
    // ServerComponents's raft_service was already consumed.
    let registry = multi_raft.build_registry();
    let raft_service = meshdb_rpc::MeshRaftService::with_registry(registry);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let service_for_server = service.clone();
    let join = tokio::spawn(async move {
        let shutdown = async {
            let _ = shutdown_rx.await;
        };
        Server::builder()
            .add_service(raft_service.into_server())
            .add_service(service_for_server.clone().into_query_server())
            .add_service(service_for_server.into_write_server())
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
            .await
            .ok();
    });

    // Let the server bind before any RPCs land.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Recover any in-doubt PREPAREs from a prior crash. Runs AFTER
    // the listener is up so the resolve-transaction polls can reach
    // every peer (including this one, which the resolve loop calls
    // along with the others).
    let _ = service.recover_multi_raft_in_doubt().await;

    McPeer {
        addr,
        config,
        multi_raft,
        service,
        fault_points,
        _dir: dir,
        shutdown: Some(shutdown_tx),
        join: Some(join),
    }
}

/// Spawn a 3-peer multi-raft cluster with fresh fault-point handles.
/// 3 peers + rf=3 means every peer is a replica of every partition,
/// so the cluster keeps a quorum even after one peer is shut down.
async fn spawn_three_peer_multi_raft(num_partitions: u32) -> Vec<McPeer> {
    let mut listeners: Vec<TcpListener> = Vec::with_capacity(3);
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(3);
    for _ in 0..3 {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(l.local_addr().unwrap());
        listeners.push(l);
    }

    let peer_configs: Vec<PeerConfig> = addrs
        .iter()
        .enumerate()
        .map(|(i, a)| PeerConfig {
            id: (i + 1) as u64,
            address: a.to_string(),
            bolt_address: None,
        })
        .collect();

    let mut peers = Vec::with_capacity(3);
    for (i, listener) in listeners.into_iter().enumerate() {
        let dir = Arc::new(TempDir::new().unwrap());
        let cfg = make_config(
            (i + 1) as u64,
            addrs[i],
            dir.path(),
            peer_configs.clone(),
            num_partitions,
            i == 0,
        );
        let fp = Arc::new(FaultPoints::new());
        let peer = spawn_peer(cfg, listener, fp, dir).await;
        peers.push(peer);
    }

    // Wait for every group on every peer to settle on a leader. The
    // tests below rely on `multi_raft.is_local_leader(p)` being
    // accurate.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let all = peers.iter().all(|p| {
            p.multi_raft.meta.current_leader().is_some()
                && p.multi_raft
                    .partitions
                    .values()
                    .all(|r| r.current_leader().is_some())
        });
        if all {
            break;
        }
        if Instant::now() > deadline {
            panic!("multi-raft cluster failed to elect leaders within 10s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    peers
}

fn make_config(
    self_id: u64,
    addr: SocketAddr,
    data_dir: &Path,
    peers: Vec<PeerConfig>,
    num_partitions: u32,
    bootstrap: bool,
) -> ServerConfig {
    ServerConfig {
        self_id,
        listen_address: addr.to_string(),
        data_dir: data_dir.to_path_buf(),
        num_partitions,
        peers,
        bootstrap,
        bolt_address: None,
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: Some(ClusterMode::MultiRaft),
        replication_factor: Some(3),
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
    }
}

/// Cypher creating 20 nodes split across two label groups. With 20
/// random UUIDs across 4 partitions, the probability of every node
/// landing on a single partition is ≈ 4^-19, so this effectively
/// always exercises the cross-partition 2PC path.
fn cross_partition_cypher() -> String {
    let parts: Vec<String> = (0..10)
        .map(|i| format!("(:FaultA {{idx: {i}}})"))
        .chain((0..10).map(|i| format!("(:FaultB {{idx: {i}}})")))
        .collect();
    format!("CREATE {} RETURN 0", parts.join(", "))
}

async fn count_label(peer: &McPeer, label: &str) -> i64 {
    let q = format!("MATCH (n:{label}) RETURN count(n) AS c");
    let rows = peer
        .service
        .execute_cypher_local(q, std::collections::HashMap::new())
        .await
        .unwrap();
    match rows.first().and_then(|r| r.get("c")) {
        Some(meshdb_executor::Value::Property(meshdb_core::Property::Int64(c))) => *c,
        other => panic!("expected Int64 count, got {other:?}"),
    }
}

#[tokio::test]
async fn multi_raft_crash_after_first_prepared_tx_aborts_on_restart() {
    // Coordinator commits PREPARE through partition A's Raft, then
    // crashes before sending PREPARE to partition B. The coordinator
    // log has `Prepared` with no decision, so on restart the abort
    // branch fires — every replica's PartitionGraphApplier drops
    // the staged commands via `AbortTx` and no data lands.
    let mut peers = spawn_three_peer_multi_raft(4).await;

    // 20 random ids across 4 partitions — cross-partition path is
    // ~certain to fire (probability of all-same-partition is 4^-19).
    peers[0]
        .fault_points
        .multi_raft_crash_after_first_prepared_tx
        .store(true, Ordering::SeqCst);
    let res = peers[0]
        .service
        .execute_cypher_local(cross_partition_cypher(), std::collections::HashMap::new())
        .await;
    peers[0]
        .fault_points
        .multi_raft_crash_after_first_prepared_tx
        .store(false, Ordering::SeqCst);
    let err = res.expect_err("fault should have fired");
    assert!(
        err.message()
            .contains("multi_raft_crash_after_first_prepared_tx"),
        "unexpected error: {err}"
    );

    // Restart peer 0 — recovery should resolve the in-doubt PREPARE
    // by aborting (no decision in the coordinator log).
    let peer0 = peers.remove(0);
    let suspended = peer0.shutdown_and_wait().await;
    let listener = rebind(suspended.addr).await;
    let fp = Arc::new(FaultPoints::new());
    let peer0_restart = spawn_peer(suspended.config, listener, fp, suspended.dir).await;

    // Give recovery a moment to finish — `recover_multi_raft_in_doubt`
    // ran inline before serve() but the AbortTx propose is async and
    // followers' apply lags slightly behind the leader's commit.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Every peer should report zero of either label.
    for peer in [&peer0_restart, &peers[0], &peers[1]] {
        assert_eq!(
            count_label(peer, "FaultA").await,
            0,
            "peer {} sees a FaultA node — abort recovery missed it",
            peer.config.self_id
        );
        assert_eq!(
            count_label(peer, "FaultB").await,
            0,
            "peer {} sees a FaultB node — abort recovery missed it",
            peer.config.self_id
        );
    }
}

#[tokio::test]
async fn multi_raft_crash_after_commit_decision_commits_on_restart() {
    // Coordinator runs PREPARE successfully on every touched
    // partition, fsyncs `CommitDecision` to its log, then crashes
    // before sending the first CommitTx. On restart,
    // `recover_multi_raft_in_doubt` finds PreparedTx in pending_txs
    // for partitions this peer leads, polls `ResolveTransaction`
    // across peers (the local log has the decision), and proposes
    // CommitTx through each partition Raft to converge.
    let mut peers = spawn_three_peer_multi_raft(4).await;

    // 20-node cypher — cross-partition path always fires.
    peers[0]
        .fault_points
        .multi_raft_crash_after_commit_decision
        .store(true, Ordering::SeqCst);
    let res = peers[0]
        .service
        .execute_cypher_local(cross_partition_cypher(), std::collections::HashMap::new())
        .await;
    peers[0]
        .fault_points
        .multi_raft_crash_after_commit_decision
        .store(false, Ordering::SeqCst);
    let err = res.expect_err("fault should have fired");
    assert!(
        err.message()
            .contains("multi_raft_crash_after_commit_decision"),
        "unexpected error: {err}"
    );

    // Restart peer 0 — recovery's commit branch fires on its
    // partition appliers. Peers 1 and 2 didn't restart so their
    // pending_txs is still populated from the original PREPARE
    // replication; explicitly fire recovery on them too. In
    // production a periodic background task or a leadership-change
    // hook would do this; here we simulate that with a direct call.
    let peer0 = peers.remove(0);
    let suspended = peer0.shutdown_and_wait().await;
    let listener = rebind(suspended.addr).await;
    let fp = Arc::new(FaultPoints::new());
    let peer0_restart = spawn_peer(suspended.config, listener, fp, suspended.dir).await;
    for peer in &peers {
        let _ = peer.service.recover_multi_raft_in_doubt().await;
    }

    // Wait for recovery to drive CommitTx through every partition's
    // Raft and for followers to apply.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut all_have = true;
        for peer in [&peer0_restart, &peers[0], &peers[1]] {
            for label in ["FaultA", "FaultB"] {
                if count_label(peer, label).await != 10 {
                    all_have = false;
                    break;
                }
            }
            if !all_have {
                break;
            }
        }
        if all_have {
            break;
        }
        if Instant::now() > deadline {
            for peer in [&peer0_restart, &peers[0], &peers[1]] {
                eprintln!(
                    "peer {}: FaultA = {}, FaultB = {}",
                    peer.config.self_id,
                    count_label(peer, "FaultA").await,
                    count_label(peer, "FaultB").await
                );
            }
            panic!("commit recovery didn't converge within 10s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn multi_raft_reject_forward_write_retries_after_cache_refresh() {
    // The non-leader peer that received the write proxies via
    // `ForwardWrite` to the partition leader. We inject a one-shot
    // rejection on the leader, expect the caller's leader cache to
    // be invalidated, and verify a fresh write succeeds against the
    // actually-current leader.
    let peers = spawn_three_peer_multi_raft(2).await;

    // Pick a partition's leader and inject the rejection on it.
    let p0 = PartitionId(0);
    let leader_id = peers[0].multi_raft.leader_of(p0).expect("leader known");
    let leader_idx = peers
        .iter()
        .position(|p| p.config.self_id == leader_id)
        .expect("leader peer in cluster");
    peers[leader_idx]
        .fault_points
        .multi_raft_reject_forward_write
        .store(true, Ordering::SeqCst);

    // Issue the write through a non-leader peer of partition 0 so
    // the forward path activates. With rf=3 every peer is a
    // partition-0 replica, so any non-leader works.
    let initiator_idx = (0..3)
        .find(|i| peers[*i].config.self_id != leader_id)
        .unwrap();

    // First attempt should fail because the leader is rejecting.
    let err = peers[initiator_idx]
        .service
        .execute_cypher_local(
            "CREATE (:Forwarded)".to_string(),
            std::collections::HashMap::new(),
        )
        .await;
    assert!(err.is_err(), "first attempt expected to fail");

    // Confirm the rejection actually reached the forward handler.
    assert!(
        peers[leader_idx]
            .fault_points
            .forward_write_call_count
            .load(Ordering::SeqCst)
            >= 1,
        "forward_write should have fired against the leader"
    );

    // Clear the rejection — the second attempt routes to the same
    // leader (or a re-elected one) and lands.
    peers[leader_idx]
        .fault_points
        .multi_raft_reject_forward_write
        .store(false, Ordering::SeqCst);

    // The leader_cache may need a refresh; the next call's cache
    // miss will repopulate it from local Raft metrics. Issue with a
    // short retry loop to absorb that.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let r = peers[initiator_idx]
            .service
            .execute_cypher_local(
                "CREATE (:Forwarded)".to_string(),
                std::collections::HashMap::new(),
            )
            .await;
        if r.is_ok() {
            break;
        }
        if Instant::now() > deadline {
            panic!("retry attempt failed within 5s: {:?}", r);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Every replica should see exactly one Forwarded node.
    for peer in &peers {
        assert_eq!(
            count_label(peer, "Forwarded").await,
            1,
            "peer {} sees wrong Forwarded count",
            peer.config.self_id
        );
    }
}

#[tokio::test]
async fn multi_raft_crash_after_first_commit_tx_recovers_holdouts() {
    // Coordinator runs PREPARE on every touched partition, fsyncs
    // CommitDecision, fires CommitTx on the FIRST partition (it
    // applies on every replica), then crashes before the next
    // partition's CommitTx ships. After the crash we have a split
    // state: some partitions committed, the rest still PREPAREd.
    // Recovery on every replica polls `ResolveTransaction`, finds
    // `Committed` in the coordinator log, and proposes CommitTx on
    // the holdout partitions to converge.
    let mut peers = spawn_three_peer_multi_raft(4).await;

    peers[0]
        .fault_points
        .multi_raft_crash_after_kth_commit_tx
        .store(1, Ordering::SeqCst);
    let res = peers[0]
        .service
        .execute_cypher_local(cross_partition_cypher(), std::collections::HashMap::new())
        .await;
    peers[0]
        .fault_points
        .multi_raft_crash_after_kth_commit_tx
        .store(-1, Ordering::SeqCst);
    let err = res.expect_err("kth-commit fault should have fired");
    assert!(
        err.message()
            .contains("multi_raft_crash_after_kth_commit_tx"),
        "unexpected error: {err}"
    );

    // Restart peer 0 and trigger recovery on every peer — the
    // PREPAREd-but-not-committed partitions need a CommitTx to
    // converge.
    let peer0 = peers.remove(0);
    let suspended = peer0.shutdown_and_wait().await;
    let listener = rebind(suspended.addr).await;
    let fp = Arc::new(FaultPoints::new());
    let peer0_restart = spawn_peer(suspended.config, listener, fp, suspended.dir).await;
    for peer in &peers {
        let _ = peer.service.recover_multi_raft_in_doubt().await;
    }

    // Both labels should reach 10 nodes everywhere.
    let deadline = Instant::now() + Duration::from_secs(10);
    'outer: loop {
        for peer in [&peer0_restart, &peers[0], &peers[1]] {
            for label in ["FaultA", "FaultB"] {
                if count_label(peer, label).await != 10 {
                    if Instant::now() > deadline {
                        panic!(
                            "peer {} stuck at count(:{label}) != 10",
                            peer.config.self_id
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue 'outer;
                }
            }
        }
        break;
    }
}

#[tokio::test]
async fn multi_raft_periodic_recovery_loop_fires_on_interval() {
    // The periodic recovery loop should call
    // `recover_multi_raft_in_doubt` at the configured interval. Use
    // a 100ms tick so the test bounds at <1s. After waiting for a
    // few ticks the call count should be at least 2.
    let peers = spawn_three_peer_multi_raft(2).await;

    let handle = peers[0]
        .service
        .spawn_multi_raft_recovery_loop(Duration::from_millis(100));
    let handle = handle.expect("multi-raft mode → recovery loop spawned");

    // Wait three ticks. The loop skips the immediate first tick
    // (startup recovery already ran), so n ticks ≈ n calls.
    tokio::time::sleep(Duration::from_millis(450)).await;

    let count = peers[0]
        .fault_points
        .recover_multi_raft_call_count
        .load(Ordering::SeqCst);
    handle.abort();
    let _ = handle.await;

    assert!(
        count >= 2,
        "expected periodic recovery to fire >= 2 times in 450ms, got {count}"
    );
}

#[tokio::test]
async fn multi_raft_concurrent_in_doubt_recovery_is_idempotent() {
    // Pin the contract that running `recover_multi_raft_in_doubt`
    // simultaneously on every replica produces the same result as
    // running it once. The first replica's CommitTx propose drains
    // pending_txs everywhere via the partition Raft; subsequent
    // attempts on the same txid are no-ops because pending_txs is
    // already cleared. Without this property, every periodic
    // recovery tick on a busy cluster would proliferate redundant
    // CommitTx entries in the partition Raft log.
    let mut peers = spawn_three_peer_multi_raft(4).await;

    // Get the cluster into a "post-decision, pre-fanout" state by
    // injecting the commit-decision fault.
    peers[0]
        .fault_points
        .multi_raft_crash_after_commit_decision
        .store(true, Ordering::SeqCst);
    let res = peers[0]
        .service
        .execute_cypher_local(cross_partition_cypher(), std::collections::HashMap::new())
        .await;
    peers[0]
        .fault_points
        .multi_raft_crash_after_commit_decision
        .store(false, Ordering::SeqCst);
    let _err = res.expect_err("commit-decision fault should have fired");

    // Restart peer 0 (its in-process recovery runs once during spawn).
    // Then concurrently fire recovery on every peer to race them
    // against each other.
    let peer0 = peers.remove(0);
    let suspended = peer0.shutdown_and_wait().await;
    let listener = rebind(suspended.addr).await;
    let fp = Arc::new(FaultPoints::new());
    let peer0_restart = spawn_peer(suspended.config, listener, fp, suspended.dir).await;

    // Concurrent recovery on every peer. tokio::join! waits for all
    // three to complete; any "redundant CommitTx" race surfaces as
    // an inconsistent count or a hard error.
    let (r0, r1, r2) = tokio::join!(
        peer0_restart.service.recover_multi_raft_in_doubt(),
        peers[0].service.recover_multi_raft_in_doubt(),
        peers[1].service.recover_multi_raft_in_doubt(),
    );
    r0.expect("peer 0 recovery");
    r1.expect("peer 1 recovery");
    r2.expect("peer 2 recovery");

    // Convergence: every label has 10 nodes on every peer.
    let deadline = Instant::now() + Duration::from_secs(10);
    'outer: loop {
        for peer in [&peer0_restart, &peers[0], &peers[1]] {
            for label in ["FaultA", "FaultB"] {
                if count_label(peer, label).await != 10 {
                    if Instant::now() > deadline {
                        panic!(
                            "concurrent recovery didn't converge: peer {} count(:{label}) != 10",
                            peer.config.self_id
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue 'outer;
                }
            }
        }
        break;
    }

    // Recovery should have fired on every peer at least once each.
    for peer in [&peer0_restart, &peers[0], &peers[1]] {
        let n = peer
            .fault_points
            .recover_multi_raft_call_count
            .load(Ordering::SeqCst);
        assert!(
            n >= 1,
            "peer {} didn't run recovery at least once (count = {n})",
            peer.config.self_id
        );
    }
}
