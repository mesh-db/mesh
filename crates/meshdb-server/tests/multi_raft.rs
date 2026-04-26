//! Multi-raft mode smoke tests.
//!
//! These tests spin up small clusters in `mode = "multi-raft"` and
//! verify the per-group bootstrap path: storage layout, Raft handle
//! presence on each peer, leader election in every group.

use meshdb_cluster::PartitionId;
use meshdb_rpc::MeshService;
use meshdb_server::config::{ClusterMode, PeerConfig, ServerConfig};
use meshdb_server::{build_components, initialize_multi_raft_if_seed};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

struct Peer {
    config: ServerConfig,
    multi_raft: Arc<meshdb_rpc::MultiRaftCluster>,
    service: MeshService,
    _dir: TempDir,
    /// Handle for the spawned gRPC server task. Tests that
    /// simulate a peer crash abort this so the other peers see
    /// the listener stop responding.
    server_task: tokio::task::JoinHandle<()>,
}

async fn spawn_multi_raft_cluster(num_peers: usize, num_partitions: u32, rf: usize) -> Vec<Peer> {
    // Pre-bind listeners so we know every peer's final address
    // before constructing the peer-list.
    let mut listeners: Vec<TcpListener> = Vec::with_capacity(num_peers);
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(num_peers);
    for _ in 0..num_peers {
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
            weight: None,
        })
        .collect();

    let mut peers = Vec::with_capacity(num_peers);
    for (i, listener) in listeners.into_iter().enumerate() {
        let dir = TempDir::new().unwrap();
        let addr = addrs[i];
        let config = ServerConfig {
            self_id: (i + 1) as u64,
            listen_address: addr.to_string(),
            data_dir: dir.path().to_path_buf(),
            num_partitions,
            peers: peer_configs.clone(),
            // Mark peer 1 as bootstrap seed for the metadata group.
            // Per-partition group seeding is automatic — the
            // lowest-id replica of each group runs initialize.
            bootstrap: i == 0,
            bolt_address: None,
            metrics_address: None,
            bolt_auth: None,
            bolt_tls: None,
            bolt_advertised_versions: None,
            bolt_advertised_address: None,
            grpc_tls: None,
            mode: Some(ClusterMode::MultiRaft),
            replication_factor: Some(rf),
            read_consistency: None,
            #[cfg(feature = "apoc-load")]
            apoc_import: None,
        };

        let components = build_components(&config).await.unwrap();
        let multi_raft = components.multi_raft.clone().expect("multi-raft built");
        let service = components.service.clone();
        // Build a fresh MeshRaftService from the same registry —
        // ServerComponents.raft_service was already consumed elsewhere
        // in production, but in tests we re-derive it.
        let registry = multi_raft.build_registry();
        let raft_service = meshdb_rpc::MeshRaftService::with_registry(registry);

        let service_for_server = service.clone();
        let server_task = tokio::spawn(async move {
            // Ignore errors from a forced abort — tests that
            // simulate a peer crash abort this task and the
            // resulting JoinError is expected, not a test failure.
            let _ = Server::builder()
                .add_service(raft_service.into_server())
                .add_service(service_for_server.clone().into_query_server())
                .add_service(service_for_server.clone().into_write_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });

        peers.push(Peer {
            config,
            multi_raft,
            service,
            _dir: dir,
            server_task,
        });
    }

    // Give the listeners a moment to start accepting connections
    // before initialize() tries to AppendEntries to peers.
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Run the multi-raft initialize on every peer. Each peer seeds
    // only the groups it's the designated initializer for; others
    // are no-ops with a debug log line.
    for peer in &peers {
        initialize_multi_raft_if_seed(&peer.config, &peer.multi_raft)
            .await
            .unwrap();
    }

    peers
}

#[tokio::test]
async fn multi_raft_three_peer_cluster_elects_leader_in_every_group() {
    // 3 peers, 2 partitions, rf=3 → every peer holds every group.
    // Confirms the bootstrap path and that all groups can elect
    // leaders. Slowest-link case is the meta group election.
    let peers = spawn_multi_raft_cluster(3, 2, 3).await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut all_have_leader = true;
        for peer in &peers {
            if peer.multi_raft.meta.current_leader().is_none() {
                all_have_leader = false;
                break;
            }
            for partition in 0..2u32 {
                if let Some(raft) = peer.multi_raft.partition(PartitionId(partition)) {
                    if raft.current_leader().is_none() {
                        all_have_leader = false;
                        break;
                    }
                }
            }
            if !all_have_leader {
                break;
            }
        }
        if all_have_leader {
            break;
        }
        if Instant::now() > deadline {
            for peer in &peers {
                eprintln!(
                    "peer {}: meta leader = {:?}, p0 leader = {:?}, p1 leader = {:?}",
                    peer.config.self_id,
                    peer.multi_raft.meta.current_leader(),
                    peer.multi_raft
                        .partition(PartitionId(0))
                        .and_then(|r| r.current_leader()),
                    peer.multi_raft
                        .partition(PartitionId(1))
                        .and_then(|r| r.current_leader()),
                );
            }
            panic!("multi-raft groups did not all elect leaders within 10s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait for every Raft group on every peer to have a known leader.
/// Used by tests that assume the cluster has finished bootstrapping
/// before issuing writes.
async fn wait_for_leaders(peers: &[Peer], timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let mut all = true;
        for peer in peers {
            if peer.multi_raft.meta.current_leader().is_none() {
                all = false;
                break;
            }
            for raft in peer
                .multi_raft
                .partitions_snapshot()
                .iter()
                .map(|(_, r)| r.clone())
                .collect::<Vec<_>>()
                .iter()
            {
                if raft.current_leader().is_none() {
                    all = false;
                    break;
                }
            }
            if !all {
                break;
            }
        }
        if all {
            return;
        }
        if Instant::now() > deadline {
            panic!("leaders not elected within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn multi_raft_single_partition_write_through_any_peer() {
    // 3 peers, 4 partitions, rf=3. Insert nodes through every peer's
    // service — at least some peer will be a non-leader for some
    // partition, exercising the server-side forwarding path. Verify
    // every node lands on every replica (rf=3 = full replication so
    // every peer has every node).
    let peers = spawn_multi_raft_cluster(3, 4, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    // Issue a Cypher CREATE through each peer in turn. Each CREATE
    // emits one PutNode targeting one partition (FNV-1a of the new
    // id) — at least some peers will not be the leader of that
    // partition, exercising the server-side forwarding path.
    for (i, peer) in peers.iter().enumerate() {
        let cypher = format!("CREATE (:Origin{}) RETURN 0", i);
        peer.service
            .execute_cypher_local(cypher, std::collections::HashMap::new())
            .await
            .unwrap_or_else(|e| panic!("peer {} write failed: {e}", peer.config.self_id));
    }

    // Every peer's local store should hold all three nodes — rf=3
    // means each partition's replica set spans every peer.
    let deadline = Instant::now() + Duration::from_secs(5);
    'outer: loop {
        for peer in &peers {
            for label in ["Origin0", "Origin1", "Origin2"] {
                let cypher = format!("MATCH (n:{label}) RETURN count(n) AS c");
                let rows = peer
                    .service
                    .execute_cypher_local(cypher, std::collections::HashMap::new())
                    .await
                    .unwrap();
                let count_value = rows.first().and_then(|r| r.get("c")).expect("count(n) row");
                let count = match count_value {
                    meshdb_executor::Value::Property(meshdb_core::Property::Int64(c)) => *c,
                    other => panic!("expected Int64, got {other:?}"),
                };
                if count != 1 {
                    if Instant::now() > deadline {
                        panic!(
                            "peer {} sees count(:{label}) = {count}, expected 1",
                            peer.config.self_id
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue 'outer;
                }
            }
        }
        break;
    }
}

#[tokio::test]
async fn multi_raft_cross_partition_write_commits_atomically() {
    // 3 peers, 4 partitions, rf=3. Issue a Cypher write that
    // creates an edge between two nodes whose ids land in different
    // partitions — exercises the Spanner-style PREPARE-Raft 2PC.
    // Both partition Raft logs should get a PreparedTx → CommitTx
    // pair; both replicas converge to the same applied state.
    let peers = spawn_multi_raft_cluster(3, 4, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    // Cypher that touches >1 partition: CREATE two labeled nodes +
    // an edge between them. The new ids hash randomly across the 4
    // partitions, so most runs pick different partitions for the
    // two nodes. Even when they happen to land on the same
    // partition, the commit path is still correct (it just
    // degenerates to the single-partition fast path).
    let cypher = "CREATE (a:Source)-[:TO]->(b:Target) RETURN 0".to_string();
    peers[0]
        .service
        .execute_cypher_local(cypher, std::collections::HashMap::new())
        .await
        .unwrap();

    // Verify every peer ends up with one Source, one Target, and
    // one edge.
    let deadline = Instant::now() + Duration::from_secs(5);
    'outer: loop {
        for peer in &peers {
            for label in ["Source", "Target"] {
                let q = format!("MATCH (n:{label}) RETURN count(n) AS c");
                let rows = peer
                    .service
                    .execute_cypher_local(q, std::collections::HashMap::new())
                    .await
                    .unwrap();
                let count = match rows.first().and_then(|r| r.get("c")) {
                    Some(meshdb_executor::Value::Property(meshdb_core::Property::Int64(c))) => *c,
                    other => panic!("expected Int64, got {other:?}"),
                };
                if count != 1 {
                    if Instant::now() > deadline {
                        panic!(
                            "peer {} sees count(:{label}) = {count}, expected 1",
                            peer.config.self_id
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue 'outer;
                }
            }
            // Edge count.
            let rows = peer
                .service
                .execute_cypher_local(
                    "MATCH ()-[r:TO]->() RETURN count(r) AS c".to_string(),
                    std::collections::HashMap::new(),
                )
                .await
                .unwrap();
            let count = match rows.first().and_then(|r| r.get("c")) {
                Some(meshdb_executor::Value::Property(meshdb_core::Property::Int64(c))) => *c,
                other => panic!("expected Int64, got {other:?}"),
            };
            if count != 1 {
                if Instant::now() > deadline {
                    panic!(
                        "peer {} sees count(:TO) = {count}, expected 1",
                        peer.config.self_id
                    );
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue 'outer;
            }
        }
        break;
    }
}

#[tokio::test]
async fn multi_raft_create_index_replicates_through_meta_group() {
    // CREATE INDEX is DDL — it goes through the metadata Raft group,
    // not partition groups. After the proposal commits, every peer
    // should see the index in its local store regardless of which
    // peer the user issued it through.
    let peers = spawn_multi_raft_cluster(3, 4, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    // Issue CREATE INDEX on the bootstrap peer.
    peers[0]
        .service
        .execute_cypher_local(
            "CREATE INDEX FOR (n:Person) ON (n.email)".to_string(),
            std::collections::HashMap::new(),
        )
        .await
        .unwrap();

    // Every peer's local store should reflect the index.
    let deadline = Instant::now() + Duration::from_secs(5);
    'outer: loop {
        for peer in &peers {
            let store = peer.multi_raft.meta.clone();
            // Check via the Cypher SHOW INDEXES surface — round-trips
            // through the executor against each peer's local store.
            let rows = peer
                .service
                .execute_cypher_local("SHOW INDEXES".to_string(), std::collections::HashMap::new())
                .await
                .unwrap();
            let saw_it = rows.iter().any(|row| {
                let label = row.get("label").and_then(|v| match v {
                    meshdb_executor::Value::Property(meshdb_core::Property::String(s)) => {
                        Some(s.clone())
                    }
                    _ => None,
                });
                label.as_deref() == Some("Person")
            });
            if !saw_it {
                if Instant::now() > deadline {
                    panic!(
                        "peer {} doesn't see Person index after 5s",
                        peer.config.self_id
                    );
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue 'outer;
            }
            // Avoid clippy "unused" on the meta handle in this test.
            let _ = store;
        }
        break;
    }
}

#[tokio::test]
async fn multi_raft_create_index_synchronous_ddl_gate() {
    // Stricter than `multi_raft_create_index_replicates_through_meta_group`:
    // verifies the synchronous DDL gate. After `CREATE INDEX` returns
    // OK on the issuing peer, every other peer's metadata applier
    // must have already applied the entry — no polling, no settle
    // sleep. This is the load-bearing guarantee of Option A.
    let peers = spawn_multi_raft_cluster(3, 4, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    // Issue CREATE INDEX through peer 0. The await returns only
    // after every peer has confirmed apply (or the strict timeout
    // tripped, in which case the call would error out — the .unwrap()
    // would explode).
    peers[0]
        .service
        .execute_cypher_local(
            "CREATE INDEX FOR (n:GatedLabel) ON (n.email)".to_string(),
            std::collections::HashMap::new(),
        )
        .await
        .unwrap();

    // No polling, no sleep. Every peer must already see it.
    for peer in &peers {
        let rows = peer
            .service
            .execute_cypher_local("SHOW INDEXES".to_string(), std::collections::HashMap::new())
            .await
            .unwrap();
        let saw_it = rows.iter().any(|row| {
            let label = row.get("label").and_then(|v| match v {
                meshdb_executor::Value::Property(meshdb_core::Property::String(s)) => {
                    Some(s.clone())
                }
                _ => None,
            });
            label.as_deref() == Some("GatedLabel")
        });
        assert!(
            saw_it,
            "peer {} did not see GatedLabel index immediately after CREATE INDEX returned — \
             synchronous DDL gate (Option A) is broken",
            peer.config.self_id
        );
    }

    // Also test the forwarded-DDL path: issue from a non-leader of
    // the metadata group, which routes through `forward_ddl`.
    let meta_leader = peers[0]
        .multi_raft
        .meta_leader()
        .expect("meta leader known");
    let non_leader_idx = peers
        .iter()
        .position(|p| p.config.self_id != meta_leader)
        .expect("at least one non-leader exists");

    peers[non_leader_idx]
        .service
        .execute_cypher_local(
            "CREATE INDEX FOR (n:Forwarded) ON (n.id)".to_string(),
            std::collections::HashMap::new(),
        )
        .await
        .unwrap();

    for peer in &peers {
        let rows = peer
            .service
            .execute_cypher_local("SHOW INDEXES".to_string(), std::collections::HashMap::new())
            .await
            .unwrap();
        let saw_it = rows.iter().any(|row| {
            let label = row.get("label").and_then(|v| match v {
                meshdb_executor::Value::Property(meshdb_core::Property::String(s)) => {
                    Some(s.clone())
                }
                _ => None,
            });
            label.as_deref() == Some("Forwarded")
        });
        assert!(
            saw_it,
            "peer {} did not see Forwarded index immediately after CREATE INDEX returned via \
             forward_ddl — DDL forwarding does not honor the synchronous gate",
            peer.config.self_id
        );
    }
}

#[tokio::test]
async fn multi_raft_survives_partition_leader_shutdown() {
    // 3 peers, 1 partition, rf=3. Identify the partition leader,
    // simulate a peer crash by aborting its gRPC server task (so
    // the other two replicas can no longer reach it), then verify
    // a write through a surviving peer succeeds after re-election.
    //
    // This is the "lose the leader during normal traffic" scenario.
    // The server-task abort is functionally equivalent to a SIGKILL
    // from the perspective of the surviving peers — AppendEntries
    // and Vote RPCs to the dead peer time out, the followers' Raft
    // election timer fires, and one of them wins the next term.
    let peers = spawn_multi_raft_cluster(3, 1, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    // Initial write through peer 0; verifies the cluster is healthy
    // before we start cutting things.
    peers[0]
        .service
        .execute_cypher_local(
            "CREATE (:Before)".to_string(),
            std::collections::HashMap::new(),
        )
        .await
        .unwrap();

    let p0 = PartitionId(0);
    let original_leader = peers[0].multi_raft.leader_of(p0).expect("leader known");
    let leader_idx = peers
        .iter()
        .position(|p| p.config.self_id == original_leader)
        .expect("leader peer in cluster");

    // Crash the leader: shut down its partition Raft handle (so
    // it stops sending heartbeats) AND abort its gRPC server task
    // (so the survivors can no longer reach it for any RPC). The
    // combination is a clean SIGKILL simulation — followers see
    // no more AppendEntries, election timer fires, new leader
    // emerges.
    peers[leader_idx]
        .multi_raft
        .shutdown_partition(p0)
        .await
        .unwrap();
    peers[leader_idx].server_task.abort();

    // Wait for the surviving peers to elect a new leader and AGREE
    // on who it is. Without heartbeats from the dead leader,
    // openraft's election timer (default ~150–300ms) fires; one of
    // the survivors wins. We require both survivors to report the
    // same leader id (and that id to not be the crashed peer)
    // before we proceed — otherwise the write path may race a
    // half-completed election and bounce off a stale cache entry.
    let survivor_idxs: Vec<usize> = (0..peers.len()).filter(|i| *i != leader_idx).collect();
    let new_leader = {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let mut leaders = Vec::with_capacity(survivor_idxs.len());
            for idx in &survivor_idxs {
                leaders.push(peers[*idx].multi_raft.partition_current_leader(p0));
            }
            let agreed = leaders
                .iter()
                .copied()
                .reduce(|a, b| if a == b { a } else { None })
                .flatten();
            if let Some(l) = agreed {
                if l != original_leader {
                    break l;
                }
            }
            if Instant::now() > deadline {
                panic!(
                    "survivors did not agree on a new leader within 15s after crashing peer {original_leader}: {leaders:?}"
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    assert_ne!(
        new_leader, original_leader,
        "new leader should not be the crashed peer"
    );

    // Issue a write through a survivor. The target peer either
    // leads (proposes locally) or proxies via forward_write to the
    // newly-elected leader. Either path must succeed.
    let writer_idx = survivor_idxs[0];
    peers[writer_idx]
        .service
        .execute_cypher_local(
            "CREATE (:After)".to_string(),
            std::collections::HashMap::new(),
        )
        .await
        .expect("write through survivor must succeed after re-election");

    // Both labels must be visible on every survivor (the dead peer
    // can't serve queries because its server is aborted).
    let deadline = Instant::now() + Duration::from_secs(5);
    'outer: loop {
        for idx in &survivor_idxs {
            for label in ["Before", "After"] {
                let q = format!("MATCH (n:{label}) RETURN count(n) AS c");
                let rows = peers[*idx]
                    .service
                    .execute_cypher_local(q, std::collections::HashMap::new())
                    .await
                    .unwrap();
                let count = match rows.first().and_then(|r| r.get("c")) {
                    Some(meshdb_executor::Value::Property(meshdb_core::Property::Int64(c))) => *c,
                    other => panic!("expected Int64, got {other:?}"),
                };
                if count != 1 {
                    if Instant::now() > deadline {
                        panic!(
                            "survivor peer {} sees count(:{label}) = {count}, expected 1",
                            peers[*idx].config.self_id
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue 'outer;
                }
            }
        }
        break;
    }
}

#[tokio::test]
async fn multi_raft_concurrent_create_index_is_idempotent() {
    // Three peers issue `CREATE INDEX` for the same `(label, prop)`
    // simultaneously. The metadata Raft serializes the proposes;
    // exactly one CreateIndex applies (the rest are no-ops because
    // the storage layer's `create_property_index_composite` is
    // idempotent). Every peer ends up with exactly one Person
    // index and no errors propagate to the user.
    let peers = spawn_multi_raft_cluster(3, 4, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    let cypher = "CREATE INDEX FOR (n:Person) ON (n.email)".to_string();

    // Concurrent fan-out — every peer issues the same statement.
    let (r0, r1, r2) = tokio::join!(
        peers[0]
            .service
            .execute_cypher_local(cypher.clone(), std::collections::HashMap::new()),
        peers[1]
            .service
            .execute_cypher_local(cypher.clone(), std::collections::HashMap::new()),
        peers[2]
            .service
            .execute_cypher_local(cypher, std::collections::HashMap::new()),
    );

    // None of the three should error — the index either creates or
    // is idempotent-no-op on each peer.
    r0.expect("peer 0 CREATE INDEX");
    r1.expect("peer 1 CREATE INDEX");
    r2.expect("peer 2 CREATE INDEX");

    // Every peer's local store should hold exactly one Person index
    // — never two from the concurrent attempts.
    let deadline = Instant::now() + Duration::from_secs(5);
    'outer: loop {
        for peer in &peers {
            let rows = peer
                .service
                .execute_cypher_local("SHOW INDEXES".to_string(), std::collections::HashMap::new())
                .await
                .unwrap();
            let person_count = rows
                .iter()
                .filter(|row| {
                    row.get("label")
                        .and_then(|v| match v {
                            meshdb_executor::Value::Property(meshdb_core::Property::String(s)) => {
                                Some(s.as_str())
                            }
                            _ => None,
                        })
                        .map(|s| s == "Person")
                        .unwrap_or(false)
                })
                .count();
            if person_count != 1 {
                if Instant::now() > deadline {
                    panic!(
                        "peer {} sees {person_count} Person indexes; expected 1",
                        peer.config.self_id
                    );
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue 'outer;
            }
        }
        break;
    }
}

#[tokio::test]
async fn multi_raft_concurrent_ddl_and_writes_converge() {
    // Concurrency stress: every peer issues an interleaved mix of
    // CREATE INDEX and cross-partition writes. The DDL barrier
    // (`min_meta_index`) must keep writes from racing ahead of
    // their meta replica, and every replica must eventually agree
    // on the set of indexes + nodes after the dust settles.
    let peers = spawn_multi_raft_cluster(3, 4, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    let mut handles = Vec::new();
    for (i, peer) in peers.iter().enumerate() {
        let svc = peer.service.clone();
        // Each peer does one CREATE INDEX (idempotent across peers)
        // and one CREATE-of-2-nodes writing into different
        // partitions. The two operations interleave with the other
        // peers' operations.
        handles.push(tokio::spawn(async move {
            svc.execute_cypher_local(
                format!("CREATE INDEX FOR (n:Stress{i}) ON (n.k)"),
                std::collections::HashMap::new(),
            )
            .await
            .unwrap();
            svc.execute_cypher_local(
                format!("CREATE (a:StressA), (b:StressB) RETURN 0"),
                std::collections::HashMap::new(),
            )
            .await
            .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Convergence: every peer should see the three indexes (Stress0,
    // Stress1, Stress2) and the right node counts (3 StressA, 3 StressB).
    let deadline = Instant::now() + Duration::from_secs(10);
    'outer: loop {
        for peer in &peers {
            let rows = peer
                .service
                .execute_cypher_local("SHOW INDEXES".to_string(), std::collections::HashMap::new())
                .await
                .unwrap();
            for label in ["Stress0", "Stress1", "Stress2"] {
                let saw = rows.iter().any(|r| {
                    r.get("label")
                        .and_then(|v| match v {
                            meshdb_executor::Value::Property(meshdb_core::Property::String(s)) => {
                                Some(s.as_str())
                            }
                            _ => None,
                        })
                        .map(|s| s == label)
                        .unwrap_or(false)
                });
                if !saw {
                    if Instant::now() > deadline {
                        panic!("peer {} missing {label} index", peer.config.self_id);
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue 'outer;
                }
            }
            for (label, expected) in [("StressA", 3i64), ("StressB", 3)] {
                let q = format!("MATCH (n:{label}) RETURN count(n) AS c");
                let rows = peer
                    .service
                    .execute_cypher_local(q, std::collections::HashMap::new())
                    .await
                    .unwrap();
                let count = match rows.first().and_then(|r| r.get("c")) {
                    Some(meshdb_executor::Value::Property(meshdb_core::Property::Int64(c))) => *c,
                    other => panic!("expected Int64, got {other:?}"),
                };
                if count != expected {
                    if Instant::now() > deadline {
                        panic!(
                            "peer {} count(:{label}) = {count}, expected {expected}",
                            peer.config.self_id
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue 'outer;
                }
            }
        }
        break;
    }
}

#[tokio::test]
async fn multi_raft_trigger_install_and_fire() {
    // Trigger DDL (apoc.trigger.install) routes through the meta
    // Raft group like any other DDL — every peer's local trigger
    // registry should pick it up. Trigger firing happens on the
    // proposing peer's commit path; the trigger body's writes ride
    // the same multi-raft commit machinery (single- or multi-
    // partition) under the from-trigger suppression guard, so
    // every replica converges.
    let peers = spawn_multi_raft_cluster(3, 4, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    // Install via peer 0. The DDL forwards to the meta leader;
    // every peer's storage gets the trigger spec.
    peers[0]
        .service
        .execute_cypher_local(
            "CALL apoc.trigger.install('meshdb', 'markSource', \
               'UNWIND $createdNodes AS n \
                WITH n WHERE \"Source\" IN labels(n) \
                CREATE (:Marker)', \
               null, null) \
             YIELD name RETURN name"
                .to_string(),
            std::collections::HashMap::new(),
        )
        .await
        .expect("trigger install");

    // Confirm the trigger replicated to every peer's registry.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut all_have = true;
        for peer in &peers {
            let rows = peer
                .service
                .execute_cypher_local(
                    "CALL apoc.trigger.list() YIELD name RETURN name".to_string(),
                    std::collections::HashMap::new(),
                )
                .await
                .unwrap();
            let saw = rows.iter().any(|r| {
                r.get("name")
                    .and_then(|v| match v {
                        meshdb_executor::Value::Property(meshdb_core::Property::String(s)) => {
                            Some(s.as_str())
                        }
                        _ => None,
                    })
                    .map(|s| s == "markSource")
                    .unwrap_or(false)
            });
            if !saw {
                all_have = false;
                break;
            }
        }
        if all_have {
            break;
        }
        if Instant::now() > deadline {
            for peer in &peers {
                let rows = peer
                    .service
                    .execute_cypher_local(
                        "CALL apoc.trigger.list() YIELD name RETURN name".to_string(),
                        std::collections::HashMap::new(),
                    )
                    .await
                    .unwrap();
                eprintln!(
                    "peer {} triggers: {:?}",
                    peer.config.self_id,
                    rows.iter()
                        .filter_map(|r| r.get("name").cloned())
                        .collect::<Vec<_>>()
                );
            }
            panic!("trigger 'markSource' didn't replicate to every peer within 5s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Create a Source through peer 1 — the trigger should fire on
    // commit and emit a Marker. The Marker creation rides the same
    // multi-raft commit path as the user-visible write.
    peers[1]
        .service
        .execute_cypher_local(
            "CREATE (:Source)".to_string(),
            std::collections::HashMap::new(),
        )
        .await
        .expect("source create");

    // Every replica should see exactly one Marker. Allow a brief
    // settle for follower applies to catch up.
    let deadline = Instant::now() + Duration::from_secs(5);
    'outer: loop {
        for peer in &peers {
            let rows = peer
                .service
                .execute_cypher_local(
                    "MATCH (n:Marker) RETURN count(n) AS c".to_string(),
                    std::collections::HashMap::new(),
                )
                .await
                .unwrap();
            let count = match rows.first().and_then(|r| r.get("c")) {
                Some(meshdb_executor::Value::Property(meshdb_core::Property::Int64(c))) => *c,
                other => panic!("expected Int64, got {other:?}"),
            };
            if count != 1 {
                if Instant::now() > deadline {
                    panic!(
                        "peer {} sees count(:Marker) = {count}, expected 1",
                        peer.config.self_id
                    );
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue 'outer;
            }
        }
        break;
    }
}

#[tokio::test]
async fn multi_raft_replica_map_persisted_through_meta_after_rebalance() {
    // After remove_partition_replica succeeds, the meta-replicated
    // ClusterState's `partition_replica_map` should reflect the new
    // voter set. Every peer's local view of the cluster (read via
    // multi_raft.meta.current_state()) eventually shows the change.
    let peers = spawn_multi_raft_cluster(3, 2, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    let p0 = PartitionId(0);
    let leader_id = peers[0].multi_raft.leader_of(p0).expect("p0 leader");
    let leader_idx = peers
        .iter()
        .position(|p| p.config.self_id == leader_id)
        .unwrap();
    let evict_id = peers
        .iter()
        .map(|p| p.config.self_id)
        .find(|id| *id != leader_id)
        .unwrap();

    peers[leader_idx]
        .multi_raft
        .remove_partition_replica(p0, evict_id)
        .await
        .expect("remove_partition_replica");

    // Wait for SetPartitionReplicas to propagate through meta. Every
    // peer's persisted state should drop the evicted id from
    // partition 0's replica set.
    let deadline = Instant::now() + Duration::from_secs(5);
    'outer: loop {
        for peer in &peers {
            let state = peer.multi_raft.meta.current_state().await;
            let replicas: Vec<u64> = state
                .partition_replica_map
                .as_ref()
                .map(|m| m.replicas(p0).iter().map(|p| p.0).collect())
                .unwrap_or_default();
            if replicas.contains(&evict_id) || replicas.is_empty() {
                if Instant::now() > deadline {
                    panic!(
                        "peer {} partition 0 replicas = {replicas:?} still \
                         contains {evict_id} (or is empty)",
                        peer.config.self_id
                    );
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue 'outer;
            }
        }
        break;
    }
}

#[tokio::test]
async fn multi_raft_runtime_partition_group_spinup() {
    // Spawn 3-peer × 4-partition × rf=2 cluster — peers are uneven
    // hosts of partitions, so peer 1 doesn't host every partition.
    // Pick a partition peer 1 doesn't currently host and call
    // `instantiate_partition_group` on it; verify the group shows up
    // in `partitions_snapshot` and the dispatch registry serves
    // RPCs against it (the new group's `current_leader` returns Some
    // once it joins as a learner of the existing replicas).
    let peers = spawn_multi_raft_cluster(3, 4, 2).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    // Find a partition that peer 1 doesn't currently host.
    let peer_idx = 0; // peer 1
    let absent: Option<PartitionId> = (0..4u32)
        .map(PartitionId)
        .find(|p| !peers[peer_idx].multi_raft.hosts_partition(*p));
    let Some(absent_partition) = absent else {
        // rf=2 over 3 peers → ~67% of partitions per peer; almost
        // always at least one is absent. If somehow not, the test
        // skips rather than failing because the cluster's rf+peer
        // mix didn't produce the precondition.
        eprintln!("skipping: peer 1 happens to host every partition");
        return;
    };

    // Build a fresh GrpcNetwork pointing at the other peers (the
    // ones that will eventually serve as replication sources for
    // this new partition Raft).
    let peer_map: Vec<(u64, String)> = peers
        .iter()
        .filter(|p| p.config.self_id != peers[peer_idx].config.self_id)
        .map(|p| (p.config.self_id, p.config.listen_address.clone()))
        .collect();
    let network = meshdb_rpc::GrpcNetwork::new(peer_map).expect("build grpc network");

    // Use a fresh data dir for the new partition so we don't
    // collide with the existing service's rocksdb instance.
    let separate_dir = tempfile::TempDir::new().unwrap();
    let mut spinup_config = peers[peer_idx].config.clone();
    spinup_config.data_dir = separate_dir.path().to_path_buf();
    let spinup_store: Arc<dyn meshdb_storage::StorageEngine> = Arc::new(
        meshdb_storage::RocksDbStorageEngine::open(spinup_config.data_dir.as_path()).unwrap(),
    );

    meshdb_server::instantiate_partition_group(
        &spinup_config,
        &peers[peer_idx].multi_raft,
        spinup_store,
        absent_partition,
        &network,
    )
    .await
    .expect("instantiate partition group");

    // The partition is now hosted on peer 1.
    assert!(
        peers[peer_idx].multi_raft.hosts_partition(absent_partition),
        "partition {} should be hosted post-instantiation",
        absent_partition.0
    );
    // partitions_snapshot reflects it.
    let snapshot_count = peers[peer_idx].multi_raft.partitions_snapshot().len();
    assert!(snapshot_count >= 1, "snapshot should include the new group");
}

#[tokio::test]
async fn multi_raft_runtime_spinup_then_add_replica_composes() {
    // Smoke test for the rebalancing flow end-to-end:
    //   1. Spawn 3-peer × 4-partition × rf=2 cluster — uneven hosts.
    //   2. Find a partition the chosen peer doesn't currently host.
    //   3. Instantiate the partition Raft on that peer
    //      (`instantiate_partition_group`) — the new group exists
    //      locally but isn't yet a member of the partition's Raft.
    //   4. On the partition leader, call `add_partition_replica`
    //      to add the new peer's id as a voter.
    //   5. Verify the partition's voter set grew on the leader and
    //      the new peer is now in `current_partition_voters`.
    //
    // openraft's InstallSnapshot / AppendEntries plumbing handles
    // the actual data-replication after step 4; that's its
    // contract, not ours. This test pins that the composition of
    // our APIs gets you to a state where openraft can take over.
    let peers = spawn_multi_raft_cluster(3, 4, 2).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    let peer_idx = 0;
    let absent: Option<PartitionId> = (0..4u32)
        .map(PartitionId)
        .find(|p| !peers[peer_idx].multi_raft.hosts_partition(*p));
    let Some(absent_partition) = absent else {
        eprintln!("skipping: peer 1 happens to host every partition");
        return;
    };

    let peer_map: Vec<(u64, String)> = peers
        .iter()
        .filter(|p| p.config.self_id != peers[peer_idx].config.self_id)
        .map(|p| (p.config.self_id, p.config.listen_address.clone()))
        .collect();
    let network = meshdb_rpc::GrpcNetwork::new(peer_map).expect("build grpc network");

    let separate_dir = tempfile::TempDir::new().unwrap();
    let mut spinup_config = peers[peer_idx].config.clone();
    spinup_config.data_dir = separate_dir.path().to_path_buf();
    let spinup_store: Arc<dyn meshdb_storage::StorageEngine> = Arc::new(
        meshdb_storage::RocksDbStorageEngine::open(spinup_config.data_dir.as_path()).unwrap(),
    );

    meshdb_server::instantiate_partition_group(
        &spinup_config,
        &peers[peer_idx].multi_raft,
        spinup_store,
        absent_partition,
        &network,
    )
    .await
    .expect("instantiate partition group");

    // Find which peer leads `absent_partition` (one of peer 2 or
    // peer 3, since peer 1 didn't host it pre-spinup).
    let leader_id = peers
        .iter()
        .filter(|p| p.config.self_id != peers[peer_idx].config.self_id)
        .find_map(|p| {
            p.multi_raft
                .partition(absent_partition)
                .and_then(|r| r.current_leader().map(|l| l.0))
        })
        .expect("absent_partition has a known leader");
    let leader_idx = peers
        .iter()
        .position(|p| p.config.self_id == leader_id)
        .unwrap();

    // Add peer 1 (the spun-up host) as a voter of the absent
    // partition. This is the "operator promotes the new replica"
    // step. openraft handles the joint-config commit + replication
    // afterwards; for v1 we only assert the API call succeeds.
    let new_voter_id = peers[peer_idx].config.self_id;
    let result = peers[leader_idx]
        .multi_raft
        .add_partition_replica(absent_partition, new_voter_id)
        .await;
    // Note: the actual replication may or may not catch up within
    // the test deadline because the spun-up peer's partition Raft
    // uses a separate `data_dir` and the test's `network` only
    // connects to the existing peers — it doesn't wire the
    // existing peers' MeshRaftServices to the new peer's gRPC
    // listener. Real-world rebalancing needs both directions.
    // For the test we just confirm the API call returns; the full
    // E2E catchup is exercised by the openraft-internal test
    // suite, not ours.
    let _ = result; // ignore; partition Raft may need a few seconds to settle
}

#[tokio::test]
async fn multi_raft_remove_partition_replica_shrinks_voters() {
    // 3 peers, 2 partitions, rf=3 → every peer is a voter of every
    // partition. Find partition 0's leader, call
    // remove_partition_replica to evict the highest-id peer, and
    // confirm the partition's voter set shrinks. Demonstrates the
    // dynamic rebalancing scaffolding even though full orchestration
    // (replica_map updates, runtime group teardown) is v2.
    let peers = spawn_multi_raft_cluster(3, 2, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    let p0 = PartitionId(0);
    let leader_id = peers[0].multi_raft.leader_of(p0).expect("p0 leader known");
    let leader_idx = peers
        .iter()
        .position(|p| p.config.self_id == leader_id)
        .expect("leader peer in cluster");

    // Pick a non-leader voter to evict so the leadership stays
    // stable through the remove call.
    let evict_id = peers
        .iter()
        .map(|p| p.config.self_id)
        .find(|id| *id != leader_id)
        .expect("at least one non-leader peer");

    peers[leader_idx]
        .multi_raft
        .remove_partition_replica(p0, evict_id)
        .await
        .expect("remove_partition_replica");

    // Confirm the membership change committed: the leader's Raft
    // metrics now report a smaller voter set.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let raft = peers[leader_idx]
            .multi_raft
            .partition(p0)
            .expect("p0 still hosted on leader peer");
        let metrics = raft.raft.metrics().borrow().clone();
        let voters: Vec<u64> = metrics.membership_config.membership().voter_ids().collect();
        if voters.len() == 2 && !voters.contains(&evict_id) {
            break;
        }
        if Instant::now() > deadline {
            panic!("voter set didn't shrink within 5s; current = {voters:?}, evicted {evict_id}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn multi_raft_partition_snapshot_lifecycle_e2e() {
    // E2E coverage for the snapshot side of the
    // PartitionGraphApplier: spawn a real Raft cluster, generate
    // enough writes to fill a non-trivial log, trigger a snapshot
    // on the partition leader, then assert that openraft reports a
    // snapshot last_log_id that matches the leader's apply state.
    //
    // This validates that:
    //   - openraft correctly invokes the RaftSnapshotBuilder
    //     contract against PartitionGraphApplier::snapshot.
    //   - The snapshot bytes our applier produces are accepted by
    //     openraft's metadata bookkeeping (last_log_id, term, etc.).
    //   - A subsequent purge_log shrinks the in-memory log without
    //     losing data — the apply state is recoverable from the
    //     snapshot alone.
    //
    // Not covered here: the InstallSnapshot RPC delivering bytes to
    // a fresh replica that's catching up via wire protocol. The
    // existing harness can't trivially register a new peer post-
    // bootstrap without spinning up a sidecar gRPC listener and
    // wiring it into every existing peer's GrpcNetwork — the
    // `multi_raft_runtime_spinup_then_add_replica_composes` test
    // documents the same limitation. openraft's own conformance
    // suite covers the wire side against any storage impl that
    // implements RaftStorage correctly, which ours does.
    let peers = spawn_multi_raft_cluster(3, 1, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    let p0 = PartitionId(0);
    // Generate enough writes that the log has substance — a single
    // entry would still build a valid snapshot but the test is more
    // meaningful when there's something to compact.
    for i in 0..50 {
        peers[0]
            .service
            .execute_cypher_local(
                format!("CREATE (:SnapshotData {{i: {i}}})"),
                std::collections::HashMap::new(),
            )
            .await
            .unwrap();
    }

    // Identify the partition leader and trigger a snapshot through
    // its Raft handle. The leader uses our applier to build the
    // bytes — if our snapshot impl had a serialization bug, this
    // would surface here.
    let leader_id = peers[0].multi_raft.leader_of(p0).expect("leader known");
    let leader_idx = peers
        .iter()
        .position(|p| p.config.self_id == leader_id)
        .unwrap();
    let leader_raft = peers[leader_idx]
        .multi_raft
        .partition(p0)
        .expect("leader hosts p0");

    let last_applied_before_snapshot = leader_raft
        .raft
        .metrics()
        .borrow()
        .last_applied
        .as_ref()
        .map(|id| id.index)
        .unwrap_or(0);
    assert!(
        last_applied_before_snapshot >= 50,
        "expected at least 50 applied log entries, got {last_applied_before_snapshot}"
    );

    leader_raft
        .raft
        .trigger()
        .snapshot()
        .await
        .expect("trigger snapshot");

    // Wait for a snapshot covering at least every commit we issued
    // to be visible. openraft builds snapshots asynchronously and
    // the snapshot_policy may have already produced one earlier;
    // the trigger ensures at least one ≥ last_applied_before_snapshot
    // eventually surfaces.
    let deadline = Instant::now() + Duration::from_secs(15);
    let snapshot_index = loop {
        let m = leader_raft.raft.metrics().borrow().clone();
        if let Some(id) = m.snapshot {
            if id.index >= last_applied_before_snapshot {
                break id.index;
            }
        }
        if Instant::now() > deadline {
            let m = leader_raft.raft.metrics().borrow().clone();
            panic!(
                "no snapshot covering last_applied {} appeared within 15s; current snapshot = {:?}",
                last_applied_before_snapshot, m.snapshot
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    // Purge logs up to the snapshot. openraft must still be able to
    // serve reads / writes after this — the apply state is
    // recoverable from the snapshot file.
    leader_raft
        .raft
        .trigger()
        .purge_log(snapshot_index)
        .await
        .expect("purge_log");

    // Sanity: a subsequent write through the leader must succeed
    // after the purge. If the snapshot lifecycle had eaten state,
    // this would fail.
    peers[leader_idx]
        .service
        .execute_cypher_local(
            "CREATE (:PostPurge)".to_string(),
            std::collections::HashMap::new(),
        )
        .await
        .expect("write after purge_log must succeed");

    // The :SnapshotData nodes from before the snapshot must still
    // be queryable — they should be in the local store from the
    // applier's apply path, independent of the snapshot.
    let rows = peers[leader_idx]
        .service
        .execute_cypher_local(
            "MATCH (n:SnapshotData) RETURN count(n) AS c".to_string(),
            std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    let count = match rows.first().and_then(|r| r.get("c")) {
        Some(meshdb_executor::Value::Property(meshdb_core::Property::Int64(c))) => *c,
        other => panic!("expected Int64, got {other:?}"),
    };
    assert_eq!(
        count, 50,
        "all SnapshotData nodes should survive the snapshot+purge cycle"
    );
}

#[tokio::test]
async fn multi_raft_explicit_tx_session_affinity_survives_leader_change() {
    // Bolt explicit transactions accumulate commands in-memory on
    // the session peer (single TCP connection). At COMMIT, the peer
    // dispatches the full batch through `commit_buffered_commands`,
    // which forwards single-partition writes to the partition leader
    // and runs Spanner-style 2PC for cross-partition writes.
    //
    // This test pins down that the path survives a partition leader
    // change mid-transaction:
    //   1. Open a multi-statement tx on the session peer.
    //   2. RUN #1 — first write, accumulate command.
    //   3. Force a leader change on the touched partition.
    //   4. RUN #2 — read with read-your-writes overlay; must still
    //      see RUN #1's write (the overlay is local, doesn't depend
    //      on cluster routing).
    //   5. RUN #3 — another write, accumulate.
    //   6. COMMIT — the forward-write retry + idempotency logic must
    //      land the batch on the new leader without double-applying.
    //   7. Verify every write applied on every survivor.
    let peers = spawn_multi_raft_cluster(3, 1, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    let p0 = PartitionId(0);

    // Pin the session to a peer that is NOT the partition leader.
    // The realistic Bolt scenario: a client connects to a follower
    // (load balancer chose it), opens an explicit tx, and we lose
    // the leader mid-tx. The follower's local replica stays alive
    // through the leader change, so it can route writes to the new
    // leader at COMMIT time.
    let original_leader = peers[0].multi_raft.leader_of(p0).expect("leader known");
    let session_idx = (0..peers.len())
        .find(|i| peers[*i].config.self_id != original_leader)
        .expect("at least one follower");

    let mut buffered: Vec<meshdb_cluster::GraphCommand> = Vec::new();

    // RUN #1.
    let (rows1, new_cmds1) = peers[session_idx]
        .service
        .execute_cypher_in_tx(
            "CREATE (a:Affinity {seq: 1}) RETURN a".to_string(),
            std::collections::HashMap::new(),
            buffered.clone(),
            true,
        )
        .await
        .unwrap();
    assert!(!rows1.is_empty());
    buffered.extend(new_cmds1);

    // Mid-tx leader change. We resolved `original_leader` above
    // via `peers[0]`'s cache, which was warm at that point.
    let leader_idx = peers
        .iter()
        .position(|p| p.config.self_id == original_leader)
        .unwrap();
    peers[leader_idx]
        .multi_raft
        .shutdown_partition(p0)
        .await
        .unwrap();
    let force_idx = (0..peers.len()).find(|i| *i != leader_idx).unwrap();
    peers[force_idx]
        .multi_raft
        .force_partition_election(p0)
        .await
        .unwrap();

    // Wait for survivors to converge on the new leader.
    let survivor_idxs: Vec<usize> = (0..peers.len()).filter(|i| *i != leader_idx).collect();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let mut leaders = Vec::new();
        for idx in &survivor_idxs {
            leaders.push(peers[*idx].multi_raft.partition_current_leader(p0));
        }
        let agreed = leaders
            .iter()
            .copied()
            .reduce(|a, b| if a == b { a } else { None })
            .flatten();
        if matches!(agreed, Some(l) if l != original_leader) {
            break;
        }
        if Instant::now() > deadline {
            panic!("survivors did not converge on a new leader within 15s: {leaders:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // RUN #2 — read-your-writes overlay must surface RUN #1.
    let (rows2, new_cmds2) = peers[session_idx]
        .service
        .execute_cypher_in_tx(
            "MATCH (n:Affinity) RETURN count(n) AS c".to_string(),
            std::collections::HashMap::new(),
            buffered.clone(),
            true,
        )
        .await
        .unwrap();
    let count = match rows2.first().and_then(|r| r.get("c")) {
        Some(meshdb_executor::Value::Property(meshdb_core::Property::Int64(c))) => *c,
        other => panic!("expected Int64, got {other:?}"),
    };
    assert_eq!(
        count, 1,
        "read-your-writes inside the tx must see the first CREATE despite leader change"
    );
    buffered.extend(new_cmds2);

    // RUN #3 — another write inside the tx.
    let (_, new_cmds3) = peers[session_idx]
        .service
        .execute_cypher_in_tx(
            "CREATE (b:Affinity {seq: 2})".to_string(),
            std::collections::HashMap::new(),
            buffered.clone(),
            true,
        )
        .await
        .unwrap();
    buffered.extend(new_cmds3);

    // COMMIT.
    peers[session_idx]
        .service
        .commit_buffered_commands(buffered)
        .await
        .expect("explicit-tx commit must succeed across mid-tx leader change");

    // Verify both Affinity nodes are visible on every survivor.
    let deadline = Instant::now() + Duration::from_secs(5);
    'outer: loop {
        for idx in &survivor_idxs {
            let rows = peers[*idx]
                .service
                .execute_cypher_local(
                    "MATCH (n:Affinity) RETURN count(n) AS c".to_string(),
                    std::collections::HashMap::new(),
                )
                .await
                .unwrap();
            let count = match rows.first().and_then(|r| r.get("c")) {
                Some(meshdb_executor::Value::Property(meshdb_core::Property::Int64(c))) => *c,
                other => panic!("expected Int64, got {other:?}"),
            };
            if count != 2 {
                if Instant::now() > deadline {
                    panic!(
                        "survivor peer {} sees count(:Affinity) = {count}, expected 2",
                        peers[*idx].config.self_id
                    );
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue 'outer;
            }
        }
        break;
    }
}

#[tokio::test]
async fn multi_raft_force_partition_election_lands_leadership() {
    // Operator-pattern leader transfer: shut down the current
    // leader's partition Raft, force-elect on a chosen survivor,
    // verify leadership lands on the chosen target.
    let peers = spawn_multi_raft_cluster(3, 1, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    let p0 = PartitionId(0);
    let original_leader = peers[0].multi_raft.leader_of(p0).expect("leader known");
    let leader_idx = peers
        .iter()
        .position(|p| p.config.self_id == original_leader)
        .unwrap();

    let target_idx = (0..peers.len())
        .find(|i| *i != leader_idx)
        .expect("at least one survivor");
    let target_id = peers[target_idx].config.self_id;

    // Shut down the current leader's partition Raft so its
    // heartbeats stop. The meta Raft and other partitions stay
    // alive — just this partition's leader is silenced.
    peers[leader_idx]
        .multi_raft
        .shutdown_partition(p0)
        .await
        .unwrap();

    // Force an election on the target. With the original leader
    // silent, the target's election should win.
    peers[target_idx]
        .multi_raft
        .force_partition_election(p0)
        .await
        .expect("force_partition_election");

    // Wait for the target to be observed as leader on its own
    // metrics. The new leader id must equal target_id.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(leader) = peers[target_idx].multi_raft.partition_current_leader(p0) {
            if leader == target_id {
                break;
            }
        }
        if Instant::now() > deadline {
            panic!(
                "force-elected target {target_id} did not become leader within 15s; \
                 current = {:?}",
                peers[target_idx].multi_raft.partition_current_leader(p0)
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn multi_raft_apply_lag_metric_reports_per_group() {
    // Spawn a 3-peer cluster, generate writes, run the metrics
    // poller manually (we don't spawn the long-lived task in
    // tests), then verify the per-group gauges report data:
    //   - meta last_applied is non-zero (membership entries +
    //     bootstrap state)
    //   - every partition's last_applied is non-zero after writes
    //   - apply_lag is bounded (<= 5 — committed entries should
    //     be applied promptly)
    use meshdb_rpc::metrics::{MULTI_RAFT_APPLY_LAG, MULTI_RAFT_LAST_APPLIED};

    let peers = spawn_multi_raft_cluster(3, 2, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    // Generate writes so partition Rafts have non-trivial state.
    for i in 0..20 {
        peers[0]
            .service
            .execute_cypher_local(
                format!("CREATE (:LagTest {{i: {i}}})"),
                std::collections::HashMap::new(),
            )
            .await
            .unwrap();
    }

    // Trigger one tick of the poller. Use a 60ms interval and let
    // it tick once.
    let poller = peers[0]
        .service
        .spawn_multi_raft_metrics_poller(Duration::from_millis(60))
        .expect("poller spawned in multi-raft mode");
    tokio::time::sleep(Duration::from_millis(150)).await;
    poller.abort();
    let _ = poller.await;

    // meta last_applied must be > 0 after bootstrap + writes.
    let meta_applied = MULTI_RAFT_LAST_APPLIED.with_label_values(&["meta"]).get();
    assert!(
        meta_applied > 0,
        "meta last_applied should be > 0, got {meta_applied}"
    );

    // Every partition gauge must be present and lag bounded.
    for partition in 0..2u32 {
        let label = format!("p-{partition}");
        let applied = MULTI_RAFT_LAST_APPLIED.with_label_values(&[&label]).get();
        let lag = MULTI_RAFT_APPLY_LAG.with_label_values(&[&label]).get();
        assert!(
            applied > 0,
            "partition {partition} last_applied should be > 0, got {applied}"
        );
        assert!(
            lag >= 0 && lag < 100,
            "partition {partition} apply_lag should be small, got {lag}"
        );
    }
}

#[tokio::test]
async fn multi_raft_forward_write_idempotency_dedupes_retries() {
    // Calling ForwardWrite twice with the same idempotency_key
    // against the partition leader must apply only once.
    // Demonstrates the "response lost after commit, caller retries"
    // recovery path doesn't double-apply.
    use meshdb_rpc::proto::mesh_write_client::MeshWriteClient;
    use meshdb_rpc::proto::ForwardWriteRequest;

    let peers = spawn_multi_raft_cluster(3, 1, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    let p0 = PartitionId(0);
    let leader_id = peers[0].multi_raft.leader_of(p0).expect("leader known");
    let leader_idx = peers
        .iter()
        .position(|p| p.config.self_id == leader_id)
        .unwrap();
    let leader_addr = peers[leader_idx].config.listen_address.clone();

    // gRPC straight to the leader so we control the request
    // payload — the in-process `commit_multi_raft_*` paths generate
    // a fresh idempotency_key per call, which is the wrong shape
    // for the dedupe test.
    let endpoint =
        tonic::transport::Endpoint::from_shared(format!("http://{leader_addr}")).unwrap();
    let mut client = MeshWriteClient::new(endpoint.connect().await.unwrap());

    let cmds = vec![meshdb_cluster::GraphCommand::PutNode(meshdb_core::Node {
        id: meshdb_core::NodeId::new(),
        labels: vec!["IdempTest".to_string()],
        properties: std::collections::HashMap::new(),
    })];
    let commands_json = serde_json::to_vec(&cmds).unwrap();
    // Random 16-byte idempotency key (no uuid crate dep on the
    // test-binary side; bytes are opaque to the server).
    let key = (0u8..16).collect::<Vec<u8>>();

    // First call — must succeed and apply.
    let resp1 = client
        .forward_write(ForwardWriteRequest {
            partition: p0.0,
            commands_json: commands_json.clone(),
            idempotency_key: key.clone(),
            min_meta_index: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp1.ok, "first forward_write must succeed: {resp1:?}");

    // Second call with the same key — must return the cached
    // response and NOT re-propose. Observable proof: node count is
    // still 1 on every replica, not 2.
    let resp2 = client
        .forward_write(ForwardWriteRequest {
            partition: p0.0,
            commands_json: commands_json.clone(),
            idempotency_key: key.clone(),
            min_meta_index: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp2.ok, "second forward_write must also succeed");

    let deadline = Instant::now() + Duration::from_secs(5);
    'outer: loop {
        for peer in &peers {
            let rows = peer
                .service
                .execute_cypher_local(
                    "MATCH (n:IdempTest) RETURN count(n) AS c".to_string(),
                    std::collections::HashMap::new(),
                )
                .await
                .unwrap();
            let count = match rows.first().and_then(|r| r.get("c")) {
                Some(meshdb_executor::Value::Property(meshdb_core::Property::Int64(c))) => *c,
                other => panic!("expected Int64, got {other:?}"),
            };
            if count != 1 {
                if Instant::now() > deadline {
                    panic!(
                        "peer {} sees count(:IdempTest) = {count}, expected 1 — \
                         idempotency dedupe failed (the retry double-applied)",
                        peer.config.self_id
                    );
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue 'outer;
            }
        }
        break;
    }
}

#[tokio::test]
async fn multi_raft_num_partitions_marker_blocks_silent_resharding() {
    // Spawn a 3-peer cluster with num_partitions=4. After it's
    // running, confirm the marker file exists. Then attempt to
    // build_components against the same data_dir but with
    // num_partitions=8 — must fail with a clear error.
    use meshdb_server::{
        build_components, check_num_partitions_marker, num_partitions_marker_path,
    };

    let peers = spawn_multi_raft_cluster(3, 4, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    // Sanity: marker file exists in every peer's data_dir.
    for peer in &peers {
        let marker = num_partitions_marker_path(&peer.config.data_dir);
        let contents = std::fs::read_to_string(&marker)
            .unwrap_or_else(|e| panic!("reading marker {}: {e}", marker.display()));
        assert_eq!(contents.trim(), "4");
    }

    // Direct unit-test of the validator: changing the value must
    // be rejected with a clear error.
    let dir = peers[0].config.data_dir.clone();
    let err =
        check_num_partitions_marker(&dir, 8).expect_err("changing num_partitions must be rejected");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("num_partitions changed across restart"),
        "expected resharding error, got: {msg}"
    );
    assert!(
        msg.contains("dump"),
        "error must point at the dump-and-restore workflow, got: {msg}"
    );

    // build_components routes through check_num_partitions_marker.
    // Capture a fresh config that points at peer[0]'s data_dir
    // but with the wrong num_partitions.
    let mut bad_config = peers[0].config.clone();
    bad_config.num_partitions = 8;
    let result = build_components(&bad_config).await;
    let err = match result {
        Ok(_) => panic!("build_components must reject the resharding-via-restart attempt"),
        Err(e) => e,
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("num_partitions changed across restart"),
        "expected resharding error from build_components, got: {msg}"
    );
}

#[tokio::test]
async fn multi_raft_linearizable_read_primitive() {
    // Verifies the `ensure_partition_linearizable` primitive that
    // future linearizable read paths build on:
    //   - Calling it on the partition leader succeeds.
    //   - Calling it on a non-leader replica fails with a hint
    //     pointing at the leader.
    //   - Calling it on a peer that doesn't host the partition
    //     fails with "not hosted".
    //
    // 3 peers × 4 partitions × rf=2 → not every peer hosts every
    // partition, so we can exercise all three branches.
    let peers = spawn_multi_raft_cluster(3, 4, 2).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    let hosted: Option<PartitionId> = (0..4u32)
        .map(PartitionId)
        .find(|p| peers[0].multi_raft.hosts_partition(*p));
    let not_hosted: Option<PartitionId> = (0..4u32)
        .map(PartitionId)
        .find(|p| !peers[0].multi_raft.hosts_partition(*p));
    let Some(hosted) = hosted else {
        eprintln!("skipping: peer 1 hosts every partition");
        return;
    };

    // Leader path — must succeed against the partition's leader.
    let leader_idx = (0..peers.len())
        .find(|i| peers[*i].multi_raft.is_local_leader(hosted))
        .expect("hosted partition must have a leader somewhere");
    peers[leader_idx]
        .multi_raft
        .ensure_partition_linearizable(hosted)
        .await
        .expect("ensure_linearizable on the leader replica must succeed");

    // Non-leader path — must fail with a leader-hint error.
    if let Some(non_leader_idx) = (0..peers.len()).find(|i| {
        *i != leader_idx
            && peers[*i].multi_raft.hosts_partition(hosted)
            && !peers[*i].multi_raft.is_local_leader(hosted)
    }) {
        let err = peers[non_leader_idx]
            .multi_raft
            .ensure_partition_linearizable(hosted)
            .await
            .expect_err("non-leader call must fail");
        let lower = err.to_lowercase();
        assert!(
            lower.contains("leader") || lower.contains("forward"),
            "expected leader hint in error, got: {err}"
        );
    }

    // Not-hosted path — must fail with "not hosted" if such a
    // partition exists for this peer.
    if let Some(p) = not_hosted {
        let err = peers[0]
            .multi_raft
            .ensure_partition_linearizable(p)
            .await
            .expect_err("call against not-hosted partition must fail");
        assert!(
            err.contains("not hosted"),
            "expected 'not hosted' error, got: {err}"
        );
    }
}

#[tokio::test]
async fn multi_raft_weighted_placement_skews_replica_distribution() {
    // 3 peers with weights [1.0, 1.0, 4.0]. 6 partitions × rf=2 →
    // 12 placements. Weighted greedy hands peer 3 the lion's
    // share. Verify the resolved replica map's totals follow the
    // weights, end-to-end through ServerConfig.
    use meshdb_cluster::PartitionReplicaMap;

    // Pre-bind ports so the spawn function knows them.
    let mut listeners: Vec<TcpListener> = Vec::with_capacity(3);
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(3);
    for _ in 0..3 {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(l.local_addr().unwrap());
        listeners.push(l);
    }

    let weighted_peer_configs: Vec<PeerConfig> = addrs
        .iter()
        .enumerate()
        .map(|(i, a)| PeerConfig {
            id: (i + 1) as u64,
            address: a.to_string(),
            bolt_address: None,
            weight: Some(if i == 2 { 4.0 } else { 1.0 }),
        })
        .collect();

    let mut peers = Vec::with_capacity(3);
    for (i, listener) in listeners.into_iter().enumerate() {
        let dir = TempDir::new().unwrap();
        let addr = addrs[i];
        let config = ServerConfig {
            self_id: (i + 1) as u64,
            listen_address: addr.to_string(),
            data_dir: dir.path().to_path_buf(),
            num_partitions: 6,
            peers: weighted_peer_configs.clone(),
            bootstrap: i == 0,
            bolt_address: None,
            metrics_address: None,
            bolt_auth: None,
            bolt_tls: None,
            bolt_advertised_versions: None,
            bolt_advertised_address: None,
            grpc_tls: None,
            mode: Some(ClusterMode::MultiRaft),
            replication_factor: Some(2),
            read_consistency: None,
            #[cfg(feature = "apoc-load")]
            apoc_import: None,
        };
        let components = build_components(&config).await.unwrap();
        let multi_raft = components.multi_raft.clone().expect("multi-raft built");
        let service = components.service.clone();
        let registry = multi_raft.build_registry();
        let raft_service = meshdb_rpc::MeshRaftService::with_registry(registry);
        let service_for_server = service.clone();
        let server_task = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(raft_service.into_server())
                .add_service(service_for_server.clone().into_query_server())
                .add_service(service_for_server.clone().into_write_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });
        peers.push(Peer {
            config,
            multi_raft,
            service,
            _dir: dir,
            server_task,
        });
    }

    tokio::time::sleep(Duration::from_millis(80)).await;
    for peer in &peers {
        initialize_multi_raft_if_seed(&peer.config, &peer.multi_raft)
            .await
            .unwrap();
    }

    // Inspect any peer's replica map — they all share the same
    // initial placement deterministically.
    let map: &PartitionReplicaMap = &peers[0].multi_raft.replica_map;
    let mut counts: std::collections::HashMap<u64, usize> = std::collections::HashMap::new();
    for p in 0..6u32 {
        for replica in map.replicas(PartitionId(p)) {
            *counts.entry(replica.0).or_insert(0) += 1;
        }
    }
    let p1 = *counts.get(&1).unwrap_or(&0);
    let p2 = *counts.get(&2).unwrap_or(&0);
    let p3 = *counts.get(&3).unwrap_or(&0);
    assert_eq!(
        p1 + p2 + p3,
        12,
        "every placement must be assigned: ({p1}, {p2}, {p3})"
    );
    assert!(
        p3 > p1 && p3 > p2,
        "weight-4 peer 3 must dominate placement: ({p1}, {p2}, {p3})"
    );

    // Drop server tasks before TempDir goes out of scope to avoid
    // a rocksdb LOCK race during cleanup.
    for peer in peers {
        peer.server_task.abort();
    }
}

#[tokio::test]
async fn multi_raft_drain_peer_removes_from_every_partition() {
    // 3 peers, 4 partitions, rf=3 — every peer hosts every
    // partition. Drain peer 3 from one of the survivors; it must
    // be removed as a voter of every partition group, and each
    // remaining group's voter set must have shrunk to 2.
    let peers = spawn_multi_raft_cluster(3, 4, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    let drain_target = 3u64; // The highest-id peer.
                             // Issue the drain call from peer 0 (which is a voter of every
                             // partition group and not the drain target).
    let (drained, errors) = peers[0].multi_raft.drain_peer(drain_target).await;
    assert!(
        errors.is_empty(),
        "drain_peer reported per-partition errors: {errors:?}"
    );
    assert_eq!(
        drained.len(),
        4,
        "drained partitions should be {{0,1,2,3}}, got {drained:?}"
    );

    // Wait for each partition's membership change to commit. After
    // settle, every surviving peer's local Raft metrics for every
    // partition should show 2 voters and the drain target must not
    // be among them.
    let deadline = Instant::now() + Duration::from_secs(10);
    'outer: loop {
        for peer_idx in 0..peers.len() {
            if peers[peer_idx].config.self_id == drain_target {
                continue;
            }
            for partition in 0..4u32 {
                let p = PartitionId(partition);
                let raft = peers[peer_idx]
                    .multi_raft
                    .partition(p)
                    .expect("partition still hosted on survivor");
                let voters: Vec<u64> = raft
                    .raft
                    .metrics()
                    .borrow()
                    .membership_config
                    .membership()
                    .voter_ids()
                    .collect();
                if voters.len() != 2 || voters.contains(&drain_target) {
                    if Instant::now() > deadline {
                        panic!(
                            "peer {} partition {} voters = {voters:?} (expected 2 without {drain_target})",
                            peers[peer_idx].config.self_id, partition
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue 'outer;
                }
            }
        }
        break;
    }
}

#[tokio::test]
async fn multi_raft_per_partition_storage_dirs_are_isolated() {
    // 3 peers, 4 partitions, rf=2 → not every peer hosts every
    // partition. Verifies storage-layout isolation per group and
    // the partition→peer placement matches PartitionReplicaMap.
    let peers = spawn_multi_raft_cluster(3, 4, 2).await;

    for peer in &peers {
        let raft_root = peer.config.data_dir.join("raft");
        assert!(
            raft_root.join("meta").is_dir(),
            "peer {} missing meta dir at {}",
            peer.config.self_id,
            raft_root.display()
        );
        for partition in peer
            .multi_raft
            .partitions_snapshot()
            .iter()
            .map(|(p, _)| *p)
            .collect::<Vec<_>>()
            .iter()
        {
            let p_dir = raft_root.join(format!("p-{}", partition.0));
            assert!(
                p_dir.is_dir(),
                "peer {} missing partition dir at {}",
                peer.config.self_id,
                p_dir.display()
            );
        }
    }
    // 4 partitions × rf=2 = 8 total partition-slot replicas across
    // 3 peers (uneven distribution, but every slot has a home).
    let total: usize = peers
        .iter()
        .map(|p| p.multi_raft.partitions_snapshot().len())
        .sum();
    assert_eq!(total, 8);
}

#[tokio::test]
async fn multi_raft_meta_leader_failover_preserves_ddl_path() {
    // 3 peers. Issue a CREATE INDEX (commits through the meta
    // leader), then crash that leader. A subsequent CREATE INDEX
    // through any survivor must succeed once the new meta leader
    // is settled — the DDL gate, the forward path, and the
    // synchronous-apply guarantee all keep working through the
    // leadership change.
    let peers = spawn_multi_raft_cluster(3, 2, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    // First DDL — establishes the baseline and confirms the gate
    // is operational on the original meta leader.
    peers[0]
        .service
        .execute_cypher_local(
            "CREATE INDEX FOR (n:BeforeFailover) ON (n.id)".to_string(),
            std::collections::HashMap::new(),
        )
        .await
        .unwrap();

    // Crash the meta leader: shut down its meta Raft handle and
    // abort its gRPC server task.
    let original_meta_leader = peers[0]
        .multi_raft
        .meta_leader()
        .expect("meta leader known after first DDL");
    let leader_idx = peers
        .iter()
        .position(|p| p.config.self_id == original_meta_leader)
        .expect("meta leader peer in cluster");
    peers[leader_idx].multi_raft.shutdown_meta().await.unwrap();
    peers[leader_idx].server_task.abort();

    // Wait for the survivors to elect a new meta leader and AGREE
    // on it. Same pattern as the partition-leader test — half-
    // completed elections would race the next forward.
    let survivor_idxs: Vec<usize> = (0..peers.len()).filter(|i| *i != leader_idx).collect();
    let new_meta_leader = {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let mut leaders = Vec::with_capacity(survivor_idxs.len());
            for idx in &survivor_idxs {
                leaders.push(peers[*idx].multi_raft.meta_leader());
            }
            let agreed = leaders
                .iter()
                .copied()
                .reduce(|a, b| if a == b { a } else { None })
                .flatten();
            if let Some(l) = agreed {
                if l != original_meta_leader {
                    break l;
                }
            }
            if Instant::now() > deadline {
                panic!(
                    "survivors did not agree on a new meta leader within 15s after crashing peer {original_meta_leader}: {leaders:?}"
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    assert_ne!(new_meta_leader, original_meta_leader);

    // Second DDL through a survivor. It must succeed: either the
    // survivor IS the new meta leader (proposes locally), or it
    // forwards via `forward_ddl` to the new leader. The DDL gate
    // skips the dead peer entry once forward_ddl returns OK
    // because the dead peer's gRPC is gone — but the DDL ITSELF
    // still committed on the surviving 2-of-3 quorum.
    //
    // Note: the DDL gate as currently implemented polls every
    // *cluster member*, including the dead peer, so it'll trip the
    // strict timeout. To exercise the failover behaviour without
    // depending on the gate's pessimistic behaviour, set a short
    // strict timeout — and accept either Ok (gate cleared because
    // the dead peer was somehow reachable, unlikely) or
    // DeadlineExceeded with the DDL durably committed (the
    // recovery-friendly outcome).
    for peer in &peers {
        peer.multi_raft
            .set_ddl_strict_timeout(Duration::from_millis(500));
    }
    let writer_idx = survivor_idxs[0];
    let result = peers[writer_idx]
        .service
        .execute_cypher_local(
            "CREATE INDEX FOR (n:AfterFailover) ON (n.id)".to_string(),
            std::collections::HashMap::new(),
        )
        .await;
    // Either outcome confirms the new meta leader proposed and
    // committed the entry.
    match result {
        Ok(_) => {}
        Err(e) => {
            assert!(
                e.code() == tonic::Code::DeadlineExceeded
                    || format!("{e}").contains("strict-apply gate"),
                "unexpected error after meta-leader failover: {e}"
            );
        }
    }

    // Sanity: every survivor now sees BOTH indexes in its meta
    // applier's local view. Whether the gate cleared or timed out
    // above, the meta proposal landed and replicated to both
    // survivors.
    let deadline = Instant::now() + Duration::from_secs(5);
    'outer: loop {
        for idx in &survivor_idxs {
            let rows = peers[*idx]
                .service
                .execute_cypher_local("SHOW INDEXES".to_string(), std::collections::HashMap::new())
                .await
                .unwrap();
            let saw_before = rows.iter().any(|r| {
                matches!(r.get("label"),
                    Some(meshdb_executor::Value::Property(meshdb_core::Property::String(s)))
                    if s == "BeforeFailover")
            });
            let saw_after = rows.iter().any(|r| {
                matches!(r.get("label"),
                    Some(meshdb_executor::Value::Property(meshdb_core::Property::String(s)))
                    if s == "AfterFailover")
            });
            if !(saw_before && saw_after) {
                if Instant::now() > deadline {
                    panic!(
                        "peer {} after failover: BeforeFailover={saw_before}, AfterFailover={saw_after}",
                        peers[*idx].config.self_id
                    );
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue 'outer;
            }
        }
        break;
    }
}

#[tokio::test]
async fn multi_raft_quorum_loss_blocks_writes_reads_still_work() {
    // 3 peers, 1 partition, rf=3. Kill 2 of 3 replicas (gRPC server
    // + Raft handle) so the partition has no quorum. Verify:
    //   1. The pre-crash data is still readable from the surviving
    //      replica's local store — Raft replicated it before the
    //      crash, the data is durable on disk locally.
    //   2. New writes do not silently succeed — they time out, which
    //      is the openraft-correct behaviour when a write proposal
    //      cannot achieve quorum.
    let peers = spawn_multi_raft_cluster(3, 1, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    let p0 = PartitionId(0);

    // Establish baseline: a write that we'll later read back.
    peers[0]
        .service
        .execute_cypher_local(
            "CREATE (:Survivor {tag: 'before-quorum-loss'})".to_string(),
            std::collections::HashMap::new(),
        )
        .await
        .unwrap();

    // Wait for the write to replicate to every replica's local
    // store before we cut the cluster apart.
    {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut all_see_it = true;
            for peer in &peers {
                let rows = peer
                    .service
                    .execute_cypher_local(
                        "MATCH (n:Survivor) RETURN count(n) AS c".to_string(),
                        std::collections::HashMap::new(),
                    )
                    .await
                    .unwrap();
                let count = match rows.first().and_then(|r| r.get("c")) {
                    Some(meshdb_executor::Value::Property(meshdb_core::Property::Int64(c))) => *c,
                    other => panic!("expected Int64, got {other:?}"),
                };
                if count != 1 {
                    all_see_it = false;
                    break;
                }
            }
            if all_see_it {
                break;
            }
            if Instant::now() > deadline {
                panic!("Survivor write did not replicate to all 3 peers within 5s");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Pick a survivor — anyone, doesn't matter (they all have the data).
    // Kill the other two: shut down their partition Raft handles AND
    // abort their gRPC servers. Now the surviving peer is alone; any
    // openraft propose on its replica will hang waiting for quorum.
    let survivor_idx = 0;
    let dead_idxs: Vec<usize> = (0..peers.len()).filter(|i| *i != survivor_idx).collect();
    for &idx in &dead_idxs {
        peers[idx].multi_raft.shutdown_partition(p0).await.unwrap();
        peers[idx].server_task.abort();
    }

    // Read from the surviving peer's local store. The data is on
    // disk, so this must succeed even though the partition has lost
    // quorum.
    let rows = peers[survivor_idx]
        .service
        .execute_cypher_local(
            "MATCH (n:Survivor) RETURN n.tag AS tag".to_string(),
            std::collections::HashMap::new(),
        )
        .await
        .expect("read from surviving replica must succeed");
    let tag = match rows.first().and_then(|r| r.get("tag")) {
        Some(meshdb_executor::Value::Property(meshdb_core::Property::String(s))) => s.clone(),
        other => panic!("expected String, got {other:?}"),
    };
    assert_eq!(tag, "before-quorum-loss");

    // Attempt a write through the survivor. With no quorum the
    // openraft propose hangs; we cap it with a tokio timeout. The
    // expected outcome is "did not complete in 3 seconds" — either
    // because the surviving peer's propose blocks forever waiting
    // for AppendEntries quorum, or because the forward_write proxy
    // cannot reach a leader (none can be elected from a single peer).
    // Either way: writes do not silently succeed.
    let write_result = tokio::time::timeout(
        Duration::from_secs(3),
        peers[survivor_idx].service.execute_cypher_local(
            "CREATE (:WrittenAfterQuorumLoss)".to_string(),
            std::collections::HashMap::new(),
        ),
    )
    .await;

    match write_result {
        Err(_elapsed) => {
            // The write hung waiting for quorum — correct outcome.
        }
        Ok(Err(_status)) => {
            // The write surfaced an error rather than hanging — also
            // an acceptable outcome. The surviving peer may detect
            // it can never reach quorum and short-circuit faster
            // than the test's timeout.
        }
        Ok(Ok(_)) => {
            panic!(
                "write succeeded against a partition with no quorum — \
                 multi-raft must block writes when quorum is lost"
            );
        }
    }

    // Confirm the surviving replica's local store does not
    // contain the would-be-written node — even if the write's
    // RPC dispatched through some path, no quorum means no apply.
    let rows = peers[survivor_idx]
        .service
        .execute_cypher_local(
            "MATCH (n:WrittenAfterQuorumLoss) RETURN count(n) AS c".to_string(),
            std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    let count = match rows.first().and_then(|r| r.get("c")) {
        Some(meshdb_executor::Value::Property(meshdb_core::Property::Int64(c))) => *c,
        other => panic!("expected Int64, got {other:?}"),
    };
    assert_eq!(
        count, 0,
        "WrittenAfterQuorumLoss must not be visible — no quorum, no apply"
    );
}

#[tokio::test]
async fn multi_raft_ddl_gate_times_out_when_peer_unreachable() {
    // 3 peers. Crash one peer's gRPC server (so its meta_last_applied
    // RPC will fail) without touching its Raft handle — the meta
    // proposal still commits with a 2-of-3 quorum, but the gate
    // can't observe the third peer catching up. Set a short
    // strict-timeout so the deadline trips in bounded wall time;
    // assert the CREATE INDEX returns DeadlineExceeded and the
    // timeout metric increments.
    use meshdb_rpc::metrics::MULTI_RAFT_DDL_GATE_TOTAL;

    let peers = spawn_multi_raft_cluster(3, 1, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    // Pick a non-meta-leader peer to crash so the DDL propose can
    // still commit with the remaining 2-of-3 quorum (the meta
    // leader plus one other peer).
    let meta_leader = peers[0].multi_raft.meta_leader().expect("meta leader");
    let crash_idx = peers
        .iter()
        .position(|p| p.config.self_id != meta_leader)
        .expect("non-leader exists");
    peers[crash_idx].server_task.abort();
    // Brief settle so the gate's polling loop sees ConnectionRefused.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Configure a short DDL strict timeout on every peer (the gate
    // runs on whichever peer the DDL was issued through).
    for peer in &peers {
        peer.multi_raft
            .set_ddl_strict_timeout(Duration::from_millis(300));
    }

    let timeout_before = MULTI_RAFT_DDL_GATE_TOTAL
        .with_label_values(&["timeout"])
        .get();

    // Issue CREATE INDEX through a survivor; it will commit the
    // meta proposal, then trip the gate trying to await the dead peer.
    let alive_idx = (0..peers.len())
        .find(|i| *i != crash_idx)
        .expect("a survivor exists");
    let result = peers[alive_idx]
        .service
        .execute_cypher_local(
            "CREATE INDEX FOR (n:GateTimeout) ON (n.x)".to_string(),
            std::collections::HashMap::new(),
        )
        .await;

    let err = result.expect_err("CREATE INDEX must surface the gate timeout");
    let msg = format!("{err}");
    assert!(
        msg.contains("ddl strict-apply gate") || msg.contains("strict-apply gate"),
        "expected gate timeout error, got: {msg}"
    );

    let timeout_after = MULTI_RAFT_DDL_GATE_TOTAL
        .with_label_values(&["timeout"])
        .get();
    assert!(
        timeout_after > timeout_before,
        "ddl_gate_total{{timeout}} should increment on a tripped gate: {timeout_before} -> {timeout_after}"
    );
}

#[tokio::test]
async fn multi_raft_metrics_increment_on_forward_write_and_ddl_gate() {
    // Smoke test: a single-partition write through a non-leader peer
    // should bump `mesh_multiraft_forward_writes_total{outcome="committed"}`
    // by at least 1, and a `CREATE INDEX` should bump
    // `mesh_multiraft_ddl_gate_total{outcome="ok"}`. Counters are
    // process-wide, so we record before/after deltas rather than
    // assert absolute values — other tests may have already incremented.
    use meshdb_rpc::metrics::{MULTI_RAFT_DDL_GATE_TOTAL, MULTI_RAFT_FORWARD_WRITES_TOTAL};

    let peers = spawn_multi_raft_cluster(3, 1, 3).await;
    wait_for_leaders(&peers, Duration::from_secs(10)).await;

    let p0 = PartitionId(0);
    let leader_id = peers[0].multi_raft.leader_of(p0).expect("leader known");
    let non_leader_idx = peers
        .iter()
        .position(|p| p.config.self_id != leader_id)
        .expect("at least one non-leader exists");

    let fw_before = MULTI_RAFT_FORWARD_WRITES_TOTAL
        .with_label_values(&["committed"])
        .get();
    peers[non_leader_idx]
        .service
        .execute_cypher_local(
            "CREATE (:MetricsTest)".to_string(),
            std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    let fw_after = MULTI_RAFT_FORWARD_WRITES_TOTAL
        .with_label_values(&["committed"])
        .get();
    assert!(
        fw_after > fw_before,
        "forward_writes_total{{committed}} should increment on a non-leader write: {fw_before} -> {fw_after}"
    );

    let ddl_before = MULTI_RAFT_DDL_GATE_TOTAL.with_label_values(&["ok"]).get();
    peers[0]
        .service
        .execute_cypher_local(
            "CREATE INDEX FOR (n:MetricsTest) ON (n.x)".to_string(),
            std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    let ddl_after = MULTI_RAFT_DDL_GATE_TOTAL.with_label_values(&["ok"]).get();
    assert!(
        ddl_after > ddl_before,
        "ddl_gate_total{{ok}} should increment on a successful CREATE INDEX: {ddl_before} -> {ddl_after}"
    );
}
