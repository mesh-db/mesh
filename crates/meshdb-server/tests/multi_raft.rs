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
        tokio::spawn(async move {
            Server::builder()
                .add_service(raft_service.into_server())
                .add_service(service_for_server.clone().into_query_server())
                .add_service(service_for_server.clone().into_write_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        peers.push(Peer {
            config,
            multi_raft,
            service,
            _dir: dir,
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
                if let Some(raft) = peer.multi_raft.partitions.get(&PartitionId(partition)) {
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
                        .partitions
                        .get(&PartitionId(0))
                        .and_then(|r| r.current_leader()),
                    peer.multi_raft
                        .partitions
                        .get(&PartitionId(1))
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
            for raft in peer.multi_raft.partitions.values() {
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
        for partition in peer.multi_raft.partitions.keys() {
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
    let total: usize = peers.iter().map(|p| p.multi_raft.partitions.len()).sum();
    assert_eq!(total, 8);
}
