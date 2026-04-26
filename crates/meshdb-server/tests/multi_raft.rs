//! Multi-raft mode smoke tests.
//!
//! These tests spin up small clusters in `mode = "multi-raft"` and
//! verify the per-group bootstrap path: storage layout, Raft handle
//! presence on each peer, leader election in every group.

use meshdb_cluster::PartitionId;
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
        // Build a fresh MeshRaftService from the same registry —
        // ServerComponents.raft_service was already consumed elsewhere
        // in production, but in tests we re-derive it.
        let registry = multi_raft.build_registry();
        let raft_service = meshdb_rpc::MeshRaftService::with_registry(registry);

        tokio::spawn(async move {
            Server::builder()
                .add_service(raft_service.into_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        peers.push(Peer {
            config,
            multi_raft,
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
