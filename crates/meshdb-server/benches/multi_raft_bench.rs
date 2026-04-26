//! End-to-end Cypher benchmarks through `mode = "multi-raft"`.
//!
//! Spins up a 3-peer cluster with rf=3 (every peer hosts every
//! partition), waits for leader election, and measures the cost of
//! commit paths an operator actually exercises:
//!
//! * single-partition write through the local leader
//!   (one Raft round-trip).
//! * single-partition write proxied via `forward_write` from a
//!   non-leader peer (one internal hop + one Raft round-trip).
//! * cross-partition write through Spanner-style 2PC
//!   (two Raft round-trips per partition).
//!
//! Run with:
//!     cargo bench -p meshdb-server --bench multi_raft_bench
//!     cargo bench -p meshdb-server --bench multi_raft_bench -- --quick
//!
//! Reports land in `target/criterion/<bench_name>/report/index.html`.
//! Cluster setup is ~500ms once per bench group; per-iteration cost
//! is just the Cypher write.

#![recursion_limit = "256"]

use criterion::{criterion_group, criterion_main, Criterion};
use meshdb_cluster::PartitionId;
use meshdb_rpc::{MeshService, MultiRaftCluster};
use meshdb_server::config::{ClusterMode, PeerConfig, ServerConfig};
use meshdb_server::{build_components, initialize_multi_raft_if_seed};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Bench harness for one peer of the cluster — kept alive (the
/// TempDir, the spawned server task) for the full run via the
/// `Drop` semantics of `Cluster`.
#[allow(dead_code)]
struct BenchPeer {
    addr: SocketAddr,
    config: ServerConfig,
    multi_raft: Arc<MultiRaftCluster>,
    service: MeshService,
    _dir: TempDir,
}

#[allow(dead_code)]
struct Cluster {
    peers: Vec<BenchPeer>,
}

async fn spawn_three_peer_cluster(num_partitions: u32, rf: usize) -> Cluster {
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
        let dir = TempDir::new().unwrap();
        let addr = addrs[i];
        let config = ServerConfig {
            self_id: (i + 1) as u64,
            listen_address: addr.to_string(),
            data_dir: dir.path().to_path_buf(),
            num_partitions,
            peers: peer_configs.clone(),
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
                .ok();
        });
        peers.push(BenchPeer {
            addr,
            config,
            multi_raft,
            service,
            _dir: dir,
        });
    }
    tokio::time::sleep(Duration::from_millis(80)).await;
    for peer in &peers {
        initialize_multi_raft_if_seed(&peer.config, &peer.multi_raft)
            .await
            .unwrap();
    }
    // Wait for every group to elect a leader.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let all = peers.iter().all(|p| {
            p.multi_raft.meta.current_leader().is_some()
                && p.multi_raft
                    .partitions_snapshot()
                    .iter()
                    .all(|(_, r)| r.current_leader().is_some())
        });
        if all {
            break;
        }
        if Instant::now() > deadline {
            panic!("multi-raft cluster failed to elect leaders within 10s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Cluster { peers }
}

fn bench_single_partition_write_through_leader(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let cluster = rt.block_on(spawn_three_peer_cluster(4, 3));
    // Pick the partition-0 leader so writes that hash to partition 0
    // go through the local-propose path.
    let p0 = PartitionId(0);
    let leader_id = cluster.peers[0].multi_raft.leader_of(p0).unwrap();
    let leader_idx = cluster
        .peers
        .iter()
        .position(|p| p.config.self_id == leader_id)
        .unwrap();
    let svc = cluster.peers[leader_idx].service.clone();

    c.bench_function("multi_raft_single_partition_local_leader_create", |b| {
        b.iter(|| {
            rt.block_on(async {
                svc.execute_cypher_local("CREATE (:Bench)".into(), HashMap::new())
                    .await
                    .unwrap()
            })
        });
    });
}

fn bench_single_partition_write_via_forward(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let cluster = rt.block_on(spawn_three_peer_cluster(4, 3));
    // Issue from a peer that's NOT the partition-0 leader so the
    // forward_write proxy hop fires.
    let p0 = PartitionId(0);
    let leader_id = cluster.peers[0].multi_raft.leader_of(p0).unwrap();
    let initiator = cluster
        .peers
        .iter()
        .find(|p| p.config.self_id != leader_id)
        .unwrap();
    let svc = initiator.service.clone();

    c.bench_function("multi_raft_single_partition_forward_create", |b| {
        b.iter(|| {
            rt.block_on(async {
                svc.execute_cypher_local("CREATE (:BenchFwd)".into(), HashMap::new())
                    .await
                    .unwrap()
            })
        });
    });
}

fn bench_cross_partition_write(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let cluster = rt.block_on(spawn_three_peer_cluster(4, 3));
    let svc = cluster.peers[0].service.clone();

    // 20 nodes across 4 partitions ≈ certain to hit the
    // cross-partition path (probability of all-same is 4^-19).
    let cypher = {
        let parts: Vec<String> = (0..20)
            .map(|i| format!("(:BenchCp {{idx: {i}}})"))
            .collect();
        format!("CREATE {} RETURN 0", parts.join(", "))
    };

    c.bench_function("multi_raft_cross_partition_2pc_create_20", |b| {
        b.iter(|| {
            let q = cypher.clone();
            rt.block_on(async { svc.execute_cypher_local(q, HashMap::new()).await.unwrap() })
        });
    });
}

criterion_group!(
    benches,
    bench_single_partition_write_through_leader,
    bench_single_partition_write_via_forward,
    bench_cross_partition_write,
);
criterion_main!(benches);
