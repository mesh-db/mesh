use mesh_cluster::raft::RaftCluster;
use mesh_cluster::ClusterCommand;
use mesh_cluster::PeerId;
use mesh_core::Node;
use std::sync::Arc;
use mesh_rpc::convert::{node_to_proto, uuid_to_proto};
use mesh_rpc::proto::mesh_query_client::MeshQueryClient;
use mesh_rpc::proto::mesh_write_client::MeshWriteClient;
use mesh_rpc::proto::{GetNodeRequest, HealthRequest, PutNodeRequest};
use mesh_server::config::{PeerConfig, ServerConfig};
use mesh_server::ServerComponents;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

#[test]
fn config_parses_minimal_single_node_toml() {
    let toml = r#"
        self_id = 1
        listen_address = "127.0.0.1:7001"
        data_dir = "/tmp/mesh"
    "#;
    let config = ServerConfig::from_toml_str(toml).unwrap();
    assert_eq!(config.self_id, 1);
    assert_eq!(config.listen_address, "127.0.0.1:7001");
    assert_eq!(config.data_dir, PathBuf::from("/tmp/mesh"));
    assert_eq!(config.num_partitions, 4); // default
    assert!(config.peers.is_empty());
}

#[test]
fn config_parses_multi_peer_toml() {
    let toml = r#"
        self_id = 1
        listen_address = "127.0.0.1:7001"
        data_dir = "/var/lib/mesh/node1"
        num_partitions = 8

        [[peers]]
        id = 1
        address = "127.0.0.1:7001"

        [[peers]]
        id = 2
        address = "127.0.0.1:7002"

        [[peers]]
        id = 3
        address = "127.0.0.1:7003"
    "#;
    let config = ServerConfig::from_toml_str(toml).unwrap();
    assert_eq!(config.num_partitions, 8);
    assert_eq!(config.peers.len(), 3);
    assert_eq!(config.peers[0].id, 1);
    assert_eq!(config.peers[2].address, "127.0.0.1:7003");
}

#[test]
fn config_rejects_unknown_fields() {
    let toml = r#"
        self_id = 1
        listen_address = "127.0.0.1:7001"
        data_dir = "/tmp/mesh"
        bogus_field = true
    "#;
    assert!(ServerConfig::from_toml_str(toml).is_err());
}

#[test]
fn config_peer_rejects_unknown_fields() {
    let toml = r#"
        self_id = 1
        listen_address = "127.0.0.1:7001"
        data_dir = "/tmp/mesh"

        [[peers]]
        id = 1
        address = "127.0.0.1:7001"
        bogus = "value"
    "#;
    assert!(ServerConfig::from_toml_str(toml).is_err());
}

struct Harness {
    address: String,
    _dir: TempDir,
}

async fn spawn_single_node_server() -> Harness {
    let dir = TempDir::new().unwrap();
    let config = ServerConfig {
        self_id: 1,
        listen_address: "127.0.0.1:0".into(),
        data_dir: dir.path().to_path_buf(),
        num_partitions: 4,
        peers: vec![],
        bootstrap: false,
    };

    let service = mesh_server::build_service(&config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(service.clone().into_query_server())
            .add_service(service.into_write_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    Harness {
        address: format!("http://{}", addr),
        _dir: dir,
    }
}

async fn spawn_two_peer_cluster() -> (Harness, Harness) {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let peers = vec![
        PeerConfig {
            id: 1,
            address: addr_a.to_string(),
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
        },
    ];

    let config_a = ServerConfig {
        self_id: 1,
        listen_address: addr_a.to_string(),
        data_dir: dir_a.path().to_path_buf(),
        num_partitions: 4,
        peers: peers.clone(),
        bootstrap: false,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
    };

    let service_a = mesh_server::build_service(&config_a).unwrap();
    let service_b = mesh_server::build_service(&config_b).unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(service_a.clone().into_query_server())
            .add_service(service_a.into_write_server())
            .serve_with_incoming(TcpListenerStream::new(listener_a))
            .await
            .unwrap();
    });
    tokio::spawn(async move {
        Server::builder()
            .add_service(service_b.clone().into_query_server())
            .add_service(service_b.into_write_server())
            .serve_with_incoming(TcpListenerStream::new(listener_b))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(80)).await;

    (
        Harness {
            address: format!("http://{}", addr_a),
            _dir: dir_a,
        },
        Harness {
            address: format!("http://{}", addr_b),
            _dir: dir_b,
        },
    )
}

#[tokio::test]
async fn server_health_check_through_binary_path() {
    let h = spawn_single_node_server().await;
    let mut c = MeshQueryClient::connect(h.address.clone()).await.unwrap();
    let resp = c.health(HealthRequest {}).await.unwrap();
    assert!(resp.into_inner().serving);
}

#[tokio::test]
async fn server_write_then_read_single_node() {
    let h = spawn_single_node_server().await;

    let node = Node::new()
        .with_label("Person")
        .with_property("name", "Ada");
    let node_id = node.id;
    let proto_node = node_to_proto(&node).unwrap();

    let mut writer = MeshWriteClient::connect(h.address.clone()).await.unwrap();
    writer
        .put_node(PutNodeRequest {
            node: Some(proto_node),
            local_only: false,
        })
        .await
        .unwrap();

    let mut reader = MeshQueryClient::connect(h.address.clone()).await.unwrap();
    let resp = reader
        .get_node(GetNodeRequest {
            id: Some(uuid_to_proto(node_id.as_uuid())),
        })
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert!(inner.found);
    assert_eq!(inner.node.unwrap().labels, vec!["Person"]);
}

#[tokio::test]
async fn two_peer_cluster_via_config_routes_writes_and_reads() {
    let (h_a, _h_b) = spawn_two_peer_cluster().await;

    // Write 30 nodes through peer A; each should land on its partition owner.
    let mut writer = MeshWriteClient::connect(h_a.address.clone()).await.unwrap();
    let mut ids = Vec::new();
    for i in 0..30 {
        let n = Node::new()
            .with_label("Employee")
            .with_property("i", i as i64);
        ids.push(n.id);
        writer
            .put_node(PutNodeRequest {
                node: Some(node_to_proto(&n).unwrap()),
                local_only: false,
            })
            .await
            .unwrap();
    }

    // Read each back through peer A; the routed reader must fetch from both
    // peers depending on which partition owns each id.
    let mut reader = MeshQueryClient::connect(h_a.address.clone()).await.unwrap();
    for id in &ids {
        let resp = reader
            .get_node(GetNodeRequest {
                id: Some(uuid_to_proto(id.as_uuid())),
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(inner.found, "routed get_node miss for {id}");
        assert_eq!(inner.node.unwrap().labels, vec!["Employee"]);
    }
}

#[test]
fn config_bootstrap_flag_defaults_to_false_and_parses_when_set() {
    let toml_no_bootstrap = r#"
        self_id = 1
        listen_address = "127.0.0.1:7001"
        data_dir = "/tmp/mesh"
    "#;
    let cfg = ServerConfig::from_toml_str(toml_no_bootstrap).unwrap();
    assert!(!cfg.bootstrap);

    let toml_bootstrap = r#"
        self_id = 1
        listen_address = "127.0.0.1:7001"
        data_dir = "/tmp/mesh"
        bootstrap = true

        [[peers]]
        id = 1
        address = "127.0.0.1:7001"

        [[peers]]
        id = 2
        address = "127.0.0.1:7002"
    "#;
    let cfg = ServerConfig::from_toml_str(toml_bootstrap).unwrap();
    assert!(cfg.bootstrap);
    assert_eq!(cfg.peers.len(), 2);
}

#[tokio::test]
async fn build_components_single_node_has_no_raft() {
    let dir = TempDir::new().unwrap();
    let config = ServerConfig {
        self_id: 1,
        listen_address: "127.0.0.1:0".into(),
        data_dir: dir.path().to_path_buf(),
        num_partitions: 4,
        peers: vec![],
        bootstrap: false,
    };
    let components = mesh_server::build_components(&config).await.unwrap();
    assert!(components.raft.is_none());
    assert!(components.raft_service.is_none());
}

#[tokio::test]
async fn build_components_multi_peer_builds_raft() {
    let dir = TempDir::new().unwrap();
    let config = ServerConfig {
        self_id: 1,
        listen_address: "127.0.0.1:0".into(),
        data_dir: dir.path().to_path_buf(),
        num_partitions: 4,
        peers: vec![
            PeerConfig {
                id: 1,
                address: "127.0.0.1:7001".into(),
            },
            PeerConfig {
                id: 2,
                address: "127.0.0.1:7002".into(),
            },
        ],
        bootstrap: false,
    };
    let components = mesh_server::build_components(&config).await.unwrap();
    assert!(components.raft.is_some());
    assert!(components.raft_service.is_some());
}

#[tokio::test]
async fn write_to_follower_is_forwarded_to_leader_and_replicates() {
    // The leader-forwarding payoff: a `MeshWrite::PutNode` RPC sent to peer B
    // (the follower) is transparently forwarded by B's MeshService to peer A
    // (the leader) over gRPC. The leader proposes via Raft, the follower's
    // state machine applies the commit, and reads on either peer find the
    // node. Clients can write to *any* peer in the cluster.

    use std::time::Duration;

    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let peers = vec![
        PeerConfig {
            id: 1,
            address: addr_a.to_string(),
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
        },
    ];
    let config_a = ServerConfig {
        self_id: 1,
        listen_address: addr_a.to_string(),
        data_dir: dir_a.path().to_path_buf(),
        num_partitions: 4,
        peers: peers.clone(),
        bootstrap: true,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
    };

    let components_a = mesh_server::build_components(&config_a).await.unwrap();
    let components_b = mesh_server::build_components(&config_b).await.unwrap();

    let raft_a = components_a.raft.clone().unwrap();

    let ServerComponents {
        service: service_a,
        raft: _,
        raft_service: raft_service_a,
    } = components_a;
    let ServerComponents {
        service: service_b,
        raft: _,
        raft_service: raft_service_b,
    } = components_b;
    let raft_service_a = raft_service_a.unwrap();
    let raft_service_b = raft_service_b.unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(service_a.clone().into_query_server())
            .add_service(service_a.into_write_server())
            .add_service(raft_service_a.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener_a))
            .await
            .unwrap();
    });
    tokio::spawn(async move {
        Server::builder()
            .add_service(service_b.clone().into_query_server())
            .add_service(service_b.into_write_server())
            .add_service(raft_service_b.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener_b))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(80)).await;

    mesh_server::initialize_if_seed(&config_a, &raft_a)
        .await
        .unwrap();
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if raft_a.raft.metrics().borrow().current_leader == Some(1) {
            break;
        }
    }

    // *** The actual test: write to peer B, the follower. ***
    let mut writer_to_follower = MeshWriteClient::connect(format!("http://{}", addr_b))
        .await
        .unwrap();
    let node = Node::new()
        .with_label("Person")
        .with_property("name", "Grace");
    let node_id = node.id;
    writer_to_follower
        .put_node(PutNodeRequest {
            node: Some(node_to_proto(&node).unwrap()),
            local_only: false,
        })
        .await
        .expect("follower should transparently forward to leader");

    // Visible on the leader (peer A).
    let mut reader_a = MeshQueryClient::connect(format!("http://{}", addr_a))
        .await
        .unwrap();
    let resp = reader_a
        .get_node(GetNodeRequest {
            id: Some(uuid_to_proto(node_id.as_uuid())),
        })
        .await
        .unwrap();
    assert!(
        resp.into_inner().found,
        "leader should hold the forwarded write"
    );

    // Visible on the follower (peer B) after Raft replication.
    let mut reader_b = MeshQueryClient::connect(format!("http://{}", addr_b))
        .await
        .unwrap();
    let mut visible_on_b = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let resp = reader_b
            .get_node(GetNodeRequest {
                id: Some(uuid_to_proto(node_id.as_uuid())),
            })
            .await
            .unwrap();
        if resp.into_inner().found {
            visible_on_b = true;
            break;
        }
    }
    assert!(
        visible_on_b,
        "follower didn't see its own forwarded-then-replicated write"
    );
}

#[tokio::test]
async fn write_via_grpc_replicates_through_raft_to_follower() {
    // The user-facing payoff: a `MeshWrite::PutNode` RPC sent to peer A goes
    // through Raft consensus, replicates to peer B, and a `MeshQuery::GetNode`
    // RPC sent to peer B finds the node — without any direct Store access
    // from the test code. This proves the full binary-equivalent path:
    //   client → MeshWrite gRPC → MeshService::with_raft → propose_graph
    //          → Raft commit → AppendEntries to follower → state machine
    //          → StoreGraphApplier → store.put_node on follower
    //          ← MeshQuery gRPC ← MeshService → store.get_node

    use std::time::Duration;

    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let peers = vec![
        PeerConfig {
            id: 1,
            address: addr_a.to_string(),
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
        },
    ];
    let config_a = ServerConfig {
        self_id: 1,
        listen_address: addr_a.to_string(),
        data_dir: dir_a.path().to_path_buf(),
        num_partitions: 4,
        peers: peers.clone(),
        bootstrap: true,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
    };

    let components_a = mesh_server::build_components(&config_a).await.unwrap();
    let components_b = mesh_server::build_components(&config_b).await.unwrap();

    let raft_a = components_a.raft.clone().unwrap();

    let ServerComponents {
        service: service_a,
        raft: _,
        raft_service: raft_service_a,
    } = components_a;
    let ServerComponents {
        service: service_b,
        raft: _,
        raft_service: raft_service_b,
    } = components_b;

    let raft_service_a = raft_service_a.unwrap();
    let raft_service_b = raft_service_b.unwrap();

    // Spawn both binary-equivalent gRPC servers (Query + Write + Raft).
    tokio::spawn(async move {
        Server::builder()
            .add_service(service_a.clone().into_query_server())
            .add_service(service_a.into_write_server())
            .add_service(raft_service_a.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener_a))
            .await
            .unwrap();
    });
    tokio::spawn(async move {
        Server::builder()
            .add_service(service_b.clone().into_query_server())
            .add_service(service_b.into_write_server())
            .add_service(raft_service_b.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener_b))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(80)).await;

    // Bootstrap A; wait for leadership.
    mesh_server::initialize_if_seed(&config_a, &raft_a)
        .await
        .unwrap();
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if raft_a.raft.metrics().borrow().current_leader == Some(1) {
            break;
        }
    }

    // Send a PutNode to peer A's MeshWrite over gRPC.
    let mut writer = MeshWriteClient::connect(format!("http://{}", addr_a))
        .await
        .unwrap();
    let node = Node::new()
        .with_label("Person")
        .with_property("name", "Ada");
    let node_id = node.id;
    writer
        .put_node(PutNodeRequest {
            node: Some(node_to_proto(&node).unwrap()),
            local_only: false,
        })
        .await
        .expect("put_node should commit on leader");

    // Connect a reader to peer A and immediately confirm the write is visible
    // (the leader's state machine ran on commit).
    let mut reader_a = MeshQueryClient::connect(format!("http://{}", addr_a))
        .await
        .unwrap();
    let resp = reader_a
        .get_node(GetNodeRequest {
            id: Some(uuid_to_proto(node_id.as_uuid())),
        })
        .await
        .unwrap();
    assert!(resp.into_inner().found, "leader should hold the new node");

    // Connect a reader to peer B and poll for replication.
    let mut reader_b = MeshQueryClient::connect(format!("http://{}", addr_b))
        .await
        .unwrap();
    let mut replicated = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let resp = reader_b
            .get_node(GetNodeRequest {
                id: Some(uuid_to_proto(node_id.as_uuid())),
            })
            .await
            .unwrap();
        if resp.into_inner().found {
            replicated = true;
            break;
        }
    }
    assert!(
        replicated,
        "follower didn't receive the replicated PutNode via the binary path"
    );
}

#[tokio::test]
async fn peer_restart_recovers_persistent_raft_state() {
    // The payoff from persistent raft storage: peer B can be killed and
    // brought back up against its existing data_dir, and it rejoins the
    // cluster as a functioning follower. Nodes written before the restart
    // survive (log + vote hydrated from disk → state machine replayed),
    // and nodes written after the restart replicate normally.

    struct RunningPeer {
        shutdown: tokio::sync::oneshot::Sender<()>,
        join: tokio::task::JoinHandle<()>,
    }

    fn spawn_peer(listener: TcpListener, components: ServerComponents) -> RunningPeer {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let ServerComponents {
            service,
            raft: _,
            raft_service,
        } = components;
        let raft_service = raft_service.unwrap();
        let join = tokio::spawn(async move {
            let shutdown = async {
                let _ = shutdown_rx.await;
            };
            Server::builder()
                .add_service(service.clone().into_query_server())
                .add_service(service.into_write_server())
                .add_service(raft_service.into_server())
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    shutdown,
                )
                .await
                .unwrap();
        });
        RunningPeer {
            shutdown: shutdown_tx,
            join,
        }
    }

    async fn wait_for_node(
        reader: &mut MeshQueryClient<tonic::transport::Channel>,
        id: mesh_core::NodeId,
    ) -> bool {
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let resp = reader
                .get_node(GetNodeRequest {
                    id: Some(uuid_to_proto(id.as_uuid())),
                })
                .await
                .unwrap();
            if resp.into_inner().found {
                return true;
            }
        }
        false
    }

    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let peers = vec![
        PeerConfig {
            id: 1,
            address: addr_a.to_string(),
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
        },
    ];
    let config_a = ServerConfig {
        self_id: 1,
        listen_address: addr_a.to_string(),
        data_dir: dir_a.path().to_path_buf(),
        num_partitions: 4,
        peers: peers.clone(),
        bootstrap: true,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
    };

    let components_a = mesh_server::build_components(&config_a).await.unwrap();
    let components_b = mesh_server::build_components(&config_b).await.unwrap();

    let raft_a: Arc<RaftCluster> = components_a.raft.clone().unwrap();
    let raft_b_v1: Arc<RaftCluster> = components_b.raft.clone().unwrap();

    let _peer_a = spawn_peer(listener_a, components_a);
    let peer_b_v1 = spawn_peer(listener_b, components_b);

    tokio::time::sleep(Duration::from_millis(80)).await;

    mesh_server::initialize_if_seed(&config_a, &raft_a)
        .await
        .unwrap();
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if raft_a.raft.metrics().borrow().current_leader == Some(1) {
            break;
        }
    }

    // Pre-restart: write node X via the leader and wait for it to replicate
    // to peer B. This gives B a non-trivial persistent log to recover from.
    let mut writer = MeshWriteClient::connect(format!("http://{}", addr_a))
        .await
        .unwrap();
    let node_x = Node::new()
        .with_label("Person")
        .with_property("name", "X");
    let x_id = node_x.id;
    writer
        .put_node(PutNodeRequest {
            node: Some(node_to_proto(&node_x).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();

    let mut reader_b = MeshQueryClient::connect(format!("http://{}", addr_b))
        .await
        .unwrap();
    assert!(
        wait_for_node(&mut reader_b, x_id).await,
        "pre-restart: node X didn't replicate to B"
    );
    drop(reader_b);

    // Shut peer B down. Order matters: stop openraft's internal task first
    // so the tonic server task isn't handling raft RPCs against a half-dead
    // raft; then stop the server; then release the Arc so the underlying
    // rocksdb handles drop and the data_dir lock is freed.
    raft_b_v1.raft.shutdown().await.unwrap();
    peer_b_v1.shutdown.send(()).unwrap();
    peer_b_v1.join.await.unwrap();
    drop(raft_b_v1);

    // Give the runtime a tick to actually drop the service/store Arcs that
    // were moved into the server task.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restart peer B on the same address + same data_dir. build_components
    // hits MemStore::open_persistent, which reads vote + log back from disk.
    let listener_b2 = TcpListener::bind(addr_b).await.unwrap();
    let components_b2 = mesh_server::build_components(&config_b).await.unwrap();
    let _peer_b_v2 = spawn_peer(listener_b2, components_b2);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Post-restart: write node Y on the leader. With 2 peers and quorum = 2,
    // this only commits once the restarted B is back in the replication set,
    // so success here proves B is a functioning follower again.
    let node_y = Node::new()
        .with_label("Person")
        .with_property("name", "Y");
    let y_id = node_y.id;
    writer
        .put_node(PutNodeRequest {
            node: Some(node_to_proto(&node_y).unwrap()),
            local_only: false,
        })
        .await
        .expect("post-restart write should commit once B rejoins");

    // Reads on the restarted B should see both X (persisted pre-restart)
    // and Y (replicated post-restart).
    let mut reader_b2 = MeshQueryClient::connect(format!("http://{}", addr_b))
        .await
        .unwrap();
    assert!(
        wait_for_node(&mut reader_b2, x_id).await,
        "restarted B lost pre-restart node X — persistence failed"
    );
    assert!(
        wait_for_node(&mut reader_b2, y_id).await,
        "restarted B didn't receive post-restart node Y — not a functioning follower"
    );
}

#[tokio::test]
async fn two_peer_raft_replicates_via_server_components() {
    use std::time::Duration;

    // Bind two real listeners so we know the addresses up front.
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let peers = vec![
        PeerConfig {
            id: 1,
            address: addr_a.to_string(),
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
        },
    ];
    let config_a = ServerConfig {
        self_id: 1,
        listen_address: addr_a.to_string(),
        data_dir: dir_a.path().to_path_buf(),
        num_partitions: 4,
        peers: peers.clone(),
        bootstrap: true,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
    };

    // Build components for both peers.
    let components_a = mesh_server::build_components(&config_a).await.unwrap();
    let components_b = mesh_server::build_components(&config_b).await.unwrap();

    // Pull the Raft handles before destructuring components into the server tasks.
    let raft_a = components_a.raft.clone().unwrap();
    let raft_b = components_b.raft.clone().unwrap();

    let ServerComponents {
        service: service_a,
        raft: _,
        raft_service: raft_service_a,
    } = components_a;
    let ServerComponents {
        service: service_b,
        raft: _,
        raft_service: raft_service_b,
    } = components_b;

    let raft_service_a = raft_service_a.unwrap();
    let raft_service_b = raft_service_b.unwrap();

    // Spawn both binary-equivalent gRPC servers (Query + Write + Raft).
    tokio::spawn(async move {
        Server::builder()
            .add_service(service_a.clone().into_query_server())
            .add_service(service_a.into_write_server())
            .add_service(raft_service_a.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener_a))
            .await
            .unwrap();
    });
    tokio::spawn(async move {
        Server::builder()
            .add_service(service_b.clone().into_query_server())
            .add_service(service_b.into_write_server())
            .add_service(raft_service_b.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener_b))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(80)).await;

    // Bootstrap only peer A (matches the binary's serve() flow).
    mesh_server::initialize_if_seed(&config_a, &raft_a)
        .await
        .expect("seed peer should bootstrap");
    // (initialize_if_seed on B is a no-op because bootstrap = false.)
    mesh_server::initialize_if_seed(&config_b, &raft_b)
        .await
        .expect("non-seed peer should be a no-op");

    // Wait for leadership to converge.
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let metrics = raft_a.raft.metrics().borrow().clone();
        if metrics.current_leader == Some(1) {
            break;
        }
    }

    // Propose a real ClusterCommand on the leader.
    raft_a
        .propose(ClusterCommand::AddPeer {
            id: PeerId(3),
            address: "10.0.0.3:7003".into(),
        })
        .await
        .expect("propose should succeed on leader");

    let state_a = raft_a.current_state().await;
    assert!(state_a.membership.contains(PeerId(3)));

    // Peer B should see the replicated change within 4 seconds.
    let mut replicated = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let state_b = raft_b.current_state().await;
        if state_b.membership.contains(PeerId(3)) {
            assert_eq!(
                state_b.membership.address(PeerId(3)),
                Some("10.0.0.3:7003")
            );
            replicated = true;
            break;
        }
    }
    assert!(
        replicated,
        "peer B did not receive the replicated command via the binary path"
    );
}
