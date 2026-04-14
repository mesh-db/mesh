use mesh_cluster::raft::RaftCluster;
use mesh_cluster::ClusterCommand;
use mesh_cluster::PeerId;
use mesh_core::Node;
use mesh_rpc::convert::{node_to_proto, uuid_to_proto};
use mesh_rpc::proto::mesh_query_client::MeshQueryClient;
use mesh_rpc::proto::mesh_write_client::MeshWriteClient;
use mesh_rpc::proto::{ExecuteCypherRequest, GetNodeRequest, HealthRequest, PutNodeRequest};
use mesh_server::config::{PeerConfig, ServerConfig};
use mesh_server::ServerComponents;
use std::path::PathBuf;
use std::sync::Arc;
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
        bolt_address: None,
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
        bolt_address: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
        bolt_address: None,
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
            local_only: false,
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
                local_only: false,
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
        bolt_address: None,
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
        bolt_address: None,
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
        bolt_address: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
        bolt_address: None,
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
            local_only: false,
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
                local_only: false,
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
        bolt_address: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
        bolt_address: None,
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
            local_only: false,
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
                local_only: false,
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
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
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
                    local_only: false,
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
        bolt_address: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
        bolt_address: None,
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
    let node_x = Node::new().with_label("Person").with_property("name", "X");
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
    let node_y = Node::new().with_label("Person").with_property("name", "Y");
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
async fn cypher_create_replicates_through_raft_to_follower() {
    // End-to-end payoff: a Cypher CREATE sent to peer A's ExecuteCypher RPC
    // lands on peer A's executor, which routes its writes through the
    // RaftGraphWriter → propose_graph → commit → StoreGraphApplier on every
    // replica. Peer B can then serve both a MATCH RETURN query over Cypher
    // and a raw GetNode RPC for the created node — proving the executor's
    // writes replicate exactly like direct MeshWrite PutNode.

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
        bolt_address: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
        bolt_address: None,
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

    // CREATE via Cypher on the leader. The executor's CreatePathOp routes
    // its single put_node through RaftGraphWriter → propose_graph → commit.
    let mut query_a = MeshQueryClient::connect(format!("http://{}", addr_a))
        .await
        .unwrap();
    query_a
        .execute_cypher(ExecuteCypherRequest {
            query: "CREATE (n:Person {name: 'Ada', age: 36}) RETURN n".into(),
            params_json: vec![],
        })
        .await
        .expect("CREATE through Cypher should commit on leader");

    // MATCH on peer B via Cypher; poll until the committed entry has been
    // applied on B's state machine.
    let mut query_b = MeshQueryClient::connect(format!("http://{}", addr_b))
        .await
        .unwrap();
    let mut matched_rows: Option<serde_json::Value> = None;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let resp = query_b
            .execute_cypher(ExecuteCypherRequest {
                query: "MATCH (n:Person) RETURN n.name AS name, n.age AS age".into(),
                params_json: vec![],
            })
            .await
            .unwrap();
        let rows: serde_json::Value = serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
        if rows.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
            matched_rows = Some(rows);
            break;
        }
    }
    let rows = matched_rows.expect("follower didn't see Cypher-created node");
    let row = &rows.as_array().unwrap()[0];
    // Value is untagged serde (default: external tag), Property is
    // internally tagged `{type, value}`. So:
    //   Value::Property(Property::String("Ada"))
    //     → {"Property":{"type":"String","value":"Ada"}}
    assert_eq!(
        row["name"]["Property"]["value"].as_str().unwrap(),
        "Ada",
        "row: {row}"
    );
    assert_eq!(row["age"]["Property"]["value"].as_i64().unwrap(), 36);
}

#[tokio::test]
async fn wiped_follower_catches_up_via_install_snapshot() {
    // The end-to-end snapshot catch-up payoff: a follower can be wiped
    // entirely (data_dir gone — no log, no vote, no state machine state,
    // no graph data) and rejoin the cluster purely through an InstallSnapshot
    // RPC from the leader. Locks in:
    //   - leader auto-snapshots fire and compact older log entries
    //   - the snapshot wire format (PersistedSnapshot) carries graph data
    //   - StoreGraphApplier::restore wipes + repopulates the local store
    //   - the wiped follower's metrics show a received snapshot AND it
    //     serves correct reads for nodes created before the wipe

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
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
                .await
                .unwrap();
        });
        RunningPeer {
            shutdown: shutdown_tx,
            join,
        }
    }

    let dir_a = TempDir::new().unwrap();
    let dir_b1 = TempDir::new().unwrap();
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
        bolt_address: None,
    };
    let config_b1 = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b1.path().to_path_buf(),
        num_partitions: 4,
        peers: peers.clone(),
        bootstrap: false,
        bolt_address: None,
    };

    let components_a = mesh_server::build_components(&config_a).await.unwrap();
    let components_b = mesh_server::build_components(&config_b1).await.unwrap();
    let raft_a = components_a.raft.clone().unwrap();
    let raft_b1 = components_b.raft.clone().unwrap();

    let _peer_a = spawn_peer(listener_a, components_a);
    let peer_b1 = spawn_peer(listener_b, components_b);

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

    // Pre-wipe writes: 25 nodes via Cypher. Tagged with a label so we can
    // count them post-restore via nodes_by_label, plus we capture the
    // first node's id to verify a specific entity round-trips.
    let mut query_a = MeshQueryClient::connect(format!("http://{}", addr_a))
        .await
        .unwrap();
    let mut writer_a = MeshWriteClient::connect(format!("http://{}", addr_a))
        .await
        .unwrap();

    // Use a direct PutNode for the first one so we know its id (the
    // executor mints fresh ids that we'd have to parse out of returned
    // rows otherwise).
    let pinned = Node::new()
        .with_label("Marker")
        .with_property("name", "pinned");
    let pinned_id = pinned.id;
    writer_a
        .put_node(PutNodeRequest {
            node: Some(node_to_proto(&pinned).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();

    for i in 0..24 {
        query_a
            .execute_cypher(ExecuteCypherRequest {
                query: format!("CREATE (n:Marker {{idx: {i}}}) RETURN n"),
                params_json: vec![],
            })
            .await
            .unwrap();
    }

    // Wait until the leader has built an auto-snapshot. With
    // snapshot_policy = LogsSinceLast(16) and ~25 commits this fires once.
    let mut leader_snapshot = None;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(snap) = raft_a.raft.metrics().borrow().snapshot {
            leader_snapshot = Some(snap);
            break;
        }
    }
    assert!(
        leader_snapshot.is_some(),
        "leader should auto-snapshot before wiping the follower"
    );

    // Shut down peer B fully so its rocksdb file lock is released.
    raft_b1.raft.shutdown().await.unwrap();
    peer_b1.shutdown.send(()).unwrap();
    peer_b1.join.await.unwrap();
    drop(raft_b1);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Wipe: drop the old data_dir, allocate a brand-new one. Restart B
    // against the new (empty) directory but the same listen address so
    // peer A's GrpcNetwork channel reconnects transparently.
    drop(dir_b1);
    let dir_b2 = TempDir::new().unwrap();
    let config_b2 = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b2.path().to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
        bolt_address: None,
    };

    let listener_b2 = TcpListener::bind(addr_b).await.unwrap();
    let components_b2 = mesh_server::build_components(&config_b2).await.unwrap();
    let raft_b2 = components_b2.raft.clone().unwrap();
    let _peer_b2 = spawn_peer(listener_b2, components_b2);

    // The restarted B starts with an empty log. The leader's next
    // AppendEntries hits a follower whose next_index is below A's
    // purged log range, so openraft falls back to InstallSnapshot. Wait
    // for B's metrics to reflect a received snapshot — that's the
    // mechanism we're locking in.
    let mut received_snapshot = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if raft_b2.raft.metrics().borrow().snapshot.is_some() {
            received_snapshot = true;
            break;
        }
    }
    assert!(
        received_snapshot,
        "wiped follower should receive an InstallSnapshot from the leader"
    );

    // The pinned node from before the wipe should now be visible on B
    // via a local GetNode — proving applier.restore wiped + repopulated
    // the graph store with the snapshot's payload.
    let mut query_b = MeshQueryClient::connect(format!("http://{}", addr_b))
        .await
        .unwrap();
    let mut found_pinned = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let resp = query_b
            .get_node(GetNodeRequest {
                id: Some(uuid_to_proto(pinned_id.as_uuid())),
                local_only: false,
            })
            .await
            .unwrap();
        if resp.into_inner().found {
            found_pinned = true;
            break;
        }
    }
    assert!(
        found_pinned,
        "wiped follower should serve the pinned node after snapshot install"
    );

    // And a Cypher MATCH should see most of the Markers (some may have
    // been written after the snapshot and replayed via AppendEntries).
    let resp = query_b
        .execute_cypher(ExecuteCypherRequest {
            query: "MATCH (n:Marker) RETURN n".into(),
            params_json: vec![],
        })
        .await
        .unwrap();
    let rows: serde_json::Value = serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
    let count = rows.as_array().map(|a| a.len()).unwrap_or(0);
    assert!(
        count >= 20,
        "wiped follower should rebuild most of the graph from snapshot + tail-of-log replay, got {count}"
    );
}

#[tokio::test]
async fn auto_snapshot_fires_and_persists_graph_data() {
    // Helper local to this test — spawns the server with a shutdown
    // channel so we can fully release the rocksdb file lock and reopen
    // the store from disk in the verification phase.
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
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
                .await
                .unwrap();
        });
        RunningPeer {
            shutdown: shutdown_tx,
            join,
        }
    }

    // Two-part validation of auto-snapshots:
    //   1. After enough committed log entries (snapshot_policy.LogsSinceLast(16)),
    //      the leader produces a snapshot whose meta is reflected in raft
    //      metrics and whose persisted blob round-trips through restore on
    //      a fresh local store.
    //   2. The snapshot blob is a `PersistedSnapshot` carrying both
    //      ClusterState AND the graph applier's nodes/edges — so a brand
    //      new replica receiving InstallSnapshot ends up with the leader's
    //      full graph data, not just cluster metadata.

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
        bolt_address: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
        bolt_address: None,
    };

    let components_a = mesh_server::build_components(&config_a).await.unwrap();
    let components_b = mesh_server::build_components(&config_b).await.unwrap();
    let raft_a = components_a.raft.clone().unwrap();

    let peer_a = spawn_peer(listener_a, components_a);
    let _peer_b = spawn_peer(listener_b, components_b);

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

    // Drive enough Cypher creates to cross the snapshot threshold.
    let mut query_a = MeshQueryClient::connect(format!("http://{}", addr_a))
        .await
        .unwrap();
    for i in 0..20 {
        query_a
            .execute_cypher(ExecuteCypherRequest {
                query: format!("CREATE (n:Person {{idx: {i}}}) RETURN n"),
                params_json: vec![],
            })
            .await
            .unwrap();
    }

    // Poll the leader's metrics until the snapshot field reflects an
    // auto-built snapshot. openraft fires the snapshot asynchronously
    // after the threshold is crossed, so a small wait is required.
    let mut snapshot_seen = None;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(snap) = raft_a.raft.metrics().borrow().snapshot {
            snapshot_seen = Some(snap);
            break;
        }
    }
    let snap_meta = snapshot_seen.expect("leader should have built a snapshot after 20 commits");
    // openraft fires a snapshot once enough entries have accumulated since
    // the previous one; the threshold is approximate. We just need the
    // auto-snapshot path to have run at all.
    assert!(
        snap_meta.index >= 10,
        "snapshot should cover a meaningful chunk of the log, got {snap_meta:?}"
    );

    // Verify the snapshot persisted to disk by re-opening the leader's
    // raft store directly. The PersistedSnapshot blob should contain BOTH
    // a non-empty graph payload and the cluster state.
    use mesh_cluster::raft::{MemStore, PersistedSnapshot};
    use mesh_cluster::{ClusterState, Membership, PartitionMap, Peer};

    // Stop A's raft cleanly so we can reopen the rocksdb handle. Order:
    // shut openraft's internal task, then stop the gRPC server (which
    // drops the Arc<RaftCluster> inside MeshService), then release our
    // local Arc. After that, the rocksdb file lock for dir_a/raft is
    // free for a fresh MemStore to take.
    raft_a.raft.shutdown().await.unwrap();
    peer_a.shutdown.send(()).unwrap();
    peer_a.join.await.unwrap();
    drop(raft_a);
    drop(query_a);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let reopened = MemStore::open_persistent(
        dir_a.path().join("raft"),
        ClusterState::new(
            Membership::new(vec![Peer::new(PeerId(1), addr_a.to_string())]),
            PartitionMap::round_robin(&[PeerId(1)], 1).unwrap(),
        ),
        None,
    )
    .unwrap();

    let inner = reopened.inner();
    let guard = inner.read().await;
    let (_meta, raw) = guard
        .snapshot
        .as_ref()
        .expect("auto-snapshot should be persisted on disk");
    let payload: PersistedSnapshot =
        serde_json::from_slice(raw).expect("snapshot must be a PersistedSnapshot");
    assert!(
        !payload.graph.is_empty(),
        "snapshot's graph blob should be populated after CREATE-heavy workload"
    );
    // ClusterState should at least carry the bootstrapped 2-peer membership.
    assert_eq!(payload.cluster.membership.len(), 2);

    // Decode the graph blob shape — it's the StoreGraphApplier's
    // GraphSnapshot { nodes, edges } JSON. We only care that nodes are present.
    let graph_value: serde_json::Value = serde_json::from_slice(&payload.graph).unwrap();
    let nodes = graph_value["nodes"].as_array().unwrap();
    // The snapshot may have fired before every CREATE was applied — what
    // matters is that the graph payload carries *some* of the data, not
    // just the cluster metadata. A previous-version snapshot with only
    // ClusterState would land here with `nodes` absent or empty.
    assert!(
        nodes.len() >= 5,
        "graph snapshot should contain a meaningful slice of created nodes, got {}",
        nodes.len()
    );
}

#[tokio::test]
async fn cypher_merge_replicates_through_raft() {
    // MERGE through the gRPC + Raft path: the create branch goes through
    // BufferingGraphWriter → propose_graph just like CREATE, the match
    // branch does no writes, and a follower MERGE re-finds the same node
    // (idempotent across the whole cluster).

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
        bolt_address: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
        bolt_address: None,
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

    let mut query_a = MeshQueryClient::connect(format!("http://{}", addr_a))
        .await
        .unwrap();

    // First MERGE creates.
    let resp = query_a
        .execute_cypher(ExecuteCypherRequest {
            query: "MERGE (n:Person {name: 'Ada'}) RETURN n.name AS name".into(),
            params_json: vec![],
        })
        .await
        .unwrap();
    let rows: serde_json::Value = serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert_eq!(
        rows[0]["name"]["Property"]["value"].as_str().unwrap(),
        "Ada"
    );

    // Second MERGE matches — must not duplicate.
    query_a
        .execute_cypher(ExecuteCypherRequest {
            query: "MERGE (n:Person {name: 'Ada'}) RETURN n".into(),
            params_json: vec![],
        })
        .await
        .unwrap();

    // Wait for replication to peer B, then verify B sees exactly one Person.
    let mut query_b = MeshQueryClient::connect(format!("http://{}", addr_b))
        .await
        .unwrap();
    let mut count = 0usize;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let resp = query_b
            .execute_cypher(ExecuteCypherRequest {
                query: "MATCH (n:Person) RETURN n.name AS name".into(),
                params_json: vec![],
            })
            .await
            .unwrap();
        let rows: serde_json::Value = serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
        count = rows.as_array().map(|a| a.len()).unwrap_or(0);
        if count == 1 {
            break;
        }
    }
    assert_eq!(
        count, 1,
        "follower should see exactly one Person after MERGE×2 — no duplicates"
    );
}

#[tokio::test]
async fn cypher_multi_write_query_commits_as_single_raft_entry() {
    // Atomicity payoff: a Cypher query that writes two nodes and an edge
    // should commit through Raft as exactly ONE log entry, so a crash
    // mid-query can't leave a partial result on any replica. Verifies
    // both the post-commit state on the follower and the leader's
    // last_log_index advancing by 1, not 3.

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
        bolt_address: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
        bolt_address: None,
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

    let last_log_before = raft_a.raft.metrics().borrow().last_log_index.unwrap_or(0);

    // Multi-write Cypher query: 2 PutNode + 1 PutEdge under the old
    // one-entry-per-call model. With buffering this becomes 1 batch entry.
    let mut query_a = MeshQueryClient::connect(format!("http://{}", addr_a))
        .await
        .unwrap();
    query_a
        .execute_cypher(ExecuteCypherRequest {
            query:
                "CREATE (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Grace'}) RETURN a, b"
                    .into(),
            params_json: vec![],
        })
        .await
        .expect("multi-write CREATE should commit");

    let last_log_after = raft_a.raft.metrics().borrow().last_log_index.unwrap_or(0);
    assert_eq!(
        last_log_after - last_log_before,
        1,
        "multi-write query should consume exactly 1 raft log entry, \
         got {} (before={last_log_before}, after={last_log_after})",
        last_log_after - last_log_before
    );

    // Both nodes and the relationship should be visible on the follower
    // after the single commit replicates.
    let mut query_b = MeshQueryClient::connect(format!("http://{}", addr_b))
        .await
        .unwrap();
    let mut visible = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let resp = query_b
            .execute_cypher(ExecuteCypherRequest {
                query: "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS a_name, b.name AS b_name".into(),
                params_json: vec![],
            })
            .await
            .unwrap();
        let rows: serde_json::Value = serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
        let arr = rows.as_array().unwrap();
        if arr.len() == 1 {
            let row = &arr[0];
            assert_eq!(row["a_name"]["Property"]["value"].as_str().unwrap(), "Ada");
            assert_eq!(
                row["b_name"]["Property"]["value"].as_str().unwrap(),
                "Grace"
            );
            visible = true;
            break;
        }
    }
    assert!(
        visible,
        "follower didn't see the batched create-path replicate"
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
        bolt_address: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
        num_partitions: 4,
        peers,
        bootstrap: false,
        bolt_address: None,
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
            assert_eq!(state_b.membership.address(PeerId(3)), Some("10.0.0.3:7003"));
            replicated = true;
            break;
        }
    }
    assert!(
        replicated,
        "peer B did not receive the replicated command via the binary path"
    );
}
