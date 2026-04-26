use meshdb_cluster::raft::RaftCluster;
use meshdb_cluster::ClusterCommand;
use meshdb_cluster::PeerId;
use meshdb_core::Node;
use meshdb_rpc::convert::{node_to_proto, uuid_to_proto};
use meshdb_rpc::proto::mesh_query_client::MeshQueryClient;
use meshdb_rpc::proto::mesh_write_client::MeshWriteClient;
use meshdb_rpc::proto::{ExecuteCypherRequest, GetNodeRequest, HealthRequest, PutNodeRequest};
use meshdb_server::config::{PeerConfig, ServerConfig};
use meshdb_server::ServerComponents;
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
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };

    let service = meshdb_server::build_service(&config).unwrap();
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
            bolt_address: None,
            weight: None,
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
            bolt_address: None,
            weight: None,
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
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: Some(meshdb_server::config::ClusterMode::Routing),
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
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
        mode: Some(meshdb_server::config::ClusterMode::Routing),
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };

    let service_a = meshdb_server::build_service(&config_a).unwrap();
    let service_b = meshdb_server::build_service(&config_b).unwrap();

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

#[tokio::test]
async fn cypher_create_index_replicates_through_routing_fan_out() {
    // Phase C payoff: routing-mode CREATE INDEX fans out from the
    // receiving peer to every other peer in parallel, every peer's
    // local store backfills against its own slice of the graph,
    // and a subsequent MATCH on either peer plans through
    // IndexSeek + scatter-gather so the result is the union of
    // both partitions' matches.
    let (h_a, h_b) = spawn_two_peer_cluster().await;

    // Seed nodes through peer A. The routing reader sends each one
    // to its partition owner, so the 20-node set ends up roughly
    // split between A and B.
    let mut writer_a = MeshWriteClient::connect(h_a.address.clone()).await.unwrap();
    for i in 0..20 {
        let n = Node::new()
            .with_label("Person")
            .with_property("name", format!("user-{i}"))
            .with_property("age", i as i64);
        writer_a
            .put_node(PutNodeRequest {
                node: Some(node_to_proto(&n).unwrap()),
                local_only: false,
            })
            .await
            .unwrap();
    }

    let mut q_a = MeshQueryClient::connect(h_a.address.clone()).await.unwrap();
    let mut q_b = MeshQueryClient::connect(h_b.address.clone()).await.unwrap();

    // CREATE INDEX through peer A. The DDL fan-out applies locally,
    // then RPCs peer B's CreatePropertyIndex handler in parallel,
    // and each peer's backfill walks its own label index.
    q_a.execute_cypher(ExecuteCypherRequest {
        query: "CREATE INDEX FOR (p:Person) ON (p.name)".into(),
        params_json: vec![],
    })
    .await
    .expect("CREATE INDEX in routing mode should fan out cleanly");

    // SHOW INDEXES on peer B sees the replicated registry entry.
    let show_resp = q_b
        .execute_cypher(ExecuteCypherRequest {
            query: "SHOW INDEXES".into(),
            params_json: vec![],
        })
        .await
        .unwrap();
    let show_rows: serde_json::Value =
        serde_json::from_slice(&show_resp.into_inner().rows_json).unwrap();
    let arr = show_rows.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(
        arr[0]["label"]["Property"]["value"].as_str().unwrap(),
        "Person"
    );

    // MATCH from peer A pulls through the partitioned reader's
    // nodes_by_property scatter-gather: peer A returns its local
    // matches, peer B returns its local matches, the union is the
    // full row. Try a name we know is in the seed set.
    let match_resp = q_a
        .execute_cypher(ExecuteCypherRequest {
            query: "MATCH (p:Person {name: 'user-7'}) RETURN p.age AS age".into(),
            params_json: vec![],
        })
        .await
        .unwrap();
    let match_rows: serde_json::Value =
        serde_json::from_slice(&match_resp.into_inner().rows_json).unwrap();
    let arr = match_rows.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["age"]["Property"]["value"].as_i64().unwrap(), 7);

    // Same query from peer B — different scatter-gather origin,
    // identical answer.
    let match_resp_b = q_b
        .execute_cypher(ExecuteCypherRequest {
            query: "MATCH (p:Person {name: 'user-13'}) RETURN p.age AS age".into(),
            params_json: vec![],
        })
        .await
        .unwrap();
    let arr_b: serde_json::Value =
        serde_json::from_slice(&match_resp_b.into_inner().rows_json).unwrap();
    let arr_b = arr_b.as_array().unwrap();
    assert_eq!(arr_b.len(), 1);
    assert_eq!(arr_b[0]["age"]["Property"]["value"].as_i64().unwrap(), 13);

    // DROP INDEX also fans out. After the round-trip, SHOW on A
    // returns empty.
    q_a.execute_cypher(ExecuteCypherRequest {
        query: "DROP INDEX FOR (p:Person) ON (p.name)".into(),
        params_json: vec![],
    })
    .await
    .unwrap();
    let after_drop = q_a
        .execute_cypher(ExecuteCypherRequest {
            query: "SHOW INDEXES".into(),
            params_json: vec![],
        })
        .await
        .unwrap();
    let drop_rows: serde_json::Value =
        serde_json::from_slice(&after_drop.into_inner().rows_json).unwrap();
    assert!(drop_rows.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn cypher_create_edge_index_replicates_through_routing_fan_out() {
    // Edge-index analogue of the node-index routing-mode test. Every
    // replica must end up with the `(edge_type, property)` index so
    // relationship-UNIQUE constraint enforcement and future edge-
    // IndexSeek lookups see the same catalog regardless of which
    // peer receives the read.
    let (h_a, h_b) = spawn_two_peer_cluster().await;

    let mut q_a = MeshQueryClient::connect(h_a.address.clone()).await.unwrap();
    let mut q_b = MeshQueryClient::connect(h_b.address.clone()).await.unwrap();

    q_a.execute_cypher(ExecuteCypherRequest {
        query: "CREATE INDEX FOR ()-[r:KNOWS]-() ON (r.since)".into(),
        params_json: vec![],
    })
    .await
    .expect("CREATE edge INDEX in routing mode should fan out cleanly");

    // Peer B observes the replicated registry entry.
    let show_resp = q_b
        .execute_cypher(ExecuteCypherRequest {
            query: "SHOW INDEXES".into(),
            params_json: vec![],
        })
        .await
        .unwrap();
    let show_rows: serde_json::Value =
        serde_json::from_slice(&show_resp.into_inner().rows_json).unwrap();
    let arr = show_rows.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(
        arr[0]["scope"]["Property"]["value"].as_str().unwrap(),
        "RELATIONSHIP"
    );
    assert_eq!(
        arr[0]["edge_type"]["Property"]["value"].as_str().unwrap(),
        "KNOWS"
    );

    // DROP edge INDEX also fans out.
    q_a.execute_cypher(ExecuteCypherRequest {
        query: "DROP INDEX FOR ()-[r:KNOWS]-() ON (r.since)".into(),
        params_json: vec![],
    })
    .await
    .unwrap();
    let after_drop = q_b
        .execute_cypher(ExecuteCypherRequest {
            query: "SHOW INDEXES".into(),
            params_json: vec![],
        })
        .await
        .unwrap();
    let drop_rows: serde_json::Value =
        serde_json::from_slice(&after_drop.into_inner().rows_json).unwrap();
    assert!(drop_rows.as_array().unwrap().is_empty());
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

#[test]
fn sample_toml_configs_at_repo_root_parse_with_bolt_auth() {
    // Regression guard for the repo-root `mesh.toml` / `mesh-a.toml` /
    // `mesh-b.toml` / `mesh-c.toml` files referenced in the README.
    // Parses each through `ServerConfig::from_path` (so schema
    // changes + `deny_unknown_fields` + the optional `bolt_auth`
    // section all get exercised end-to-end), verifies each config
    // carries a `bolt_auth.users` entry, and runs `validate()` so
    // mode/peer consistency stays green.
    use std::path::PathBuf;
    // `CARGO_MANIFEST_DIR` points at `crates/meshdb-server`. The
    // sample configs live two levels up at the repo root.
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let repo_root = PathBuf::from(manifest)
        .join("..")
        .join("..")
        .canonicalize()
        .expect("repo root canonicalize");
    for name in ["mesh.toml", "mesh-a.toml", "mesh-b.toml", "mesh-c.toml"] {
        let path = repo_root.join(name);
        let cfg = ServerConfig::from_path(&path).unwrap_or_else(|e| panic!("{name}: {e}"));
        cfg.validate().unwrap_or_else(|e| panic!("{name}: {e}"));
        let auth = cfg
            .bolt_auth
            .as_ref()
            .unwrap_or_else(|| panic!("{name}: expected bolt_auth section"));
        assert!(
            !auth.users.is_empty(),
            "{name}: expected at least one bolt_auth user"
        );
        // End-to-end credential check: whatever's on disk —
        // plaintext or bcrypt — must verify the canonical dev
        // credentials `neo4j` / `password` so the sample
        // configs can actually be used against the README
        // quickstart without a hand-edit.
        assert!(
            auth.verify("neo4j", "password"),
            "{name}: neo4j/password should verify against the configured hash"
        );
        assert!(
            !auth.verify("neo4j", "wrong"),
            "{name}: wrong password must not verify"
        );
    }
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
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let components = meshdb_server::build_components(&config).await.unwrap();
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
                bolt_address: None,
                weight: None,
            },
            PeerConfig {
                id: 2,
                address: "127.0.0.1:7002".into(),
                bolt_address: None,
                weight: None,
            },
        ],
        bootstrap: false,
        bolt_address: None,
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let components = meshdb_server::build_components(&config).await.unwrap();
    assert!(components.raft.is_some());
    assert!(components.raft_service.is_some());
}

#[tokio::test]
async fn build_components_routing_mode_has_coordinator_log_and_no_raft() {
    // The whole point of the ClusterMode::Routing wiring: the async
    // `build_components` path used by `serve()` must reach the routing
    // service + durable coordinator log, not the Raft bootstrap path.
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
                bolt_address: None,
                weight: None,
            },
            PeerConfig {
                id: 2,
                address: "127.0.0.1:7002".into(),
                bolt_address: None,
                weight: None,
            },
        ],
        bootstrap: false,
        bolt_address: None,
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: Some(meshdb_server::config::ClusterMode::Routing),
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let components = meshdb_server::build_components(&config).await.unwrap();
    assert!(components.raft.is_none());
    assert!(components.raft_service.is_none());
    // Opening the coordinator log creates the file on disk — this is the
    // observable signal that routing mode was actually wired in.
    let log_path = meshdb_server::coordinator_log_path(&config.data_dir);
    assert!(
        log_path.exists(),
        "routing build_components should open the coordinator log at {}",
        log_path.display()
    );
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
            bolt_address: None,
            weight: None,
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
            bolt_address: None,
            weight: None,
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
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
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
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };

    let components_a = meshdb_server::build_components(&config_a).await.unwrap();
    let components_b = meshdb_server::build_components(&config_b).await.unwrap();

    let raft_a = components_a.raft.clone().unwrap();

    let ServerComponents {
        service: service_a,
        raft: _,
        raft_service: raft_service_a,
        multi_raft: _,
    } = components_a;
    let ServerComponents {
        service: service_b,
        raft: _,
        raft_service: raft_service_b,
        multi_raft: _,
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

    meshdb_server::initialize_if_seed(&config_a, &raft_a)
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
            bolt_address: None,
            weight: None,
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
            bolt_address: None,
            weight: None,
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
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
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
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };

    let components_a = meshdb_server::build_components(&config_a).await.unwrap();
    let components_b = meshdb_server::build_components(&config_b).await.unwrap();

    let raft_a = components_a.raft.clone().unwrap();

    let ServerComponents {
        service: service_a,
        raft: _,
        raft_service: raft_service_a,
        multi_raft: _,
    } = components_a;
    let ServerComponents {
        service: service_b,
        raft: _,
        raft_service: raft_service_b,
        multi_raft: _,
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
    meshdb_server::initialize_if_seed(&config_a, &raft_a)
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
            multi_raft: _,
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
        id: meshdb_core::NodeId,
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
            bolt_address: None,
            weight: None,
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
            bolt_address: None,
            weight: None,
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
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
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
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };

    let components_a = meshdb_server::build_components(&config_a).await.unwrap();
    let components_b = meshdb_server::build_components(&config_b).await.unwrap();

    let raft_a: Arc<RaftCluster> = components_a.raft.clone().unwrap();
    let raft_b_v1: Arc<RaftCluster> = components_b.raft.clone().unwrap();

    let _peer_a = spawn_peer(listener_a, components_a);
    let peer_b_v1 = spawn_peer(listener_b, components_b);

    tokio::time::sleep(Duration::from_millis(80)).await;

    meshdb_server::initialize_if_seed(&config_a, &raft_a)
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
    //
    // Bind with a bounded retry: the previous listener's socket sits in
    // the kernel's release path briefly after the server task stops, and
    // on a loaded CI runner the 200ms sleep above isn't always enough to
    // clear it. 20×50ms = 1s total ceiling, plenty of headroom without
    // masking a real regression.
    let listener_b2 = {
        let mut last_err: Option<std::io::Error> = None;
        let mut bound: Option<TcpListener> = None;
        for _ in 0..20 {
            match TcpListener::bind(addr_b).await {
                Ok(l) => {
                    bound = Some(l);
                    break;
                }
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        bound
            .unwrap_or_else(|| panic!("could not rebind {addr_b} after 20 retries: {:?}", last_err))
    };
    let components_b2 = meshdb_server::build_components(&config_b).await.unwrap();
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
            bolt_address: None,
            weight: None,
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
            bolt_address: None,
            weight: None,
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
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
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
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };

    let components_a = meshdb_server::build_components(&config_a).await.unwrap();
    let components_b = meshdb_server::build_components(&config_b).await.unwrap();
    let raft_a = components_a.raft.clone().unwrap();

    let ServerComponents {
        service: service_a,
        raft: _,
        raft_service: raft_service_a,
        multi_raft: _,
    } = components_a;
    let ServerComponents {
        service: service_b,
        raft: _,
        raft_service: raft_service_b,
        multi_raft: _,
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

    meshdb_server::initialize_if_seed(&config_a, &raft_a)
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
async fn cypher_create_index_replicates_through_raft_and_is_used_on_follower() {
    // Phase B payoff: `CREATE INDEX` issued on the leader lands as
    // a `GraphCommand::CreateIndex` in the Raft log, every peer's
    // `StoreGraphApplier` runs `store.create_property_index`
    // locally (backfilling its own replica), and a subsequent
    // `MATCH` on the *follower* plans through `IndexSeek` — proving
    // that both the write (DDL propagation) and the read (planner
    // context sourced from local `Store::list_property_indexes`)
    // are consistent across replicas.
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
            bolt_address: None,
            weight: None,
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
            bolt_address: None,
            weight: None,
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
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
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
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };

    let components_a = meshdb_server::build_components(&config_a).await.unwrap();
    let components_b = meshdb_server::build_components(&config_b).await.unwrap();
    let raft_a = components_a.raft.clone().unwrap();

    let ServerComponents {
        service: service_a,
        raft: _,
        raft_service: raft_service_a,
        multi_raft: _,
    } = components_a;
    let ServerComponents {
        service: service_b,
        raft: _,
        raft_service: raft_service_b,
        multi_raft: _,
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
    meshdb_server::initialize_if_seed(&config_a, &raft_a)
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
    // Seed with a few pre-existing nodes so the index's backfill
    // path gets exercised on every peer's local store.
    query_a
        .execute_cypher(ExecuteCypherRequest {
            query: "CREATE (n:Person {name: 'Ada', age: 37})".into(),
            params_json: vec![],
        })
        .await
        .unwrap();
    query_a
        .execute_cypher(ExecuteCypherRequest {
            query: "CREATE (n:Person {name: 'Bob', age: 28})".into(),
            params_json: vec![],
        })
        .await
        .unwrap();

    // CREATE INDEX — goes through the Raft log as a GraphCommand::CreateIndex.
    let create_resp = query_a
        .execute_cypher(ExecuteCypherRequest {
            query: "CREATE INDEX FOR (p:Person) ON (p.name)".into(),
            params_json: vec![],
        })
        .await
        .expect("CREATE INDEX via Cypher should commit on leader");
    let create_rows: serde_json::Value =
        serde_json::from_slice(&create_resp.into_inner().rows_json).unwrap();
    assert_eq!(create_rows.as_array().unwrap().len(), 1);

    // Wait for follower B to apply the committed CreateIndex entry.
    // We probe via SHOW INDEXES on peer B.
    let mut query_b = MeshQueryClient::connect(format!("http://{}", addr_b))
        .await
        .unwrap();
    let mut show_rows_on_b: Option<serde_json::Value> = None;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let resp = query_b
            .execute_cypher(ExecuteCypherRequest {
                query: "SHOW INDEXES".into(),
                params_json: vec![],
            })
            .await
            .unwrap();
        let rows: serde_json::Value = serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
        if rows.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
            show_rows_on_b = Some(rows);
            break;
        }
    }
    let rows_b = show_rows_on_b.expect("follower never saw the replicated CREATE INDEX");
    let first = &rows_b.as_array().unwrap()[0];
    assert_eq!(
        first["label"]["Property"]["value"].as_str().unwrap(),
        "Person"
    );
    assert_eq!(
        first["property"]["Property"]["value"].as_str().unwrap(),
        "name"
    );

    // MATCH with pattern property on the follower. The follower's
    // local `Store::list_property_indexes` returns the replicated
    // spec, so the planner rewrites to `IndexSeek` and the backfill
    // entries written by B's applier satisfy the lookup.
    let match_resp = query_b
        .execute_cypher(ExecuteCypherRequest {
            query: "MATCH (p:Person {name: 'Ada'}) RETURN p.age AS age".into(),
            params_json: vec![],
        })
        .await
        .unwrap();
    let match_rows: serde_json::Value =
        serde_json::from_slice(&match_resp.into_inner().rows_json).unwrap();
    let arr = match_rows.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["age"]["Property"]["value"].as_i64().unwrap(), 37);

    // DROP INDEX — also replicates. After apply, SHOW INDEXES on B
    // should come back empty.
    query_a
        .execute_cypher(ExecuteCypherRequest {
            query: "DROP INDEX FOR (p:Person) ON (p.name)".into(),
            params_json: vec![],
        })
        .await
        .unwrap();
    let mut drained = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let resp = query_b
            .execute_cypher(ExecuteCypherRequest {
                query: "SHOW INDEXES".into(),
                params_json: vec![],
            })
            .await
            .unwrap();
        let rows: serde_json::Value = serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
        if rows.as_array().map(|a| a.is_empty()).unwrap_or(false) {
            drained = true;
            break;
        }
    }
    assert!(
        drained,
        "follower never saw the replicated DROP INDEX clear the registry"
    );
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
            multi_raft: _,
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
            bolt_address: None,
            weight: None,
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
            bolt_address: None,
            weight: None,
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
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let config_b1 = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b1.path().to_path_buf(),
        num_partitions: 4,
        peers: peers.clone(),
        bootstrap: false,
        bolt_address: None,
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };

    let components_a = meshdb_server::build_components(&config_a).await.unwrap();
    let components_b = meshdb_server::build_components(&config_b1).await.unwrap();
    let raft_a = components_a.raft.clone().unwrap();
    let raft_b1 = components_b.raft.clone().unwrap();

    let _peer_a = spawn_peer(listener_a, components_a);
    let peer_b1 = spawn_peer(listener_b, components_b);

    tokio::time::sleep(Duration::from_millis(80)).await;

    meshdb_server::initialize_if_seed(&config_a, &raft_a)
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
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };

    let listener_b2 = TcpListener::bind(addr_b).await.unwrap();
    let components_b2 = meshdb_server::build_components(&config_b2).await.unwrap();
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
    // Poll: under heavy CI load, tail-of-log replay lags the snapshot
    // apply — the single-shot version of this read went flaky when
    // `found_pinned` passed on the snapshot alone and tail replay
    // hadn't landed the later CREATEs yet.
    let mut count = 0;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let resp = query_b
            .execute_cypher(ExecuteCypherRequest {
                query: "MATCH (n:Marker) RETURN n".into(),
                params_json: vec![],
            })
            .await
            .unwrap();
        let rows: serde_json::Value = serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
        count = rows.as_array().map(|a| a.len()).unwrap_or(0);
        if count >= 20 {
            break;
        }
    }
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
            multi_raft: _,
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
            bolt_address: None,
            weight: None,
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
            bolt_address: None,
            weight: None,
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
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
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
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };

    let components_a = meshdb_server::build_components(&config_a).await.unwrap();
    let components_b = meshdb_server::build_components(&config_b).await.unwrap();
    let raft_a = components_a.raft.clone().unwrap();

    let peer_a = spawn_peer(listener_a, components_a);
    let _peer_b = spawn_peer(listener_b, components_b);

    tokio::time::sleep(Duration::from_millis(80)).await;

    meshdb_server::initialize_if_seed(&config_a, &raft_a)
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
    use meshdb_cluster::raft::{MemStore, PersistedSnapshot};
    use meshdb_cluster::{ClusterState, Membership, PartitionMap, Peer};

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

    // Decode the graph blob by feeding it through the applier's
    // restore path into a fresh Store, then inspect the result.
    // The blob is a binary rocksdb checkpoint archive (see
    // meshdb_rpc::raft_applier) — the previous test decoded it as
    // JSON, but that format only existed in the old Vec<Node>-of-the-
    // world implementation and isn't how real production snapshots
    // are shaped anymore.
    let restored_dir = tempfile::TempDir::new().unwrap();
    let restored_store: std::sync::Arc<dyn meshdb_storage::StorageEngine> = std::sync::Arc::new(
        meshdb_storage::RocksDbStorageEngine::open(restored_dir.path()).unwrap(),
    );
    let applier = meshdb_rpc::StoreGraphApplier::new(restored_store.clone());
    use meshdb_cluster::raft::GraphStateMachine;
    applier
        .restore(&payload.graph)
        .expect("restoring the graph snapshot into a fresh store");
    let nodes = restored_store.all_nodes().unwrap();
    // The snapshot may have fired before every CREATE was applied — what
    // matters is that the graph payload carries *some* of the data, not
    // just the cluster metadata. A previous-version snapshot with only
    // ClusterState would land here with zero nodes.
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
            bolt_address: None,
            weight: None,
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
            bolt_address: None,
            weight: None,
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
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
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
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };

    let components_a = meshdb_server::build_components(&config_a).await.unwrap();
    let components_b = meshdb_server::build_components(&config_b).await.unwrap();
    let raft_a = components_a.raft.clone().unwrap();

    let ServerComponents {
        service: service_a,
        raft: _,
        raft_service: raft_service_a,
        multi_raft: _,
    } = components_a;
    let ServerComponents {
        service: service_b,
        raft: _,
        raft_service: raft_service_b,
        multi_raft: _,
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

    meshdb_server::initialize_if_seed(&config_a, &raft_a)
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
            bolt_address: None,
            weight: None,
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
            bolt_address: None,
            weight: None,
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
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
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
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };

    let components_a = meshdb_server::build_components(&config_a).await.unwrap();
    let components_b = meshdb_server::build_components(&config_b).await.unwrap();
    let raft_a = components_a.raft.clone().unwrap();

    let ServerComponents {
        service: service_a,
        raft: _,
        raft_service: raft_service_a,
        multi_raft: _,
    } = components_a;
    let ServerComponents {
        service: service_b,
        raft: _,
        raft_service: raft_service_b,
        multi_raft: _,
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

    meshdb_server::initialize_if_seed(&config_a, &raft_a)
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
            bolt_address: None,
            weight: None,
        },
        PeerConfig {
            id: 2,
            address: addr_b.to_string(),
            bolt_address: None,
            weight: None,
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
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
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
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };

    // Build components for both peers.
    let components_a = meshdb_server::build_components(&config_a).await.unwrap();
    let components_b = meshdb_server::build_components(&config_b).await.unwrap();

    // Pull the Raft handles before destructuring components into the server tasks.
    let raft_a = components_a.raft.clone().unwrap();
    let raft_b = components_b.raft.clone().unwrap();

    let ServerComponents {
        service: service_a,
        raft: _,
        raft_service: raft_service_a,
        multi_raft: _,
    } = components_a;
    let ServerComponents {
        service: service_b,
        raft: _,
        raft_service: raft_service_b,
        multi_raft: _,
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
    meshdb_server::initialize_if_seed(&config_a, &raft_a)
        .await
        .expect("seed peer should bootstrap");
    // (initialize_if_seed on B is a no-op because bootstrap = false.)
    meshdb_server::initialize_if_seed(&config_b, &raft_b)
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

/// Parsed server-side ROUTE response. `send_route` returns this to
/// both the single-ROUTE and handoff tests below so they can assert
/// on the shape without re-implementing the packstream dance.
struct RouteReply {
    route_addrs: Vec<String>,
    read_addrs: Vec<String>,
    write_addrs: Vec<String>,
    ttl: i64,
    db: String,
}

/// Drive a real `neo4j://`-flavoured ROUTE exchange against a Bolt
/// listener: handshake, HELLO, LOGON (Bolt 5.1+ gates ROUTE behind
/// it), ROUTE. Parses the `rt` metadata into per-role address lists
/// plus the TTL and db name. Test-only helper — inline everything so
/// a failure stack trace points at the caller, not a shared layer.
async fn send_route(bolt_addr: &str) -> RouteReply {
    use meshdb_bolt::{
        perform_client_handshake, read_message, write_message, BoltMessage, BoltValue, BOLT_5_4,
    };
    use tokio::net::TcpStream;

    let mut sock = TcpStream::connect(bolt_addr).await.unwrap();
    let preferences = [BOLT_5_4, [0; 4], [0; 4], [0; 4]];
    let agreed = perform_client_handshake(&mut sock, &preferences)
        .await
        .unwrap();
    assert_eq!(agreed, BOLT_5_4);

    write_message(
        &mut sock,
        &BoltMessage::Hello {
            extra: BoltValue::map([
                ("user_agent", BoltValue::String("mesh-test/0.1".into())),
                ("scheme", BoltValue::String("none".into())),
            ]),
        }
        .encode(),
    )
    .await
    .unwrap();
    let _hello_ack = read_message(&mut sock).await.unwrap();
    // Bolt 5.1+ needs LOGON before ROUTE.
    write_message(
        &mut sock,
        &BoltMessage::Logon {
            auth: BoltValue::map([("scheme", BoltValue::String("none".into()))]),
        }
        .encode(),
    )
    .await
    .unwrap();
    let _logon_ack = read_message(&mut sock).await.unwrap();

    write_message(
        &mut sock,
        &BoltMessage::Route {
            routing: BoltValue::Map(vec![]),
            bookmarks: BoltValue::List(vec![]),
            extra: BoltValue::Map(vec![]),
        }
        .encode(),
    )
    .await
    .unwrap();
    let raw = read_message(&mut sock).await.unwrap();
    let reply = BoltMessage::decode(&raw).unwrap();
    let metadata = match reply {
        BoltMessage::Success { metadata } => metadata,
        other => panic!("expected SUCCESS, got {other:?}"),
    };
    let rt = metadata.get("rt").expect("rt metadata").clone();
    let servers = rt
        .get("servers")
        .and_then(|v| match v {
            BoltValue::List(xs) => Some(xs.clone()),
            _ => None,
        })
        .expect("servers list");
    assert_eq!(servers.len(), 3, "ROUTE + READ + WRITE");
    let addrs_for = |role: &str| -> Vec<String> {
        servers
            .iter()
            .find_map(|s| {
                let role_match = s.get("role").and_then(|v| v.as_str()) == Some(role);
                if !role_match {
                    return None;
                }
                s.get("addresses").map(|a| match a {
                    BoltValue::List(xs) => xs
                        .iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect(),
                    _ => Vec::new(),
                })
            })
            .unwrap_or_default()
    };
    let ttl = match rt.get("ttl") {
        Some(BoltValue::Int(n)) => *n,
        _ => 0,
    };
    let db = rt
        .get("db")
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default();
    RouteReply {
        route_addrs: addrs_for("ROUTE"),
        read_addrs: addrs_for("READ"),
        write_addrs: addrs_for("WRITE"),
        ttl,
        db,
    }
}

#[tokio::test]
async fn route_success_lists_peers_and_leader_under_the_right_roles() {
    // 2-peer Raft cluster with Bolt listeners on both peers. Driver
    // ROUTE arrives at the follower; the response must carry:
    //   - ROUTE and READ: both peers' Bolt addresses.
    //   - WRITE: only the current Raft leader's Bolt address.
    //
    // Skipping the usual `serve()` flow here because `serve()` binds
    // the Bolt listener internally from `config.bolt_address` —
    // which would require knowing the Bolt port before the config
    // exists. Instead we pre-bind both the gRPC and Bolt listeners,
    // construct configs with the already-bound addresses in
    // `peers[].bolt_address`, and spawn the Bolt listener ourselves
    // with a hand-crafted `RouteContext`. Exercises the same
    // `route_success` path the binary hits.
    use meshdb_cluster::{Membership, Peer};
    use meshdb_server::bolt::{run_listener, RouteContext};
    use tokio::time::{sleep, Duration};

    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let grpc_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bolt_a_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bolt_b_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr_a = grpc_a.local_addr().unwrap().to_string();
    let grpc_addr_b = grpc_b.local_addr().unwrap().to_string();
    let bolt_addr_a = bolt_a_listener.local_addr().unwrap().to_string();
    let bolt_addr_b = bolt_b_listener.local_addr().unwrap().to_string();

    let peers = vec![
        PeerConfig {
            id: 1,
            address: grpc_addr_a.clone(),
            bolt_address: Some(bolt_addr_a.clone()),
            weight: None,
        },
        PeerConfig {
            id: 2,
            address: grpc_addr_b.clone(),
            bolt_address: Some(bolt_addr_b.clone()),
            weight: None,
        },
    ];
    let mk_config = |self_id: u64, data_dir: PathBuf, grpc: &str, bootstrap: bool| ServerConfig {
        self_id,
        listen_address: grpc.to_string(),
        data_dir,
        num_partitions: 4,
        peers: peers.clone(),
        bootstrap,
        bolt_address: None, // we spawn the Bolt listener manually
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let config_a = mk_config(1, dir_a.path().to_path_buf(), &grpc_addr_a, true);
    let config_b = mk_config(2, dir_b.path().to_path_buf(), &grpc_addr_b, false);

    let components_a = meshdb_server::build_components(&config_a).await.unwrap();
    let components_b = meshdb_server::build_components(&config_b).await.unwrap();
    let raft_a = components_a.raft.clone().unwrap();
    let raft_b = components_b.raft.clone().unwrap();

    let ServerComponents {
        service: service_a,
        raft: _,
        raft_service: raft_service_a,
        multi_raft: _,
    } = components_a;
    let ServerComponents {
        service: service_b,
        raft: _,
        raft_service: raft_service_b,
        multi_raft: _,
    } = components_b;
    let raft_service_a = raft_service_a.unwrap();
    let raft_service_b = raft_service_b.unwrap();

    let service_a = Arc::new(service_a);
    let service_b = Arc::new(service_b);

    // gRPC servers for Raft replication + Query/Write.
    tokio::spawn({
        let svc = service_a.clone();
        async move {
            Server::builder()
                .add_service((*svc).clone().into_query_server())
                .add_service((*svc).clone().into_write_server())
                .add_service(raft_service_a.into_server())
                .serve_with_incoming(TcpListenerStream::new(grpc_a))
                .await
                .unwrap();
        }
    });
    tokio::spawn({
        let svc = service_b.clone();
        async move {
            Server::builder()
                .add_service((*svc).clone().into_query_server())
                .add_service((*svc).clone().into_write_server())
                .add_service(raft_service_b.into_server())
                .serve_with_incoming(TcpListenerStream::new(grpc_b))
                .await
                .unwrap();
        }
    });

    // Build Membership snapshots (both peers see both addresses).
    let membership_for_route = Arc::new(Membership::new(vec![
        Peer::new(PeerId(1), grpc_addr_a.clone()).with_bolt_address(bolt_addr_a.clone()),
        Peer::new(PeerId(2), grpc_addr_b.clone()).with_bolt_address(bolt_addr_b.clone()),
    ]));

    // Manually spawn each peer's Bolt listener with a RouteContext
    // that can see the full peer set + its own Raft handle.
    tokio::spawn({
        let service = service_a.clone();
        let ctx = Arc::new(RouteContext {
            local_advertised: bolt_addr_a.clone(),
            peers: membership_for_route.clone(),
            raft: Some(raft_a.clone()),
        });
        async move {
            let _ = run_listener(bolt_a_listener, service, None, None, None, ctx).await;
        }
    });
    tokio::spawn({
        let service = service_b.clone();
        let ctx = Arc::new(RouteContext {
            local_advertised: bolt_addr_b.clone(),
            peers: membership_for_route.clone(),
            raft: Some(raft_b.clone()),
        });
        async move {
            let _ = run_listener(bolt_b_listener, service, None, None, None, ctx).await;
        }
    });

    sleep(Duration::from_millis(80)).await;

    meshdb_server::initialize_if_seed(&config_a, &raft_a)
        .await
        .unwrap();

    // Wait for leadership — peer A bootstraps, should become leader.
    let mut leader_id: Option<u64> = None;
    for _ in 0..60 {
        sleep(Duration::from_millis(100)).await;
        if let Some(id) = raft_a.raft.metrics().borrow().current_leader {
            leader_id = Some(id);
            break;
        }
    }
    let leader = leader_id.expect("raft should elect a leader within 6s");
    assert_eq!(leader, 1, "peer A bootstrapped so it should be leader");

    // Ask the follower for its routing table.
    let reply = send_route(&bolt_addr_b).await;

    // ROUTE and READ both contain both peer addresses (order may vary,
    // self goes first). We check set membership, not order.
    for role_addrs in [&reply.route_addrs, &reply.read_addrs] {
        assert!(
            role_addrs.iter().any(|a| a == &bolt_addr_a),
            "role list should include leader {bolt_addr_a}: {role_addrs:?}"
        );
        assert!(
            role_addrs.iter().any(|a| a == &bolt_addr_b),
            "role list should include follower {bolt_addr_b}: {role_addrs:?}"
        );
    }
    // WRITE lists only the leader (peer A).
    assert_eq!(
        reply.write_addrs,
        vec![bolt_addr_a.clone()],
        "WRITE should name the leader only: {:?}",
        reply.write_addrs
    );
    assert!(reply.ttl > 0, "ttl should be positive");
    assert_eq!(reply.db, "neo4j");
}

#[tokio::test]
async fn route_write_role_tracks_raft_leader_across_handoff() {
    // 3-peer Raft cluster. After a steady-state ROUTE confirms the
    // seed is leader, shut down the seed's Raft and wait for the
    // survivors to elect a new one. A second ROUTE on a still-running
    // peer must reflect the new leader under WRITE — proving the
    // handler resolves `current_leader` dynamically per request
    // rather than snapshotting at listener-startup time.
    //
    // Needs 3 peers: with 2, quorum is 2 and losing one leaves the
    // survivor unable to win an election, so a handoff test would
    // never converge. With 3, quorum is 2 and killing any one peer
    // still lets the remaining pair elect.
    use meshdb_cluster::{Membership, Peer};
    use meshdb_server::bolt::{run_listener, RouteContext};
    use tokio::time::{sleep, Duration};

    // --- Setup: 3 peers × (gRPC listener, Bolt listener, dir). ---
    let dirs: [TempDir; 3] = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    let mut grpc_listeners = Vec::new();
    let mut bolt_listeners = Vec::new();
    let mut grpc_addrs = Vec::new();
    let mut bolt_addrs = Vec::new();
    for _ in 0..3 {
        let g = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        grpc_addrs.push(g.local_addr().unwrap().to_string());
        bolt_addrs.push(b.local_addr().unwrap().to_string());
        grpc_listeners.push(g);
        bolt_listeners.push(b);
    }

    let peers = (0..3)
        .map(|i| PeerConfig {
            id: (i + 1) as u64,
            address: grpc_addrs[i].clone(),
            bolt_address: Some(bolt_addrs[i].clone()),
            weight: None,
        })
        .collect::<Vec<_>>();

    let mk_config = |self_id: u64, data_dir: PathBuf, grpc: &str, bootstrap: bool| ServerConfig {
        self_id,
        listen_address: grpc.to_string(),
        data_dir,
        num_partitions: 4,
        peers: peers.clone(),
        bootstrap,
        bolt_address: None,
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };
    let configs = [
        mk_config(1, dirs[0].path().to_path_buf(), &grpc_addrs[0], true),
        mk_config(2, dirs[1].path().to_path_buf(), &grpc_addrs[1], false),
        mk_config(3, dirs[2].path().to_path_buf(), &grpc_addrs[2], false),
    ];

    // --- Build components for all three peers. ---
    let mut rafts = Vec::new();
    let mut services: Vec<Arc<meshdb_rpc::MeshService>> = Vec::new();
    let mut raft_services = Vec::new();
    for cfg in &configs {
        let comp = meshdb_server::build_components(cfg).await.unwrap();
        rafts.push(comp.raft.clone().unwrap());
        let ServerComponents {
            service,
            raft: _,
            raft_service,
            multi_raft: _,
        } = comp;
        services.push(Arc::new(service));
        raft_services.push(raft_service.unwrap());
    }

    // --- Spawn gRPC + Bolt for each peer. ---
    // `raft_services` holds owned values that must each be consumed
    // exactly once by `.into_server()`; drain in sync with
    // `grpc_listeners` so a zip gives each spawn its own pair.
    for ((grpc_l, svc), raft_svc) in grpc_listeners
        .into_iter()
        .zip(services.iter().cloned())
        .zip(raft_services.into_iter())
    {
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service((*svc).clone().into_query_server())
                .add_service((*svc).clone().into_write_server())
                .add_service(raft_svc.into_server())
                .serve_with_incoming(TcpListenerStream::new(grpc_l))
                .await;
        });
    }

    let membership_for_route = Arc::new(Membership::new(
        (0..3)
            .map(|i| {
                Peer::new(PeerId((i + 1) as u64), grpc_addrs[i].clone())
                    .with_bolt_address(bolt_addrs[i].clone())
            })
            .collect::<Vec<_>>(),
    ));

    for (i, bolt_l) in bolt_listeners.into_iter().enumerate() {
        let service = services[i].clone();
        let ctx = Arc::new(RouteContext {
            local_advertised: bolt_addrs[i].clone(),
            peers: membership_for_route.clone(),
            raft: Some(rafts[i].clone()),
        });
        tokio::spawn(async move {
            let _ = run_listener(bolt_l, service, None, None, None, ctx).await;
        });
    }

    sleep(Duration::from_millis(80)).await;

    meshdb_server::initialize_if_seed(&configs[0], &rafts[0])
        .await
        .unwrap();

    // Wait for the seed to become leader.
    for _ in 0..60 {
        sleep(Duration::from_millis(100)).await;
        if rafts[0].raft.metrics().borrow().current_leader == Some(1) {
            break;
        }
    }
    assert_eq!(
        rafts[0].raft.metrics().borrow().current_leader,
        Some(1),
        "peer 1 (seed) should become leader within 6s"
    );

    // Steady-state ROUTE: WRITE = peer 1.
    let pre = send_route(&bolt_addrs[2]).await;
    assert_eq!(
        pre.write_addrs,
        vec![bolt_addrs[0].clone()],
        "pre-handoff WRITE should name the leader (peer 1)"
    );

    // Stop peer 1's Raft. Its gRPC + Bolt listeners keep running, but
    // it no longer participates in consensus — peers 2 and 3 lose
    // heartbeats, time out, and one wins the new election.
    rafts[0]
        .raft
        .shutdown()
        .await
        .expect("shutdown peer 1 raft");

    // Wait for a new leader ≠ 1. Poll both survivors — whichever
    // campaigns wins first.
    let mut new_leader: Option<u64> = None;
    for _ in 0..80 {
        sleep(Duration::from_millis(100)).await;
        for idx in [1usize, 2] {
            if let Some(id) = rafts[idx].raft.metrics().borrow().current_leader {
                if id != 1 {
                    new_leader = Some(id);
                    break;
                }
            }
        }
        if new_leader.is_some() {
            break;
        }
    }
    let new_leader = new_leader.expect("survivors should elect a new leader within 8s");
    assert!(matches!(new_leader, 2 | 3), "new leader must be 2 or 3");
    let new_leader_bolt = bolt_addrs[(new_leader - 1) as usize].clone();

    // Ask a survivor for the new routing table. Pick the non-leader
    // survivor so we're exercising follower-path ROUTE.
    let follower_bolt = if new_leader == 2 {
        &bolt_addrs[2]
    } else {
        &bolt_addrs[1]
    };
    let post = send_route(follower_bolt).await;
    assert_eq!(
        post.write_addrs,
        vec![new_leader_bolt.clone()],
        "post-handoff WRITE should name the new leader ({new_leader}): {post:?}",
        post = post.write_addrs
    );

    // ROUTE/READ still list all three peers (Membership is static —
    // we don't drop peer 1 from the routing table just because its
    // Raft is down; the driver will discover the outage itself).
    for role_addrs in [&post.route_addrs, &post.read_addrs] {
        for addr in &bolt_addrs {
            assert!(
                role_addrs.iter().any(|a| a == addr),
                "role list should include {addr}: {role_addrs:?}"
            );
        }
    }
}

#[tokio::test]
async fn metrics_endpoint_serves_prometheus_text_with_workload() {
    // Spin up a single-node service, mount the metrics axum app on a
    // separate listener, run a workload that exercises both a
    // generic Cypher query and an indexed MATCH, then HTTP GET the
    // metrics endpoint and verify the counters reflect the work.
    //
    // Counters are process-global, so we capture before/after and
    // assert deltas rather than absolute values — the test is
    // robust to other tests running in parallel within the same
    // cargo binary.
    let dir = TempDir::new().unwrap();
    let config = ServerConfig {
        self_id: 1,
        listen_address: "127.0.0.1:0".into(),
        data_dir: dir.path().to_path_buf(),
        num_partitions: 4,
        peers: vec![],
        bootstrap: false,
        bolt_address: None,
        metrics_address: None,
        bolt_auth: None,
        bolt_tls: None,
        bolt_advertised_versions: None,
        bolt_advertised_address: None,
        grpc_tls: None,
        mode: None,
        replication_factor: None,
        read_consistency: None,
        #[cfg(feature = "apoc-load")]
        apoc_import: None,
        cluster_auth: None,
    };

    let service = meshdb_server::build_service(&config).unwrap();
    let grpc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr = grpc_listener.local_addr().unwrap();
    let metrics_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metrics_addr = metrics_listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(service.clone().into_query_server())
            .add_service(service.into_write_server())
            .serve_with_incoming(TcpListenerStream::new(grpc_listener))
            .await
            .unwrap();
    });
    tokio::spawn(async move {
        meshdb_server::metrics::run_listener(metrics_listener)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(80)).await;

    let queries_before = meshdb_rpc::metrics::CYPHER_QUERIES_TOTAL
        .with_label_values(&[meshdb_rpc::metrics::MODE_SINGLE])
        .get();
    let seeks_before = meshdb_rpc::metrics::CYPHER_INDEX_SEEKS_TOTAL.get();

    // Workload: create an index, seed nodes, run an indexed MATCH.
    let mut q = MeshQueryClient::connect(format!("http://{}", grpc_addr))
        .await
        .unwrap();
    q.execute_cypher(ExecuteCypherRequest {
        query: "CREATE INDEX FOR (p:Person) ON (p.name)".into(),
        params_json: vec![],
    })
    .await
    .unwrap();
    q.execute_cypher(ExecuteCypherRequest {
        query: "CREATE (:Person {name: 'Ada'})".into(),
        params_json: vec![],
    })
    .await
    .unwrap();
    q.execute_cypher(ExecuteCypherRequest {
        query: "MATCH (p:Person {name: 'Ada'}) RETURN p.name AS n".into(),
        params_json: vec![],
    })
    .await
    .unwrap();

    let queries_after = meshdb_rpc::metrics::CYPHER_QUERIES_TOTAL
        .with_label_values(&[meshdb_rpc::metrics::MODE_SINGLE])
        .get();
    let seeks_after = meshdb_rpc::metrics::CYPHER_INDEX_SEEKS_TOTAL.get();

    assert!(
        queries_after - queries_before >= 3,
        "expected at least 3 queries to register on counter, before={queries_before} after={queries_after}"
    );
    assert!(
        seeks_after - seeks_before >= 1,
        "indexed MATCH should have bumped the IndexSeek counter, before={seeks_before} after={seeks_after}"
    );

    // HTTP scrape — verify the endpoint actually returns Prometheus
    // text and includes our metric names.
    let body = reqwest::get(format!("http://{}/metrics", metrics_addr))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        body.contains("meshdb_cypher_queries_total"),
        "metrics body missing cypher counter: {body}"
    );
    assert!(
        body.contains("meshdb_cypher_index_seeks_total"),
        "metrics body missing index seek counter"
    );
    assert!(
        body.contains("meshdb_cypher_query_duration_seconds"),
        "metrics body missing query duration histogram"
    );
}
