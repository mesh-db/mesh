use mesh_core::Node;
use mesh_rpc::convert::{node_to_proto, uuid_to_proto};
use mesh_rpc::proto::mesh_query_client::MeshQueryClient;
use mesh_rpc::proto::mesh_write_client::MeshWriteClient;
use mesh_rpc::proto::{GetNodeRequest, HealthRequest, PutNodeRequest};
use mesh_server::config::{PeerConfig, ServerConfig};
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
    };
    let config_b = ServerConfig {
        self_id: 2,
        listen_address: addr_b.to_string(),
        data_dir: dir_b.path().to_path_buf(),
        num_partitions: 4,
        peers,
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
