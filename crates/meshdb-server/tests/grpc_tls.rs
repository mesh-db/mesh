//! End-to-end tests for the gRPC TLS listener. Generates a fresh
//! self-signed certificate per test via `rcgen`, wires it into a
//! single-node `MeshService`, and verifies that:
//!
//!   * a tonic client configured with the matching CA bundle can
//!     complete health + cypher round-trips over TLS
//!   * a plaintext client is rejected (the TLS handshake fails before
//!     any gRPC message is exchanged)
//!
//! The cert carries `DNS:localhost` and `IP:127.0.0.1` SANs so it
//! works against a loopback `127.0.0.1:<port>` listener.

use meshdb_rpc::proto::mesh_query_client::MeshQueryClient;
use meshdb_rpc::proto::{ExecuteCypherRequest, HealthRequest};
use meshdb_rpc::tls::{
    build_client_tls_config, build_server_tls_config, install_default_crypto_provider,
};
use meshdb_rpc::MeshService;
use meshdb_storage::{RocksDbStorageEngine as Store, StorageEngine};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};

/// Generate a self-signed cert + key inside `dir`. The cert is its
/// own CA for test purposes, so `ca_path` and `cert_path` point at
/// the same file.
struct TlsFixture {
    cert_path: PathBuf,
    key_path: PathBuf,
    ca_path: PathBuf,
}

fn write_self_signed(dir: &TempDir) -> TlsFixture {
    // Cover both the DNS name and the literal 127.0.0.1 IP so peer
    // URIs that use either form verify successfully.
    let params =
        rcgen::CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .unwrap();
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();

    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key_pair.serialize_pem()).unwrap();

    TlsFixture {
        ca_path: cert_path.clone(),
        cert_path,
        key_path,
    }
}

struct TlsServerHandle {
    addr: String,
    fixture: TlsFixture,
    _dir: TempDir,
}

async fn spawn_tls_server() -> TlsServerHandle {
    install_default_crypto_provider();
    let dir = TempDir::new().unwrap();
    let fixture = write_self_signed(&dir);

    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path().join("db")).unwrap());
    let service = MeshService::new(store);

    let server_tls = build_server_tls_config(&fixture.cert_path, &fixture.key_path).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls)
            .unwrap()
            .add_service(service.clone().into_query_server())
            .add_service(service.into_write_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Give the listener a moment to enter its accept loop.
    tokio::time::sleep(Duration::from_millis(80)).await;

    TlsServerHandle {
        addr: addr.to_string(),
        fixture,
        _dir: dir,
    }
}

async fn tls_client_channel(handle: &TlsServerHandle) -> Channel {
    let client_tls = build_client_tls_config(&handle.fixture.ca_path).unwrap();
    let uri = format!(
        "https://localhost:{}",
        handle.addr.split(':').nth(1).unwrap()
    );
    Endpoint::from_shared(uri)
        .unwrap()
        .tls_config(client_tls)
        .unwrap()
        .connect()
        .await
        .unwrap()
}

#[tokio::test]
async fn grpc_tls_health_succeeds_with_matching_ca() {
    let handle = spawn_tls_server().await;
    let mut client = MeshQueryClient::new(tls_client_channel(&handle).await);
    let resp = client.health(HealthRequest {}).await.unwrap();
    assert!(resp.into_inner().serving);
}

#[tokio::test]
async fn grpc_tls_cypher_create_and_match_round_trips() {
    let handle = spawn_tls_server().await;
    let mut client = MeshQueryClient::new(tls_client_channel(&handle).await);

    // Write + read path over TLS.
    let params = serde_json::to_vec(&serde_json::json!({})).unwrap();
    client
        .execute_cypher(ExecuteCypherRequest {
            query: "CREATE (n:T {v: 42})".into(),
            params_json: params.clone(),
        })
        .await
        .unwrap();

    let resp = client
        .execute_cypher(ExecuteCypherRequest {
            query: "MATCH (n:T) RETURN n.v AS v".into(),
            params_json: params,
        })
        .await
        .unwrap();
    let rows: serde_json::Value = serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
    let arr = rows.as_array().expect("rows should be a JSON array");
    assert_eq!(arr.len(), 1);
    // Row shape: {"v": {"Property": {"type": "Int64", "value": 42}}}.
    // See the sibling Raft-integration test for the full decoding note.
    assert_eq!(
        arr[0]["v"]["Property"]["value"].as_i64().unwrap(),
        42,
        "row: {}",
        arr[0]
    );
}

#[tokio::test]
async fn grpc_tls_rejects_plaintext_client() {
    let handle = spawn_tls_server().await;
    // No tls_config on the endpoint — this should fail at connect
    // time because the server expects a TLS handshake.
    let uri = format!("http://{}", handle.addr);
    let endpoint = Endpoint::from_shared(uri)
        .unwrap()
        .connect_timeout(Duration::from_millis(500));
    let result = endpoint.connect().await;
    // tonic may surface this as either a transport error at `connect`
    // or as an error on the first RPC; we just require that a plain
    // HTTP/2 talker cannot successfully complete a health check.
    match result {
        Err(_) => {}
        Ok(channel) => {
            let mut client = MeshQueryClient::new(channel);
            let resp = client.health(HealthRequest {}).await;
            assert!(
                resp.is_err(),
                "plaintext client unexpectedly succeeded against TLS-only listener"
            );
        }
    }
}
