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

/// Capturing verifier for the gRPC hot-reload test below — bypasses
/// peer-cert validation (the cert is self-signed, intentionally) and
/// records the leaf cert DER for comparison across reloads.
#[derive(Debug)]
struct CapturingGrpcVerifier {
    captured: std::sync::Mutex<Option<Vec<u8>>>,
}

impl CapturingGrpcVerifier {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            captured: std::sync::Mutex::new(None),
        })
    }
    fn captured(&self) -> Option<Vec<u8>> {
        self.captured.lock().unwrap().clone()
    }
}

impl rustls::client::danger::ServerCertVerifier for CapturingGrpcVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        *self.captured.lock().unwrap() = Some(end_entity.as_ref().to_vec());
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _msg: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _msg: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}

/// Connect to `addr` over TLS, send one Health RPC, and return
/// the leaf cert the server presented.
async fn fetch_grpc_presented_cert(addr: &str) -> Vec<u8> {
    let verifier = CapturingGrpcVerifier::new();
    let client_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier.clone())
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    // Just complete the TLS handshake — no need to drive an actual
    // gRPC RPC, the cert verifier captures during handshake.
    let _tls_stream = connector.connect(server_name, stream).await.unwrap();
    verifier.captured().expect("verifier captured a cert")
}

/// gRPC TLS hot-reload swaps the server cert without dropping
/// the listener. Connect once → see cert A. Replace cert files
/// in place. Wait past the reload interval. Connect again → see
/// cert B.
#[tokio::test]
async fn grpc_tls_hot_reload_picks_up_replaced_cert() {
    use meshdb_server::tls_reload::build_hot_reloading_tls_acceptor;

    install_default_crypto_provider();
    let dir = TempDir::new().unwrap();
    let fixture = write_self_signed(&dir);

    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path().join("db")).unwrap());
    let service = MeshService::new(store);

    let (acceptor, _reload_handle) = build_hot_reloading_tls_acceptor(
        &fixture.cert_path,
        &fixture.key_path,
        Duration::from_millis(50),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Custom incoming: TLS handshake per-connection, plaintext
    // streams forwarded to tonic. Mirrors the lib.rs serve() path.
    let (tx, rx) = tokio::sync::mpsc::channel::<
        std::result::Result<tokio_rustls::server::TlsStream<tokio::net::TcpStream>, std::io::Error>,
    >(32);
    tokio::spawn(async move {
        loop {
            let (tcp, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => continue,
            };
            let acceptor = acceptor.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Ok(s) = acceptor.accept(tcp).await {
                    let _ = tx.send(Ok(s)).await;
                }
            });
        }
    });
    tokio::spawn(async move {
        Server::builder()
            .add_service(service.clone().into_query_server())
            .add_service(service.into_write_server())
            .serve_with_incoming(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(80)).await;

    let cert_a = fetch_grpc_presented_cert(&addr.to_string()).await;

    // Sleep past mtime resolution, swap cert files.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let params2 =
        rcgen::CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .unwrap();
    let key_pair2 = rcgen::KeyPair::generate().unwrap();
    let cert2 = params2.self_signed(&key_pair2).unwrap();
    std::fs::write(&fixture.cert_path, cert2.pem()).unwrap();
    std::fs::write(&fixture.key_path, key_pair2.serialize_pem()).unwrap();

    // Wait for at least one reload tick to land.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let cert_b = fetch_grpc_presented_cert(&addr.to_string()).await;

    assert_ne!(
        cert_a, cert_b,
        "after hot-reload the gRPC listener should present the new cert"
    );
}
