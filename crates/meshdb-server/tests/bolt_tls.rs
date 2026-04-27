//! End-to-end tests for the Bolt TLS listener. Generates a fresh
//! self-signed certificate per test via `rcgen`, writes it to a
//! tempdir, and drives a Bolt RUN/PULL round-trip over TLS from a
//! `tokio-rustls` client with certificate verification disabled (the
//! cert is self-signed; we're testing the transport, not a PKI).

// Same recursion_limit bump as meshdb-server::lib — Bolt's nested
// async dispatch overflows the default 128 query depth.
#![recursion_limit = "256"]

use meshdb_bolt::{
    perform_client_handshake, read_message, write_message, BoltMessage, BoltValue, BOLT_4_4,
};
use meshdb_rpc::MeshService;
use meshdb_server::bolt::{
    build_tls_acceptor, build_tls_acceptor_with_reload, install_default_crypto_provider,
    run_listener, RouteContext,
};
use meshdb_storage::{RocksDbStorageEngine as Store, StorageEngine};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsConnector;

/// Self-signed cert/key pair written to `dir` as `cert.pem` /
/// `key.pem`. The cert covers `localhost` as its only SAN, which is
/// enough for a loopback test even though we bypass verification
/// client-side.
fn write_self_signed_pair(dir: &TempDir) -> (PathBuf, PathBuf) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();
    (cert_path, key_path)
}

/// Spawn a single-node mesh service + TLS Bolt listener on an
/// ephemeral port. Returns the bound address and the tempdir that
/// holds both the RocksDB store and the cert material so they stay
/// alive for the duration of the test.
async fn spawn_tls_bolt_server() -> (String, TempDir) {
    install_default_crypto_provider();

    let dir = TempDir::new().unwrap();
    let (cert_path, key_path) = write_self_signed_pair(&dir);

    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path().join("db")).unwrap());
    let service = Arc::new(MeshService::new(store));

    let acceptor = build_tls_acceptor(&cert_path, &key_path).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let _ = run_listener(
            listener,
            service,
            None,
            Some(acceptor),
            None,
            Arc::new(RouteContext {
                local_advertised: addr.to_string(),
                peers: Arc::new(meshdb_cluster::Membership::new(std::iter::empty())),
                raft: None,
                multi_raft: None,
                routing_ttl_seconds: None,
            }),
        )
        .await;
    });

    // Give the listener a moment to enter its accept loop.
    tokio::time::sleep(Duration::from_millis(25)).await;

    (addr.to_string(), dir)
}

#[derive(Debug)]
struct NoVerification;

impl ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::ED25519,
        ]
    }
}

async fn tls_connect(addr: &str) -> tokio_rustls::client::TlsStream<TcpStream> {
    let client_cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerification))
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_cfg));
    let tcp = TcpStream::connect(addr).await.unwrap();
    let server_name = ServerName::try_from("localhost").unwrap();
    connector.connect(server_name, tcp).await.unwrap()
}

#[tokio::test]
async fn bolt_tls_run_pull_round_trip() {
    let (addr, _dir) = spawn_tls_bolt_server().await;
    let mut sock = tls_connect(&addr).await;

    // Handshake over TLS.
    let prefs = [BOLT_4_4, [0; 4], [0; 4], [0; 4]];
    let agreed = perform_client_handshake(&mut sock, &prefs).await.unwrap();
    assert_eq!(agreed, BOLT_4_4);

    // HELLO (scheme=none — no bolt_auth configured on the server).
    let hello = BoltMessage::Hello {
        extra: BoltValue::map([
            ("user_agent", BoltValue::String("mesh-test/0.1".into())),
            ("scheme", BoltValue::String("none".into())),
        ]),
    };
    write_message(&mut sock, &hello.encode()).await.unwrap();
    let hello_reply = BoltMessage::decode(&read_message(&mut sock).await.unwrap()).unwrap();
    assert!(
        matches!(hello_reply, BoltMessage::Success { .. }),
        "HELLO should succeed over TLS, got {:?}",
        hello_reply
    );

    // One CREATE + one MATCH to prove the full request/response loop
    // is intact across the TLS layer.
    let run = BoltMessage::Run {
        query: "CREATE (n:T {v: 1}) RETURN n.v AS v".into(),
        params: BoltValue::Map(vec![]),
        extra: BoltValue::Map(vec![]),
    };
    write_message(&mut sock, &run.encode()).await.unwrap();
    let run_reply = BoltMessage::decode(&read_message(&mut sock).await.unwrap()).unwrap();
    assert!(matches!(run_reply, BoltMessage::Success { .. }));

    let pull = BoltMessage::Pull {
        extra: BoltValue::map([("n", BoltValue::Int(-1))]),
    };
    write_message(&mut sock, &pull.encode()).await.unwrap();

    let record = BoltMessage::decode(&read_message(&mut sock).await.unwrap()).unwrap();
    let fields = match record {
        BoltMessage::Record { fields } => fields,
        other => panic!("expected RECORD, got {:?}", other),
    };
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].as_int(), Some(1));

    let summary = BoltMessage::decode(&read_message(&mut sock).await.unwrap()).unwrap();
    assert!(matches!(summary, BoltMessage::Success { .. }));

    write_message(&mut sock, &BoltMessage::Goodbye.encode())
        .await
        .unwrap();
}

/// Extract the error from `build_tls_acceptor` as a string. Needed
/// because `TlsAcceptor` isn't `Debug`, so `Result::unwrap_err` can't
/// be used directly.
fn acceptor_err(result: Result<tokio_rustls::TlsAcceptor, anyhow::Error>) -> String {
    match result {
        Ok(_) => panic!("expected build_tls_acceptor to fail"),
        Err(e) => format!("{e:#}"),
    }
}

#[tokio::test]
async fn build_tls_acceptor_rejects_missing_cert_file() {
    install_default_crypto_provider();
    let dir = TempDir::new().unwrap();
    let (_cert, key) = write_self_signed_pair(&dir);
    let missing = dir.path().join("does-not-exist.pem");
    let err = acceptor_err(build_tls_acceptor(&missing, &key));
    assert!(
        err.contains("reading bolt tls cert"),
        "expected a `reading bolt tls cert` error, got: {err}"
    );
}

#[tokio::test]
async fn build_tls_acceptor_rejects_empty_cert_file() {
    install_default_crypto_provider();
    let dir = TempDir::new().unwrap();
    let (_cert, key) = write_self_signed_pair(&dir);
    let empty = dir.path().join("empty.pem");
    std::fs::write(&empty, b"").unwrap();
    let err = acceptor_err(build_tls_acceptor(&empty, &key));
    assert!(
        err.contains("no CERTIFICATE PEM blocks"),
        "expected an empty-cert error, got: {err}"
    );
}

#[tokio::test]
async fn build_tls_acceptor_rejects_empty_key_file() {
    install_default_crypto_provider();
    let dir = TempDir::new().unwrap();
    let (cert, _key) = write_self_signed_pair(&dir);
    let empty = dir.path().join("empty.pem");
    std::fs::write(&empty, b"").unwrap();
    let err = acceptor_err(build_tls_acceptor(&cert, &empty));
    assert!(
        err.contains("no PRIVATE KEY PEM block"),
        "expected an empty-key error, got: {err}"
    );
}

/// Custom server-cert verifier that captures the leaf cert DER
/// for inspection. Bypasses validation (the cert is self-signed)
/// and records the presented cert on every handshake.
#[derive(Debug)]
struct CapturingVerifier {
    captured: std::sync::Mutex<Option<Vec<u8>>>,
}

impl CapturingVerifier {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            captured: std::sync::Mutex::new(None),
        })
    }
    fn captured(&self) -> Option<Vec<u8>> {
        self.captured.lock().unwrap().clone()
    }
}

impl ServerCertVerifier for CapturingVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        *self.captured.lock().unwrap() = Some(end_entity.as_ref().to_vec());
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _msg: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _msg: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}

/// Connect TLS to a Bolt listener, do the handshake, capture the
/// leaf cert presented, and disconnect. Returns the cert DER.
async fn fetch_presented_cert(addr: &str) -> Vec<u8> {
    let verifier = CapturingVerifier::new();
    let client_config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier.clone())
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_config));
    let stream = TcpStream::connect(addr).await.unwrap();
    let server_name = ServerName::try_from("localhost").unwrap();
    let mut tls_stream = connector.connect(server_name, stream).await.unwrap();
    // Need at least one read to ensure handshake is complete and
    // the verifier has captured. Send the Bolt preamble + version
    // proposal so the server replies.
    let preferences = [BOLT_4_4, [0; 4], [0; 4], [0; 4]];
    let _ = perform_client_handshake(&mut tls_stream, &preferences)
        .await
        .unwrap();
    verifier
        .captured()
        .expect("verifier should have captured the leaf cert")
}

/// Hot-reload swaps the cert without taking the listener down.
/// Connect once → see cert A. Replace cert files in place. Wait
/// past the reload interval. Connect again → see cert B.
#[tokio::test]
async fn bolt_tls_hot_reload_picks_up_replaced_cert() {
    install_default_crypto_provider();

    let dir = TempDir::new().unwrap();
    let (cert_path, key_path) = write_self_signed_pair(&dir);

    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path().join("db")).unwrap());
    let service = Arc::new(MeshService::new(store));

    let (acceptor, _reload_handle) =
        build_tls_acceptor_with_reload(&cert_path, &key_path, Duration::from_millis(50)).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = run_listener(
            listener,
            service,
            None,
            Some(acceptor),
            None,
            Arc::new(RouteContext {
                local_advertised: addr.to_string(),
                peers: Arc::new(meshdb_cluster::Membership::new(std::iter::empty())),
                raft: None,
                multi_raft: None,
                routing_ttl_seconds: None,
            }),
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let cert_a = fetch_presented_cert(&addr.to_string()).await;

    // Sleep just past the filesystem's mtime resolution so the
    // second write has a different mtime (ext4 / xfs go to ns; APFS
    // / older fs may be coarser). 1.1s covers the worst common case.
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // Replace cert + key with a freshly-generated pair. rcgen
    // produces a new cert with a new public key, so the leaf DER
    // differs.
    let new_cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    std::fs::write(&cert_path, new_cert.cert.pem()).unwrap();
    std::fs::write(&key_path, new_cert.key_pair.serialize_pem()).unwrap();

    // Wait long enough for at least one reload tick after the swap.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let cert_b = fetch_presented_cert(&addr.to_string()).await;

    assert_ne!(
        cert_a, cert_b,
        "after hot-reload the listener should present the new cert"
    );
}
