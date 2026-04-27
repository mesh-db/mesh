//! TLS configuration helpers for the gRPC listener and inter-peer
//! client channels.
//!
//! Every mesh peer is both a gRPC server (for the Raft / 2PC / query
//! services it hosts) and a gRPC client (for outgoing Raft heartbeats,
//! leader forwarding, and scatter-gather reads against its peers). A
//! single TLS configuration therefore describes *two* things:
//!
//!   * **Server identity** — the certificate chain + private key this
//!     peer presents to incoming connections. Built with
//!     [`build_server_tls_config`].
//!   * **Client trust** — the CA bundle this peer uses to verify
//!     certificates presented by remote peers. Built with
//!     [`build_client_tls_config`].
//!
//! For small clusters and local development, one shared self-signed
//! cert (that acts as its own CA) can cover both roles on every peer.
//! Production deployments typically use a private CA that signs a
//! unique cert per peer.
//!
//! The helpers return tonic's [`ServerTlsConfig`] / [`ClientTlsConfig`]
//! values directly; callers thread them into
//! [`tonic::transport::Server::builder`] and
//! [`tonic::transport::Endpoint::tls_config`] respectively.

use std::path::{Path, PathBuf};

use thiserror::Error;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

/// Install the default rustls crypto provider (aws-lc-rs). Both
/// tonic's TLS stack and the Bolt TLS listener use rustls 0.23, which
/// requires a process-wide crypto provider before any
/// `ClientConfig::builder()` / `ServerConfig::builder()` call. Calling
/// this more than once is safe — subsequent calls are silently
/// ignored, which is what we want in test binaries that spawn
/// multiple servers in the same process.
pub fn install_default_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

#[derive(Debug, Error)]
pub enum TlsConfigError {
    #[error("reading {role} at {path}: {source}")]
    Read {
        role: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

fn read_pem(role: &'static str, path: &Path) -> Result<Vec<u8>, TlsConfigError> {
    std::fs::read(path).map_err(|source| TlsConfigError::Read {
        role,
        path: path.to_path_buf(),
        source,
    })
}

/// Build a [`ServerTlsConfig`] from PEM-encoded certificate and key
/// files. The certificate file may hold a leaf certificate followed
/// by any intermediates (leaf first); the key file holds the private
/// key matching the leaf.
///
/// When `client_ca_path` is `Some`, the listener requires every
/// inbound connection to present a client certificate that
/// validates against the CA bundle — this is mTLS, every peer
/// must hold a cert chained to the same CA. Combined with the
/// shared-secret `cluster_auth` token this gives both
/// transport-level peer authentication and application-level
/// authentication.
pub fn build_server_tls_config(
    cert_path: &Path,
    key_path: &Path,
) -> Result<ServerTlsConfig, TlsConfigError> {
    let cert = read_pem("grpc tls cert", cert_path)?;
    let key = read_pem("grpc tls key", key_path)?;
    let identity = Identity::from_pem(cert, key);
    Ok(ServerTlsConfig::new().identity(identity))
}

/// Build a [`ServerTlsConfig`] that pairs the server's identity
/// with a client-CA bundle for mTLS. Every inbound connection
/// must present a cert chain validating against `client_ca_path`
/// or the TLS handshake fails before any gRPC bytes flow.
pub fn build_server_tls_config_mtls(
    cert_path: &Path,
    key_path: &Path,
    client_ca_path: &Path,
) -> Result<ServerTlsConfig, TlsConfigError> {
    let cert = read_pem("grpc tls cert", cert_path)?;
    let key = read_pem("grpc tls key", key_path)?;
    let client_ca = read_pem("grpc tls client ca bundle", client_ca_path)?;
    let identity = Identity::from_pem(cert, key);
    Ok(ServerTlsConfig::new()
        .identity(identity)
        .client_ca_root(Certificate::from_pem(client_ca)))
}

/// Build a [`ClientTlsConfig`] that verifies peer certificates against
/// the CA bundle at `ca_path` (PEM, one or more `CERTIFICATE` blocks).
/// Without a domain override this uses the hostname from the endpoint
/// URI — peer certs must carry a SAN matching each peer's configured
/// address, which for loopback setups means the cert needs both
/// `DNS:localhost` and `IP:127.0.0.1`.
pub fn build_client_tls_config(ca_path: &Path) -> Result<ClientTlsConfig, TlsConfigError> {
    let ca = read_pem("grpc tls ca bundle", ca_path)?;
    Ok(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca)))
}

/// mTLS variant of [`build_client_tls_config`]: also presents
/// `cert_path` / `key_path` as the client identity. Required
/// whenever the server is configured with
/// [`build_server_tls_config_mtls`] — both ends have to opt in.
pub fn build_client_tls_config_mtls(
    ca_path: &Path,
    cert_path: &Path,
    key_path: &Path,
) -> Result<ClientTlsConfig, TlsConfigError> {
    let ca = read_pem("grpc tls ca bundle", ca_path)?;
    let cert = read_pem("grpc tls client cert", cert_path)?;
    let key = read_pem("grpc tls client key", key_path)?;
    Ok(ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(cert, key)))
}
