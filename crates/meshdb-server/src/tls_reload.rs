//! Hot-reload-capable TLS acceptor shared by the Bolt and gRPC
//! listeners.
//!
//! Both listeners terminate TLS via rustls 0.23 and want the same
//! semantics around cert rotation:
//!
//! 1. The cert + key files on disk are the source of truth.
//! 2. A background task polls their mtimes at a configurable
//!    interval and rebuilds the in-memory `CertifiedKey` when
//!    either changes.
//! 3. New TLS handshakes pick up the rotated cert immediately;
//!    in-flight handshakes retain the cert their handshake
//!    completed against (rustls clones the Arc into per-connection
//!    state).
//! 4. Reload errors (file missing, malformed PEM) log at WARN and
//!    keep the previous cert — the listener never goes down.
//!
//! tonic 0.12's built-in `ServerTlsConfig` doesn't expose rustls's
//! `ResolvesServerCert` hook, so the gRPC path bypasses tonic's
//! TLS termination entirely: we accept TCP + run the TLS
//! handshake ourselves with a `tokio_rustls::TlsAcceptor`, then
//! feed the resulting `TlsStream<TcpStream>` (which tonic 0.12
//! does provide a `Connected` impl for) into
//! `serve_with_incoming_shutdown`.

use anyhow::Context;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::ResolvesServerCert;
use rustls::ServerConfig;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;

/// Load + parse a PEM cert chain and private key into a
/// rustls-ready `CertifiedKey`. Used by both the initial build
/// and every reload tick.
fn load_certified_key(
    cert_path: &Path,
    key_path: &Path,
) -> anyhow::Result<Arc<rustls::sign::CertifiedKey>> {
    let cert_bytes = std::fs::read(cert_path)
        .with_context(|| format!("reading tls cert {}", cert_path.display()))?;
    let key_bytes = std::fs::read(key_path)
        .with_context(|| format!("reading tls key {}", key_path.display()))?;

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_bytes.as_slice())
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("parsing tls cert {}", cert_path.display()))?;
    if certs.is_empty() {
        anyhow::bail!(
            "tls cert {} contained no CERTIFICATE PEM blocks",
            cert_path.display()
        );
    }

    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_bytes.as_slice())
        .with_context(|| format!("parsing tls key {}", key_path.display()))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "tls key {} contained no PRIVATE KEY PEM block",
                key_path.display()
            )
        })?;

    let signing_key = rustls::crypto::aws_lc_rs::sign::any_supported_type(&key)
        .context("building signing key from private key")?;

    Ok(Arc::new(rustls::sign::CertifiedKey::new(
        certs,
        signing_key,
    )))
}

/// `ResolvesServerCert` impl backed by a swap-on-reload mutex.
/// The TLS handshake hits this on every accept; the background
/// reload task replaces the inner Arc on each successful reload.
#[derive(Debug)]
pub struct HotReloadingCertResolver {
    current: std::sync::Mutex<Arc<rustls::sign::CertifiedKey>>,
}

impl HotReloadingCertResolver {
    fn new(initial: Arc<rustls::sign::CertifiedKey>) -> Self {
        Self {
            current: std::sync::Mutex::new(initial),
        }
    }

    fn replace(&self, next: Arc<rustls::sign::CertifiedKey>) {
        *self.current.lock().expect("cert mutex poisoned") = next;
    }
}

impl ResolvesServerCert for HotReloadingCertResolver {
    fn resolve(
        &self,
        _hello: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(self.current.lock().expect("cert mutex poisoned").clone())
    }
}

/// Build a static [`TlsAcceptor`] that loads its cert + key once
/// and never rotates. The caller is responsible for installing a
/// rustls crypto provider before this runs.
pub fn build_static_tls_acceptor(cert_path: &Path, key_path: &Path) -> anyhow::Result<TlsAcceptor> {
    let certified = load_certified_key(cert_path, key_path)?;
    let resolver = Arc::new(HotReloadingCertResolver::new(certified));
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_cert_resolver(resolver);
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Build a [`TlsAcceptor`] that hot-reloads its cert when the
/// underlying files change. Spawns a background task that polls
/// the cert + key mtimes every `reload_interval` and rebuilds the
/// in-memory `CertifiedKey` on change. Returns the acceptor + the
/// reload task's `JoinHandle` so the caller can abort it at
/// shutdown.
pub fn build_hot_reloading_tls_acceptor(
    cert_path: &Path,
    key_path: &Path,
    reload_interval: std::time::Duration,
) -> anyhow::Result<(TlsAcceptor, tokio::task::JoinHandle<()>)> {
    let certified = load_certified_key(cert_path, key_path)?;
    let resolver = Arc::new(HotReloadingCertResolver::new(certified));
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_cert_resolver(resolver.clone());
    let acceptor = TlsAcceptor::from(Arc::new(config));

    let cert_path: PathBuf = cert_path.to_path_buf();
    let key_path: PathBuf = key_path.to_path_buf();
    let mut last_cert_mtime = std::fs::metadata(&cert_path)
        .and_then(|m| m.modified())
        .ok();
    let mut last_key_mtime = std::fs::metadata(&key_path).and_then(|m| m.modified()).ok();
    let handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(reload_interval);
        // Skip the first immediate tick (we just loaded synchronously).
        tick.tick().await;
        loop {
            tick.tick().await;
            let cert_mtime = std::fs::metadata(&cert_path)
                .and_then(|m| m.modified())
                .ok();
            let key_mtime = std::fs::metadata(&key_path).and_then(|m| m.modified()).ok();
            if cert_mtime == last_cert_mtime && key_mtime == last_key_mtime {
                continue;
            }
            match load_certified_key(&cert_path, &key_path) {
                Ok(next) => {
                    resolver.replace(next);
                    last_cert_mtime = cert_mtime;
                    last_key_mtime = key_mtime;
                    tracing::info!(
                        cert_path = %cert_path.display(),
                        key_path = %key_path.display(),
                        "tls cert reloaded"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        cert_path = %cert_path.display(),
                        error = %e,
                        "tls cert reload failed; keeping previous cert"
                    );
                }
            }
        }
    });

    Ok((acceptor, handle))
}
