//! Bolt protocol listener for meshdb-server.
//!
//! Bridges the pure `meshdb-bolt` protocol primitives (PackStream, chunked
//! framing, handshake, typed messages) onto a live TCP socket and drives
//! a per-connection state machine that dispatches `RUN` messages into
//! [`MeshService::execute_cypher_local`] (or `execute_cypher_buffered`
//! plus `commit_buffered_commands` when the connection is inside an
//! explicit transaction).
//!
//! Supports Bolt 4.4, 5.0, 5.1, 5.2, 5.3, and 5.4. Auth accepts any
//! `HELLO` unconditionally when no `bolt_auth` config is loaded.
//!
//! State machine (per connection):
//!
//! ```text
//!     Connected ──handshake──> Negotiated
//!     (Bolt 4.4)           Negotiated ──HELLO──> Ready
//!     (Bolt 5.0)           Negotiated ──HELLO──> Ready  (auth in HELLO extras)
//!     (Bolt 5.1+)          Negotiated ──HELLO──> Authenticating ──LOGON──> Ready
//!     Ready ──RUN──> Streaming
//!     Streaming ──PULL──> Ready
//!     Ready ──BEGIN──> InTxReady
//!     InTxReady ──RUN──> InTxStreaming
//!     InTxStreaming ──PULL──> InTxReady
//!     InTxReady ──COMMIT──> Ready  (dispatches accumulated batch)
//!     InTxReady ──ROLLBACK──> Ready  (drops accumulated batch)
//!     *         ──RESET──> Ready  (also clears Failed and any tx)
//!     *         ──GOODBYE──> <close>  (implicit rollback)
//!     (Bolt 5.1+) Ready ──LOGOFF──> Authenticating  (needs fresh LOGON)
//!     (Bolt 5.4+) *     ──TELEMETRY──> same-state  (server SUCCESSes, no-op)
//!     (Bolt 4.4+) *     ──ROUTE──> same-state  (server replies with table)
//! ```
//!
//! ## Explicit-transaction semantics
//!
//! `BEGIN` opens a transaction: every subsequent `RUN` buffers its
//! writes into a per-connection `Vec<GraphCommand>` and the whole
//! buffer is dispatched at `COMMIT` through
//! [`MeshService::commit_buffered_commands`], which uses the same
//! backend (Raft propose, routing 2PC, or direct store apply) the
//! auto-commit path uses for a single `RUN`. The transaction lands
//! atomically — or, on failure, not at all.
//!
//! **Read-your-writes inside a transaction is supported.** A `MATCH`
//! issued after a `CREATE` in the same `BEGIN` / `COMMIT` block sees
//! the buffered node: the in-tx RUN handler calls
//! [`MeshService::execute_cypher_in_tx`] with the accumulated buffer,
//! which constructs a `TxOverlayState` and wraps the normal base
//! reader in an `OverlayGraphReader`. Every `get_node`, `get_edge`,
//! `all_node_ids`, `nodes_by_label`, `outgoing`, and `incoming` call
//! the executor makes sees the overlay's view: uncommitted puts are
//! visible, uncommitted deletes are hidden, and `DetachDeleteNode`
//! cascades implicitly to incident edges. See
//! `meshdb-rpc/src/tx_overlay.rs` for the exact semantics.

use crate::value_conv::{bolt_params_to_param_map, field_names_from_rows, row_to_bolt_fields};
use anyhow::Context;
use meshdb_bolt::{
    perform_server_handshake_with, read_message, write_message, BoltError, BoltMessage, BoltValue,
    BOLT_4_4, BOLT_5_0, BOLT_5_1, BOLT_5_2, BOLT_5_3, BOLT_5_4, SUPPORTED,
};
use meshdb_cluster::raft::RaftCluster;
use meshdb_cluster::{GraphCommand, Membership};
use meshdb_executor::Row;
use meshdb_rpc::MeshService;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// Context the ROUTE handler needs to assemble a spec-shaped Neo4j
/// routing table. Assembled once at listener startup and cloned into
/// each connection task.
///
/// Field semantics:
///   * `local_advertised` — the address this server echoes back when
///     the routing table would otherwise be empty (single-node mode,
///     or a multi-peer config where no peer has declared a
///     `bolt_address`). Also the fallback WRITE target when a Raft
///     leader has no published Bolt endpoint.
///   * `peers` — the full peer list with each peer's optional Bolt
///     address. Built from `ServerConfig.peers` at startup; static
///     for the server's lifetime. Single-node deploys pass an empty
///     `Membership`.
///   * `raft` — the local Raft handle when running in `mode = "raft"`.
///     Used to resolve the current leader on every ROUTE so the WRITE
///     role points at whoever's actually serving writes right now.
///     `None` in routing and single-node modes.
pub struct RouteContext {
    pub local_advertised: String,
    pub peers: Arc<Membership>,
    pub raft: Option<Arc<RaftCluster>>,
    /// Multi-raft cluster handle when `mode = "multi-raft"`. Drives
    /// the ROUTE response: WRITE / READ / ROUTE all list every
    /// peer's Bolt address (multi-raft does server-side leader
    /// forwarding so any peer can accept writes), and the TTL drops
    /// to `routing_ttl_seconds` so drivers re-fetch as topology
    /// drifts (membership changes, partition leader churn).
    pub multi_raft: Option<Arc<meshdb_rpc::MultiRaftCluster>>,
    /// TTL in seconds advertised on every ROUTE response. Bolt
    /// drivers cache the routing table for this long before
    /// re-fetching. `None` keeps the historical effectively-infinite
    /// TTL (~292y); a finite value is recommended for any
    /// multi-peer deployment so drivers pick up topology changes
    /// without a manual session bounce. Defaults are wired through
    /// [`ServerConfig::routing_ttl_seconds`].
    pub routing_ttl_seconds: Option<u64>,
}

/// Current connection phase used by the message-dispatch loop.
#[derive(Debug)]
enum Phase {
    /// Bolt 5.1+ only: HELLO succeeded but LOGON has not. Every
    /// message other than LOGON / RESET / GOODBYE is a protocol
    /// error in this state.
    Authenticating,
    /// Waiting for a `RUN` (auto-commit) or a `BEGIN`. `COMMIT` /
    /// `ROLLBACK` outside a transaction are protocol errors and get
    /// `IGNORED` + transition to `Failed`.
    Ready,
    /// A `RUN` succeeded outside a transaction; we're holding its rows
    /// + field names and waiting for `PULL` or `DISCARD` to hand them
    /// out. The next message returns the connection to `Ready` and
    /// auto-commits any writes that ran during the RUN.
    Streaming { rows: Vec<Row>, fields: Vec<String> },
    /// Inside an explicit transaction, idle between RUNs. `buffered`
    /// holds every `GraphCommand` that previous RUNs in this tx have
    /// accumulated; `COMMIT` dispatches the whole vector at once.
    InTxReady { buffered: Vec<GraphCommand> },
    /// Inside an explicit transaction, mid-RUN — waiting for `PULL` or
    /// `DISCARD`. `buffered` carries the accumulated batch (including
    /// the writes the *current* RUN just produced); the next message
    /// transitions back to `InTxReady` with `buffered` preserved.
    InTxStreaming {
        buffered: Vec<GraphCommand>,
        rows: Vec<Row>,
        fields: Vec<String>,
    },
    /// A prior message produced a FAILURE. Every subsequent message
    /// except `RESET` / `GOODBYE` gets `IGNORED` until the client
    /// resets the session. Transitioning here from any tx-state
    /// implicitly drops the buffered commands — failed RUNs inside a
    /// tx invalidate the whole batch.
    Failed,
}

/// Accept Bolt connections on `listener` forever, spawning a new tokio
/// task per connection that runs [`serve_connection`]. The function
/// itself returns only if the listener errors.
///
/// `auth` is the optional user table loaded from
/// [`crate::config::ServerConfig::bolt_auth`]. When `None`, the
/// HELLO handler accepts any incoming credentials (pre-auth
/// behavior). When `Some`, every HELLO is validated and rejected
/// with `Neo.ClientError.Security.Unauthorized` on mismatch.
pub async fn run_listener(
    listener: TcpListener,
    service: Arc<MeshService>,
    auth: Option<Arc<crate::config::BoltAuthConfig>>,
    tls: Option<TlsAcceptor>,
    advertised_versions: Option<Arc<Vec<[u8; 4]>>>,
    route_ctx: Arc<RouteContext>,
) -> anyhow::Result<()> {
    loop {
        let (socket, peer) = listener.accept().await?;
        tracing::debug!(%peer, "bolt connection accepted");
        let service = service.clone();
        let auth = auth.clone();
        let tls = tls.clone();
        let advertised = advertised_versions.clone();
        let ctx = route_ctx.clone();
        tokio::spawn(async move {
            // If TLS is configured, negotiate it before handing the
            // socket to the Bolt state machine. `serve_connection` is
            // generic over `AsyncRead + AsyncWrite`, so the TLS stream
            // drops in transparently once the handshake succeeds.
            match tls {
                Some(acceptor) => match acceptor.accept(socket).await {
                    Ok(tls_stream) => {
                        if let Err(e) =
                            serve_connection(tls_stream, service, auth, advertised, ctx).await
                        {
                            tracing::warn!(%peer, error = %e, "bolt connection terminated");
                        } else {
                            tracing::debug!(%peer, "bolt connection closed cleanly");
                        }
                    }
                    Err(e) => {
                        tracing::warn!(%peer, error = %e, "bolt tls handshake failed");
                    }
                },
                None => {
                    if let Err(e) = serve_connection(socket, service, auth, advertised, ctx).await {
                        tracing::warn!(%peer, error = %e, "bolt connection terminated");
                    } else {
                        tracing::debug!(%peer, "bolt connection closed cleanly");
                    }
                }
            }
        });
    }
}

/// Build a [`TlsAcceptor`] from PEM-encoded certificate + private key
/// files. The certificate file may contain one or more X.509
/// certificates (leaf first, then any intermediates); the private key
/// file may hold a PKCS#8, SEC1 (EC), or RSA-format key — the first
/// key found wins.
///
/// The caller is responsible for installing a rustls crypto provider
/// before calling this (see [`install_default_crypto_provider`]).
pub fn build_tls_acceptor(cert_path: &Path, key_path: &Path) -> anyhow::Result<TlsAcceptor> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls::ServerConfig;

    let cert_bytes = std::fs::read(cert_path)
        .with_context(|| format!("reading bolt tls cert {}", cert_path.display()))?;
    let key_bytes = std::fs::read(key_path)
        .with_context(|| format!("reading bolt tls key {}", key_path.display()))?;

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_bytes.as_slice())
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("parsing bolt tls cert {}", cert_path.display()))?;
    if certs.is_empty() {
        anyhow::bail!(
            "bolt tls cert {} contained no CERTIFICATE PEM blocks",
            cert_path.display()
        );
    }

    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_bytes.as_slice())
        .with_context(|| format!("parsing bolt tls key {}", key_path.display()))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "bolt tls key {} contained no PRIVATE KEY PEM block",
                key_path.display()
            )
        })?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("building rustls ServerConfig")?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Install the default rustls crypto provider (aws-lc-rs). Delegates
/// to [`meshdb_rpc::tls::install_default_crypto_provider`] so the Bolt
/// and gRPC listeners share one source of truth. Safe to call
/// repeatedly — subsequent calls are silently ignored.
pub fn install_default_crypto_provider() {
    meshdb_rpc::tls::install_default_crypto_provider();
}

/// Run the full Bolt lifecycle on a single socket: handshake, HELLO,
/// then a request/response loop until GOODBYE or a socket error.
pub async fn serve_connection<S>(
    socket: S,
    service: Arc<MeshService>,
    auth: Option<Arc<crate::config::BoltAuthConfig>>,
    advertised_versions: Option<Arc<Vec<[u8; 4]>>>,
    route_ctx: Arc<RouteContext>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Wrap the socket in small buffers to reduce syscall churn — Bolt
    // messages are small and chatty.
    let (reader, writer) = tokio::io::split(socket);
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    // Phase 1: handshake. perform_server_handshake_with reads the
    // preamble + 4 version slots and writes back the chosen version.
    // `advertised_versions` clamps the candidate list — None falls back
    // to the full SUPPORTED set.
    let mut handshake_io = ReadWritePair {
        r: &mut reader,
        w: &mut writer,
    };
    let advertised: &[[u8; 4]] = advertised_versions
        .as_deref()
        .map(|v| v.as_slice())
        .unwrap_or(SUPPORTED);
    let agreed = match perform_server_handshake_with(&mut handshake_io, advertised).await {
        Ok(v) => v,
        Err(BoltError::BadPreamble) => {
            tracing::warn!("bolt client sent bad preamble; closing");
            return Ok(());
        }
        Err(BoltError::NoCompatibleVersion(_)) => {
            tracing::warn!("bolt client offered no supported version; closing");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    // Validate negotiated version against what we advertise.
    let agreed_version = bolt_version_label(agreed)?;
    let auth_in_logon = is_bolt_5_1_or_newer(agreed);
    tracing::debug!(version = %agreed_version, "bolt handshake complete");

    // Phase 2: HELLO → SUCCESS (or FAILURE + close when auth is
    // configured and the credentials don't match). In Bolt 5.1+ the
    // auth fields live in LOGON instead of HELLO — we just skip the
    // credential check in HELLO for those versions and validate LOGON
    // later.
    let hello = read_message(&mut reader).await?;
    let hello_msg = BoltMessage::decode(&hello)?;
    let hello_extra = match hello_msg {
        BoltMessage::Hello { extra } => extra,
        other => anyhow::bail!("expected HELLO as first message, got {:?}", other),
    };
    if !auth_in_logon {
        // Bolt 4.4 / 5.0: auth in HELLO.
        if let Err(msg) = check_bolt_auth(auth.as_deref(), &hello_extra) {
            tracing::info!(error = %msg, "bolt auth rejected");
            send(
                &mut writer,
                &failure("Neo.ClientError.Security.Unauthorized", &msg),
            )
            .await?;
            writer.flush().await.ok();
            return Ok(());
        }
    }
    // Server agent string. The official Neo4j drivers (Python, JS,
    // Java, Go, .NET) gate connection setup on a `Neo4j/` prefix —
    // anything else is rejected client-side as `UnsupportedServerProduct`,
    // which makes them unusable against alternative Bolt servers.
    // Standard workaround used by Memgraph, FalkorDB, Neptune, etc.:
    // announce ourselves as Neo4j-compatible. We tag the build version
    // suffix-style (`Neo4j/5.4-mesh-<v>`) so the impersonation is
    // honest — operators inspecting the wire still see Mesh.
    let hello_success_meta = BoltValue::map([
        (
            "server",
            BoltValue::String(concat!("Neo4j/5.4-mesh-", env!("CARGO_PKG_VERSION")).into()),
        ),
        ("connection_id", BoltValue::String("meshdb-bolt-1".into())),
        ("hints", BoltValue::Map(vec![])),
    ]);
    send(
        &mut writer,
        &BoltMessage::Success {
            metadata: hello_success_meta,
        },
    )
    .await?;

    // Phase 3: request/response loop. For Bolt 5.1+ start in
    // Authenticating phase and require LOGON before Ready.
    let mut phase = if auth_in_logon {
        Phase::Authenticating
    } else {
        Phase::Ready
    };
    loop {
        let raw = match read_message(&mut reader).await {
            Ok(r) => r,
            Err(BoltError::Io(e)) if is_eof(&e) => {
                // Client disconnected without GOODBYE. Normal for
                // neo4j-driver's `driver.close()` when the connection
                // pool decides to drop the socket.
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        let msg = BoltMessage::decode(&raw)?;

        match (&phase, msg) {
            // -- Universal messages ------------------------------------
            (_, BoltMessage::Goodbye) => {
                // GOODBYE inside an in-tx state is an implicit
                // ROLLBACK: the buffered commands just go away with
                // the connection.
                tracing::debug!("bolt goodbye received");
                let _ = writer.flush().await;
                return Ok(());
            }
            (_, BoltMessage::Reset) => {
                // RESET clears any in-progress tx. In Bolt 5.1+ it
                // preserves the authentication state (returns to
                // Ready, not Authenticating) per spec.
                phase = Phase::Ready;
                send(&mut writer, &empty_success()).await?;
            }
            // Bolt 5.4+ TELEMETRY — no-op, SUCCESS.
            (_, BoltMessage::Telemetry { .. }) => {
                send(&mut writer, &empty_success()).await?;
            }
            // Bolt 4.4+ ROUTE — reply with a single-node routing table
            // pointing at this server.
            (_, BoltMessage::Route { .. }) => {
                send(&mut writer, &route_success(&route_ctx)).await?;
            }

            // -- Authenticating phase (Bolt 5.1+) ----------------------
            (Phase::Authenticating, BoltMessage::Logon { auth: auth_extra }) => {
                if let Err(msg) = check_bolt_auth(auth.as_deref(), &auth_extra) {
                    tracing::info!(error = %msg, "bolt auth rejected");
                    send(
                        &mut writer,
                        &failure("Neo.ClientError.Security.Unauthorized", &msg),
                    )
                    .await?;
                    writer.flush().await.ok();
                    return Ok(());
                }
                phase = Phase::Ready;
                send(&mut writer, &empty_success()).await?;
            }
            (Phase::Authenticating, _) => {
                send(
                    &mut writer,
                    &failure(
                        "Neo.ClientError.Security.Unauthorized",
                        "LOGON required before any other message",
                    ),
                )
                .await?;
                phase = Phase::Failed;
            }
            // Bolt 5.1+ LOGOFF — clear auth, return to Authenticating.
            (Phase::Ready, BoltMessage::Logoff) => {
                phase = if auth_in_logon {
                    Phase::Authenticating
                } else {
                    Phase::Ready
                };
                send(&mut writer, &empty_success()).await?;
            }
            // LOGON in Ready (re-auth): accept new credentials.
            (Phase::Ready, BoltMessage::Logon { auth: auth_extra }) => {
                if let Err(msg) = check_bolt_auth(auth.as_deref(), &auth_extra) {
                    send(
                        &mut writer,
                        &failure("Neo.ClientError.Security.Unauthorized", &msg),
                    )
                    .await?;
                    writer.flush().await.ok();
                    return Ok(());
                }
                send(&mut writer, &empty_success()).await?;
            }

            // -- Ready phase -------------------------------------------
            (Phase::Ready, BoltMessage::Run { query, params, .. }) => {
                let run_span =
                    tracing::info_span!("bolt_run", query_len = query.len(), auto_commit = true);
                let param_map = match bolt_params_to_param_map(&params) {
                    Ok(m) => m,
                    Err(e) => {
                        send(
                            &mut writer,
                            &failure("Mesh.ClientError.InvalidArgument", &e.to_string()),
                        )
                        .await?;
                        phase = Phase::Failed;
                        continue;
                    }
                };
                // Auto-commit path: execute_cypher_local already
                // dispatches buffered writes through the active backend
                // before returning rows. Instrument the call so any
                // tracing events emitted by the executor inherit the
                // bolt_run span.
                use tracing::Instrument;
                // Pre-compute RETURN column order from a parse pass
                // so we can emit it in the SUCCESS header the driver
                // reads *before* receiving any RECORD. The double
                // parse (here + inside execute_cypher_local) is
                // cheap; Row is a HashMap that doesn't preserve
                // insertion order, so we can't derive declared order
                // from the rows themselves.
                let declared_fields = meshdb_cypher::parse(&query)
                    .ok()
                    .map(|stmt| meshdb_cypher::output_columns(&stmt))
                    .unwrap_or_default();
                match service
                    .execute_cypher_local(query, param_map)
                    .instrument(run_span)
                    .await
                {
                    Ok(rows) => {
                        // Use declared order when the query had a
                        // RETURN; otherwise fall back to the
                        // row-key derivation for queries where the
                        // rows themselves name the columns
                        // (SHOW INDEXES, procedure YIELDs, etc.).
                        let fields = if declared_fields.is_empty() {
                            field_names_from_rows(&rows)
                        } else {
                            declared_fields
                        };
                        send(&mut writer, &fields_success(&fields)).await?;
                        phase = Phase::Streaming { rows, fields };
                    }
                    Err(status) => {
                        send(&mut writer, &failure_from_status(&status)).await?;
                        phase = Phase::Failed;
                    }
                }
            }
            (Phase::Ready, BoltMessage::Begin { .. }) => {
                // Open a new explicit transaction with an empty
                // accumulator. Subsequent RUNs in this connection will
                // append their writes to it until COMMIT or ROLLBACK.
                phase = Phase::InTxReady {
                    buffered: Vec::new(),
                };
                send(&mut writer, &empty_success()).await?;
            }
            (Phase::Ready, BoltMessage::Commit | BoltMessage::Rollback) => {
                // COMMIT / ROLLBACK outside of a transaction is a
                // protocol error.
                send(
                    &mut writer,
                    &failure(
                        "Mesh.ClientError.Protocol",
                        "COMMIT / ROLLBACK outside of an explicit transaction",
                    ),
                )
                .await?;
                phase = Phase::Failed;
            }
            (Phase::Ready, BoltMessage::Pull { .. } | BoltMessage::Discard { .. }) => {
                send(&mut writer, &BoltMessage::Ignored).await?;
                phase = Phase::Failed;
            }

            // -- Streaming phase (auto-commit) -------------------------
            (Phase::Streaming { .. }, BoltMessage::Pull { .. }) => {
                let (rows, fields) = match std::mem::replace(&mut phase, Phase::Ready) {
                    Phase::Streaming { rows, fields } => (rows, fields),
                    _ => unreachable!(),
                };
                stream_records(&mut writer, &rows, &fields, agreed).await?;
            }
            (Phase::Streaming { .. }, BoltMessage::Discard { .. }) => {
                phase = Phase::Ready;
                send(&mut writer, &discard_success()).await?;
            }
            (Phase::Streaming { .. }, BoltMessage::Run { .. }) => {
                send(&mut writer, &BoltMessage::Ignored).await?;
                phase = Phase::Failed;
            }

            // -- InTxReady phase (between RUNs in an explicit tx) ------
            (Phase::InTxReady { .. }, BoltMessage::Run { query, params, .. }) => {
                let run_span =
                    tracing::info_span!("bolt_run", query_len = query.len(), auto_commit = false);
                let param_map = match bolt_params_to_param_map(&params) {
                    Ok(m) => m,
                    Err(e) => {
                        send(
                            &mut writer,
                            &failure("Mesh.ClientError.InvalidArgument", &e.to_string()),
                        )
                        .await?;
                        phase = Phase::Failed;
                        continue;
                    }
                };
                // Take the existing buffer out of the phase so we can
                // extend it with this RUN's commands and rebuild the
                // tx phase below. We pass the buffer as prev_commands
                // so `execute_cypher_in_tx` overlays it on top of the
                // base reader, giving this RUN read-your-writes
                // semantics for everything committed by earlier RUNs
                // in the same transaction.
                let mut buffered = match std::mem::replace(&mut phase, Phase::Ready) {
                    Phase::InTxReady { buffered } => buffered,
                    _ => unreachable!(),
                };
                use tracing::Instrument;
                let declared_fields = meshdb_cypher::parse(&query)
                    .ok()
                    .map(|stmt| meshdb_cypher::output_columns(&stmt))
                    .unwrap_or_default();
                match service
                    .execute_cypher_in_tx(query, param_map, buffered.clone(), true)
                    .instrument(run_span)
                    .await
                {
                    Ok((rows, mut commands)) => {
                        buffered.append(&mut commands);
                        let fields = if declared_fields.is_empty() {
                            field_names_from_rows(&rows)
                        } else {
                            declared_fields
                        };
                        send(&mut writer, &fields_success(&fields)).await?;
                        phase = Phase::InTxStreaming {
                            buffered,
                            rows,
                            fields,
                        };
                    }
                    Err(status) => {
                        // Drop the entire tx buffer on error — the user
                        // must RESET and start over. Drivers expect a
                        // failed RUN inside a tx to invalidate the
                        // whole transaction, which is exactly what
                        // dropping the buffer does.
                        send(&mut writer, &failure_from_status(&status)).await?;
                        phase = Phase::Failed;
                    }
                }
            }
            (Phase::InTxReady { .. }, BoltMessage::Commit) => {
                // Drain the buffered commands and dispatch as one
                // batch. Empty batch is fine — equivalent to BEGIN +
                // immediate COMMIT, which COMMITs nothing.
                let buffered = match std::mem::replace(&mut phase, Phase::Ready) {
                    Phase::InTxReady { buffered } => buffered,
                    _ => unreachable!(),
                };
                match service.commit_buffered_commands(buffered).await {
                    Ok(()) => {
                        send(&mut writer, &commit_success()).await?;
                    }
                    Err(status) => {
                        send(&mut writer, &failure_from_status(&status)).await?;
                        phase = Phase::Failed;
                    }
                }
            }
            (Phase::InTxReady { .. }, BoltMessage::Rollback) => {
                // Drop the accumulated buffer and return to Ready.
                phase = Phase::Ready;
                send(&mut writer, &empty_success()).await?;
            }
            (Phase::InTxReady { .. }, BoltMessage::Begin { .. }) => {
                // Nested transactions are not supported — Bolt 4.4
                // doesn't model nesting either.
                send(
                    &mut writer,
                    &failure("Mesh.ClientError.Protocol", "nested BEGIN is not supported"),
                )
                .await?;
                phase = Phase::Failed;
            }
            (Phase::InTxReady { .. }, BoltMessage::Pull { .. } | BoltMessage::Discard { .. }) => {
                send(&mut writer, &BoltMessage::Ignored).await?;
                phase = Phase::Failed;
            }

            // -- InTxStreaming phase -----------------------------------
            (Phase::InTxStreaming { .. }, BoltMessage::Pull { .. }) => {
                let (buffered, rows, fields) = match std::mem::replace(&mut phase, Phase::Ready) {
                    Phase::InTxStreaming {
                        buffered,
                        rows,
                        fields,
                    } => (buffered, rows, fields),
                    _ => unreachable!(),
                };
                stream_records(&mut writer, &rows, &fields, agreed).await?;
                // Stay in the tx — only PULL drains the rows, the
                // accumulated write buffer is preserved.
                phase = Phase::InTxReady { buffered };
            }
            (Phase::InTxStreaming { .. }, BoltMessage::Discard { .. }) => {
                let (buffered, _rows, _fields) = match std::mem::replace(&mut phase, Phase::Ready) {
                    Phase::InTxStreaming {
                        buffered,
                        rows,
                        fields,
                    } => (buffered, rows, fields),
                    _ => unreachable!(),
                };
                send(&mut writer, &discard_success()).await?;
                phase = Phase::InTxReady { buffered };
            }
            (Phase::InTxStreaming { .. }, BoltMessage::Run { .. }) => {
                send(&mut writer, &BoltMessage::Ignored).await?;
                phase = Phase::Failed;
            }

            // -- Failed phase ------------------------------------------
            (Phase::Failed, _) => {
                send(&mut writer, &BoltMessage::Ignored).await?;
            }

            // -- Any other unexpected combination ----------------------
            (_, other) => {
                tracing::warn!(?other, "unexpected bolt message in current phase");
                send(
                    &mut writer,
                    &failure(
                        "Mesh.ClientError.Protocol",
                        &format!("unexpected message: {:?}", other),
                    ),
                )
                .await?;
                phase = Phase::Failed;
            }
        }
    }
}

/// Stream RECORDs for one buffered result set, then send the trailing
/// SUCCESS with `type=r`, `has_more=false`, and the record count.
/// `bolt_version` is the handshake-negotiated version; the encoder
/// uses it to pick version-specific struct tags (notably DateTime:
/// 0x46 local-wall-clock under 4.4 vs 0x49 UTC under 5.0+).
async fn stream_records<W>(
    writer: &mut W,
    rows: &[Row],
    fields: &[String],
    bolt_version: [u8; 4],
) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let rows_len = rows.len();
    for row in rows {
        let values = row_to_bolt_fields(row, fields, bolt_version);
        send(writer, &BoltMessage::Record { fields: values }).await?;
    }
    send(
        writer,
        &BoltMessage::Success {
            metadata: BoltValue::map([
                ("type", BoltValue::String("r".into())),
                ("has_more", BoltValue::Bool(false)),
                ("record_count", BoltValue::Int(rows_len as i64)),
            ]),
        },
    )
    .await?;
    Ok(())
}

fn fields_success(fields: &[String]) -> BoltMessage {
    BoltMessage::Success {
        metadata: BoltValue::map([(
            "fields",
            BoltValue::List(
                fields
                    .iter()
                    .map(|f| BoltValue::String(f.clone()))
                    .collect(),
            ),
        )]),
    }
}

fn discard_success() -> BoltMessage {
    BoltMessage::Success {
        metadata: BoltValue::map([
            ("type", BoltValue::String("r".into())),
            ("has_more", BoltValue::Bool(false)),
        ]),
    }
}

fn commit_success() -> BoltMessage {
    BoltMessage::Success {
        metadata: BoltValue::map([("bookmark", BoltValue::String("mesh:0".into()))]),
    }
}

async fn send<W>(writer: &mut W, msg: &BoltMessage) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_message(writer, &msg.encode()).await?;
    Ok(())
}

fn empty_success() -> BoltMessage {
    BoltMessage::Success {
        metadata: BoltValue::Map(vec![]),
    }
}

/// True if the negotiated version is Bolt 5.1 or newer. Those versions
/// require auth in LOGON rather than HELLO.
fn is_bolt_5_1_or_newer(v: [u8; 4]) -> bool {
    // Version bytes are [0, range, minor, major]. We compare by (major, minor).
    let major = v[3];
    let minor = v[2];
    major > 5 || (major == 5 && minor >= 1)
}

/// Map a negotiated version tuple to a display label, or bail if it's
/// something we shouldn't have agreed on.
fn bolt_version_label(v: [u8; 4]) -> anyhow::Result<&'static str> {
    match v {
        BOLT_5_4 => Ok("5.4"),
        BOLT_5_3 => Ok("5.3"),
        BOLT_5_2 => Ok("5.2"),
        BOLT_5_1 => Ok("5.1"),
        BOLT_5_0 => Ok("5.0"),
        BOLT_4_4 => Ok("4.4"),
        other => anyhow::bail!("unexpected agreed bolt version {:?}", other),
    }
}

/// Reply to a ROUTE request with a Neo4j-spec routing table. The
/// table has three `{addresses, role}` entries — ROUTE, READ, WRITE —
/// each with at least one address the driver can reach.
///
/// Shape depends on what the [`RouteContext`] has to work with:
///
///   * **Single-node** (empty peer list) or **no peer has a
///     `bolt_address` configured yet**: every role lists
///     `ctx.local_advertised`. This is the pre-Phase-4 behaviour and
///     keeps existing configs working unchanged.
///
///   * **Routing mode** (`ctx.raft == None`) with at least one peer
///     advertising a Bolt address: ROUTE, READ, WRITE all list every
///     peer's Bolt address. Any peer can coordinate a 2PC round, so
///     drivers spread writes across them.
///
///   * **Raft mode** (`ctx.raft == Some`): ROUTE and READ list every
///     peer's Bolt address (all can serve reads). WRITE lists only
///     the current leader's Bolt address — queried from Raft metrics
///     at ROUTE time so a leader change is reflected on the next
///     ROUTE. If the leader is unknown (election in progress) or
///     has no published Bolt address, WRITE falls back to
///     `local_advertised`; writes against a non-leader still reach
///     the leader via the existing `ForwardToLeader` path.
///
/// TTL is ~292 years. Drivers that cache the table effectively never
/// re-query; leader-handoff recovery is handled by the existing
/// write-forwarding behaviour rather than table refresh.
fn route_success(ctx: &RouteContext) -> BoltMessage {
    // Assemble the per-peer Bolt address list, preserving the
    // cluster's deterministic PeerId ordering so the routing table
    // is stable across calls. `local_advertised` is always included
    // as the first entry, so a driver connecting to this peer sees
    // itself in the table.
    let mut advertised: Vec<String> = Vec::new();
    advertised.push(ctx.local_advertised.clone());
    for (_, _, bolt) in ctx.peers.iter_full() {
        if let Some(b) = bolt {
            if b != ctx.local_advertised {
                advertised.push(b.to_string());
            }
        }
    }

    // WRITE list. Raft mode wants just the leader; routing,
    // multi-raft, and single-node accept the full advertised list
    // (multi-raft does server-side leader forwarding so any peer
    // accepts writes). Any fallback lands on `local_advertised` so
    // the table always has at least one WRITE entry.
    let write_addrs: Vec<String> = match ctx.raft.as_ref() {
        Some(raft) => {
            let leader_bolt = raft
                .current_leader()
                .and_then(|id| ctx.peers.bolt_address(id).map(str::to_string));
            match leader_bolt {
                Some(addr) => vec![addr],
                None => vec![ctx.local_advertised.clone()],
            }
        }
        None => advertised.clone(),
    };

    let to_bolt_list = |addrs: &[String]| -> BoltValue {
        BoltValue::List(addrs.iter().map(|a| BoltValue::String(a.clone())).collect())
    };
    let role_entry = |addrs: &[String], role: &str| -> BoltValue {
        BoltValue::map([
            ("addresses", to_bolt_list(addrs)),
            ("role", BoltValue::String(role.to_string())),
        ])
    };
    // TTL: long-cache historical default (~292y) when unset, finite
    // value when configured. Multi-raft mode defaults to a finite
    // TTL via `serve()` so drivers re-fetch as partitions reshuffle.
    let ttl_seconds = ctx
        .routing_ttl_seconds
        .map(|t| t as i64)
        .unwrap_or(9_223_372_036);
    let rt = BoltValue::map([
        ("ttl", BoltValue::Int(ttl_seconds)),
        (
            "servers",
            BoltValue::List(vec![
                role_entry(&advertised, "ROUTE"),
                role_entry(&advertised, "READ"),
                role_entry(&write_addrs, "WRITE"),
            ]),
        ),
        ("db", BoltValue::String("neo4j".into())),
    ]);
    BoltMessage::Success {
        metadata: BoltValue::map([("rt", rt)]),
    }
}

fn failure_from_status(status: &tonic::Status) -> BoltMessage {
    // Map gRPC Status codes onto Bolt-style `Mesh.ClientError.*` /
    // `Mesh.ServerError.*` codes so drivers can distinguish client
    // mistakes from server-side failures.
    let code = match status.code() {
        tonic::Code::InvalidArgument => "Mesh.ClientError.InvalidArgument",
        tonic::Code::NotFound => "Mesh.ClientError.NotFound",
        tonic::Code::AlreadyExists => "Mesh.ClientError.AlreadyExists",
        tonic::Code::FailedPrecondition => "Mesh.ClientError.FailedPrecondition",
        _ => "Mesh.ServerError.Unknown",
    };
    failure(code, status.message())
}

/// Validate a Bolt HELLO's auth fields against the configured
/// user table. Returns `Ok(())` when the HELLO is accepted and
/// `Err(reason)` otherwise — the caller turns the reason into a
/// `Neo.ClientError.Security.Unauthorized` failure and closes
/// the connection.
///
/// Accept paths:
///   * `auth = None` → accept any HELLO (pre-auth behavior).
///   * `auth = Some(cfg)` with `cfg.users.is_empty()` → also
///     accept any HELLO; an empty users list is a config error
///     the operator can surface via startup validation later,
///     but at runtime we don't want to lock everyone out.
///   * `auth = Some(cfg)` with `scheme = "basic"` and a matching
///     `principal` / `credentials` pair.
///
/// Reject paths (all map to Unauthorized):
///   * Missing `scheme` field or `scheme != "basic"` (explicitly
///     including `scheme = "none"`).
///   * `principal` / `credentials` missing or non-string.
///   * Credentials present but the user table doesn't contain
///     the pair.
fn check_bolt_auth(
    auth: Option<&crate::config::BoltAuthConfig>,
    extra: &BoltValue,
) -> std::result::Result<(), String> {
    let Some(cfg) = auth else {
        return Ok(());
    };
    if cfg.users.is_empty() {
        return Ok(());
    }
    let scheme = extra.get("scheme").and_then(|v| v.as_str()).unwrap_or("");
    if scheme != "basic" {
        return Err(format!(
            "authentication required; scheme `{}` not supported",
            scheme
        ));
    }
    let principal = extra
        .get("principal")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "missing principal in HELLO".to_string())?;
    let credentials = extra
        .get("credentials")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "missing credentials in HELLO".to_string())?;
    if !cfg.verify(principal, credentials) {
        return Err("invalid username or password".into());
    }
    Ok(())
}

fn failure(code: &str, message: &str) -> BoltMessage {
    BoltMessage::Failure {
        metadata: BoltValue::map([
            ("code", BoltValue::String(code.to_string())),
            ("message", BoltValue::String(message.to_string())),
        ]),
    }
}

fn is_eof(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
    )
}

/// Small adapter so `perform_server_handshake` can drive both halves of
/// a split socket via a single `AsyncRead + AsyncWrite` object.
struct ReadWritePair<'a, R, W> {
    r: &'a mut R,
    w: &'a mut W,
}

impl<'a, R, W> AsyncRead for ReadWritePair<'a, R, W>
where
    R: AsyncRead + Unpin,
    W: Unpin,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().r).poll_read(cx, buf)
    }
}

impl<'a, R, W> AsyncWrite for ReadWritePair<'a, R, W>
where
    R: Unpin,
    W: AsyncWrite + Unpin,
{
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().w).poll_write(cx, buf)
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().w).poll_flush(cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().w).poll_shutdown(cx)
    }
}
