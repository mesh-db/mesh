//! Bolt protocol listener for mesh-server.
//!
//! Bridges the pure `mesh-bolt` protocol primitives (PackStream, chunked
//! framing, handshake, typed messages) onto a live TCP socket and drives
//! a per-connection state machine that dispatches `RUN` messages into
//! [`MeshService::execute_cypher_local`].
//!
//! Supports Bolt 4.4 only. Auth accepts any `HELLO` unconditionally —
//! the server is not multi-tenant and there's no user store yet.
//!
//! State machine (per connection):
//!
//! ```text
//!     Connected ──handshake──> Negotiated ──HELLO──> Ready
//!     Ready ──RUN──> Streaming
//!     Streaming ──PULL──> Ready
//!     *         ──RESET──> Ready          (also clears Failed)
//!     *         ──GOODBYE──> <close>
//! ```

use crate::value_conv::{field_names_from_rows, row_to_bolt_fields};
use mesh_bolt::{
    perform_server_handshake, read_message, write_message, BoltError, BoltMessage, BoltValue,
    BOLT_4_4,
};
use mesh_executor::Row;
use mesh_rpc::MeshService;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpListener;

/// Current connection phase used by the message-dispatch loop.
#[derive(Debug)]
enum Phase {
    /// Waiting for a `RUN`. `BEGIN` / `COMMIT` / `ROLLBACK` are accepted
    /// and replied to with `SUCCESS` as no-ops (all statements are
    /// auto-committed inside `execute_cypher_local`).
    Ready,
    /// A `RUN` succeeded; we're holding its rows + field names and
    /// waiting for `PULL` or `DISCARD` to hand them out.
    Streaming { rows: Vec<Row>, fields: Vec<String> },
    /// A prior message produced a FAILURE. Every subsequent message
    /// except `RESET` / `GOODBYE` gets `IGNORED` until the client
    /// resets the session.
    Failed,
}

/// Accept Bolt connections on `listener` forever, spawning a new tokio
/// task per connection that runs [`serve_connection`]. The function
/// itself returns only if the listener errors.
pub async fn run_listener(listener: TcpListener, service: Arc<MeshService>) -> anyhow::Result<()> {
    loop {
        let (socket, peer) = listener.accept().await?;
        tracing::debug!(%peer, "bolt connection accepted");
        let service = service.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_connection(socket, service).await {
                tracing::warn!(%peer, error = %e, "bolt connection terminated");
            } else {
                tracing::debug!(%peer, "bolt connection closed cleanly");
            }
        });
    }
}

/// Run the full Bolt lifecycle on a single socket: handshake, HELLO,
/// then a request/response loop until GOODBYE or a socket error.
pub async fn serve_connection<S>(socket: S, service: Arc<MeshService>) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Wrap the socket in small buffers to reduce syscall churn — Bolt
    // messages are small and chatty.
    let (reader, writer) = tokio::io::split(socket);
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    // Phase 1: handshake. perform_server_handshake reads the preamble
    // + 4 version slots and writes back the chosen version.
    let mut handshake_io = ReadWritePair {
        r: &mut reader,
        w: &mut writer,
    };
    let agreed = match perform_server_handshake(&mut handshake_io).await {
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
    if agreed != BOLT_4_4 {
        // We only advertise Bolt 4.4, so anything else is a bug in
        // perform_server_handshake.
        anyhow::bail!("unexpected agreed version {:?}", agreed);
    }
    tracing::debug!("bolt handshake complete, speaking 4.4");

    // Phase 2: HELLO → SUCCESS. Accept any metadata; return a fixed
    // server identification string so drivers log it cleanly.
    let hello = read_message(&mut reader).await?;
    let hello_msg = BoltMessage::decode(&hello)?;
    match hello_msg {
        BoltMessage::Hello { .. } => {
            let metadata = BoltValue::map([
                ("server", BoltValue::String("Mesh/0.1.0".into())),
                ("connection_id", BoltValue::String("mesh-bolt-1".into())),
            ]);
            send(&mut writer, &BoltMessage::Success { metadata }).await?;
        }
        other => {
            anyhow::bail!("expected HELLO as first message, got {:?}", other);
        }
    }

    // Phase 3: request/response loop. Each iteration reads one message
    // and dispatches according to the current Phase.
    let mut phase = Phase::Ready;
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
                tracing::debug!("bolt goodbye received");
                let _ = writer.flush().await;
                return Ok(());
            }
            (_, BoltMessage::Reset) => {
                phase = Phase::Ready;
                send(&mut writer, &empty_success()).await?;
            }

            // -- Ready phase -------------------------------------------
            (Phase::Ready, BoltMessage::Run { query, .. }) => {
                match service.execute_cypher_local(query).await {
                    Ok(rows) => {
                        let fields = field_names_from_rows(&rows);
                        let success = BoltMessage::Success {
                            metadata: BoltValue::map([(
                                "fields",
                                BoltValue::List(
                                    fields
                                        .iter()
                                        .map(|f| BoltValue::String(f.clone()))
                                        .collect(),
                                ),
                            )]),
                        };
                        send(&mut writer, &success).await?;
                        phase = Phase::Streaming { rows, fields };
                    }
                    Err(status) => {
                        send(&mut writer, &failure_from_status(&status)).await?;
                        phase = Phase::Failed;
                    }
                }
            }
            (Phase::Ready, BoltMessage::Begin { .. }) => {
                // Explicit transactions are treated as no-op wrappers —
                // every RUN inside Mesh is auto-committed. Accepting
                // BEGIN keeps drivers that open explicit transactions
                // (neo4j-driver's `session.begin_transaction()`)
                // functional, at the cost of not actually isolating
                // the inner statements from each other.
                send(&mut writer, &empty_success()).await?;
            }
            (Phase::Ready, BoltMessage::Commit | BoltMessage::Rollback) => {
                // Matching close for the BEGIN shim above. Mesh has
                // already committed each RUN individually, so there's
                // nothing left to do on COMMIT / ROLLBACK.
                send(
                    &mut writer,
                    &BoltMessage::Success {
                        metadata: BoltValue::map([(
                            "bookmark",
                            BoltValue::String("mesh:0".into()),
                        )]),
                    },
                )
                .await?;
            }
            (Phase::Ready, BoltMessage::Pull { .. } | BoltMessage::Discard { .. }) => {
                // PULL / DISCARD outside of a streaming phase is a
                // protocol error on the client side. Reply IGNORED and
                // let the client recover via RESET.
                send(&mut writer, &BoltMessage::Ignored).await?;
                phase = Phase::Failed;
            }

            // -- Streaming phase ---------------------------------------
            (Phase::Streaming { .. }, BoltMessage::Pull { .. }) => {
                // Consume the streaming state by value; we'll return
                // to Ready once all records have been sent.
                let (rows, fields) = match std::mem::replace(&mut phase, Phase::Ready) {
                    Phase::Streaming { rows, fields } => (rows, fields),
                    _ => unreachable!(),
                };
                let rows_len = rows.len();
                for row in &rows {
                    let values = row_to_bolt_fields(row, &fields);
                    send(&mut writer, &BoltMessage::Record { fields: values }).await?;
                }
                // Trailing SUCCESS metadata is what drivers look at for
                // `result_consumed_after`, `type`, etc. Keep it minimal.
                send(
                    &mut writer,
                    &BoltMessage::Success {
                        metadata: BoltValue::map([
                            ("type", BoltValue::String("r".into())),
                            ("has_more", BoltValue::Bool(false)),
                            ("record_count", BoltValue::Int(rows_len as i64)),
                        ]),
                    },
                )
                .await?;
            }
            (Phase::Streaming { .. }, BoltMessage::Discard { .. }) => {
                phase = Phase::Ready;
                send(
                    &mut writer,
                    &BoltMessage::Success {
                        metadata: BoltValue::map([
                            ("type", BoltValue::String("r".into())),
                            ("has_more", BoltValue::Bool(false)),
                        ]),
                    },
                )
                .await?;
            }
            (Phase::Streaming { .. }, BoltMessage::Run { .. }) => {
                // Nested RUN is a protocol violation.
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
