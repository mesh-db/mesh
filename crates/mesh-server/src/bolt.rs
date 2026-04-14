//! Bolt protocol listener for mesh-server.
//!
//! Bridges the pure `mesh-bolt` protocol primitives (PackStream, chunked
//! framing, handshake, typed messages) onto a live TCP socket and drives
//! a per-connection state machine that dispatches `RUN` messages into
//! [`MeshService::execute_cypher_local`] (or `execute_cypher_buffered`
//! plus `commit_buffered_commands` when the connection is inside an
//! explicit transaction).
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
//!     Ready ──BEGIN──> InTxReady
//!     InTxReady ──RUN──> InTxStreaming
//!     InTxStreaming ──PULL──> InTxReady
//!     InTxReady ──COMMIT──> Ready  (dispatches accumulated batch)
//!     InTxReady ──ROLLBACK──> Ready  (drops accumulated batch)
//!     *         ──RESET──> Ready  (also clears Failed and any tx)
//!     *         ──GOODBYE──> <close>  (implicit rollback)
//! ```
//!
//! ## Explicit-transaction semantics
//!
//! `BEGIN` opens a write-batch transaction: every subsequent `RUN`
//! executes against the live store for **reads**, but mutations are
//! buffered into a per-connection `Vec<GraphCommand>`. `COMMIT`
//! dispatches the whole buffer through [`MeshService::commit_buffered_commands`],
//! which uses the same backend (Raft propose, routing 2PC, or direct
//! store apply) the auto-commit path uses for a single `RUN`. The
//! whole transaction lands atomically — or, on failure, not at all.
//!
//! **Limitation: no read-after-write inside a transaction.** A `MATCH`
//! issued after a `CREATE` in the same `BEGIN`/`COMMIT` block sees the
//! store as it was at `BEGIN` time and will *not* observe the buffered
//! mutations. This matches the "tx is a write batch" model and is
//! sufficient for the common idiom of wrapping multiple writes in one
//! atomic commit; it's insufficient for drivers that rely on
//! read-your-writes semantics inside a transaction. Implementing
//! overlay-storage-style read-your-writes is a separate change.

use crate::value_conv::{bolt_params_to_param_map, field_names_from_rows, row_to_bolt_fields};
use mesh_bolt::{
    perform_server_handshake, read_message, write_message, BoltError, BoltMessage, BoltValue,
    BOLT_4_4,
};
use mesh_cluster::GraphCommand;
use mesh_executor::Row;
use mesh_rpc::MeshService;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpListener;

/// Current connection phase used by the message-dispatch loop.
#[derive(Debug)]
enum Phase {
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
                // GOODBYE inside an in-tx state is an implicit
                // ROLLBACK: the buffered commands just go away with
                // the connection.
                tracing::debug!("bolt goodbye received");
                let _ = writer.flush().await;
                return Ok(());
            }
            (_, BoltMessage::Reset) => {
                // RESET also implicitly rolls back any in-progress tx.
                phase = Phase::Ready;
                send(&mut writer, &empty_success()).await?;
            }

            // -- Ready phase -------------------------------------------
            (Phase::Ready, BoltMessage::Run { query, params, .. }) => {
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
                // before returning rows.
                match service.execute_cypher_local(query, param_map).await {
                    Ok(rows) => {
                        let fields = field_names_from_rows(&rows);
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
                stream_records(&mut writer, &rows, &fields).await?;
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
                // tx phase below.
                let mut buffered = match std::mem::replace(&mut phase, Phase::Ready) {
                    Phase::InTxReady { buffered } => buffered,
                    _ => unreachable!(),
                };
                match service.execute_cypher_buffered(query, param_map).await {
                    Ok((rows, mut commands)) => {
                        buffered.append(&mut commands);
                        let fields = field_names_from_rows(&rows);
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
                stream_records(&mut writer, &rows, &fields).await?;
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
async fn stream_records<W>(writer: &mut W, rows: &[Row], fields: &[String]) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let rows_len = rows.len();
    for row in rows {
        let values = row_to_bolt_fields(row, fields);
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
