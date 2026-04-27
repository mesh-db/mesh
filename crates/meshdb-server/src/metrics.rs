//! Tiny axum app that exposes the `meshdb_rpc::metrics` registry on
//! `GET /metrics` in Prometheus text format, plus k8s-style
//! liveness / readiness endpoints. Mounted by [`crate::serve`]
//! when the `metrics_address` config field is set.
//!
//! Routes:
//!
//! * `GET /metrics` — Prometheus text encoding of every registered
//!   counter / gauge / histogram.
//! * `GET /livez` — process is up. Always returns 200 unless the
//!   axum task itself has died (in which case the kubelet's TCP
//!   probe will fail anyway).
//! * `GET /readyz` — peer is ready to serve traffic. Returns 503
//!   during shutdown drain (`is_draining`) and 503 in multi-raft
//!   mode if the meta replica has reported no recent apply
//!   progress. Otherwise returns 200.
//!
//! Stays intentionally minimal: no middleware, no auth. The
//! expectation is that operators bind it to localhost or to a
//! private network interface and let their existing scrape
//! infrastructure (Prometheus, Grafana Agent, OTel Collector) handle
//! ACLs and TLS.

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;

/// Shared state the readiness handler consults. `is_draining` is
/// flipped to true the moment the SIGTERM/SIGINT path begins
/// draining partition leadership; the kubelet sees /readyz return
/// 503 on its next poll and stops sending traffic, while the
/// in-flight drain runs to completion.
///
/// `multi_raft` is the optional handle into the cluster — when
/// present, /readyz also reports unready if the meta replica's
/// metrics watcher is closed (replica shut down, election in
/// flight) so the kubelet doesn't route a fresh request at a
/// peer that can't service it.
#[derive(Clone, Default)]
pub struct ReadinessState {
    pub is_draining: Arc<AtomicBool>,
    pub multi_raft: Option<Arc<meshdb_rpc::MultiRaftCluster>>,
}

impl ReadinessState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Flip the draining bit. Idempotent — calling twice is
    /// harmless.
    pub fn mark_draining(&self) {
        self.is_draining.store(true, Ordering::Relaxed);
    }

    pub fn draining(&self) -> bool {
        self.is_draining.load(Ordering::Relaxed)
    }
}

/// Build the axum router. Pulled out so tests can mount it under
/// any listener without going through `serve()`.
fn router(state: ReadinessState) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/livez", get(livez_handler))
        .route("/readyz", get(readyz_handler))
        .with_state(state)
}

async fn metrics_handler() -> Response {
    let body = meshdb_rpc::metrics::render_text();
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

async fn livez_handler() -> Response {
    (StatusCode::OK, "ok\n").into_response()
}

async fn readyz_handler(State(state): State<ReadinessState>) -> Response {
    if state.draining() {
        return (StatusCode::SERVICE_UNAVAILABLE, "draining\n").into_response();
    }
    if let Some(multi_raft) = &state.multi_raft {
        // Meta replica is alive when we can resolve a current
        // leader (`current_leader()` already gates on
        // ServerState::Shutdown / running_state.is_err()).
        // Election in flight surfaces here too — the meta has no
        // leader, so the peer can't apply DDL nor route partition
        // writes safely.
        if multi_raft.meta.current_leader().is_none() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "meta replica has no leader\n",
            )
                .into_response();
        }
    }
    (StatusCode::OK, "ready\n").into_response()
}

/// Serve the metrics router on `listener` until the task is
/// aborted. Returns on listener errors so the caller can log
/// them — under normal operation `serve()` aborts the task at
/// shutdown rather than letting it return.
pub async fn run_listener(listener: TcpListener, state: ReadinessState) -> anyhow::Result<()> {
    axum::serve(listener, router(state)).await?;
    Ok(())
}
