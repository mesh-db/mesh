//! Tiny axum app that exposes the `meshdb_rpc::metrics` registry on
//! `GET /metrics` in Prometheus text format. Mounted by [`crate::serve`]
//! when the `metrics_address` config field is set.
//!
//! Stays intentionally minimal: one route, no middleware, no auth.
//! The expectation is that operators bind it to localhost or to a
//! private network interface and let their existing scrape
//! infrastructure (Prometheus, Grafana Agent, OTel Collector) handle
//! ACLs and TLS.

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;

/// Build the axum router. Pulled out so tests can mount it under
/// any listener without going through `serve()`.
fn router() -> Router {
    Router::new().route("/metrics", get(metrics_handler))
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

/// Serve the metrics router on `listener` until the task is
/// aborted. Returns on listener errors so the caller can log
/// them — under normal operation `serve()` aborts the task at
/// shutdown rather than letting it return.
pub async fn run_listener(listener: TcpListener) -> anyhow::Result<()> {
    axum::serve(listener, router()).await?;
    Ok(())
}
