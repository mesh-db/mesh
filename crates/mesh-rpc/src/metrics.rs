//! Process-wide Prometheus metrics for Mesh.
//!
//! All metrics live on a single global [`Registry`] so the
//! `/metrics` HTTP endpoint can serialize the whole set in one call.
//! Counters, gauges, and histograms are constructed lazily via
//! [`once_cell::sync::Lazy`] and accessed through module-level
//! functions so callers don't have to handle registration errors at
//! every call site.
//!
//! Metric naming follows the Prometheus convention: lower-snake-case,
//! `_total` suffix for counters, `_seconds` for durations, `_bytes`
//! for sizes. Labels are kept low-cardinality — the `mode` label
//! takes one of `single`, `routing`, `raft`, and the `outcome`
//! label takes one of `committed`, `aborted`, `forwarded`. Anything
//! that would balloon to per-query or per-id cardinality goes in a
//! tracing span instead.

use once_cell::sync::Lazy;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};

/// The single registry every Mesh metric registers against. The
/// HTTP endpoint serializes this on demand. Tests can build their
/// own registry, but the production path always uses this one.
pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

fn register<M: prometheus::core::Collector + Clone + 'static>(metric: M) -> M {
    REGISTRY
        .register(Box::new(metric.clone()))
        .expect("metric registration");
    metric
}

/// `mesh_cypher_queries_total{mode}` — every Cypher query that
/// reaches `MeshService::execute_cypher_in_tx`. The `mode` label
/// reports which cluster backend handled the call so dashboards
/// can split single-node, routing, and raft latencies cleanly.
pub static CYPHER_QUERIES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register(
        IntCounterVec::new(
            Opts::new(
                "mesh_cypher_queries_total",
                "Total Cypher queries executed by mode",
            ),
            &["mode"],
        )
        .expect("counter spec"),
    )
});

/// `mesh_cypher_query_duration_seconds{mode}` — wall-clock latency
/// of the executor pipeline (parse + plan + execute + commit).
/// Buckets cover the range from cheap point reads to slow
/// scatter-gather writes.
pub static CYPHER_QUERY_DURATION_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register(
        HistogramVec::new(
            HistogramOpts::new(
                "mesh_cypher_query_duration_seconds",
                "Cypher query latency in seconds, by mode",
            )
            .buckets(vec![
                0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
            &["mode"],
        )
        .expect("histogram spec"),
    )
});

/// `mesh_cypher_index_seeks_total` — number of `IndexSeek` plan
/// nodes the planner emitted across all queries. The planner walks
/// the final plan tree once after `plan_with_context` and
/// increments by however many seeks it sees, so a `MATCH` with two
/// indexed pattern equalities counts twice.
pub static CYPHER_INDEX_SEEKS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register(
        IntCounter::new(
            "mesh_cypher_index_seeks_total",
            "Number of IndexSeek plan nodes executed",
        )
        .expect("counter spec"),
    )
});

/// `mesh_two_phase_commit_total{outcome}` — completed routing-mode
/// 2PC transactions, with `outcome` in `committed` / `aborted`.
/// Aborted excludes the local "no commands to commit" no-op so
/// the rate is meaningful as a health signal.
pub static TWO_PHASE_COMMIT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register(
        IntCounterVec::new(
            Opts::new(
                "mesh_two_phase_commit_total",
                "Routing-mode 2PC transactions by outcome",
            ),
            &["outcome"],
        )
        .expect("counter spec"),
    )
});

/// `mesh_raft_proposals_total{outcome}` — Raft graph proposals,
/// with `outcome` in `committed` / `forwarded` / `failed`. Forwarded
/// counts non-leader proposals where the leader replied with a
/// redirect — they're not failures, but they're worth seeing.
pub static RAFT_PROPOSALS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register(
        IntCounterVec::new(
            Opts::new(
                "mesh_raft_proposals_total",
                "Raft graph proposals by outcome",
            ),
            &["outcome"],
        )
        .expect("counter spec"),
    )
});

/// `mesh_pending_2pc_staged` — gauge of currently-staged
/// participant batches waiting for their coordinator's COMMIT or
/// ABORT. Drift away from zero is the canonical signal that a
/// coordinator died mid-tx; the staging sweeper drops entries past
/// `DEFAULT_STAGING_TTL` so persistent drift means one is being
/// re-created as fast as the sweeper drains.
pub static PENDING_2PC_STAGED: Lazy<IntGauge> = Lazy::new(|| {
    register(
        IntGauge::new(
            "mesh_pending_2pc_staged",
            "Currently-staged participant batches awaiting commit/abort",
        )
        .expect("gauge spec"),
    )
});

/// `mesh_coordinator_log_entries` — gauge of total entries in the
/// 2PC coordinator recovery log on disk after the most recent
/// rotation pass. Useful to confirm rotation is keeping up with
/// commit traffic.
pub static COORDINATOR_LOG_ENTRIES: Lazy<IntGauge> = Lazy::new(|| {
    register(
        IntGauge::new(
            "mesh_coordinator_log_entries",
            "Entries in the 2PC coordinator recovery log",
        )
        .expect("gauge spec"),
    )
});

/// Mode label values for [`CYPHER_QUERIES_TOTAL`] /
/// [`CYPHER_QUERY_DURATION_SECONDS`]. Stringly so call sites can
/// just pass the same value to both.
pub const MODE_SINGLE: &str = "single";
pub const MODE_ROUTING: &str = "routing";
pub const MODE_RAFT: &str = "raft";

/// Render the global registry as Prometheus text-format bytes,
/// suitable for an HTTP `/metrics` endpoint response body.
pub fn render_text() -> Vec<u8> {
    let mut buf = Vec::new();
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    encoder
        .encode(&metric_families, &mut buf)
        .expect("text encoder shouldn't fail on a well-formed registry");
    buf
}
