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

/// `meshdb_cypher_queries_total{mode}` — every Cypher query that
/// reaches `MeshService::execute_cypher_in_tx`. The `mode` label
/// reports which cluster backend handled the call so dashboards
/// can split single-node, routing, and raft latencies cleanly.
pub static CYPHER_QUERIES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register(
        IntCounterVec::new(
            Opts::new(
                "meshdb_cypher_queries_total",
                "Total Cypher queries executed by mode",
            ),
            &["mode"],
        )
        .expect("counter spec"),
    )
});

/// `meshdb_cypher_query_duration_seconds{mode}` — wall-clock latency
/// of the executor pipeline (parse + plan + execute + commit).
/// Buckets cover the range from cheap point reads to slow
/// scatter-gather writes.
pub static CYPHER_QUERY_DURATION_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register(
        HistogramVec::new(
            HistogramOpts::new(
                "meshdb_cypher_query_duration_seconds",
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

/// `meshdb_cypher_index_seeks_total` — number of `IndexSeek` plan
/// nodes the planner emitted across all queries. The planner walks
/// the final plan tree once after `plan_with_context` and
/// increments by however many seeks it sees, so a `MATCH` with two
/// indexed pattern equalities counts twice.
pub static CYPHER_INDEX_SEEKS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register(
        IntCounter::new(
            "meshdb_cypher_index_seeks_total",
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

/// `mesh_multiraft_forward_writes_total{outcome}` — single-partition
/// writes that arrived on a non-leader peer and proxied through
/// `MeshWrite::ForwardWrite`. `outcome` is `committed` (the proxy
/// hop succeeded on first try), `retried` (first try hit a stale
/// leader cache, retry succeeded), or `exhausted` (both attempts
/// failed — surfaced to the client as `Unavailable`).
pub static MULTI_RAFT_FORWARD_WRITES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register(
        IntCounterVec::new(
            Opts::new(
                "mesh_multiraft_forward_writes_total",
                "Multi-raft single-partition forward_write proxy hops by outcome",
            ),
            &["outcome"],
        )
        .expect("counter spec"),
    )
});

/// `mesh_multiraft_ddl_gate_total{outcome}` — synchronous DDL gate
/// invocations. `outcome` is `ok` (every peer's metadata replica
/// caught up to the proposal index before the strict timeout) or
/// `timeout` (timeout tripped — the DDL is durably committed but
/// not yet visible everywhere). A timeout rate above zero is the
/// signal that meta-Raft replication is degraded.
pub static MULTI_RAFT_DDL_GATE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register(
        IntCounterVec::new(
            Opts::new(
                "mesh_multiraft_ddl_gate_total",
                "Multi-raft synchronous DDL gate invocations by outcome",
            ),
            &["outcome"],
        )
        .expect("counter spec"),
    )
});

/// `mesh_multiraft_indoubt_resolved_total{outcome}` — in-doubt
/// PreparedTx entries that the partition-leader recovery path
/// resolved by polling `ResolveTransaction` against the coordinator.
/// `outcome` is `committed` / `aborted` / `failed` (recovery
/// couldn't reach any peer with the decision — left for the next
/// recovery loop tick).
pub static MULTI_RAFT_INDOUBT_RESOLVED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register(
        IntCounterVec::new(
            Opts::new(
                "mesh_multiraft_indoubt_resolved_total",
                "Multi-raft in-doubt PreparedTx resolutions by outcome",
            ),
            &["outcome"],
        )
        .expect("counter spec"),
    )
});

/// `mesh_multiraft_cross_partition_total{outcome}` — completed
/// multi-raft cross-partition Spanner-style 2PC transactions, with
/// `outcome` in `committed` / `aborted`. Distinct from
/// `mesh_two_phase_commit_total`, which tracks routing-mode 2PC.
pub static MULTI_RAFT_CROSS_PARTITION_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register(
        IntCounterVec::new(
            Opts::new(
                "mesh_multiraft_cross_partition_total",
                "Multi-raft cross-partition 2PC transactions by outcome",
            ),
            &["outcome"],
        )
        .expect("counter spec"),
    )
});

/// `mesh_multiraft_pending_tx_staged` — gauge of the total number
/// of `PreparedTx` entries currently staged across every partition
/// applier on this peer, awaiting `CommitTx` or `AbortTx`. A
/// healthy steady state is near zero; persistent drift indicates
/// a stuck cross-partition transaction.
pub static MULTI_RAFT_PENDING_TX_STAGED: Lazy<IntGauge> = Lazy::new(|| {
    register(
        IntGauge::new(
            "mesh_multiraft_pending_tx_staged",
            "Currently-staged PreparedTx entries across all partitions on this peer",
        )
        .expect("gauge spec"),
    )
});

/// `mesh_multiraft_apply_lag{group}` — number of log entries this
/// peer has committed but not yet applied for `group` ∈
/// `meta` / `p-<id>`. Healthy steady state is near zero; persistent
/// drift means the applier has fallen behind Raft commit (slow
/// disk, blocked on a long apply, deadlock in the state machine).
/// Distinct from openraft's internal "replication lag" — this is
/// the *local* applier-side gap.
pub static MULTI_RAFT_APPLY_LAG: Lazy<prometheus::IntGaugeVec> = Lazy::new(|| {
    register(
        prometheus::IntGaugeVec::new(
            Opts::new(
                "mesh_multiraft_apply_lag",
                "Per-group commit-vs-applied lag (entries not yet applied) on this peer",
            ),
            &["group"],
        )
        .expect("gauge spec"),
    )
});

/// `mesh_multiraft_last_applied{group}` — most recent `last_applied`
/// log index per group. Useful to confirm the applier is making
/// progress — a flat gauge under traffic is the canonical
/// stuck-applier signal.
pub static MULTI_RAFT_LAST_APPLIED: Lazy<prometheus::IntGaugeVec> = Lazy::new(|| {
    register(
        prometheus::IntGaugeVec::new(
            Opts::new(
                "mesh_multiraft_last_applied",
                "Per-group last_applied log index on this peer",
            ),
            &["group"],
        )
        .expect("gauge spec"),
    )
});

/// `mesh_multiraft_partitions_led` — number of partition Raft groups
/// this peer is currently the leader of. Used by operators to
/// detect leader skew across peers (an even cluster has roughly
/// `num_partitions / num_peers` per peer; persistent imbalance
/// signals a missing rebalance). Updated by the same periodic
/// poller that fills the per-group apply-lag gauges.
pub static MULTI_RAFT_PARTITIONS_LED: Lazy<IntGauge> = Lazy::new(|| {
    register(
        IntGauge::new(
            "mesh_multiraft_partitions_led",
            "Number of partition Raft groups this peer currently leads",
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
pub const MODE_MULTI_RAFT: &str = "multi-raft";

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
