# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-04-27

### Added

- **`allShortestPaths(...)`** is now a supported planner +
  executor surface. The shared layered-BFS operator builds a
  parent DAG that records every `(parent, edge)` pair at each
  shortest-path level, and the reconstruction walk enumerates
  every minimum-length path (vs. `shortestPath(...)`'s single-
  path early exit). Cycles, parallel edges, undirected
  patterns, reverse direction, edge-type unions, and self-loops
  are all covered by integration tests in
  `crates/meshdb-executor/tests/integration.rs`. The capability
  was already present in code but documented as deferred — this
  release sheds the deferral and locks in the behavior with
  paranoia tests.

## [0.1.0] - 2026-04-24

First stable minor cut from the alpha line. No breaking changes to
on-disk format, network protocols, or config shape from
`0.1.0-alpha.9`; upgrade is a process restart on the same data
directory.

### Highlights

- **Distributed 2PC is now test-verified**, not just documented.
  Four new failure-injection integration tests cover coordinator
  crash pre-decision (restart drives `ABORT`), coordinator crash
  mid-COMMIT fanout (restart resumes `COMMIT` idempotently),
  participant crash after `PREPARE`-ACK (`ResolveTransaction`
  cross-peer recovery), and PREPARE-rejection synchronous
  rollback. Deterministic injection via a cfg-gated `FaultPoints`
  struct — zero cost in release builds.
- **`apoc.periodic.iterate` now honors `parallel: true` /
  `concurrency: N`.** Batches dispatch through a
  `tokio::sync::Semaphore`-bounded worker pool with
  `Arc<Mutex<...>>`-guarded stats accumulators. Throughput wins
  materialize fully in single-node mode; Raft and routing-2PC
  modes still serialize commits internally, so the gain there is
  limited to hiding per-batch planning / read latency.
- **Criterion benches for storage and query hot paths** shipped
  under `crates/meshdb-storage/benches/` and
  `crates/meshdb-rpc/benches/`. 11 benches covering point lookup,
  single / batched writes, adjacency scans, label / indexed
  seeks, and end-to-end Cypher (`CREATE`, `MATCH`, one-hop,
  `count`). Local-run only — no CI regression gating yet.
- **Public error enums are `#[non_exhaustive]`**. Future variants
  on `meshdb-core::Error`, `meshdb-storage::Error`,
  `meshdb-cypher::Error`, `meshdb-executor::Error`,
  `meshdb-cluster::Error`, `meshdb-bolt::BoltError`,
  `meshdb-rpc::ConvertError`, and `meshdb-apoc::ApocError` can be
  added without a breaking-change bump.

### Known limitations deferred past 0.1.0

- LDG streaming partitioner is not implemented — only the
  FNV-1a hash partitioner. Adequate for uniform workloads;
  skew-sensitive workloads will want the streaming partitioner.
- Vectorized / columnar execution is out of scope. The current
  Volcano/iterator model is well-suited to OLTP; analytical
  workloads at 100M+ rows will eventually want a vectorized
  runtime.
- `apoc.periodic.iterate` parallel gains in Raft / routing-2PC
  modes are limited by internal serialization of the commit
  path. Single-node mode gets the full throughput benefit.
- Criterion benches run locally only. A CI perf-regression gate
  (baseline + variance budget) is a follow-up.

### Stability commitments for 0.x

- **Bolt wire protocol (v4.4–5.4):** stable within a minor;
  breaking additions cut a new minor.
- **gRPC internal protocol (`proto/mesh.proto`):** intra-cluster
  only; treated as stable within a minor but no external
  guarantee.
- **On-disk data layout (RocksDB column families):** breaking
  changes will call out a migration path in release notes. No
  breaks in `0.1.x`.
- **TOML config schema:** additive within a minor; fields may be
  added, none removed. Unknown fields continue to error via
  serde's `deny_unknown_fields`.
- **Public Rust API:** error enums may grow variants at any
  point (hence the `#[non_exhaustive]` annotation); other public
  types may break at minor bumps (`0.2`, `0.3`, etc.) without
  notice until 1.0 crystallizes the surface. Consumers
  embedding Mesh as a library should pin to exact minor
  versions.

### Upgrade from `0.1.0-alpha.9`

No action required beyond a binary restart. No data migration,
no config changes, no protocol changes. Alpha releases have been
wire- and storage-compatible with 0.1.0 throughout.

## [0.1.0-alpha.5](https://github.com/mesh-db/meshdb/compare/v0.1.0-alpha.3...v0.1.0-alpha.5) - 2026-04-21

### Other

- add relationship-scope property indexes

## [0.1.0-alpha.4](https://github.com/mesh-db/meshdb/compare/v0.1.0-alpha.3...v0.1.0-alpha.4) - 2026-04-21

### Other

- add relationship-scope property indexes

## [0.1.0-alpha.3](https://github.com/mesh-db/meshdb/compare/v0.1.0-alpha.2...v0.1.0-alpha.3) - 2026-04-21

### Other

- add Point type and openCypher point functions
