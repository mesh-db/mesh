# Mesh — Architecture Spec

## Project Overview

**Mesh** is a distributed, Cypher-compliant graph database written in Rust. Supports horizontal scaling via configurable clusters with graph data partitioned across multiple nodes.

## Workspace Layout

```
mesh/
├── Cargo.toml              # Workspace root
├── crates/
│   ├── meshdb-core/          # Node, Edge, Property types, ID generation
│   ├── meshdb-storage/       # RocksDB-backed store, indexes, transactions
│   ├── meshdb-cypher/        # Lexer, parser, logical plan, optimizer
│   ├── meshdb-executor/      # Physical operators, iterator model
│   ├── meshdb-cluster/       # Raft, partitioning, membership, replication
│   ├── meshdb-rpc/           # tonic/gRPC service definitions + client
│   ├── meshdb-bolt/          # Bolt v4.4–5.4 packstream, messages, framing
│   ├── meshdb-client/        # In-process + Bolt client library
│   ├── meshdb-tck/           # openCypher TCK harness (cucumber-driven)
│   └── meshdb-server/        # Binary: config, startup, Bolt + gRPC listeners
```

## Architecture Decisions

### Storage Engine (`meshdb-storage`)
- **RocksDB** via `rust-rocksdb` crate as the backing store (same approach as Dgraph, Nebula Graph, ArangoDB).
- Column families for: nodes, edges (forward adjacency), edges (reverse adjacency), properties, indexes.
- Each node/edge gets a globally unique 128-bit ID.
- Edges stored twice (forward + reverse) for fast bidirectional traversal.
- Transactions use RocksDB WriteBatch + snapshot isolation (single-partition initially).

### Core Data Model (`meshdb-core`)
- `NodeId` / `EdgeId` — 128-bit unique identifiers (UUID v7 or similar for ordering).
- `Property` — enum covering: `Null`, `String`, `Int64`, `Float64`, `Bool`, `List`, `Map`, temporal (`DateTime`, `LocalDateTime`, `Date`, `Time`, `Duration`), and spatial (`Point` — Cartesian 2D/3D, WGS-84 2D/3D, EPSG-tagged).
- `Node` — id, labels (`Vec<String>`), properties (`HashMap<String, Property>`).
- `Edge` — id, edge_type (String), source NodeId, target NodeId, properties.

### Cypher Query Engine (`meshdb-cypher`)
- Hand-rolled recursive-descent parser emitting a logical plan (tree of relational + graph operators). Planner converts logical → physical plan.
- Key logical operators: NodeScan, EdgeExpand, Filter, Project, Aggregate, Sort, Limit.
- **Implemented Cypher surface:**
  - Read: `MATCH`, `OPTIONAL MATCH`, `WHERE`, `RETURN` (with aliases, `DISTINCT`), `WITH` (re-projection + filter), `ORDER BY`, `LIMIT`, `SKIP`, `UNION` / `UNION ALL`.
  - Write: `CREATE`, `MERGE` (with `ON CREATE SET` / `ON MATCH SET`), `SET` (property, `+=` merge, label), `REMOVE` (property, label), `DELETE` / `DETACH DELETE`, `FOREACH`.
  - Flow: `UNWIND`, `CASE ... WHEN ... ELSE ... END` (simple and generic), parameters (`$name`).
  - Patterns: variable-length paths `()-[*1..3]->()`, Neo4j 5 quantifier shorthand (`->+` = `*1..`, `->*` = `*0..`, `->{n}` / `->{n,m}` / `->{n,}` / `->{,m}`), `shortestPath(...)` (`allShortestPaths` parses but plan rejects).
  - Expressions: list comprehensions, pattern comprehensions, `reduce`, quantifier predicates (`all`/`any`/`none`/`single`), `EXISTS { ... }`, `COUNT { ... }`, and `COLLECT { ... }` subquery expressions.
  - Procedures / subqueries: `CALL { ... }` (unit and returning), `CALL proc YIELD ...` against a runtime-extensible registry in `meshdb-executor` with built-in `db.labels()` / `db.relationshipTypes()` / `db.propertyKeys()` installed by default.
  - Schema: `CREATE INDEX` / `DROP INDEX` / `SHOW INDEXES` on node label+property tuples (`FOR (n:Label) ON (n.prop)` or composite `ON (n.a, n.b, ...)`) *and* relationship type+property tuples (`FOR ()-[r:TYPE]-() ON (r.prop)` or composite); the planner uses composite indexes for prefix-matched equality seeks (`MATCH (n:L {a: 1, b: 2})` or WHERE conjuncts). Edge property predicates with both endpoints unbound lower to `EdgeSeek` through the edge index. `CREATE POINT INDEX` / `DROP POINT INDEX` / `SHOW POINT INDEXES` on single-property `Property::Point` columns for both node and relationship scope — backed by a Z-order (Morton) cell quantizer in separate CFs with per-SRID domains; the planner rewrites `WHERE point.withinbbox(var.p, lo, hi)` and `WHERE point.distance(var.p, center) <[=] r` into `PointIndexSeek` (node scope) or `EdgePointIndexSeek` (relationship scope, on unbound-endpoints patterns `MATCH ()-[r:T]-()`), with the distance predicate kept as a residual Filter to cull the enclosing-bbox overshoot. `CREATE CONSTRAINT` / `DROP CONSTRAINT` / `SHOW CONSTRAINTS` for UNIQUE, NOT NULL, `IS :: <TYPE>` (STRING/INTEGER/FLOAT/BOOLEAN), and composite `IS NODE KEY` — node scope `FOR (n:Label)` or relationship scope `FOR ()-[r:TYPE]-()` — with optional name + `IF [NOT] EXISTS`, replicated across Raft and routing clusters; UNIQUE and NODE KEY both auto-provision a backing property index (composite for NODE KEY) so enforcement stays O(log N) per insert; built-in `db.constraints()` procedure.
  - Scalars: full openCypher scalar surface (string, math, temporal, spatial) plus the widely-expected Neo4j extensions (`*OrNull`, `*List`, `valueType`, `randomUUID`, `round` with precision+mode, `char_length`).
- **Not yet implemented:** APOC procedure library.

### Query Execution (`meshdb-executor`)
- Volcano/iterator (pull-based) model — each operator implements `next() -> Option<Row>`.
- Operators: Scan, IndexSeek (composite-capable — tuple prefix seeks on the index CF), EdgeSeek (relationship-scope analogue for unbound-endpoints patterns), PointIndexSeek + EdgePointIndexSeek (Z-order bbox range + SRID-aware enclosing-bbox for distance queries, node and relationship scope), Expand, Filter, Project, Aggregate, Sort, Limit, CreateNode, CreateEdge, Delete.
- Later optimization: vectorized execution for analytical queries.

### Distribution / Clustering (`meshdb-cluster`)
- **Partitioning:** FNV-1a hash over NodeId maps each node to a `PartitionId`. Edges live on the partition that owns the source vertex, with ghost/stub copies on the target's partition for reverse traversal.
- **Consensus:** Raft via `openraft` 0.9 for metadata and write coordination — full `RaftStorage` impl, AppendEntries / Vote / InstallSnapshot wired up.
- **Reads:** Any node can serve a query; multi-partition traversals scatter-gather through `meshdb-rpc::partitioned_reader`.
- **Writes (routing mode):** Multi-peer Cypher writes ride a durable 2PC. Coordinator side logs PREPARE / CommitDecision / AbortDecision / Completed to `coordinator-log.jsonl`; participant side logs PREPARE / Committed / Aborted to `participant-log.jsonl` and fsyncs before each RPC ACKs. Both logs survive independent coordinator/participant crashes — on startup each peer rehydrates staging from its log, polls peers via `ResolveTransaction` to learn the coordinator's decision for any in-doubt txid, and only then opens the gRPC listener. Per-phase RPC timeouts (10s PREPARE / 30s COMMIT / 10s ABORT by default) bound a stalled peer's ability to block the round. PREPARE retries with identical commands are idempotent; conflicting payloads fail loudly.
- **Future:** LDG streaming partitioner as an alternative to hash partitioning.

### Inter-Node Communication (`meshdb-rpc`)
- **gRPC** via `tonic` crate.
- Service definitions:
  - `WriteService` — forward writes to partition owner
  - `QueryService` — scatter-gather for cross-partition reads
  - `ClusterService` — membership, health checks, leader election
  - `SnapshotService` — snapshot transfer for new/recovering nodes
- Protobuf definitions in a shared `proto/` directory.

### Server (`meshdb-server`)
- Binary entry point.
- Config via TOML file (listen address, cluster peers, data directory, RocksDB tuning), with common knobs also settable via CLI flags / env vars.
- Bolt protocol listener (`meshdb-bolt`) supports handshake v4.4 through 5.4 — HELLO, RUN, PULL, BEGIN/COMMIT/ROLLBACK, RESET, GOODBYE, plus ROUTE for cluster-aware drivers. TLS is available for both Bolt and gRPC listeners via `tokio_rustls`.
- Neo4j drivers can connect directly; the `meshdb-client` crate exposes the same surface in-process for embedded use.

## Key Crate Dependencies

| Crate | Purpose |
|-------|---------|
| `rust-rocksdb` | Storage engine |
| `tonic` / `prost` | gRPC framework |
| `openraft` | Raft consensus (0.9) |
| `tokio` | Async runtime |
| `tokio-rustls` / `rustls` | TLS for Bolt + gRPC |
| `serde` / `serde_json` | Serialization |
| `uuid` | ID generation (v4 + v7) |
| `chrono` / `chrono-tz` | Temporal types and IANA zone resolution |
| `tracing` | Structured logging |
| `clap` | CLI argument parsing |
| `toml` | Config file parsing |
| `cucumber` | openCypher TCK test driver |

## Implementation Status

The foundational phases are all shipping — single-node graph store, Cypher parser + executor, gRPC cluster RPCs, Raft-backed cluster membership, and a Bolt-speaking server. 2PC across partitions is hardened end-to-end: durable coordinator + participant logs, per-phase timeouts, idempotent PREPARE retries, and cross-peer decision recovery via `ResolveTransaction` so coordinator and participant crashes recover independently without operator intervention. The planner-side index work is caught up: composite property indexes (DDL + storage tuple encoding + prefix-matched seeks), edge IndexSeek for unbound-endpoint patterns (single-node, in-tx overlay, and cluster-routing scatter-gather), point / spatial indexes (Z-order cells + DDL + `withinbbox` and distance-radius planner rewrites for both node and relationship scope), and Neo4j 5 quantifier shorthand all shipped. The remaining Cypher-surface gap is the APOC procedure library.

## Target System
- AMD Ryzen 9 9900X (high core count — leverage for concurrent RocksDB operations)
- Linux

## Style & Conventions
- Use `thiserror` for error types, `anyhow` in the binary crate.
- Prefer strong typing over stringly-typed interfaces.
- Each crate should have its own `Error` type.
- Integration tests in each crate's `tests/` directory.
- Benchmarks with `criterion` for storage and query hot paths.
