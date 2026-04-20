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
  - Patterns: variable-length paths `()-[*1..3]->()`, `shortestPath(...)` (`allShortestPaths` parses but plan rejects).
  - Expressions: list comprehensions, pattern comprehensions, `reduce`, quantifier predicates (`all`/`any`/`none`/`single`), `EXISTS { ... }`, `COUNT { ... }`, and `COLLECT { ... }` subquery expressions.
  - Procedures / subqueries: `CALL { ... }` (unit and returning), `CALL proc YIELD ...` against a runtime-extensible registry in `meshdb-executor` with built-in `db.labels()` / `db.relationshipTypes()` / `db.propertyKeys()` installed by default.
  - Schema: `CREATE INDEX` / `DROP INDEX` / `SHOW INDEXES` on label+property pairs; `CREATE CONSTRAINT` / `DROP CONSTRAINT` / `SHOW CONSTRAINTS` for single-property UNIQUE and NOT NULL constraints (with optional name + `IF [NOT] EXISTS`), replicated across Raft and routing clusters; built-in `db.constraints()` procedure.
  - Scalars: full openCypher scalar surface (string, math, temporal, spatial) plus the widely-expected Neo4j extensions (`*OrNull`, `*List`, `valueType`, `randomUUID`, `round` with precision+mode, `char_length`).
- **Not yet implemented:** composite `IS NODE KEY` and relationship-scope constraints, property-type constraints (`IS :: <TYPE>`); edge (relationship) indexes; quantified path patterns (`(a)-->+(b)`, Neo4j 5); APOC.

### Query Execution (`meshdb-executor`)
- Volcano/iterator (pull-based) model — each operator implements `next() -> Option<Row>`.
- Operators: Scan, IndexSeek, Expand, Filter, Project, Aggregate, Sort, Limit, CreateNode, CreateEdge, Delete.
- Later optimization: vectorized execution for analytical queries.

### Distribution / Clustering (`meshdb-cluster`)
- **Partitioning:** FNV-1a hash over NodeId maps each node to a `PartitionId`. Edges live on the partition that owns the source vertex, with ghost/stub copies on the target's partition for reverse traversal.
- **Consensus:** Raft via `openraft` 0.9 for metadata and write coordination — full `RaftStorage` impl, AppendEntries / Vote / InstallSnapshot wired up.
- **Reads:** Any node can serve a query; multi-partition traversals scatter-gather through `meshdb-rpc::partitioned_reader`.
- **Writes:** Forwarded to the partition owner. Single-partition transactions work today; distributed (2PC) writes are still to come.
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

The foundational phases are all shipping — single-node graph store, Cypher parser + executor, gRPC cluster RPCs, Raft-backed cluster membership, and a Bolt-speaking server. Current in-flight work lives at the Cypher-surface level (see the "Not yet implemented" list in the Cypher section above) and in hardening the distributed-write story (2PC across partitions, point / spatial indexes, edge indexes).

## Target System
- AMD Ryzen 9 9900X (high core count — leverage for concurrent RocksDB operations)
- Linux

## Style & Conventions
- Use `thiserror` for error types, `anyhow` in the binary crate.
- Prefer strong typing over stringly-typed interfaces.
- Each crate should have its own `Error` type.
- Integration tests in each crate's `tests/` directory.
- Benchmarks with `criterion` for storage and query hot paths.
