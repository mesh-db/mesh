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
│   └── meshdb-server/        # Binary: config, startup, bolt protocol listener
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
- `Property` — enum supporting String, Int64, Float64, Bool, List, Map.
- `Node` — id, labels (Vec<String>), properties (HashMap<String, Property>).
- `Edge` — id, edge_type (String), source NodeId, target NodeId, properties.

### Cypher Query Engine (`meshdb-cypher`)
- Parser built with `pest` or `nom`.
- **Initial Cypher subset to implement:**
  - `MATCH` with single-hop and multi-hop patterns
  - `WHERE` with equality, comparison, AND/OR/NOT, property access
  - `CREATE` nodes and relationships
  - `DELETE` / `DETACH DELETE`
  - `RETURN` with aliases, `DISTINCT`
  - `ORDER BY`, `LIMIT`, `SKIP`
  - `SET` for property updates
  - Variable-length paths: `()-[*1..3]->()`
- **Deferred:** MERGE, CASE, list comprehensions, UNWIND, APOC procedures.
- Parser emits a logical plan (tree of relational+graph operators).
- Planner converts logical plan → physical plan.
- Key logical operators: NodeScan, EdgeExpand, Filter, Project, Aggregate, Sort, Limit.

### Query Execution (`meshdb-executor`)
- Volcano/iterator (pull-based) model — each operator implements `next() -> Option<Row>`.
- Operators: Scan, IndexSeek, Expand, Filter, Project, Aggregate, Sort, Limit, CreateNode, CreateEdge, Delete.
- Later optimization: vectorized execution for analytical queries.

### Distribution / Clustering (`meshdb-cluster`)
- **Partitioning:** Hash-partition nodes by NodeId across cluster members.
  - Edges live on the partition that owns the source vertex.
  - Ghost/stub copies on the target's partition for reverse traversal.
- **Consensus:** Raft via `openraft` crate for metadata and write coordination.
- **Reads:** Any node can serve a query; multi-partition traversals use scatter-gather.
- **Writes:** Forwarded to the partition owner; single-partition transactions initially, distributed (2PC) later.
- Start with hash partitioning; consider LDG streaming partitioner later.

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
- Config via TOML file (listen address, cluster peers, data directory, RocksDB tuning).
- Eventually: Bolt protocol listener for Neo4j driver compatibility.
- Initially: gRPC or simple TCP interface for queries.

## Key Crate Dependencies

| Crate | Purpose |
|-------|---------|
| `rust-rocksdb` | Storage engine |
| `pest` or `nom` | Cypher parser |
| `tonic` / `prost` | gRPC framework |
| `openraft` | Raft consensus |
| `tokio` | Async runtime |
| `serde` / `serde_json` | Serialization |
| `uuid` | ID generation |
| `tracing` | Structured logging |
| `clap` | CLI argument parsing |
| `toml` | Config file parsing |

## Build Order / Implementation Phases

### Phase 1: Single-node graph store
1. `meshdb-core` — data model types
2. `meshdb-storage` — RocksDB CRUD, adjacency traversal, basic indexing

### Phase 2: Query engine
3. `meshdb-cypher` — parser for minimal Cypher subset, logical plan generation
4. `meshdb-executor` — wire operators to storage, end-to-end query execution

### Phase 3: Distribution
5. `meshdb-rpc` — protobuf definitions, gRPC services
6. `meshdb-cluster` — Raft, partitioning, membership, replication

### Phase 4: Server
7. `meshdb-server` — config, startup, query interface, Bolt protocol (stretch)

## Target System
- AMD Ryzen 9 9900X (high core count — leverage for concurrent RocksDB operations)
- Linux

## Style & Conventions
- Use `thiserror` for error types, `anyhow` in the binary crate.
- Prefer strong typing over stringly-typed interfaces.
- Each crate should have its own `Error` type.
- Integration tests in each crate's `tests/` directory.
- Benchmarks with `criterion` for storage and query hot paths.
