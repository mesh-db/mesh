# Mesh

A distributed, Cypher-compliant graph database written in Rust.

Mesh stores property graphs in RocksDB, parses and executes a useful subset
of Cypher, and supports two wire protocols out of the box:

- **Bolt 4.4**, so any Neo4j driver (Python, JS, Java, Go, .NET, cypher-shell)
  can connect directly.
- **gRPC** for service-to-service traffic and for the cluster's internal
  Raft / 2PC plumbing.

It runs in three modes:

| Mode | When | What it gives you |
|---|---|---|
| Single-node | One process, no `peers = [...]` | Local graph store, full Cypher subset, both Bolt and gRPC listeners. |
| Routing (sharded) | Multiple peers, no Raft | Hash-partitioned nodes across peers, partition-aware reads via scatter-gather, 2PC writes for cross-peer atomicity. |
| Raft replicated | Multiple peers + `bootstrap = true` on the seed | Single Raft group replicates the full graph to every peer; reads cheap-local, writes go through `propose_graph`. |

---

## Quick start

### 1. Build

```sh
cargo build -p mesh-server
```

Or for an optimized binary:

```sh
cargo build -p mesh-server --release
```

The build needs `clang` / `libclang-dev` on Linux because `rust-rocksdb`
generates bindings at compile time. `protoc` is **not** required —
`mesh-rpc/build.rs` uses a vendored binary.

### 2. Write a minimal config

Save as `/tmp/mesh.toml`:

```toml
self_id = 1
listen_address = "127.0.0.1:7001"
data_dir = "/tmp/mesh-data"
bolt_address = "127.0.0.1:7687"
```

This is single-node mode: no `peers`, no `bootstrap`. The server creates
`/tmp/mesh-data` on first start. `7001` is the gRPC port; `7687` is the
standard Neo4j Bolt port (every driver defaults to it).

### 3. Run the server

```sh
RUST_LOG=info ./target/debug/mesh-server --config /tmp/mesh.toml
```

You should see something like:

```
INFO mesh_server: starting mesh-server self_id=1 listen_address=127.0.0.1:7001 data_dir=/tmp/mesh-data peers=0
INFO mesh_server: mesh-server listening addr=127.0.0.1:7001
INFO mesh_server: mesh-server bolt listening addr=127.0.0.1:7687
```

Leave it running. Ctrl-C to stop. Wipe `/tmp/mesh-data` between runs if you
want a clean slate.

### 4. Connect from a Bolt client

The easiest path is the official Python driver:

```sh
python -m pip install --user neo4j
```

Save this as `/tmp/mesh_demo.py`:

```python
from neo4j import GraphDatabase

# Auth is accepted-but-ignored — any credentials work.
driver = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=("any", "any"))

with driver.session() as s:
    # Auto-commit with parameters:
    s.run("CREATE (n:Person {name: $name, age: $age})", name="Ada", age=37)
    s.run("CREATE (n:Person {name: $name, age: $age})", name="Grace", age=85)

    # Parameterized read:
    result = s.run(
        "MATCH (n:Person) WHERE n.age > $min RETURN n.name AS name, n.age AS age ORDER BY age",
        min=40,
    )
    for record in result:
        print(record["name"], record["age"])

    # Explicit transaction — both creates land atomically on commit:
    with s.begin_transaction() as tx:
        tx.run("CREATE (n:Project {title: $title})", title="mesh")
        tx.run("CREATE (n:Project {title: $title})", title="bolt-listener")

    # UNWIND with a list parameter — canonical batch idiom:
    s.run("UNWIND $items AS x CREATE (:Tag {name: x})", items=["rust", "graph", "cypher"])

    print("--- all tags ---")
    for r in s.run("MATCH (n:Tag) RETURN n.name AS name ORDER BY name"):
        print(r["name"])

driver.close()
```

Run it:

```sh
python /tmp/mesh_demo.py
```

Expected output:

```
Grace 85
--- all tags ---
cypher
graph
rust
```

That single script exercises the Bolt handshake, PackStream encoding,
parameter binding, pattern-property parameters, explicit transactions,
and `UNWIND $list` — the full driver-facing surface as of today.

---

## What works

### Cypher subset

- `MATCH` with single-hop and multi-hop patterns
- Variable-length paths `()-[*1..3]->()`
- `WHERE` with `=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`,
  property access, function calls
- `CREATE` for nodes and relationships
- `MERGE` (match-or-create)
- `DELETE` and `DETACH DELETE`
- `SET` for property updates, label additions, replace (`=`) and merge (`+=`)
  property maps
- `RETURN` with aliases and `DISTINCT`
- `ORDER BY`, `LIMIT`, `SKIP`
- Aggregates: `count`, `sum`, `avg`, `min`, `max`, `collect` (with `DISTINCT`)
- Scalar functions: `size`, `length`, `labels`, `keys`, `type`, `tolower`,
  `toupper`, `tostring`, `tointeger`, `coalesce`
- `CASE` (both simple and generic forms)
- `UNWIND`
- List literals and list comprehensions `[x IN list WHERE pred | proj]`
- Parameters: `$name` and positional `$0`, in WHERE / RETURN / SET / pattern
  property positions / UNWIND source

### Distribution

- Hash partitioning across peers
- `PartitionedGraphReader` — point reads route to partition owner; bulk
  scans (`nodes_by_label`, `all_node_ids`) scatter-gather across peers
- `RoutingGraphWriter` — point-routing for direct gRPC writes
- 2PC coordinator for multi-peer Cypher transactions (per-peer prepare,
  cross-peer commit-or-abort)
- Ghost-edge replication: cross-partition edges land on both source-owner
  and target-owner so reverse traversal works
- Raft consensus (via `openraft`) replicates the full graph in Raft mode

### Wire protocols

- **Bolt 4.4**: handshake, PackStream encoding, all standard message types
  (HELLO, RUN, PULL, DISCARD, RESET, GOODBYE, BEGIN, COMMIT, ROLLBACK),
  parameters, explicit transactions with atomic batch commit
- **gRPC** via tonic: `MeshQuery`, `MeshWrite`, `MeshRaft` services. See
  `crates/mesh-rpc/proto/mesh.proto`.

---

## Bolt authentication

By default the Bolt listener accepts any credentials — convenient for
local dev but **don't expose that on an untrusted network**. To require
authentication, add a `[[bolt_auth.users]]` section to your config:

```toml
[[bolt_auth.users]]
username = "neo4j"
password = "plaintext-works-for-dev"
```

Plain-text passwords are fine for local setups where the config file is
the source of truth. For anything shared, store a **bcrypt hash** instead
— Mesh recognizes the canonical `$2a$` / `$2b$` / `$2y$` / `$2x$`
prefixes and routes them through `bcrypt::verify`:

```toml
[[bolt_auth.users]]
username = "neo4j"
password = "$2b$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"
```

Generate a hash with any standard tool — Python's `bcrypt` package
works well:

```sh
python -c 'import bcrypt; print(bcrypt.hashpw(b"my-password", bcrypt.gensalt(rounds=12)).decode())'
```

or `htpasswd`:

```sh
htpasswd -bnBC 12 "" my-password | tr -d ':\n'
```

Mixed tables are allowed — a user can migrate from plain text to bcrypt
one row at a time.

Connect with `auth=(username, password)`:

```python
driver = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=("neo4j", "my-password"))
```

A mismatch returns `Neo.ClientError.Security.Unauthorized` and closes the
connection, which driver SDKs translate into an `AuthError`.

---

## Known limitations

- **No read-after-write inside an explicit transaction.** A `MATCH` after
  a `CREATE` in the same `BEGIN`/`COMMIT` block sees the store as it was at
  `BEGIN` time and will *not* observe the buffered write. Auto-commit RUNs
  outside a transaction are not affected.
- **Coordinator crash recovery for 2PC is not implemented yet.** If the
  coordinating peer crashes between PREPARE and COMMIT, participants are
  left with stuck staged batches until restart.
- **Snapshot serialization is JSON.** `StoreGraphApplier::snapshot`
  serializes the whole graph as one JSON document. Fine for small clusters
  and tests; will not scale to large datasets without a streaming
  checkpoint API.
- **No TLS** on either the Bolt or gRPC listener.
- **Some deferred Cypher features**: chained `WITH` / multi-MATCH
  pipelines, `CALL` procedures, regex (`=~`), temporal functions, APOC
  procedures.

---

## Workspace layout

```
mesh/
├── crates/
│   ├── mesh-core/        # NodeId / EdgeId / Property types
│   ├── mesh-storage/     # RocksDB-backed Store, indexes, batching
│   ├── mesh-cypher/      # Pest grammar, AST, parser, planner
│   ├── mesh-executor/    # Volcano-model operators, eval, GraphReader/Writer traits
│   ├── mesh-cluster/     # Raft via openraft, partitioner, cluster state
│   ├── mesh-rpc/         # tonic gRPC services, partitioned reader/writer, 2PC
│   ├── mesh-bolt/        # Pure-protocol Bolt library: PackStream, framing, handshake, messages
│   └── mesh-server/      # Binary: config, startup, gRPC listener, Bolt listener
└── .github/workflows/
    └── ci.yml            # Build + test + fmt check on push and PR
```

Each crate has its own `Error` type via `thiserror` and its own `tests/`
directory. Integration tests in `mesh-server/tests/bolt.rs` drive the full
pipeline end-to-end with a raw TCP Bolt client.

---

## Cluster mode (multi-peer)

Multi-peer configs pick one of two modes via the top-level `mode` field:

- `mode = "raft"` — single Raft group replicates the full graph to every
  peer. Reads are cheap-local everywhere; writes go through the leader
  (followers transparently forward). Default when `peers` is non-empty
  and `mode` is omitted, for backward compatibility with pre-`mode`
  configs.
- `mode = "routing"` — hash-partitioned sharding. Each node lives on
  exactly one peer; cross-peer reads scatter-gather and cross-peer
  writes go through the 2PC coordinator with a durable recovery log
  under `data_dir/coordinator-log.jsonl`. No consensus, so a peer
  crash loses that peer's shard until it restarts.

A two-peer Raft cluster, one bootstrap seed, both speaking Bolt:

`/tmp/mesh-a.toml`:

```toml
self_id = 1
listen_address = "127.0.0.1:7001"
data_dir = "/tmp/mesh-data-a"
bolt_address = "127.0.0.1:7687"
num_partitions = 4
bootstrap = true

[[peers]]
id = 1
address = "127.0.0.1:7001"

[[peers]]
id = 2
address = "127.0.0.1:7002"
```

`/tmp/mesh-b.toml`:

```toml
self_id = 2
listen_address = "127.0.0.1:7002"
data_dir = "/tmp/mesh-data-b"
bolt_address = "127.0.0.1:7688"
num_partitions = 4

[[peers]]
id = 1
address = "127.0.0.1:7001"

[[peers]]
id = 2
address = "127.0.0.1:7002"
```

Start both peers in separate terminals (peer A first, since it bootstraps):

```sh
./target/debug/mesh-server --config /tmp/mesh-a.toml
./target/debug/mesh-server --config /tmp/mesh-b.toml
```

Connect Bolt clients to either `127.0.0.1:7687` (peer A) or
`127.0.0.1:7688` (peer B). In Raft mode every peer holds the full graph,
so reads are cheap on either side; writes go through the leader (with
transparent forwarding from followers in the auto-commit path).

To run the same pair in routing (sharded) mode instead, add
`mode = "routing"` to both configs and drop the `bootstrap` line — no
seed is needed since there's no Raft group to initialize:

```toml
self_id = 1
listen_address = "127.0.0.1:7001"
data_dir = "/tmp/mesh-data-a"
bolt_address = "127.0.0.1:7687"
num_partitions = 4
mode = "routing"

[[peers]]
id = 1
address = "127.0.0.1:7001"

[[peers]]
id = 2
address = "127.0.0.1:7002"
```

---

## Development

Run the full test suite:

```sh
cargo test --workspace
```

Format check (CI gates on this):

```sh
cargo fmt --all -- --check
```

Build with warnings as errors (matches CI):

```sh
RUSTFLAGS="-D warnings" cargo build --workspace --all-targets
```

The `.github/workflows/ci.yml` workflow runs all three on every push and
pull request, against Ubuntu + stable Rust, with `Swatinem/rust-cache` for
fast incremental rebuilds.

---

## License

MIT OR Apache-2.0.
