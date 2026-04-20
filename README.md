<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark.png">
    <img src="assets/logo.png" alt="Mesh" width="1280"/>
  </picture>
</p>

# MeshDB

A distributed, Cypher-compliant graph database written in Rust.

Mesh stores property graphs in RocksDB, parses and executes a useful subset
of Cypher, and supports two wire protocols out of the box:

- **Bolt 5** (5.0 – 5.4, with 4.4 still negotiated for older clients), so any
  Neo4j driver (Python, JS, Java, Go, .NET, cypher-shell) can connect directly.
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
- `WHERE` with `=`, `<>`, `<`, `<=`, `>`, `>=`, `=~` (regex), `AND`, `OR`,
  `NOT`, property access, function calls
- `CREATE` for nodes and relationships
- `MERGE` (match-or-create)
- `DELETE` and `DETACH DELETE`
- `SET` for property updates, label additions, replace (`=`) and merge (`+=`)
  property maps
- `RETURN` with aliases and `DISTINCT`
- `WITH` for chained query pipelines (multi-stage `MATCH ... WITH ... MATCH`)
- `CALL` procedures (pluggable registry; no APOC library ships by default)
- `ORDER BY`, `LIMIT`, `SKIP`
- Aggregates: `count`, `sum`, `avg`, `min`, `max`, `collect` (with `DISTINCT`)
- Scalar functions: `size`, `length`, `labels`, `keys`, `type`, `tolower`,
  `toupper`, `tostring`, `tointeger`, `coalesce`
- Temporal functions: `datetime`, `localdatetime`, `date`, `time`,
  `localtime`, `duration`
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

- **Bolt 5.0 – 5.4** (plus legacy 4.4): handshake, PackStream encoding, all
  standard message types (HELLO, LOGON, LOGOFF, RUN, PULL, DISCARD, RESET,
  GOODBYE, BEGIN, COMMIT, ROLLBACK, TELEMETRY), parameters, explicit
  transactions with atomic batch commit
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

## Bolt TLS

The Bolt listener can terminate TLS directly. Add a `[bolt_tls]` section
pointing at PEM-encoded certificate and key files:

```toml
[bolt_tls]
cert_path = "/etc/mesh/bolt-cert.pem"
key_path = "/etc/mesh/bolt-key.pem"
```

The cert file may contain a leaf certificate followed by any
intermediates (leaf first); the key file may be PKCS#8, SEC1 (EC), or
RSA — the first private key found wins. A short-lived self-signed
pair for local dev is one command:

```sh
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -keyout key.pem -out cert.pem -days 365 -nodes \
  -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost'
```

Once TLS is enabled on the server, clients connect with `bolt+s://`
(TLS, verifies the chain) or `bolt+ssc://` (TLS, self-signed — skips
verification for dev):

```python
from neo4j import GraphDatabase

# Production: server presents a cert signed by a trusted CA.
driver = GraphDatabase.driver("bolt+s://mesh.example.com:7687",
                               auth=("neo4j", "my-password"))

# Local dev with the self-signed cert generated above.
driver = GraphDatabase.driver("bolt+ssc://127.0.0.1:7687",
                               auth=("neo4j", "my-password"))
```

Plain `bolt://` against a TLS listener will fail the handshake — pick
one or the other per listener. `bolt_tls` and `bolt_auth` compose, so
you can require both credentials and TLS by setting both sections.

---

## Known limitations

- **No TLS on the gRPC listener.** Bolt TLS is supported (see
  [Bolt TLS](#bolt-tls) below); the internal gRPC listener that carries
  Raft / 2PC traffic is still plaintext only.
- **No built-in APOC procedure library.** The `CALL` procedure framework
  and an extensible `ProcedureRegistry` are in place, but no APOC-compatible
  procedures ship with Mesh.

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
