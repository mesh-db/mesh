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

### 1. Get the binary

Install from crates.io (pre-1.0, so the version must be explicit):

```sh
cargo install meshdb-server --version 0.1.0-alpha.3
```

Or build from source:

```sh
cargo build -p meshdb-server --release
```

Either path needs `clang` / `libclang-dev` on Linux because `rust-rocksdb`
generates bindings at compile time. `protoc` is **not** required —
`meshdb-rpc/build.rs` uses a vendored binary.

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
RUST_LOG=info meshdb-server --config /tmp/mesh.toml
```

(If you built from source, the binary is at `./target/debug/meshdb-server`
or `./target/release/meshdb-server`.)

You should see something like:

```
INFO meshdb_server: starting meshdb-server self_id=1 listen_address=127.0.0.1:7001 data_dir=/tmp/mesh-data peers=0
INFO meshdb_server: meshdb-server listening addr=127.0.0.1:7001
INFO meshdb_server: meshdb-server bolt listening addr=127.0.0.1:7687
```

Leave it running. Ctrl-C to stop. Wipe `/tmp/mesh-data` between runs if you
want a clean slate.

#### Running without a config file

For quick tests (and Docker containers where mounting a file is awkward),
`meshdb-server` accepts CLI flags for the common-case fields. Every flag
has a matching `MESHDB_*` env var, so:

```sh
meshdb-server \
  --self-id 1 \
  --listen-address 127.0.0.1:7001 \
  --bolt-address 127.0.0.1:7687 \
  --data-dir /tmp/mesh-data
```

is equivalent to:

```sh
MESHDB_SELF_ID=1 \
MESHDB_LISTEN_ADDRESS=127.0.0.1:7001 \
MESHDB_BOLT_ADDRESS=127.0.0.1:7687 \
MESHDB_DATA_DIR=/tmp/mesh-data \
meshdb-server
```

When both `--config` and CLI flags are present, the TOML file is loaded
first and any set flags override the corresponding fields. Structured
settings — `peers`, `bolt_auth`, `bolt_tls`, `grpc_tls` — stay TOML-only.
`meshdb-server --help` lists every flag.

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

**Read:** `MATCH`, `OPTIONAL MATCH`, `WHERE`, `RETURN` (with aliases,
`DISTINCT`), `WITH` (re-projection + filter), `ORDER BY`, `LIMIT`, `SKIP`,
`UNION` / `UNION ALL`.

**Write:** `CREATE`, `MERGE` (with `ON CREATE SET` / `ON MATCH SET`), `SET`
(property, `+=` merge, label), `REMOVE` (property, label), `DELETE` /
`DETACH DELETE`, `FOREACH`, `LOAD CSV [WITH HEADERS]`.

**Flow:** `UNWIND`, `CASE ... WHEN ... ELSE ... END` (simple and generic),
parameters (`$name` and positional `$0`).

**Patterns:** variable-length paths `()-[*1..3]->()`, Neo4j 5 quantifier
shorthand (`->+` = `*1..`, `->*` = `*0..`, `->{n}` / `->{n,m}` /
`->{n,}` / `->{,m}`), `shortestPath(...)` and `allShortestPaths(...)`.

**Expressions:** list literals, list comprehensions
`[x IN list WHERE pred | proj]`, pattern comprehensions, `reduce`,
quantifier predicates (`all` / `any` / `none` / `single`),
`EXISTS { ... }`, `COUNT { ... }`, and `COLLECT { ... }` subquery
expressions.

**Procedures / subqueries:** `CALL { ... }` (unit and returning form),
`CALL proc YIELD ...` against a runtime-extensible registry. Built-in
procedures: `db.labels()`, `db.relationshipTypes()`, `db.propertyKeys()`,
`db.constraints()`. No APOC library ships by default.

**Schema:** `CREATE INDEX` / `DROP INDEX` / `SHOW INDEXES` on single-property
or composite tuples, for both node (`FOR (n:Label) ON (n.a, n.b, ...)`) and
relationship (`FOR ()-[r:TYPE]-() ON (r.p)`) scopes. The planner rewrites
pattern-property equalities and `WHERE` conjuncts to composite `IndexSeek`
when a covering prefix exists; unbound-endpoint patterns with an indexed
edge property (`MATCH (a)-[r:T {p: v}]->(b)`) lower to `EdgeSeek`.
`CREATE POINT INDEX` / `DROP POINT INDEX` / `SHOW POINT INDEXES` on
`Property::Point` columns — `FOR (n:Label) ON (n.loc)` or
`FOR ()-[r:TYPE]-() ON (r.loc)` — backed by a Z-order (Morton) cell
quantizer with per-SRID domains so bbox queries scan a tight cell range
and SRID mismatches can't alias. `WHERE point.withinbbox(var.p, lo, hi)`
and `WHERE point.distance(var.p, center) <[=] r` lower to
`PointIndexSeek` (node scope) or `EdgePointIndexSeek` (unbound-endpoint
relationship patterns `MATCH ()-[r:T]-()`); Cartesian and WGS-84
(geographic) coordinates both index, with the distance operator
computing an SRID-aware enclosing bbox and a residual Filter culling
the circle-vs-square overshoot.
`CREATE CONSTRAINT` / `DROP CONSTRAINT` / `SHOW CONSTRAINTS` for `UNIQUE`,
`NOT NULL`, `IS :: <TYPE>` (STRING/INTEGER/FLOAT/BOOLEAN), and composite
`IS NODE KEY` — node scope `FOR (n:Label)` or relationship scope
`FOR ()-[r:TYPE]-()` — with optional name and `IF [NOT] EXISTS`.
`UNIQUE` and `NODE KEY` auto-provision a backing index (single-property
or composite, respectively) so enforcement stays O(log N) per insert.
Index + constraint DDL replicates across Raft and routing clusters.

**Aggregates:** `count`, `sum`, `avg`, `min`, `max`, `collect` (all with
`DISTINCT`), `stdev`, `stdevp`, `percentileDisc`, `percentileCont`.

**Scalar functions** — the full openCypher surface plus the widely-expected
Neo4j extensions:

- *String:* `upper`, `lower`, `trim`, `ltrim`, `rtrim`, `length`,
  `char_length`, `substring`, `left`, `right`, `split`, `replace`,
  `reverse`
- *Math:* `abs`, `sqrt`, `floor`, `ceil`, `round` (with precision + mode),
  `sign`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `cot`, `haversin`,
  `exp`, `log`, `ln`, `degrees`, `radians`, `pi`, `e`, `rand`
- *Temporal:* `date`, `datetime`, `localdatetime`, `time`, `localtime`,
  `duration`, `timestamp`, plus component accessors (`year`, `month`,
  `day`, `hour`, ...)
- *Spatial:* `point`, `distance`, `cartesian`, `latitude`, `longitude`,
  `x`, `y`, `z`, `srid`, `crs`
- *Type coercion:* `toString`, `toInteger`, `toFloat`, `toBoolean`, plus
  `*OrNull` and `*List` variants, and `valueType`
- *Graph:* `id`, `elementid`, `type`, `keys`, `labels`, `properties`,
  `nodes`, `relationships`, `startnode`, `endnode`
- *Other:* `coalesce`, `head`, `last`, `tail`, `range`, `exists`,
  `randomUUID`, `isNaN`, `isEmpty`

**Data types:** openCypher scalars (string, int, float, bool, null), list,
map, temporal (`DateTime`, `LocalDateTime`, `Date`, `Time`, `LocalTime`,
`Duration` — with IANA zone resolution via the `[Region/City]` suffix),
and spatial `Point` (Cartesian 2D/3D, WGS-84 2D/3D, EPSG-tagged).

### Distribution

- Hash partitioning across peers
- `PartitionedGraphReader` — point reads route to partition owner; bulk
  scans (`nodes_by_label`, `all_node_ids`) scatter-gather across peers
- `RoutingGraphWriter` — point-routing for direct gRPC writes
- **Hardened 2PC** for multi-peer Cypher transactions:
  - Durable coordinator log at `data_dir/coordinator-log.jsonl`
    records PREPARE / CommitDecision / AbortDecision / Completed.
  - Durable participant log at `data_dir/participant-log.jsonl`
    records every PREPARE / COMMIT / ABORT the peer receives, fsync'd
    before the RPC ACKs so the staged batch survives a peer crash.
  - Per-phase RPC deadlines (10s PREPARE / 30s COMMIT / 10s ABORT by
    default) so a stalled peer can't hang the round indefinitely.
  - Idempotent PREPARE retry: a transient network glitch that causes
    the coordinator to resend PREPARE with identical commands
    returns OK; a conflicting payload still errors loudly.
  - `ResolveTransaction` RPC lets a restarted participant poll every
    peer for the coordinator's decision on any in-doubt txid — the
    recovery path applies the outcome without waiting out the
    staging TTL.
- Ghost-edge replication: cross-partition edges land on both source-owner
  and target-owner so reverse traversal works
- Raft consensus (via `openraft`) replicates the full graph in Raft mode

### Wire protocols

- **Bolt 5.0 – 5.4** (plus legacy 4.4): handshake, PackStream encoding, all
  standard message types (HELLO, LOGON, LOGOFF, RUN, PULL, DISCARD, RESET,
  GOODBYE, BEGIN, COMMIT, ROLLBACK, TELEMETRY), parameters, explicit
  transactions with atomic batch commit
- **gRPC** via tonic: `MeshQuery`, `MeshWrite`, `MeshRaft` services. See
  `crates/meshdb-rpc/proto/mesh.proto`.

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

## gRPC TLS

The gRPC listener — which carries Raft replication, 2PC coordination,
and scatter-gather queries between peers — can also terminate TLS.
Unlike Bolt, every peer is both a gRPC server *and* a gRPC client
(heartbeats, leader forwarding, remote reads), so the config describes
both roles in one section:

```toml
[grpc_tls]
cert_path = "/etc/mesh/peer-cert.pem"   # server identity presented to peers
key_path  = "/etc/mesh/peer-key.pem"    # matching private key
ca_path   = "/etc/mesh/peer-ca.pem"     # trust bundle for verifying peers
```

When `grpc_tls` is set on a peer, the URI scheme for outbound channels
flips from `http://` to `https://` automatically; the section must be
set on **every** peer in the cluster, since Mesh doesn't support
mixed TLS / plaintext clusters.

The simplest working setup for a small cluster is one shared
self-signed certificate acting as its own CA, with every peer using
the same cert/key/ca triple. The cert's Subject Alternative Names
must cover every address peers dial — for the two-peer loopback
example below that's `DNS:localhost, IP:127.0.0.1`:

```sh
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -keyout peer-key.pem -out peer-cert.pem -days 365 -nodes \
  -subj '/CN=mesh-peer' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1'
cp peer-cert.pem peer-ca.pem   # self-signed: cert is its own CA
```

Production deployments with a private CA signing one cert per peer
follow the same shape — set `cert_path` / `key_path` to the per-peer
pair, `ca_path` to the shared CA bundle.

---

## Known limitations

- **No built-in APOC procedure library.** The `CALL` procedure framework
  and an extensible `ProcedureRegistry` are in place, but no APOC-compatible
  procedures ship with Mesh.
- **GQL quantified path patterns** — parenthesized-subpath form like
  `((a)-[:T]-(b))+` — aren't parsed. The Neo4j 5 relationship-level
  shorthand (`->+`, `->*`, `->{n,m}`) is fully supported.

---

## Workspace layout

```
mesh/
├── crates/
│   ├── meshdb-core/        # NodeId / EdgeId / Property types
│   ├── meshdb-storage/     # RocksDB-backed Store, indexes, batching
│   ├── meshdb-cypher/      # Pest grammar, AST, parser, planner
│   ├── meshdb-executor/    # Volcano-model operators, eval, GraphReader/Writer traits
│   ├── meshdb-cluster/     # Raft via openraft, partitioner, cluster state
│   ├── meshdb-rpc/         # tonic gRPC services, partitioned reader/writer, 2PC, TLS helpers
│   ├── meshdb-bolt/        # Pure-protocol Bolt library: PackStream, framing, handshake, messages
│   ├── meshdb-client/      # Binary: TUI client for Bolt-compatible graph DBs (Mesh, Neo4j)
│   ├── meshdb-tck/         # openCypher TCK (Technology Compatibility Kit) runner
│   └── meshdb-server/      # Binary: config, startup, gRPC listener, Bolt listener
└── .github/workflows/
    ├── ci.yml             # Build + test + fmt check on push and PR
    └── release-plz.yml    # Release PR + crates.io publish automation
```

Each crate has its own `Error` type via `thiserror` and its own `tests/`
directory. Integration tests in `meshdb-server/tests/bolt.rs` drive the full
pipeline end-to-end with a raw TCP Bolt client; `meshdb-server/tests/bolt_tls.rs`
and `meshdb-server/tests/grpc_tls.rs` cover the TLS listeners with rcgen-generated
self-signed certs.

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
  writes go through the hardened 2PC path (coordinator + participant
  logs at `data_dir/{coordinator,participant}-log.jsonl`, per-phase
  timeouts, idempotent PREPARE retry, cross-peer `ResolveTransaction`
  recovery). No consensus, so a peer crash loses that peer's shard
  until it restarts — but the 2PC recovery logs guarantee no
  in-flight transaction is lost in the process.

A three-peer Raft cluster, one bootstrap seed, all speaking Bolt:

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

[[peers]]
id = 3
address = "127.0.0.1:7003"
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

[[peers]]
id = 3
address = "127.0.0.1:7003"
```

`/tmp/mesh-c.toml`:

```toml
self_id = 3
listen_address = "127.0.0.1:7003"
data_dir = "/tmp/mesh-data-c"
bolt_address = "127.0.0.1:7689"
num_partitions = 4

[[peers]]
id = 1
address = "127.0.0.1:7001"

[[peers]]
id = 2
address = "127.0.0.1:7002"

[[peers]]
id = 3
address = "127.0.0.1:7003"
```

Start each peer in its own terminal (peer A first, since it bootstraps):

```sh
./target/debug/meshdb-server --config /tmp/mesh-a.toml
./target/debug/meshdb-server --config /tmp/mesh-b.toml
./target/debug/meshdb-server --config /tmp/mesh-c.toml
```

Connect Bolt clients to any of `127.0.0.1:7687` (peer A),
`127.0.0.1:7688` (peer B), or `127.0.0.1:7689` (peer C). In Raft mode
every peer holds the full graph, so reads are cheap everywhere; writes
go through the leader (with transparent forwarding from followers in
the auto-commit path). Three peers also give Raft a proper quorum of
two — the cluster tolerates one peer being down without losing write
availability.

To run the same trio in routing (sharded) mode instead, add
`mode = "routing"` to every config and drop the `bootstrap` line — no
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

[[peers]]
id = 3
address = "127.0.0.1:7003"
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

Releases are managed by `release-plz` (`.github/workflows/release-plz.yml`).
On each push to `main` it opens (or updates) a single Release PR that bumps
the shared workspace version in `Cargo.toml` and updates `CHANGELOG.md`.
Merging that PR tags `v{version}`, cuts a GitHub Release, and publishes
every crate to crates.io in dependency order. Use **"Create a merge commit"**
(not "Squash and merge") when merging Release PRs — release-plz's tag
creation has a [known bug](https://github.com/release-plz/release-plz/issues/2759)
with squash merges.

---

## License

MIT.
