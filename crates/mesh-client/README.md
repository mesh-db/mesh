# mesh-client

A Rust TUI for graph databases. One binary, three tabs: a Cypher REPL, a schema
browser, and an ASCII graph view of the nodes and edges in the last result.

Speaks Bolt 5.4 / 5.0 / 4.4 (against Mesh, Neo4j, or anything else on the
wire) and Redis `GRAPH.QUERY` (against FalkorDB).

## Backends

| Backend  | Protocol                    | Notes                                          |
|----------|-----------------------------|------------------------------------------------|
| Mesh     | Bolt (5.4 / 5.0 / 4.4)      | Uses the `mesh-bolt` crate directly.           |
| Neo4j    | Bolt (5.4 / 5.0 / 4.4)      | Plaintext and TLS (`bolt+s://`, `neo4j+s://`). |
| FalkorDB | Redis `GRAPH.QUERY`         | Verbose mode; best-effort node parsing.        |

### URI schemes

| Scheme          | Transport                                               |
|-----------------|---------------------------------------------------------|
| `bolt://`, `neo4j://`     | Plaintext TCP.                                |
| `bolt+s://`, `neo4j+s://`   | TLS verified against the Mozilla root bundle (`webpki-roots`). |
| `bolt+ssc://`, `neo4j+ssc://` | TLS, accepts any server cert — dev only.  |

## Build

This crate has a `path` dependency on `../mesh/crates/mesh-bolt`, so it expects
to live next to the `mesh` repo:

```
mesh-db/
├── mesh/          # the graph database
└── mesh-client/   # this repo
```

```
cargo build --release
```

## Run

```
cargo run --release -- --profile mesh-local
```

Profiles come from `~/.config/mesh-client/config.toml`. If that file doesn't
exist, three defaults are used: `mesh-local`, `neo4j-local`, `falkor-local`.

Write your own to override — see [Config](#config) below.

## Keyboard shortcuts

All terminal-safe (no Ctrl combos that iTerm / tmux steal).

| Key           | Action                                        |
|---------------|-----------------------------------------------|
| `F1` / `F2` / `F3` | REPL / Schema / Graph tabs              |
| `F5`          | Run query                                     |
| `F6`          | Toggle focus between editor and results       |
| `F8`          | Refresh schema                                |
| `F10`         | Quit                                          |
| `Alt+↑` / `Alt+↓` | Cycle query history (in REPL editor)      |
| `Esc`         | Drop from editor into results pane            |
| `Enter` / `i` | From results, jump back into editor           |
| `h` `j` `k` `l` | Pan the graph view                          |

## Config

`~/.config/mesh-client/config.toml`:

```toml
[[profiles]]
name = "mesh-local"
kind = "mesh"                           # "mesh" | "neo4j" | "falkor"
uri = "bolt://127.0.0.1:7687"
username = "neo4j"
password = "password"

[[profiles]]
name = "neo4j-local"
kind = "neo4j"
uri = "bolt://127.0.0.1:7687"
username = "neo4j"
password = "password"
database = "neo4j"

[[profiles]]
name = "falkor-local"
kind = "falkor"
uri = "redis://127.0.0.1:6379"
graph = "my-graph"
```

Pick a profile at startup with `--profile <name>`, or point at a different
config with `--config <path>`.

## How the three tabs work together

- **REPL (F1)** — write Cypher, run with F5. Results land in a table below.
  Each successful query is pushed onto history (Alt+↑/↓ to recall).
- **Schema (F2)** — three columns: labels, relationship types, property keys.
  Loaded on startup and refreshed with F8. Populated by running
  `MATCH (n) UNWIND labels(n)...` etc., so it works against any
  Cypher-speaking backend.
- **Graph (F3)** — takes the nodes and edges from the last REPL result and
  draws them on a ratatui canvas. Circular layout today (deterministic, pans
  with hjkl); a force-directed layout would be a nice upgrade.

## Layout

```
src/
├── main.rs              # entry, terminal setup, event loop
├── app.rs               # App state, tabs, history, focus
├── config.rs            # TOML connection profiles
├── backend/
│   ├── mod.rs           # GraphBackend trait, Value / Node / Edge types
│   ├── bolt.rs          # Bolt 4.4 + 5.x client on mesh-bolt
│   └── falkor.rs        # redis::cmd("GRAPH.QUERY") + verbose-mode parsing
└── ui/
    ├── mod.rs           # top-level layout, tab bar, status line
    ├── repl.rs          # query textarea + results table
    ├── schema.rs        # three-column list view
    └── graph.rs         # canvas-based graph viewer
```

## Known limitations

- **FalkorDB parser is verbose-mode only.** Compact mode would be faster and
  give correct label/property-key resolution via `db.labels()` caches, but
  verbose mode is simpler and good enough for reading small graphs.
- **Graph layout is circular, not force-directed.** Stable and fast, but gets
  busy past ~30 nodes.
