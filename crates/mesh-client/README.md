# mesh-client

A terminal UI for Bolt-compatible graph databases. One binary, three tabs —
Cypher REPL, schema browser, and an ASCII graph view of the nodes and edges
in the last result.

Part of the [mesh](../../) workspace. Speaks Bolt 5.4 / 5.0 / 4.4, so it
works against Mesh, Neo4j, or anything else that implements the Bolt
protocol.

## Backends

| Backend | Protocol                 | Notes                                                            |
|---------|--------------------------|------------------------------------------------------------------|
| Mesh    | Bolt 5.4 / 5.0 / 4.4     | Uses the in-workspace `mesh-bolt` crate directly.                |
| Neo4j   | Bolt 5.4 / 5.0 / 4.4     | Same wire code path as Mesh. HELLO→LOGON split handled for 5.1+. |

### URI schemes

| Scheme                          | Transport                                                       |
|---------------------------------|-----------------------------------------------------------------|
| `bolt://`, `neo4j://`           | Plaintext TCP.                                                  |
| `bolt+s://`, `neo4j+s://`       | TLS verified against the Mozilla root bundle (`webpki-roots`).  |
| `bolt+ssc://`, `neo4j+ssc://`   | TLS, accepts any server cert — **dev / self-signed only**.      |

The `neo4j://` schemes are treated identically to `bolt://` — server-side
routing isn't implemented, so connections go straight to the host/port in
the URI. Default port is `7687` if omitted.

## Build

`mesh-client` is a member of the top-level `mesh` workspace, so the usual
workspace commands work from the repo root:

```sh
cargo build -p mesh-client --release
```

The binary lands at `target/release/mesh-client`.

## Run

```sh
cargo run -p mesh-client --release -- --profile mesh-local
```

On first launch with no `~/.config/mesh-client/config.toml`, two sensible
defaults are seeded in-memory: `mesh-local` and `neo4j-local`. Write your
own config (see below) to override.

Flags:

| Flag                 | Purpose                                                           |
|----------------------|-------------------------------------------------------------------|
| `-p, --profile NAME` | Which profile to connect to. Defaults to the first in the config. |
| `-c, --config PATH`  | Alternate config path. Defaults to `~/.config/mesh-client/config.toml`. |

## Keyboard shortcuts

All terminal-safe — no Ctrl combos that iTerm / tmux / terminal multiplexers
typically steal.

| Key                    | Action                                                |
|------------------------|-------------------------------------------------------|
| `F1` / `F2` / `F3`     | REPL / Schema / Graph tab                             |
| `F5`                   | Run query (REPL tab)                                  |
| `F6`                   | Toggle focus between editor and results (REPL tab)    |
| `F8`                   | Refresh schema                                        |
| `F10`                  | Quit                                                  |
| `Alt+↑` / `Alt+↓`      | Cycle query history (REPL editor only)                |
| `Esc`                  | From editor, drop into results pane                   |
| `Enter` / `i`          | From results, jump back into editor                   |
| `↑` `↓` `j` `k`        | Scroll results / schema                               |
| `PgUp` / `PgDn`        | Page-scroll results                                   |
| `Home`                 | Results/schema: top · Graph: recenter (0, 0)          |
| `h` `j` `k` `l`        | Pan the graph view                                    |

## Config

`~/.config/mesh-client/config.toml`:

```toml
[[profiles]]
name = "mesh-local"
kind = "mesh"                           # "mesh" | "neo4j"
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
name = "neo4j-tls"
kind = "neo4j"
uri = "bolt+s://graph.example.com:7687"
username = "neo4j"
password = "s3cret"
```

`username` / `password` are optional — leaving both off sends `scheme: none`
on the Bolt HELLO/LOGON. `database` is forwarded on Neo4j connections.

Pick a profile at startup with `--profile <name>`, or point at a different
config with `--config <path>`.

## How the three tabs work together

- **REPL (F1)** — write Cypher in the editor, run with F5. Results drop into
  a paged table below. Each successful query is pushed onto the session's
  history (Alt+↑/↓ to recall). The status line shows row count, `t_last`
  from Bolt, and query type when the server returns them.
- **Schema (F2)** — three columns: labels, relationship types, property keys.
  Loaded on startup, refreshed on demand with F8. Populated by running
  `MATCH (n) UNWIND labels(n) AS l RETURN DISTINCT l` etc., so it works
  against any Cypher-speaking backend without server-side introspection
  APIs.
- **Graph (F3)** — renders the nodes and edges of the last REPL result on
  a ratatui canvas. Circular layout — deterministic so panning stays
  stable; gets busy past ~30 nodes. Edges are drawn straight, labels
  next to node circles.

## Layout

```
src/
├── main.rs              # entry, terminal setup, event loop, key routing
├── app.rs               # App state: tabs, focus, history, scroll offsets
├── config.rs            # Serde-backed profile config, default-path lookup
├── backend/
│   ├── mod.rs           # GraphBackend trait, Value/Node/Edge/QueryResult types
│   └── bolt.rs          # Bolt 4.4/5.x client on mesh-bolt, with TLS
└── ui/
    ├── mod.rs           # top-level layout, tab bar, status line
    ├── repl.rs          # query textarea + results table
    ├── schema.rs        # three-column list view
    └── graph.rs         # canvas-based graph viewer with circular layout
```

## Dependencies worth knowing about

- **`mesh-bolt`** (intra-workspace) — the Bolt 5 client used for every
  backend. Picks the newest version each side supports via
  `perform_client_handshake`.
- **`tokio-rustls`** + **`webpki-roots`** — TLS for `+s://` schemes. The
  `+ssc://` variants swap in a no-op cert verifier for dev against
  self-signed servers.
- **`ratatui`** + **`crossterm`** + **`tui-textarea`** — the TUI stack.

## Known limitations

- **Graph layout is circular, not force-directed.** Stable and fast, but
  gets visually busy past ~30 nodes. Good upgrade path for later.
- **No server-side routing.** `neo4j://` URIs are treated as direct
  connections — the client talks straight to the host/port in the URI
  without consulting `ROUTE`. Fine for single-server setups.
