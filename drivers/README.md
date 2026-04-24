# Driver matrix tests

End-to-end tests that drive Mesh's Bolt listener with the official
Neo4j drivers. Catches wire-level regressions that the in-tree Bolt
tests (which use Mesh's own framing crate) can't see — packstream tag
drift, struct-shape changes, version-conditional encoding bugs,
authentication / TLS plumbing.

## Layout

```
drivers/
  common/
    gen-cert.sh          Writes cert.pem / key.pem for the TLS cell.
  python/                neo4j-python-driver tests.
  js/                    neo4j-driver (npm) tests.
  go/                    neo4j-go-driver/v5 tests.
  java/                  neo4j-java-driver (Maven) tests.
  run-matrix.sh          Orchestrator. One invocation = one matrix cell.
```

The orchestrator generates `drivers/.run/config.toml` on each run
from the axis flags — no hand-written per-cell configs to
maintain. The canonical, developer-facing Mesh config examples
are `mesh.toml` and `cluster.toml` at the repo root.

## Run a single cell

```sh
cargo build --release -p meshdb-server
PATH="$(pwd)/drivers/python/.venv/bin:$PATH" \
  drivers/run-matrix.sh --lang=py --bolt=5.4
```

Flags are orthogonal — any combination works (e.g. the "real
deployment" cell):

```sh
drivers/run-matrix.sh --lang=java --auth=basic --tls=on --bolt=5.4
```

See the per-language READMEs for venv / npm / go mod / Maven
bootstrap.

## CI

- **Per PR** (`.github/workflows/ci.yml`): each of the four
  `driver-matrix-*` jobs runs two cells — the `bolt=5.4` plaintext
  baseline and the `auth=basic, tls=on` production shape. 8
  cells total, parallelized across four jobs.
- **Nightly full axis** (`.github/workflows/driver-matrix-full.yml`):
  Monday 06:00 UTC. Walks the full Bolt version × auth × TLS grid
  per language — 12 cells × 4 drivers = 48 cells, parallelized
  via matrix strategy. Failures open (or refresh) a GitHub issue;
  the next green run closes it. Also manually dispatchable.

## Known gaps

_(None right now. Please open an issue when one surfaces — the
harness is green across all four language drivers and every
matrix cell on both this box and CI.)_
