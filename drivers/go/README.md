# Go driver matrix tests

Smoke + parity tests that drive Mesh's Bolt listener with the
official [neo4j-go-driver/v5](https://github.com/neo4j/neo4j-go-driver)
package. Run via the matrix orchestrator — it handles spinning up
the server, waiting for the Bolt port, exporting connection env
vars, and tearing down.

Uses the standard `go test` runner — no extra test framework.

## Local run

```sh
# One-time setup: fetch deps.
(cd drivers/go && go mod download)

# Build the server release binary (the orchestrator runs it).
cargo build --release -p meshdb-server

# Run a single matrix cell.
drivers/run-matrix.sh --lang=go --auth=none --tls=off --bolt=5.4
```

Other cells:

```sh
drivers/run-matrix.sh --lang=go --auth=basic
drivers/run-matrix.sh --lang=go --tls=on
drivers/run-matrix.sh --lang=go --bolt=4.4
```

Server logs land in `drivers/.run/logs/<timestamp>-go-<auth>-<tls>-<bolt>.log`.
