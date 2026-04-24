# JavaScript driver matrix tests

Smoke + parity tests that drive Mesh's Bolt listener with the official
[neo4j-driver](https://github.com/neo4js/neo4j-javascript-driver) npm
package. Run via the matrix orchestrator — it handles spinning up the
server, waiting for the Bolt port, exporting connection env vars, and
tearing down.

Uses Node's built-in test runner (`node --test`) so the only npm
dependency is the driver itself. No jest / mocha / vitest.

## Local run

```sh
# One-time setup: install the pinned driver.
cd drivers/js && npm install && cd ../..

# Build the server release binary (the orchestrator runs it).
cargo build --release -p meshdb-server

# Run a single matrix cell.
drivers/run-matrix.sh --lang=js --auth=none --tls=off --bolt=5.4
```

Other cells:

```sh
drivers/run-matrix.sh --lang=js --auth=basic
drivers/run-matrix.sh --lang=js --tls=on
drivers/run-matrix.sh --lang=js --bolt=4.4
```

Server logs land in `drivers/.run/logs/<timestamp>-js-<auth>-<tls>-<bolt>.log`.
On failure the orchestrator echoes the path to stderr.
