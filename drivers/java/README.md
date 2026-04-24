# Java driver matrix tests

Smoke + parity tests that drive Mesh's Bolt listener with the
official [neo4j-java-driver](https://github.com/neo4j/neo4j-java-driver).
Run via the matrix orchestrator — it handles spinning up the server,
waiting for the Bolt port, exporting connection env vars, and tearing
down.

Minimal JUnit 5 harness. No reactive API (that drags in RxJava and
a separate codec path that's not relevant to wire-compat testing).

## Local run

```sh
# One-time setup: Maven + JDK 17+.
(cd drivers/java && mvn -q dependency:resolve)

# Build the server release binary (the orchestrator runs it).
cargo build --release -p meshdb-server

# Run a single matrix cell.
drivers/run-matrix.sh --lang=java --auth=none --tls=off --bolt=5.4
```

Other cells:

```sh
drivers/run-matrix.sh --lang=java --auth=basic
drivers/run-matrix.sh --lang=java --tls=on
drivers/run-matrix.sh --lang=java --bolt=4.4
```

Server logs land in `drivers/.run/logs/<timestamp>-java-<auth>-<tls>-<bolt>.log`.
