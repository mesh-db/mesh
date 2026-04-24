# Python driver matrix tests

Smoke + parity tests that drive Mesh's Bolt listener with the official
[Neo4j Python driver](https://github.com/neo4j/neo4j-python-driver).
Run via the matrix orchestrator — it handles spinning up the server,
waiting for the Bolt port, exporting connection env vars, and tearing
down.

## Local run

```sh
# One-time setup: install the pinned driver into a venv.
python3 -m venv drivers/python/.venv
drivers/python/.venv/bin/pip install -r drivers/python/requirements.txt

# Build the server release binary (the orchestrator runs it).
cargo build --release -p meshdb-server

# Run a single matrix cell. PATH first so pytest resolves to the venv's bin.
PATH="$(pwd)/drivers/python/.venv/bin:$PATH" \
  drivers/run-matrix.sh --lang=py --auth=none --tls=off --bolt=5.4
```

Other cells:

```sh
# basic auth
PATH="$(pwd)/drivers/python/.venv/bin:$PATH" \
  drivers/run-matrix.sh --lang=py --auth=basic

# TLS (self-signed)
PATH="$(pwd)/drivers/python/.venv/bin:$PATH" \
  drivers/run-matrix.sh --lang=py --tls=on

# Bolt 4.4 negotiation
PATH="$(pwd)/drivers/python/.venv/bin:$PATH" \
  drivers/run-matrix.sh --lang=py --bolt=4.4
```

Server logs land in `drivers/.run/logs/<timestamp>-py-<auth>-<tls>-<bolt>.log`.
On failure the orchestrator echoes the path to stderr.
