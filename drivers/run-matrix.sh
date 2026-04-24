#!/usr/bin/env bash
# Driver-matrix orchestrator. One invocation runs a single matrix
# cell: pick a server config from drivers/common/, launch the server,
# wait for the Bolt port, run the per-language test suite against it,
# tear down on exit.
#
# Usage:
#   drivers/run-matrix.sh \
#     --lang=<py>              # which driver suite to run
#     [--auth=<none|basic>]    # default: none
#     [--tls=<off|on>]         # default: off
#     [--mode=<single>]        # default: single (routing not supported in Phase 1)
#     [--bolt=<auto|4.4|5.0|5.4>]  # default: auto (full SUPPORTED set)
#
# Env overrides:
#   MESH_BIN     — path to the meshdb-server binary (default: target/release/meshdb-server)
#
# Exit code is the test runner's exit code. Server logs go to
# drivers/.run/logs/<timestamp>.log; on test failure the path is
# echoed to stderr.

set -euo pipefail

# Resolve the repo root from the script location so the script works
# regardless of the caller's cwd.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

LANG=""
AUTH="none"
TLS="off"
MODE="single"
BOLT="auto"

for arg in "$@"; do
  case "$arg" in
    --lang=*) LANG="${arg#*=}" ;;
    --auth=*) AUTH="${arg#*=}" ;;
    --tls=*)  TLS="${arg#*=}" ;;
    --mode=*) MODE="${arg#*=}" ;;
    --bolt=*) BOLT="${arg#*=}" ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

if [[ -z "$LANG" ]]; then
  echo "--lang is required (one of: py)" >&2
  exit 2
fi
case "$LANG" in py) ;; *) echo "unsupported --lang=$LANG (Phase 1 ships py only)" >&2; exit 2 ;; esac
case "$AUTH" in none|basic) ;; *) echo "--auth must be none|basic" >&2; exit 2 ;; esac
case "$TLS"  in off|on)     ;; *) echo "--tls must be off|on" >&2; exit 2 ;; esac
case "$MODE" in single)     ;; *) echo "Phase 1 only supports --mode=single" >&2; exit 2 ;; esac
case "$BOLT" in auto|4.4|5.0|5.4) ;; *) echo "--bolt must be auto|4.4|5.0|5.4" >&2; exit 2 ;; esac

# Pick the server config for this cell. Mutually-exclusive axes
# (auth, tls, bolt-version clamp) each map to a dedicated config in
# drivers/common/. A future commit will combine axes by templating
# server configs at runtime; for Phase 1 we ship the exact files we
# need.
if   [[ "$AUTH" == "basic" ]]; then CONFIG="drivers/common/server-auth.toml"
elif [[ "$TLS"  == "on"    ]]; then CONFIG="drivers/common/server-tls.toml"
elif [[ "$BOLT" == "4.4"   ]]; then CONFIG="drivers/common/server-bolt44.toml"
elif [[ "$BOLT" == "5.0"   ]]; then CONFIG="drivers/common/server-bolt50.toml"
elif [[ "$BOLT" == "5.4"   ]]; then CONFIG="drivers/common/server-bolt54.toml"
else                                CONFIG="drivers/common/server.toml"
fi

MESH_BIN="${MESH_BIN:-target/release/meshdb-server}"
if [[ ! -x "$MESH_BIN" ]]; then
  echo "$MESH_BIN not found — run 'cargo build --release -p meshdb-server' first" >&2
  exit 2
fi

RUN_DIR="drivers/.run"
LOG_DIR="$RUN_DIR/logs"
CERTS_DIR="$RUN_DIR/certs"
mkdir -p "$LOG_DIR" "$RUN_DIR/data-single" "$RUN_DIR/data-auth" "$RUN_DIR/data-tls" \
  "$RUN_DIR/data-bolt44" "$RUN_DIR/data-bolt50" "$RUN_DIR/data-bolt54"

# Wipe whatever data dir the chosen config points at — every cell
# starts on a clean store so tests can assume an empty graph.
DATA_DIR="$(grep -E '^data_dir' "$CONFIG" | sed -E 's/.*= *"([^"]*)".*/\1/')"
rm -rf "$DATA_DIR"

# Generate certs on demand if TLS is on. The script is idempotent so
# this is cheap to re-run.
if [[ "$TLS" == "on" ]]; then
  drivers/common/gen-cert.sh "$CERTS_DIR" >/dev/null
fi

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="$LOG_DIR/$TIMESTAMP-${LANG}-${AUTH}-${TLS}-${BOLT}.log"

# Launch the server. We background it and keep its pid so the trap
# can tear it down cleanly. RUST_LOG=info gives us enough detail to
# diagnose driver-side failures from the server's perspective.
echo "[run-matrix] launching $MESH_BIN --config $CONFIG" >&2
RUST_LOG="${RUST_LOG:-meshdb_server=info,meshdb_bolt=info,meshdb_rpc=warn}" \
  "$MESH_BIN" --config "$CONFIG" >"$LOG_FILE" 2>&1 &
SERVER_PID=$!

# shellcheck disable=SC2317
cleanup() {
  if kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    # Give the server a beat to flush its rocksdb file lock so a
    # subsequent run on the same data dir doesn't fail to open.
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

# Wait for the Bolt port to accept connections. 10s ceiling — well
# beyond what cold-start takes (~200ms on this hardware) but short
# enough that a stuck server doesn't block CI for minutes.
BOLT_HOST="127.0.0.1"
BOLT_PORT="7687"
for _ in $(seq 1 100); do
  if (echo > "/dev/tcp/${BOLT_HOST}/${BOLT_PORT}") 2>/dev/null; then
    break
  fi
  sleep 0.1
done
if ! (echo > "/dev/tcp/${BOLT_HOST}/${BOLT_PORT}") 2>/dev/null; then
  echo "[run-matrix] server never opened Bolt port ${BOLT_HOST}:${BOLT_PORT}" >&2
  echo "[run-matrix] server log: $LOG_FILE" >&2
  exit 1
fi

# Hand off to the per-language runner. Each runner reads connection
# info from env vars so the harness stays language-agnostic.
export MESH_BOLT_HOST="$BOLT_HOST"
export MESH_BOLT_PORT="$BOLT_PORT"
export MESH_BOLT_AUTH="$AUTH"
export MESH_BOLT_TLS="$TLS"
export MESH_BOLT_VERSION="$BOLT"

case "$LANG" in
  py)
    echo "[run-matrix] running drivers/python suite" >&2
    if ! (cd drivers/python && python -m pytest -q); then
      echo "[run-matrix] python suite failed" >&2
      echo "[run-matrix] server log: $LOG_FILE" >&2
      exit 1
    fi
    ;;
esac

echo "[run-matrix] cell passed: lang=$LANG auth=$AUTH tls=$TLS mode=$MODE bolt=$BOLT" >&2
