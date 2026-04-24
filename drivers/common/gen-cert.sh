#!/usr/bin/env bash
# Generate a self-signed cert + key for the Bolt TLS test cell. Output
# goes to drivers/.run/certs/{cert,key}.pem; the path matches what
# server-tls.toml expects.
#
# Idempotent — re-running just overwrites the existing pair. The cert
# is valid for 365 days, covers DNS=localhost + IP=127.0.0.1, and uses
# a P-256 ECDSA key (small + universally accepted by neo4j drivers'
# rustls/openssl/jsse stacks).

set -euo pipefail

OUT_DIR="${1:?usage: gen-cert.sh <out_dir>}"
mkdir -p "$OUT_DIR"

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -keyout "$OUT_DIR/key.pem" \
  -out "$OUT_DIR/cert.pem" \
  -days 365 -nodes \
  -subj '/CN=localhost' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  >/dev/null 2>&1

echo "wrote $OUT_DIR/cert.pem + $OUT_DIR/key.pem"
