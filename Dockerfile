# Multi-stage image for `meshdb-server` in single-node mode. Build with:
#
#     docker build -t meshdb-server .
#
# Run with:
#
#     docker run --rm \
#       -p 7001:7001 -p 7687:7687 \
#       -v meshdb-data:/var/lib/meshdb \
#       meshdb-server
#
# Override the bundled single-node config by mounting your own at
# /etc/meshdb/meshdb.toml — e.g., to enable `bolt_auth`, `bolt_tls`, or
# cluster mode with `peers`:
#
#     docker run --rm \
#       -p 7001:7001 -p 7687:7687 \
#       -v $(pwd)/my.toml:/etc/meshdb/meshdb.toml:ro \
#       -v meshdb-data:/var/lib/meshdb \
#       meshdb-server
#
# For common-case fields you can also pass MESHDB_* env vars, which
# override the bundled TOML without replacing it. Useful when you just
# want to flip one setting without baking a new config file:
#
#     docker run --rm \
#       -p 7001:7001 -p 7687:7687 \
#       -e MESHDB_METRICS_ADDRESS=0.0.0.0:9090 \
#       -v meshdb-data:/var/lib/meshdb \
#       meshdb-server
#
# The entrypoint script (docker/entrypoint.sh) starts as root, chowns
# /var/lib/meshdb to the meshdb user, then drops privileges via gosu
# before exec'ing the server. This lets a freshly-created named volume
# (which Docker always creates root-owned) work on first start without
# any pre-flight setup. If you pass --user to run as non-root, the
# entrypoint skips the chown and trusts the caller's permissions —
# pre-chown your bind-mounted directory to a UID the container can write.

# ---------- build ----------
# Pin to a specific Rust toolchain so the image is reproducible. 1.95+
# is required because a transitive dep (constant_time_eq) needs it.
FROM rust:1.95-bookworm AS builder

# rust-rocksdb invokes bindgen at build time, which needs libclang.
# protoc is not needed — meshdb-rpc's build.rs uses the vendored
# `protoc-bin-vendored` crate.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        clang \
        libclang-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy the whole workspace in one shot. A dep-only prewarm layer (via
# cargo-chef or a manifests-first copy) would cache better for tight
# iteration loops, but this Dockerfile targets release builds where
# simplicity wins.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build only the server binary. `--locked` pins to Cargo.lock so the
# image can't silently pull newer transitive deps on rebuild.
RUN cargo build --release --locked -p meshdb-server \
    && strip target/release/meshdb-server

# ---------- runtime ----------
FROM debian:bookworm-slim AS runtime

# ca-certificates is here for any future outbound TLS; libgcc + libstdc++
# are needed by rocksdb's C++ runtime. Everything else the static-ish
# release binary carries in itself.
# gosu is used by the entrypoint to drop from root → meshdb after
# fixing volume ownership. It's a single static binary (~1.8MB) and
# is the recommended replacement for `su` / `sudo` in containers.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        gosu \
        libgcc-s1 \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system meshdb \
    && useradd --system --gid meshdb --home-dir /var/lib/meshdb --shell /usr/sbin/nologin meshdb \
    && mkdir -p /var/lib/meshdb /etc/meshdb \
    && chown -R meshdb:meshdb /var/lib/meshdb /etc/meshdb

COPY --from=builder /build/target/release/meshdb-server /usr/local/bin/meshdb-server
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh

# Bundled single-node config. Bind-mount over this path to override.
COPY docker/meshdb.toml /etc/meshdb/meshdb.toml

# Container starts as root so the entrypoint can chown the volume
# (named volumes are always created root:root regardless of image
# USER). The entrypoint immediately drops to meshdb via gosu.
VOLUME ["/var/lib/meshdb"]

# 7001: gRPC.  7687: Bolt (standard Neo4j driver port).
EXPOSE 7001 7687

ENV RUST_LOG=info
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["--config", "/etc/meshdb/meshdb.toml"]
