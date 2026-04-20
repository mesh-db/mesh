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
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libgcc-s1 \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system meshdb \
    && useradd --system --gid meshdb --home-dir /var/lib/meshdb --shell /usr/sbin/nologin meshdb \
    && mkdir -p /var/lib/meshdb /etc/meshdb \
    && chown -R meshdb:meshdb /var/lib/meshdb /etc/meshdb

COPY --from=builder /build/target/release/meshdb-server /usr/local/bin/meshdb-server

# Bundled single-node config. Bind-mount over this path to override.
COPY docker/meshdb.toml /etc/meshdb/meshdb.toml

USER meshdb
VOLUME ["/var/lib/meshdb"]

# 7001: gRPC.  7687: Bolt (standard Neo4j driver port).
EXPOSE 7001 7687

ENV RUST_LOG=info
ENTRYPOINT ["/usr/local/bin/meshdb-server"]
CMD ["--config", "/etc/meshdb/meshdb.toml"]
