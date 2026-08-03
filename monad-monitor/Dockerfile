# syntax=docker/dockerfile:1
#
# Builds only the `monad-monitor` binary (a pure-Rust crate — no C++/CMake
# execution-engine toolchain required, unlike monad-node/monad-rpc/etc).
#
# Build from the repository root so the whole cargo workspace is in context:
#   docker build -f monad-monitor/Dockerfile -t monad-monitor .
# (the Makefile in this directory wraps that for you: `make build`)

########################################
# Builder
########################################
FROM rust:1.91.1-bookworm AS builder

WORKDIR /usr/src/monad-bft

# secp256k1 / blst (pulled in via monad-node-config's "crypto" feature) have
# native build steps that need a C toolchain.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --release --bin monad-monitor -p monad-monitor

########################################
# Runner
########################################
FROM debian:bookworm-slim AS runner

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV RUST_LOG=info

COPY --from=builder /usr/src/monad-bft/target/release/monad-monitor /usr/local/bin/monad-monitor

ENTRYPOINT ["/usr/local/bin/monad-monitor"]
