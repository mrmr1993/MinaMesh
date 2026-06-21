# Lean MinaMesh image — just the Rust binary, for the trustless light-node + indexer
# backend (no bundled mina daemon / mina-archive / OCaml signer). Historical reads come
# from the mina-indexer (MINAMESH_INDEXER_URL); live state from the mina-light-node
# (MINAMESH_LIGHT_NODE_URL); network metadata from a Mina GraphQL proxy (MINAMESH_PROXY_URL).
FROM rust:1-bookworm AS builder
ENV SQLX_OFFLINE=true
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY .sqlx .sqlx
COPY sql sql
COPY src src
COPY static static
COPY build.rs Cargo.lock Cargo.toml ./
RUN cargo build --release --bin mina-mesh

FROM debian:bookworm-slim
# reqwest links OpenSSL dynamically (native-tls); ca-certificates for HTTPS to the proxy.
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/mina-mesh /usr/local/bin/mina-mesh
ENV RUST_LOG=info
EXPOSE 3000
# Config is taken from the environment (MINAMESH_*). Serve on all interfaces.
ENTRYPOINT ["mina-mesh", "serve", "0.0.0.0", "3000"]
