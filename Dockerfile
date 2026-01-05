# Build stage
FROM rustlang/rust:nightly-bookworm AS builder

WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build release binary
RUN cargo build --release --bin megaberg

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/megaberg /usr/local/bin/megaberg

# Config will be mounted at /config/config.json
VOLUME ["/config"]

# Keep container running for exec
CMD ["tail", "-f", "/dev/null"]
