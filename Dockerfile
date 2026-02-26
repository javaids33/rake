# ============================================================================
# RustLake Dockerfile — Multi-stage build
# Produces a minimal Debian image with rustlake + rustlake-api binaries
# ============================================================================

# ---------------------------------------------------------------------------
# Stage 1: Builder
# ---------------------------------------------------------------------------
FROM rust:1.83-bookworm AS builder

# Install build dependencies required by native crates (protobuf for tonic/prost,
# cmake for potential rdkafka builds, pkg-config for system libs)
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    cmake \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/rustlake

# Copy workspace manifests first to leverage Docker layer caching.
# If only source code changes (not Cargo.toml), the dependency layer is cached.
COPY Cargo.toml Cargo.lock ./

# Copy all crate manifests
COPY crates/rustlake-core/Cargo.toml crates/rustlake-core/Cargo.toml
COPY crates/rustlake-storage/Cargo.toml crates/rustlake-storage/Cargo.toml
COPY crates/rustlake-catalog/Cargo.toml crates/rustlake-catalog/Cargo.toml
COPY crates/rustlake-format/Cargo.toml crates/rustlake-format/Cargo.toml
COPY crates/rustlake-engine/Cargo.toml crates/rustlake-engine/Cargo.toml
COPY crates/rustlake-stream/Cargo.toml crates/rustlake-stream/Cargo.toml
COPY crates/rustlake-vector/Cargo.toml crates/rustlake-vector/Cargo.toml
COPY crates/rustlake-router/Cargo.toml crates/rustlake-router/Cargo.toml
COPY crates/rustlake-scheduler/Cargo.toml crates/rustlake-scheduler/Cargo.toml
COPY crates/rustlake-flight/Cargo.toml crates/rustlake-flight/Cargo.toml
COPY crates/rustlake-transform/Cargo.toml crates/rustlake-transform/Cargo.toml
COPY crates/rustlake-api/Cargo.toml crates/rustlake-api/Cargo.toml
COPY crates/rustlake-cli/Cargo.toml crates/rustlake-cli/Cargo.toml

# Create stub lib.rs / main.rs for each crate so cargo can resolve the
# workspace and download + compile dependencies in a cached layer.
RUN mkdir -p crates/rustlake-core/src && echo "pub fn _stub() {}" > crates/rustlake-core/src/lib.rs \
    && mkdir -p crates/rustlake-storage/src && echo "pub fn _stub() {}" > crates/rustlake-storage/src/lib.rs \
    && mkdir -p crates/rustlake-catalog/src && echo "pub fn _stub() {}" > crates/rustlake-catalog/src/lib.rs \
    && mkdir -p crates/rustlake-format/src && echo "pub fn _stub() {}" > crates/rustlake-format/src/lib.rs \
    && mkdir -p crates/rustlake-engine/src && echo "pub fn _stub() {}" > crates/rustlake-engine/src/lib.rs \
    && mkdir -p crates/rustlake-stream/src && echo "pub fn _stub() {}" > crates/rustlake-stream/src/lib.rs \
    && mkdir -p crates/rustlake-vector/src && echo "pub fn _stub() {}" > crates/rustlake-vector/src/lib.rs \
    && mkdir -p crates/rustlake-router/src && echo "pub fn _stub() {}" > crates/rustlake-router/src/lib.rs \
    && mkdir -p crates/rustlake-scheduler/src && echo "pub fn _stub() {}" > crates/rustlake-scheduler/src/lib.rs \
    && mkdir -p crates/rustlake-flight/src && echo "pub fn _stub() {}" > crates/rustlake-flight/src/lib.rs \
    && mkdir -p crates/rustlake-transform/src && echo "pub fn _stub() {}" > crates/rustlake-transform/src/lib.rs \
    && mkdir -p crates/rustlake-api/src && echo "fn main() {}" > crates/rustlake-api/src/main.rs \
    && mkdir -p crates/rustlake-cli/src && echo "fn main() {}" > crates/rustlake-cli/src/main.rs

# Build dependencies only (this layer is cached until Cargo.toml files change)
RUN cargo build --release 2>/dev/null || true

# Now copy the real source code
COPY crates/ crates/

# Touch all source files so cargo knows they are newer than the stubs
RUN find crates -name "*.rs" -exec touch {} +

# Build both binaries with release optimizations
RUN cargo build --release --bin rustlake --bin rustlake-api

# ---------------------------------------------------------------------------
# Stage 2: Runtime
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user for running the application
RUN groupadd --gid 1000 rustlake \
    && useradd --uid 1000 --gid rustlake --create-home rustlake

WORKDIR /opt/rustlake

# Copy binaries from builder
COPY --from=builder /usr/src/rustlake/target/release/rustlake /usr/local/bin/rustlake
COPY --from=builder /usr/src/rustlake/target/release/rustlake-api /usr/local/bin/rustlake-api

# Copy sample data and dashboard
COPY sample-data/ /opt/rustlake/sample-data/
COPY dashboard.html /opt/rustlake/dashboard.html

# Ensure the rustlake user owns the working directory
RUN chown -R rustlake:rustlake /opt/rustlake

USER rustlake

# API server default port
EXPOSE 3000

# Health check — the API server exposes /health
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:3000/health || exit 1

# Labels following OCI image spec
LABEL org.opencontainers.image.title="RustLake" \
      org.opencontainers.image.description="All-Rust composable data platform — batch analytics, streaming, AI/vector workloads over open table formats" \
      org.opencontainers.image.vendor="RustLake" \
      org.opencontainers.image.source="https://github.com/rustlake/rustlake" \
      org.opencontainers.image.licenses="Apache-2.0"

# Default: run the API server
CMD ["rustlake-api"]
