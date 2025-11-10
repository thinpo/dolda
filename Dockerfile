# Multi-stage build for optimal image size
FROM rust:1.75-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src
COPY benches ./benches
COPY tests ./tests
COPY examples ./examples

# Build for release
RUN cargo build --release --bin dolda

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -m -u 1000 dolda && \
    mkdir -p /data /logs && \
    chown -R dolda:dolda /data /logs

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/dolda /usr/local/bin/dolda

# Set ownership
RUN chown dolda:dolda /usr/local/bin/dolda

# Switch to app user
USER dolda

# Expose ports
# 8080: Main service port
# 8081: Health check port
# 9090: Metrics port
# 7000: Raft consensus port
EXPOSE 8080 8081 9090 7000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8081/health || exit 1

# Volume for persistent data
VOLUME ["/data", "/logs"]

# Default command
CMD ["dolda"]

