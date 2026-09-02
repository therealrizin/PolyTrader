# Build stage – Ubuntu 24.04 (glibc 2.39)
FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app
COPY . .
RUN cargo build --release

# Runtime stage – also Ubuntu 24.04
FROM ubuntu:24.04

# Install runtime dependencies: ca-certificates + netcat for healthcheck
RUN apt-get update && apt-get install -y \
    ca-certificates \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/polymarket-executor-rs /usr/local/bin/

EXPOSE 8090

CMD ["/usr/local/bin/polymarket-executor-rs"]