# Use nightly Rust (supports edition2024)
FROM rustlang/rust:nightly as builder
WORKDIR /app

# Copy source code
COPY . .

# Build in release mode
RUN cargo build --release

# Final runtime image
FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/polymarket-executor-rs /app/polymarket-executor-rs
EXPOSE 8090
CMD ["/app/polymarket-executor-rs"]