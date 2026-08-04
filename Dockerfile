# Build arguments (override at build time)
# - RUST_VERSION: Rust toolchain version
# - RUNTIME_BASE: TDengine server image (must match deployed TDengine server version)
ARG RUST_VERSION=1.96
ARG RUNTIME_BASE=docker.io/tdengine/tsdb:3.4.1.6

# ----- Stage 1: build the Rust binary -----
FROM rust:${RUST_VERSION} AS builder

WORKDIR /usr/src/data-service
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release
RUN rm -f target/release/deps/data_service*
COPY src ./src
RUN cargo build --release

# ----- Stage 2: runtime image -----
FROM ${RUNTIME_BASE}

WORKDIR /app
COPY --from=builder /usr/src/data-service/target/release/data-service ./
RUN chown appuser:appuser data-service 2>/dev/null || \
    (groupadd -g 10001 appuser && useradd -u 10000 -g appuser appuser && chown appuser:appuser data-service)
# Give appuser write access to the TDengine log dir
RUN chown -R appuser:appuser /var/log/taos 2>/dev/null || true
USER appuser

EXPOSE 50001

CMD ["./data-service"]