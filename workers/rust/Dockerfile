# Worker image (Rust). Built by the app through the Docker API the first time Rust workers are used.
# Unit tests have their own file: docker build -f workers/rust/Dockerfile.test workers/rust
# The Kafka client is librdkafka (built from source) like the Python, Node.js and Go ones, so settings are identical.
# The first build takes several minutes because librdkafka is compiled; later runs reuse the image.
FROM rust:1.99-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim
COPY --from=build /src/target/release/worker /worker
CMD ["/worker"]