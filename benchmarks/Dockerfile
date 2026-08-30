# Multi-stage build: build the uring-enabled broker and the benchmark client
# on Linux, then run both inside a slim Ubuntu runtime image.
#
# Build:   docker build -f benchmarks/Dockerfile -t zhensegg-bench .
# Run:     docker run --rm --privileged zhensegg-bench
#          (--privileged only helps if seccomp blocks io_uring on the host)

# ---- stage 1: build ----
FROM rust:1.97-bookworm AS build
WORKDIR /work

# copy manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benchmarks ./benchmarks

# build the broker with io_uring support (Linux target; the "uring" feature
# additionally pulls monoio as requested)
RUN cargo build --release --features uring --bin zhensegg-broker

# build the standalone benchmark client (also compiles zhensegg from path)
RUN cd benchmarks && cargo build --release

# ---- stage 2: runtime ----
FROM ubuntu:24.04
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 openssl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /work/target/release/zhensegg-broker /usr/local/bin/zhensegg-broker
COPY --from=build /work/benchmarks/target/release/zhensegg-bench /usr/local/bin/zhensegg-bench
COPY benchmarks/run-bench.sh /usr/local/bin/run-bench.sh

RUN mkdir -p /data && chmod +x /usr/local/bin/run-bench.sh

ENTRYPOINT ["/usr/local/bin/run-bench.sh"]
