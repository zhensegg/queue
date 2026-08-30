# Multi-stage: build the broker and the protocol fuzzer/soak harness on Linux,
# then run both inside a slim Ubuntu runtime. The fuzzer runs in-process against
# the real parser; the soak drives the live broker over TLS, and the entrypoint
# also exercises live SIGHUP cert/token rotation under load.
#
#   docker build -f fuzz/Dockerfile -t zhensegg-fuzz .
#   docker run --rm zhensegg-fuzz

# ---- stage 1: build ----
FROM rust:1.85-bookworm AS build
WORKDIR /work

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY fuzz ./fuzz

# Build the broker (Linux, with io_uring feature as the project targets).
RUN cargo build --release --features uring --bin zhensegg-broker
# Build the fuzzer harness (compiles zhensegg from path as a dependency).
RUN cd fuzz && cargo build --release

# ---- stage 2: runtime ----
FROM ubuntu:24.04
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 openssl curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /work/target/release/zhensegg-broker /usr/local/bin/zhensegg-broker
COPY --from=build /work/fuzz/target/release/zhensegg-fuzz /usr/local/bin/zhensegg-fuzz
COPY fuzz/run-fuzz.sh /usr/local/bin/run-fuzz.sh

RUN chmod +x /usr/local/bin/run-fuzz.sh && mkdir -p /data /fuzz

ENTRYPOINT ["/usr/local/bin/run-fuzz.sh"]
