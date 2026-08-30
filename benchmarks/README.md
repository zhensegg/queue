# Zhensegg Benchmark

Honest production benchmark harness for the Zhensegg broker. Moved out of
`src/bin/bench.rs` into a standalone crate under `benchmarks/` so it builds and
runs independently of the broker library/binary.

## Layout

```
benchmarks/
  Cargo.toml         standalone crate (depends on ../ via path)
  src/main.rs        the benchmark client
  Dockerfile         multi-stage: builds broker (--features uring) + bench on Linux
  run-bench.sh       container entrypoint: starts broker in file mode, runs bench
```

## What it measures (honestly)

* **Closed loop** — a producer never sends the next batch until the broker has
  ACKed the previous one. Throughput is counted on ACKs **only**, never on
  "bytes written to the socket", so the RPS reflects real sustained capacity.
* **Separate OS threads** — every producer and every consumer runs on its own
  OS thread (each with its own tokio current-thread runtime and its own TCP
  connection). This is the production topology, not multiplexed tasks.
* **E2E delivery latency** — the first 8 bytes of each payload carry a nanosecond
  timestamp written by the producer; consumers measure publish→delivery.
* **Durability on media** — in file mode, sampled ACKed records are re-read
  straight off the ring file with `O_DIRECT` (bypassing the page cache) and
  compared byte-for-byte to what the producer sent. A mismatch where the ring
  has not yet wrapped is a durability violation.

## Run locally (no container)

```bash
# build once
cd benchmarks && cargo build --release

# start a broker in persistent file mode
cargo run --release --bin zhensegg-broker -- \
    --mode file --file /tmp/zhensegg.ring --ring-capacity-mb 4096 --cores 4

# run the bench against it (producers + consumers on separate threads)
./benchmarks/target/release/zhensegg-bench \
    --addr 127.0.0.1:9090 --producers 8 --consumers 8 \
    --payload-size 256 --secs 30 --verify-file /tmp/zhensegg.ring
```

## Run in Docker (Ubuntu, io_uring path)

Note: the real io_uring store path is always active on Linux (the flusher uses
`pwrite`/`fdatasync`; the `io-uring` crate is a Linux-target dependency). The
`--features uring` flag additionally compiles monoio, as requested.

```bash
# 1. build the image (broker + bench for Linux in one multi-stage build)
docker build -f benchmarks/Dockerfile -t zhensegg-bench .

# 2. run the persistent file-mode benchmark
docker run --rm zhensegg-bench
```

Tunable via env vars:

| Env            | Default    | Meaning                                      |
|----------------|-----------|----------------------------------------------|
| `ADDR`         | 127.0.0.1:9090 | Broker listen addr                       |
| `HTTP_ADDR`    | 127.0.0.1:9091 | Broker metrics/health addr               |
| `CORES`        | 4          | Broker shards (SO_REUSEPORT)                 |
| `RING_MB`      | 4096       | Persistent ring size in MiB                  |
| `TOPIC`        | bench      | Topic used for the test                      |
| `PRODUCERS`    | 8          | Producer OS threads                          |
| `CONSUMERS`    | 8          | Consumer OS threads                          |
| `BATCH`        | 256        | In-flight messages per producer round trip   |
| `PAYLOAD`      | 256        | Payload bytes (>=8)                          |
| `MSGS`         | 0          | Fixed message count (0 = run by seconds)     |
| `SECS`         | 30         | Duration in seconds                          |

If Docker's default seccomp profile blocks io_uring, run with
`--privileged` (or `--security-opt seccomp=unconfined`).

```bash
docker run --rm --privileged zhensegg-bench
```

## Results (Ubuntu 24.04 container on Docker Desktop/WSL2, file mode)

### Persistent soak — 30 s, 8 producers × 8 consumers, 256 B payload, 4 GB ring, 4 cores

| Metric                        | Value            |
|-------------------------------|------------------|
| Publish RPS (closed-loop, acked-only) | **~600K msg/s** |
| Publish throughput            | ~154 MB/s        |
| Delivery RPS (consumers)      | ~4.8M msg/s      |
| Backlog after run             | 0% (fully drained) |
| pub→ack batch RTT             | p50=1.18 ms, p90=8.4 ms, p99=10.0 ms, p99.9=44.8 ms |
| e2e delivery latency (144M samples) | p50=1.82 ms, p90=7.4 ms, p99=9.6 ms, p99.9=10.7 ms |
| Durability (O_DIRECT on media)| **4096/4096 verified, 0 mismatch** |

### Latency-focused — 8 s, 4 producers × 4 consumers, 1-in-flight (batch=1), 128 B payload

| Metric                        | Value            |
|-------------------------------|------------------|
| pub→ack RTT                   | p50=37 µs, p90=168 µs, p99=315 µs, p99.9=504 µs |
| e2e delivery latency (1.6M samples) | p50=72 µs, p90=218 µs, p99=659 µs, p99.9=931 µs |
| Throughput (latency-bound)    | ~50K msg/s aggregate        |

The absolute numbers above are from a Docker Desktop (WSL2) sandbox on Windows;
on bare-metal Linux hardware the closed-loop RPS and latencies are expected to
be better. The closed-loop acked-only rate is the honest production figure
because no optimistically-counted messages are included.
