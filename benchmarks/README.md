# Zhensegg Benchmark

Honest production benchmark harness for the Zhensegg broker: standalone crate
under `benchmarks/`, builds and runs independently of the broker library.

Measured results, methodology and the kill -9 stress live in
[docs/BENCHMARKS.md](../docs/BENCHMARKS.md).

## Layout

```
benchmarks/
  Cargo.toml         standalone crate (depends on ../ via path)
  src/main.rs        the benchmark client (+ --verify-ring crash auditor)
  Dockerfile         multi-stage: builds broker (--features uring) + bench on Linux
  run-bench.sh       container entrypoint: starts broker in file mode, runs bench
```

## What it measures

* **Closed loop, acked-only** — a producer never sends the next batch until the
  broker ACKed the previous one; with durable group-commit an ACK means the
  record and the ring header are on media.
* **Separate OS threads** — every producer/consumer on its own thread and TCP
  connection (production topology).
* **E2E delivery latency** — first 8 payload bytes carry a producer timestamp.
* **Durability on media** — sampled ACKed records re-read with `O_DIRECT` and
  compared byte-for-byte (v2 header offset applied).
* **`--verify-ring <file>`** — standalone O_DIRECT auditor: walks the record
  chain and requires it to reach the header `committed` position exactly
  (re-syncing across generation seams); used for the kill -9 stress.

## Run locally (no container)

```bash
cd benchmarks && cargo build --release

cargo run --release --bin zhensegg-broker -- \
    --mode file --file /tmp/zhensegg.ring --ring-capacity-mb 4096 --cores 4

./benchmarks/target/release/zhensegg-bench \
    --addr 127.0.0.1:9090 --producers 8 --consumers 8 \
    --payload-size 256 --secs 30 --verify-file /tmp/zhensegg.ring
```

## Run in Docker (Ubuntu, io_uring path)

```bash
docker build -f benchmarks/Dockerfile -t zhensegg-bench .
docker run --rm zhensegg-bench
```

If Docker's seccomp profile blocks io_uring, add `--privileged`.

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
