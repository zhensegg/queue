<div align="center">

# Zhensegg Queue

**An extremely fast, honest message broker in Rust.**

Persistent ring · group-commit durability · fan-out · fetch-by-offset · TLS+auth · Prometheus

*One ring buffer. An ACK means it's on disk.*

</div>

---

## Why Zhensegg

Most brokers make you choose between speed and durability. Zhensegg doesn't.

- **Lock-free append to the bone.** Record reservation is one atomic
  `fetch_add` over a fixed ring — concurrent producers write disjoint regions.
  No shared-buffer lock storms.
- **Durable group-commit, done right.** Data + the CRC durability header are
  synced with a single `fdatasync`, and only then does the ACK go out. The
  flusher batches whole socket reads per commit, so throughput stays high even
  with every ACK on media.
- **Crash-proven, not crash-hoped.** Fail-stop flusher, header-based recovery,
  and a `kill -9` stress harness that re-reads the ring with `O_DIRECT` after
  every kill: 18.5M acked records survived 10/10 kills, 0 corrupt.
- **Boring ops, on purpose.** `/metrics`, `/health` (with `seconds_to_wrap`),
  `/ready`, SIGHUP secret/cert rotation, connection caps, systemd unit —
  built in, not bolted on.

## Performance

Closed loop, throughput counted on *broker acks only*, producers and consumers
on separate OS threads, everything fighting for the same cores (worst case for
a broker):

| mode                     | publish RPS        | notes                                      |
|--------------------------|--------------------|--------------------------------------------|
| durable file (group-commit) | **~660–695K msg/s** | 256 B payload, 10-min soak, disk-bound |
| in-memory                | ~700K msg/s        | no-disk ceiling in the same VM             |
| pub→ack RTT (batch=1)    | p50 ≈ 711 µs       | one full fdatasync per ack                 |
| e2e delivery             | p50 ≈ 10 ms        | fan-out to 8 subscribers                   |

> **Note on the numbers.** Measured in a WSL2/Docker sandbox where the soak
> sustains **~94% of the disk's measured raw `fdatasync` bandwidth** — the
> disk is the bottleneck, not the broker. On dedicated bare-metal Linux with
> NVMe, expect more; no inflated multiplier is quoted because the CPU-side
> ceiling hasn't been independently measured yet. Durability is verified
> byte-for-byte on media (`O_DIRECT`, 0 mismatches), so every counted message
> was actually persisted. See [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Killer features

| | |
|---|---|
| **Durable group-commit** | one `fdatasync` per flush cycle; ACK strictly after media |
| **Fetch by offset** | consumers rewind/replay from any committed offset |
| **Overflow policy** | `--on-overflow reject` NACKs instead of silently overwriting undelivered data; per-subscriber retention watermark |
| **Retention telemetry** | `/health` exposes `seconds_to_wrap` — size the ring from a formula, alert before it wraps |
| **Crash auditor** | `zhensegg-bench --verify-ring` proves on-media integrity after any crash |
| **TLS + token auth** | rustls TLS 1.3, constant-time shared-token gate — once per connection, never per message |
| **Zero-downtime rotation** | certs and auth tokens reload on `SIGHUP` |
| **Prometheus built-in** | message counters, latency histograms, store usage, durable lag |

## Quick start

```bash
git clone https://github.com/zhensegg/queue && cd queue

# in-memory, max speed
cargo run --release --bin zhensegg-broker -- --mode mem --mem-mb 4096

# durable, persistent ring
cargo run --release --bin zhensegg-broker -- \
    --mode file --file /tmp/zhensegg.ring --ring-capacity-mb 4096 --cores 4 \
    --on-overflow reject

# measure it yourself (10-min soak + on-media durability audit)
docker build -f benchmarks/Dockerfile -t zhensegg-bench .
docker run --rm --privileged zhensegg-bench

curl http://localhost:9091/metrics   # Prometheus
curl http://localhost:9091/health    # durable lag, seconds_to_wrap, policy
```

A production broker is one binary with a handful of flags — sizing, overflow
policy, rotation and recovery are covered in
[docs/OPERATIONS.md](docs/OPERATIONS.md).

## Docs

| doc | contents |
|---|---|
| [Benchmarks](docs/BENCHMARKS.md) | methodology, honest numbers, kill -9 stress, how to reproduce |
| [Operations](docs/OPERATIONS.md) | flags, retention & overflow, monitoring, rotation, recovery |
| [Architecture](docs/ARCHITECTURE.md) | thread-per-core, ring stores, group-commit flusher, watermark |
| [benchmarks/](benchmarks/README.md) | the harness itself: env vars, usage |
| [fuzz/](fuzz/) | protocol fuzzer + live TCP soak |
| [deploy/zhensegg.service](deploy/zhensegg.service) | hardened systemd unit |

## Status

Stable. The durable file path is the production default: group-commit
flusher, retention policy, fail-stop recovery — soak-tested at ~700K msg/s for
10-minute runs, proven against 10 consecutive `kill -9` cycles with zero data
loss, and verified on media with `O_DIRECT`.

## License

See [LICENSE](LICENSE).
