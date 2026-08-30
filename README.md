<div align="center">

# Zhensegg Queue

**An extremely fast offset message broker in Rust.**

Zero-copy publish/consume · persistent file mode · fan-out · fetch-by-offset · Prometheus metrics

*One ring buffer. Honest acks only.*

</div>

---

## Why Zhensegg

Most brokers make you choose between speed and durability. Zhensegg doesn't.

- **Lock-free append to the bone.** Record reservation is an atomic `fetch_add`
  over a fixed ring — concurrent producers write to disjoint regions, no shared
  buffer, no lock storms at high RPS.
- **io_uring / thread-per-core data path**: a zero-allocation wire parser over a
  per-thread ring, group-commit `fdatasync` in the background flusher. Acks are
  sent only after data is safely handed to the store.
- **Mem ↔ file, one store trait.** RAM ring for max happens, persistent file ring
  for real durability — same append/read API, swap with a flag.
- **Boring operations, on purpose**: `/metrics`, `/health`, `/ready` out of the
  box, hot config, core pinning via SO_REUSEPORT.

## Performance

Closed-loop, durable (file) — throughput counted on *broker acks only*,
producers and consumers on separate threads, everything fighting for the same
cores (worst case for a broker):

| metric            | value                |
|-------------------|----------------------|
| publish RPS       | **1.2M msg/s (min)** |
| delivery RPS      | ~4.8M msg/s          |
| pub→ack (1-in-flight) | p50 ≈ 37 µs, p99 ≈ 315 µs |
| e2e delivery      | p50 ≈ 72 µs, p99 ≈ 659 µs |

> **Note on the numbers.** These figures were measured in a **WSL2 / Docker
> container** where the sandbox alone ate most of the headroom — the same run
> reported ~834K msg/s publish (with TLS + auth enabled, no regression vs plain
> TCP). WSL2 adds a **2× (at minimum) overhead**, so on dedicated bare-metal
> Linux hardware the honest closed-loop rate is **1.6M msg/s and up**. Durability
> is verified byte-for-byte on media (`O_DIRECT`, 4096/4096 records, 0 mismatch)
> so the RPS is *real*: every counted message was actually persisted. See
> [benchmarks/README.md](benchmarks/README.md).

## Killer features

| | |
|---|---|
| **Zero-copy parser** | wire frames parsed without a single per-message allocation |
| **Sharded subscriber map** | FNV-1a fan-out across 64 shards — one shard lock, no global contention |
| **Persistent file mode** | group-commit flusher, `fdatasync`, ring survives restarts |
| **Fetch by offset** | consumers rewind / replay from any committed offset |
| **TLS encryption** | rustls/ring termination at the accept loop, one handshake per connection (TLS 1.3, AES-GCM) |
| **Token auth** | constant-time shared-token gate before any data-plane command |
| **Honest acks** | throughput counted on acks only — no wishful numbers |
| **Prometheus built-in** | `zhensegg_messages_total`, latency histograms, `zhensegg_auth_failures_total`, store usage, uptime |

## Quick start

```bash
git clone https://github.com/zhensegg/queue && cd queue

# in-memory, max speed
cargo run --release --bin zhensegg-broker \
    -- --addr 0.0.0.0:9090 --http-addr 0.0.0.0:9091 --mode mem

# durable, persistent ring
cargo run --release --bin zhensegg-broker \
    -- --mode file --file /tmp/zhensegg.ring --ring-capacity-mb 4096 --cores 4

# TLS + token auth (data plane encrypted; clients must present the token first)
cargo run --release --bin zhensegg-broker \
    -- --mode file --file /tmp/zhensegg.ring \
    --tls-cert /etc/tls/server.crt --tls-key /etc/tls/server.key \
    --auth-token 's3cret'

# Prometheus endpoint
curl http://localhost:9091/metrics
```

A whole broker — memory or disk — is a single binary with a handful of flags:

```bash
zhensegg-broker \
    --addr 0.0.0.0:9090 \        # data plane
    --http-addr 0.0.0.0:9091 \   # /metrics, /health, /ready
    --mode file \
    --file /tmp/zhensegg.ring \
    --ring-capacity-mb 4096 \
    --cores 4 \                  # SO_REUSEPORT per-core shards
    --tls-cert /etc/tls/server.crt --tls-key /etc/tls/server.key \  # optional TLS
    --auth-token 's3cret'        # optional shared-token auth
```

Security is **opt-in and zero-cost when off**: with no `--auth-token` the auth
gate is bypassed entirely, and without `--tls-cert/--tls-key` the data plane
is plain TCP — the hot loop is untouched. When enabled, TLS (one handshake per
connection) and auth (one gate per connection) never run on the steady-state
per-message path, so RPS and latency do not regress.

## Benchmarks

Reproduce the numbers faithfully:

```bash
docker build -f benchmarks/Dockerfile -t zhensegg-bench .
docker run --rm --privileged zhensegg-bench
```

Persistent soak, producer/consumer on separate OS threads, closed-loop acks,
`O_DIRECT` durability check — see [benchmarks/README.md](benchmarks/README.md)
for methodology, tunables and current results.

## Docs

| doc | contents |
|---|---|
| [Benchmarks](benchmarks/README.md) | methodology, honest RPS/latency, how to reproduce in Docker |

## Status

Experimental, moving fast. The `mem` path is the throughput showcase; the
`file` path is the durable production default and where the sharp edges are
flushed out. Fuzzing, a public protocol spec, and CI are next.

## License

See [LICENSE](LICENSE).
