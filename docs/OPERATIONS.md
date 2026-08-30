# Operations

Production flags, retention, monitoring, rotation and crash recovery.

## Broker flags

| flag | meaning |
|------|---------|
| `--addr` / `--http-addr` | data plane / admin plane (`/metrics`, `/health`, `/ready`) |
| `--mode mem` / `--mode file` | RAM ring / persistent ring (same store API) |
| `--file path` + `--ring-capacity-mb N` | ring file and its size (file mode) |
| `--mem-mb N` | RAM ring size (mem mode) |
| `--cores N` | per-core accept shards (`SO_REUSEPORT`, core pinning) |
| `--on-overflow overwrite\|reject` | ring-wrap policy, see below (default `overwrite`) |
| `--tls-cert` / `--tls-key` | rustls TLS 1.3 termination, reloaded on `SIGHUP` |
| `--auth-token` / `--auth-token-file` | data-plane shared-token auth (constant-time) |
| `--http-auth-token(-file)` | HTTP Basic auth for the admin plane |
| `--http-loopback-only` | force admin plane onto `127.0.0.1` |
| `--max-connections N` | hard cap via semaphore; excess connections dropped |
| `--auth-timeout-secs S` | deadline for TLS + auth phase (default 10) |
| `--durable-acks` (default on) / `--durable-ack-timeout-secs` | ACK gating on `fdatasync` completion |

Security is opt-in and zero-cost when off: without tokens/certs the hot loop is
untouched; when enabled, TLS and auth run once per connection, not per message.

## Retention & overflow

The persistent ring is a circular buffer: at sustained rate `R msg/s` with
record size `S` bytes it holds the last `ring_capacity / S` records, i.e.
`ring_capacity / (R × S)` seconds of data. Size it for your maximum acceptable
consumer outage:

```
ring_mb >= peak_RPS × record_bytes × max_consumer_downtime_s × safety(2) / 1MB
```

Example: 700K msg/s × 269 B × 60 s × 2 ≈ 21 GiB → pick the next ring size up.

- `--on-overflow overwrite` (default): oldest records are silently overwritten
  as the ring wraps — maximum throughput, retention is your responsibility.
- `--on-overflow reject`: a publish that would overwrite data not yet enqueued
  to every live subscriber is NACKed with an `Error` frame instead. The
  watermark is per subscriber (last offset enqueued); with no subscribers
  nothing is rejected. Rejections are counted in the `rejected` metric.

`/health` reports `store.seconds_to_wrap` — time until the write position wraps
at the current publish rate (`null` when idle). Alert on it (e.g. at 80% of
your worst consumer downtime) and on `store.overflow_policy` mismatches.

## Monitoring

- `/health` — status (`healthy`/`degraded`), connections, subscriptions,
  `store.{type, capacity_mb, used_mb, durable_pos, write_pos,
  seconds_to_wrap, overflow_policy}`, self-checks (`store_write`, `store_read`,
  `fsync`). `write_pos == durable_pos` at rest means everything ACKed is on
  media.
- `/metrics` — Prometheus: `zhensegg_messages_total{published|acked|delivered|
  rejected}`, latency histograms, auth failures, store usage, uptime.
- `/ready` — load-balancer probe.

## Live rotation & upgrade

`SIGHUP` atomically rereads the TLS cert/key, the data-plane token file and the
HTTP token file. Existing connections keep their snapshot; new connections pick
up the new material. No restart, no dropped connections:

```bash
kill -HUP $(pgrep zhensegg-broker)
```

A hardened systemd unit is in [`deploy/zhensegg.service`](../deploy/zhensegg.service):
`Restart=on-failure`, `LimitNOFILE`, sandbox hardening.

## Crash recovery & fail-stop

- The flusher writes data and the v2 durability header, then issues **one**
  `fdatasync` and only then advances the durable gate — an ACK never outruns
  the media.
- On I/O error the flusher **fail-stops** the process (`exit 70`); systemd
  restarts it and recovery resumes from the last synced header. No ACKed record
  can be lost by design.
- After any crash, audit the ring file:

```bash
zhensegg-bench --verify-ring /data/ring.dat
```

It re-reads the file with `O_DIRECT`, walks the record chain and requires it to
reach the header `committed` position exactly (re-syncing across generation
seams left by recovery wrap-skips). A hard structural break is a durability
violation. Validated by a 10× `kill -9` stress: 18.5M acked records survived,
0 corrupt — see [BENCHMARKS.md](BENCHMARKS.md).
