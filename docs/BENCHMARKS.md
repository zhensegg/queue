# Benchmarks

Methodology, measured results and how to reproduce them. The harness lives in
[`benchmarks/`](../benchmarks/README.md).

## Methodology (honest by construction)

- **Closed loop, acked-only.** A producer never sends the next batch until the
  broker ACKed the previous one; throughput is counted on ACKs only — never on
  "bytes written to the socket". With durable group-commit an ACK means the
  record **and** the ring header are on media (`fdatasync` completed).
- **Separate OS threads.** Every producer/consumer runs on its own thread with
  its own TCP connection — the production topology.
- **E2E latency** is measured from a timestamp in the payload (producer) to
  delivery (consumer).
- **Durability on media.** Sampled ACKed records are re-read with `O_DIRECT`
  (page cache bypassed) and compared byte-for-byte. The on-disk v2 layout has a
  128-byte durability header; the verifier applies that offset.
- **Fanout accounting.** `consumed` is the sum across consumers; with topic
  fanout every record reaches every subscriber, so summed delivery is
  `publish × consumers`. Unique delivery rate equals the publish rate.

## Environment statement

All numbers below come from a Docker Desktop / WSL2 sandbox on Windows
(28 vCPU quota, overlayfs). Measured raw `fdatasync` bandwidth of the sandbox
disk: **~164 MB/s** (`dd oflag=dsync`). The durable soak sustains ~154–168 MB/s
of it — the **disk is the bottleneck, not the broker** (the mem path, which
never touches the disk, sustains ~700K msg/s in the same throttled VM). On
bare-metal Linux with NVMe (2–3 GB/s sync bandwidth) expect materially higher
durable numbers; no multiplier is claimed here because the CPU-side ceiling has
not been measured on such hardware yet.

## Persistent soak — 30 s and 10 min, 24 producers × 8 consumers, 256 B, 4 GB ring, 4 cores

| metric | value |
|---|---|
| publish RPS (acked-only) | **658K–666K msg/s avg over 10 min** (170 MB/s) |
| per-minute stability (10 min) | 685K–695K after warm-up, spread < 1.5%, no degradation |
| delivery (sum over 8 consumers) | ~5.3M msg/s (8× publish — fanout) |
| backlog after run | 0% (fully drained) |
| pub→ack batch RTT | p50 = 8.9 ms, p90 = 11.5 ms, p99 = 17.8 ms |
| e2e delivery latency (4M samples) | p50 = 10.0 ms, p99 = 16.8 ms |
| durability (O_DIRECT) | **4096/4096 verified, 0 mismatch** (~25 ring wraps per 10 min) |
| broker memory | plateau at ring size (the ring mirrors in RAM by design) |
| durable lag (`write_pos − durable_pos`) | 0 at rest; bounded during run |

## Latency-focused — 4 × 4, batch = 1 (1-in-flight), 128 B

| metric | value |
|---|---|
| publish RPS | 3 667 msg/s |
| pub→ack RTT | p50 = 711 µs, p90 = 1 261 µs, p99 = 4 650 µs |

At batch = 1 each ACK pays one full flush cycle (data + header in a single
`fdatasync`), so this is the per-sync latency floor of the sandbox disk, not
broker overhead.

## In-memory reference (no disk)

Same harness, `--mode mem`, 24 × 8, batch 256: **~696K msg/s**, RTT p50 ≈ 8.5 ms.
This is the code's throughput ceiling inside the same CPU-throttled VM with
clients sharing cores — the baseline that grows on dedicated hardware.

## Crash safety — kill -9 stress

10 consecutive `kill -9` cycles under load (12 producers, batch 256, ~200K
msg/s, killed after a random 4–8 s): **10/10 clean recoveries**. After each
restart the ring file was re-read with `O_DIRECT`:

```
zhensegg-bench --verify-ring /data/ring.dat
[verify-ring] OK: header committed=… write_pos=…; chain reaches committed
exactly: 15966416 records walked, 4 generation seams re-synced, window
4096.0 MiB (O_DIRECT)
```

18.5M acked records in total survived the kills; 0 corrupt; broker log clean.
The walk re-syncs across generation seams left by recovery wrap-skips; a hard
structural break or a chain that fails to reach `committed` is a durability
violation.

## Overflow reject test

64 MB ring, `--on-overflow reject`, 8 producers, 1 slow consumer, 10 s:
68.8M publishes NACKed with `Error` frames instead of being silently
overwritten; the consumer received 100% of the acked records.

## Reproduce

```bash
docker build -f benchmarks/Dockerfile -t zhensegg-bench .
docker run --rm --privileged zhensegg-bench          # 30 s soak, defaults

docker run --rm --privileged zhensegg-bench \
    /usr/local/bin/zhensegg-bench --addr 127.0.0.1:9090 \
    --producers 24 --consumers 8 --payload-size 256 --batch 256 \
    --secs 600 --verify-file /data/ring.dat           # 10-min soak + verify

docker run --rm --privileged zhensegg-bench \
    /usr/local/bin/zhensegg-bench --verify-ring /data/ring.dat   # crash audit
```

Tunables and harness details: [`benchmarks/README.md`](../benchmarks/README.md).
