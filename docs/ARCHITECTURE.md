# Architecture

## Data path

- **Thread-per-core accept.** N shards (`--cores`), each with its own
  `SO_REUSEPORT` listener pinned to a core and running a single-threaded tokio
  runtime. A connection lives and dies on one core; no cross-core handoffs on
  the hot path.
- **Zero-copy wire parser.** Frames are parsed in place from the socket buffer
  (`FrameRef` borrows the parse buffer); no per-message allocation on read.
- **Connection-level group commit.** All `Publish` frames parsed from one
  socket read are appended, then a *single* durable wait covers the whole
  batch, then all ACKs go out in one write. The flusher sees large committed
  jumps instead of per-record syncs.

## Store

One trait, two backends:

- **MemRing** — fixed-capacity ring, record reservation is a single atomic
  `fetch_add`; producers write disjoint regions lock-free. `committed` advances
  per append.
- **FileRing** — the same MemRing as a RAM mirror plus a background flusher
  thread: `pwrite` committed ranges (wrap-aware) + the 128-byte v2 durability
  header (two redundant CRC slots), then **one** `fdatasync` per flush cycle.
  The durable gate advances only after the header is on media, so an ACK never
  outruns persistence. On I/O error the flusher fail-stops the process;
  recovery resumes from the last synced header.

## Fanout & retention

- Subscribers live in a 64-shard FNV-1a map; fanout sends one `Arc<Vec<u8>>`
  per record per subscriber — no copies.
- Each subscriber tracks its last enqueued offset; the minimum across
  subscribers forms the retention watermark used by `--on-overflow reject`
  (see [OPERATIONS.md](OPERATIONS.md)).

## Wire protocol

Length-prefixed frames: `Publish`, `Subscribe`, `Fetch` (offset rewind/replay),
`Ping`, `Auth`; broker replies `Ack`, `Data`, `Error` (e.g. overflow NACK).
Parsed by a fuzzed zero-copy parser (see `fuzz/`).

## Security

rustls TLS 1.3 termination once per connection; shared-token auth with
constant-time comparison; admin plane behind HTTP Basic auth; SIGHUP rotates
certs and tokens atomically without dropping connections.
