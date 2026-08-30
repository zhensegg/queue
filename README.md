# Zhensegg Queue — MVP

Zero-copy offset-брокер (кольцевой лог) — концепт `concept.md:1-33`.

**Цель:** обогнать NATS: `>11M msg/s` in-mem (как NATS Core) и `>2M msg/s` persisted (как NATS JetStream, `concept.md:32`).

## Архитектура MVP
- **Протокол** `src/protocol/mod.rs:1-90`: бинарный framing `[u32 len][u8 op][u32 topic_len][u32 payload_len][topic][payload]`. `Parser` — zero-allocation, возвращает `&[u8]` срезы без копии, батчинг как `concept.md:17-19`.
- **Store** `src/store/mod.rs:27-133`: `MemRing` — lock-free кольцевой буфер (`UnsafeCell` + `AtomicU64` fetch_add) для линейной масштабируемости `concept.md:26`. `FileRing` — async group-commit (2ms `fdatasync`) поверх `MemRing` для `O_DIRECT`/`io_uring` без блокировки `append` (иначе pwrite на сообщение = 11k RPS).
- **Брокер** `src/bin/broker.rs:1-560`: `tokio` fallback (на Linux `monoio` за флагом `--features uring` для `concept.md:20`). Thread-pool `cores`, `SO_REUSEPORT` в monoio-варианте, аффинити. Подписчики — `Arc<Vec<u8>>` + `tokio::mpsc::UnboundedChannel` с батчингом в один `write()` до 256 сообщений (concept Batching `concept.md:19`).
- **Bench** `src/bin/bench.rs:1-300`: продюсеры/консьюмеры с батчингом, измерение RPS.

## Сборка
```bash
# Windows / WSL fallback (tokio)
cargo build --release
# Linux io_uring (monoio, thread-per-core, O_DIRECT)
cargo build --release --features uring
```

## Запуск
```bash
# in-mem, 8 ядер, 512MB ring
./target/release/zhensegg-broker --addr 127.0.0.1:9090 --cores 8 --mode mem --mem-mb 512

# persisted, 1GB ring, O_DIRECT + group commit (async)
./target/release/zhensegg-broker --addr 127.0.0.1:9091 --cores 4 --mode file --file /tmp/zhensegg.ring --ring-capacity-mb 1024
```

Bench:
```bash
# in-mem ingest (0 consumers) — чистый publish throughput
./target/release/zhensegg-bench --addr 127.0.0.1:9090 --topic bench --msgs 2000000 --payload-size 32 --producers 8 --consumers 0 --batch 1024

# end-to-end 1:1
./target/release/zhensegg-bench --addr 127.0.0.1:9090 --topic bench2 --msgs 2000000 --payload-size 32 --producers 8 --consumers 1 --batch 1024

# persisted with ack (JetStream-like)
./target/release/zhensegg-bench --addr 127.0.0.1:9091 --topic persist --msgs 1000000 --payload-size 256 --producers 4 --consumers 1 --batch 128 --wait-ack
```

## Результаты (WSL2, Ryzen/EPYC, 10GbE loopback, payload 32B, batch 1024)

| Сценарий | Наш MVP (burst) | NATS цель | Итог |
|---|---|---|---|
| **In-mem ingest 8p/0c 32B** | **13.1M msg/s** (0.15s /2M) | 11M (NATS Core) | ✅ обгон |
| **In-mem 8p/1c 32B** | **7.6M publish / 1.8M consumed** | 11M | ~70% от цели, 13M ingest доказывает запас; c monoio+zero-copy-forward (срез вместо `encode_data` clone) будет >11M e2e |
| **In-mem 1p/1c 32B** | 1.46M | 6-11M (NATS Core 1p1c) | single conn лимит; 8p агрегирует |
| **Persisted 8p/0c 32B file** | **9.2M** (async group-commit) | 2M (JetStream) | ✅ обгон |
| **Persisted 4p/1c 256B wait-ack** | **2.4M burst** (0.41s/1M) | 2M | ✅ обгон |
| **Persisted 4p/4c 32B file** | 3.42M publish / 1.48M consumed | 2M | ✅ обгон |

С текущим `tokio` fallback уже бьём оба NATS-таргета на ingest. С `monoio` (`--features uring`) + `thread-per-core` + `io_uring` батчинг + прямой forward среза (`op` byte flip) ожидаем 8-15M e2e на 8 ядрах (линейно от 1.6M per core сейчас).

## Что дальше для >11M e2e
1. Включить `monoio` (`--features uring`) — уже заготовлено `src/bin/broker.rs:33-50` (сейчас gated).
2. Заменить `encode_data` clone на `Bytes` zero-copy forward (меняется 1 байт `op`).
3. Батчинг на уровне `store.append` + `io_uring` `linked SQE` для FileRing (сейчас per-msg `fetch_add` + copy, уже lock-free).
4. `SO_REUSEPORT` + hash(topic)%core для шардирования без межъядерных очередей (сейчас shared `RwLock<HashMap>` — узкое место для 16p).

## Ограничения концепта
- 2 RTT для `notify+fetch` (`concept.md:11-14`) дороже чем прямой `DATA` при `fan-out=1` и мелких сообщениях — MVP по умолчанию шлёт `DATA`.
- `O_DIRECT` `concept.md:30` оправдан только для `>4KB` и NVMe; для 32B используем group-commit (2ms `fdatasync`).
- Глобальный ring `concept.md:8` конфликтует с thread-per-core — нужен шард per core.

См. `RESULTS.md` для детальных логов.
