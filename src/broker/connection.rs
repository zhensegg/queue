//! Per-connection processing: auth gate, parser loop, publish/subscribe/fetch,
//! outbound fan-out.
//!
//! The work lives in [`conn_core`], generic over the read/write halves so the
//! same code drives both a plain TCP connection (`into_split`, zero shared
//! state) and a TLS connection (`tokio::io::split`). Authentication and TLS
//! handoff happen once, up front, and never touch the steady-state hot loop.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::debug;

use crate::metrics::Metrics;
use crate::protocol::{encode_ack, encode_data, Op, Parser as ZParser};
use crate::security::AccessControl;
use crate::store::Store;
use crate::subscription::{Subscriber, SubscriberMap};

// ===== provided-buffers style slab: per-thread free-list, zero alloc on hot path =====
thread_local! {
    static BUF_POOL: std::cell::RefCell<Vec<Vec<u8>>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn take_buf(min: usize) -> Vec<u8> {
    BUF_POOL.with(|p| {
        let mut pool = p.borrow_mut();
        // scan from end (most recently freed = hot in cache)
        for i in (0..pool.len()).rev() {
            if pool[i].capacity() >= min {
                return pool.swap_remove(i);
            }
        }
        Vec::with_capacity(min.max(64))
    })
}

fn give_buf(mut b: Vec<u8>) {
    b.clear();
    let cap = b.capacity();
    if cap <= 256 * 1024 {
        BUF_POOL.with(|p| {
            let mut pool = p.borrow_mut();
            if pool.len() < 512 {
                pool.push(b);
            }
        });
    }
}

/// Handle one plain (non-TLS) tokio connection. Uses `into_split` owned halves
/// for the zero-overhead hot path. `auth_timeout` bounds how long a connection
/// may stay unauthenticated before it is dropped (None = unlimited).
pub async fn handle_tokio_conn(
    stream: tokio::net::TcpStream,
    id: u64,
    store: Arc<dyn Store>,
    subs: SubscriberMap,
    metrics: Arc<Metrics>,
    auth: AccessControl,
    auth_timeout: Option<Duration>,
) -> std::io::Result<()> {
    let (read_half, write_half) = stream.into_split();
    conn_core(read_half, write_half, id, store, subs, metrics, auth, auth_timeout).await
}

/// Handle one TLS-terminated tokio connection (already handshaken by the accept
/// loop). `tokio::io::split` is used because `TlsStream` has no owned halfs.
/// `auth_timeout` bounds how long a connection may stay unauthenticated.
pub async fn handle_tls_conn(
    stream: tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    id: u64,
    store: Arc<dyn Store>,
    subs: SubscriberMap,
    metrics: Arc<Metrics>,
    auth: AccessControl,
    auth_timeout: Option<Duration>,
) -> std::io::Result<()> {
    let (read_half, write_half) = tokio::io::split(stream);
    conn_core(read_half, write_half, id, store, subs, metrics, auth, auth_timeout).await
}

/// Core connection driver, generic over the transport halves.
#[allow(clippy::too_many_arguments)]
pub async fn conn_core<R, W>(
    mut read_half: R,
    mut write_half: W,
    id: u64,
    store: Arc<dyn Store>,
    subs: SubscriberMap,
    metrics: Arc<Metrics>,
    auth: AccessControl,
    auth_timeout: Option<Duration>,
) -> std::io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Arc<Vec<u8>>>();

    metrics.connections_total.inc();

    // writer task: batched zero-copy flush (single write per batch)
    let writer = tokio::spawn(async move {
        let mut pending: Vec<Arc<Vec<u8>>> = Vec::with_capacity(128);
        loop {
            // wait for at least one message
            let first = rx.recv().await;
            if first.is_none() {
                break;
            }
            pending.push(first.unwrap());
            // drain additional without blocking, up to 256 for higher batching
            while pending.len() < 256 {
                match rx.try_recv() {
                    Ok(m) => pending.push(m),
                    Err(_) => break,
                }
            }
            // coalesce into single buffer for one syscall (smart batching)
            let total: usize = pending.iter().map(|v| v.len()).sum();
            let mut out = take_buf(total);
            for m in pending.drain(..) {
                // recycle buffer backing if this is the last Arc holder (provided-buffers style)
                if Arc::strong_count(&m) == 1 {
                    match Arc::try_unwrap(m) {
                        Ok(raw) => {
                            out.extend_from_slice(&raw);
                            give_buf(raw);
                        }
                        Err(m) => out.extend_from_slice(&m),
                    }
                } else {
                    out.extend_from_slice(&m);
                }
            }
            if write_half.write_all(&out).await.is_err() {
                break;
            }
            give_buf(out);
        }
    });

    // ---- auth gate: run before any data-plane command is honoured ----
    let mut authenticated = auth.initially_authenticated();

    let mut parser = ZParser::new(64 * 1024);
    let mut read_buf = vec![0u8; 64 * 1024];
    let mut my_topics: Vec<Vec<u8>> = Vec::new();
    let auth_deadline = auth_timeout.map(|d| Instant::now() + d);

    let res: std::io::Result<()> = async {
        loop {
            let n = if !authenticated {
                if let Some(deadline) = auth_deadline {
                    let now = Instant::now();
                    if now >= deadline {
                        metrics.auth_failures_total.inc();
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "authentication timeout",
                        ));
                    }
                    // Bound the read itself so a silent client cannot stall the
                    // auth phase forever.
                    match tokio::time::timeout(deadline - now, read_half.read(&mut read_buf)).await {
                        Ok(Ok(n)) => n,
                        Ok(Err(e)) => return Err(e),
                        Err(_) => {
                            metrics.auth_failures_total.inc();
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "authentication timeout",
                            ));
                        }
                    }
                } else {
                    read_half.read(&mut read_buf).await?
                }
            } else {
                read_half.read(&mut read_buf).await?
            };
            if n == 0 {
                break Ok(());
            }
            parser.feed(&read_buf[..n]);
            while let Some(frame) = parser.try_parse() {
                // Every unauthenticated connection may only send an Auth frame.
                if !authenticated {
                    if frame.op != Op::Auth {
                        metrics.auth_failures_total.inc();
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            "authentication required",
                        ));
                    }
                    if auth.verify(frame.payload) {
                        authenticated = true;
                        let mut ack = Vec::with_capacity(32);
                        encode_ack(&mut ack, b"auth", 0, 0);
                        let _ = tx.send(Arc::new(ack));
                        metrics.auth_successes_total.inc();
                        debug!(connection_id = id, "authenticated");
                    } else {
                        metrics.auth_failures_total.inc();
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            "invalid auth token",
                        ));
                    }
                    parser.consume();
                    continue;
                }

                match frame.op {
                    Op::Publish => {
                        let t0 = std::time::Instant::now();
                        let topic_slice = frame.topic;
                        let payload_slice = frame.payload;
                        let (offset, rec_len) = store
                            .append(topic_slice, payload_slice)
                            .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
                        metrics.append_latency
                            .with_label_values(&["disk"])
                            .observe(t0.elapsed().as_secs_f64());
                        metrics.messages_total.with_label_values(&["published"]).inc();
                        metrics.messages_bytes_total.with_label_values(&["published"]).inc_by(payload_slice.len() as f64);

                        let mut ack = Vec::with_capacity(32);
                        encode_ack(&mut ack, topic_slice, offset, rec_len);
                        let _ = tx.send(Arc::new(ack));
                        metrics.messages_total.with_label_values(&["acked"]).inc();
                        metrics.messages_bytes_total.with_label_values(&["acked"]).inc_by(payload_slice.len() as f64);

                        let guard = subs.read(topic_slice);
                            if let Some(list) = guard.get(topic_slice).filter(|l| !l.is_empty()) {
                                let mut data = Vec::with_capacity(13 + topic_slice.len() + payload_slice.len());
                                encode_data(&mut data, topic_slice, payload_slice);
                                let arc = Arc::new(data);
                                for sub in list.iter() {
                                    let _ = sub.tx.send(arc.clone());
                                    metrics.messages_total.with_label_values(&["delivered"]).inc();
                                    metrics.messages_bytes_total.with_label_values(&["delivered"]).inc_by(payload_slice.len() as f64);
                                }
                            }
                        debug!(connection_id = id, topic = %String::from_utf8_lossy(topic_slice), offset, "published");
                    }
                    Op::Subscribe => {
                        let topic = frame.topic.to_vec();
                        let sub = Arc::new(Subscriber { id, tx: tx.clone() });
                        {
                            let mut g = subs.write(&topic);
                            g.entry(topic.clone()).or_default().push(sub);
                        }
                        my_topics.push(topic.clone());
                        metrics.subscriptions_total.inc();
                        let mut ack = Vec::with_capacity(32);
                        encode_ack(&mut ack, &topic, 0, 0);
                        let _ = tx.send(Arc::new(ack));
                        metrics.messages_total.with_label_values(&["acked"]).inc();
                        debug!(connection_id = id, topic = %String::from_utf8_lossy(&topic), "subscribed");
                    }
                    Op::Fetch => {
                        if frame.payload.len() >= 12 {
                            let off = u64::from_be_bytes(frame.payload[0..8].try_into().unwrap());
                            let len = u32::from_be_bytes(frame.payload[8..12].try_into().unwrap());
                            let mut raw = Vec::new();
                            if store.read(off, len, &mut raw).is_ok() && raw.len() >= 8 {
                                let tl = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
                                let pl = u32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]) as usize;
                                if raw.len() >= 8 + tl + pl {
                                    let st = &raw[8..8 + tl];
                                    let sp = &raw[8 + tl..8 + tl + pl];
                                    let mut data = Vec::new();
                                    encode_data(&mut data, st, sp);
                                    let _ = tx.send(Arc::new(data));
                                }
                            }
                        }
                    }
                    Op::Ping => {
                        let mut pong = Vec::new();
                        encode_ack(&mut pong, b"pong", 0, 0);
                        let _ = tx.send(Arc::new(pong));
                    }
                    Op::Auth => {
                        // Re-auth is a no-op for an already authenticated connection.
                        let mut ack = Vec::with_capacity(32);
                        encode_ack(&mut ack, b"auth", 0, 0);
                        let _ = tx.send(Arc::new(ack));
                    }
                    _ => {}
                }
                parser.consume();
            }
        }
    }
    .await;

    // cleanup subscriptions (per-topic shard lock)
    for t in my_topics {
        let mut g = subs.write(&t);
        if let Some(list) = g.get_mut(&t) {
            list.retain(|s| s.id != id);
            if list.is_empty() {
                g.remove(&t);
            }
        }
        metrics.subscriptions_total.dec();
    }
    metrics.connections_total.dec();
    // close writer channel to terminate writer task
    drop(tx);
    let _ = writer.await;
    res
}
