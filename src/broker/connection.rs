use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tracing::debug;

use crate::metrics::Metrics;
use crate::protocol::{encode_ack, encode_data, encode_error, Op, Parser as ZParser};
use crate::security::AccessControl;
use crate::store::{wait_durable, Store};
use crate::subscription::{Subscriber, SubscriberMap};

thread_local! {
    static BUF_POOL: std::cell::RefCell<Vec<Vec<u8>>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn take_buf(min: usize) -> Vec<u8> {
    BUF_POOL.with(|p| {
        let mut pool = p.borrow_mut();
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

struct PubEntry {
    topic: Vec<u8>,
    payload: Vec<u8>,
    offset: u64,
    rec_len: u32,
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_tokio_conn(
    stream: tokio::net::TcpStream,
    id: u64,
    store: Arc<dyn Store>,
    subs: SubscriberMap,
    metrics: Arc<Metrics>,
    auth: AccessControl,
    auth_timeout: Option<Duration>,
    durable_acks: bool,
    durable_ack_timeout: Duration,
) -> std::io::Result<()> {
    let (read_half, write_half) = stream.into_split();
    conn_core(read_half, write_half, id, store, subs, metrics, auth, auth_timeout, durable_acks, durable_ack_timeout).await
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_tls_conn(
    stream: tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    id: u64,
    store: Arc<dyn Store>,
    subs: SubscriberMap,
    metrics: Arc<Metrics>,
    auth: AccessControl,
    auth_timeout: Option<Duration>,
    durable_acks: bool,
    durable_ack_timeout: Duration,
) -> std::io::Result<()> {
    let (read_half, write_half) = tokio::io::split(stream);
    conn_core(read_half, write_half, id, store, subs, metrics, auth, auth_timeout, durable_acks, durable_ack_timeout).await
}

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
    durable_acks: bool,
    durable_ack_timeout: Duration,
) -> std::io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Arc<Vec<u8>>>();

    metrics.connections_total.inc();

    let writer = tokio::spawn(async move {
        let mut pending: Vec<Arc<Vec<u8>>> = Vec::with_capacity(128);
        loop {
            let first = rx.recv().await;
            if first.is_none() {
                break;
            }
            pending.push(first.unwrap());
            while pending.len() < 256 {
                match rx.try_recv() {
                    Ok(m) => pending.push(m),
                    Err(_) => break,
                }
            }
            let total: usize = pending.iter().map(|v| v.len()).sum();
            let mut out = take_buf(total);
            for m in pending.drain(..) {
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

    let mut authenticated = auth.initially_authenticated();

    let mut parser = ZParser::new(256 * 1024);
    let mut read_buf = vec![0u8; 256 * 1024];
    let mut my_topics: Vec<Vec<u8>> = Vec::new();
    let auth_deadline = auth_timeout.map(|d| Instant::now() + d);
    let mut batch: Vec<PubEntry> = Vec::with_capacity(256);

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
                        match store.append(topic_slice, payload_slice) {
                            Ok((offset, rec_len)) => {
                                metrics.append_latency
                                    .with_label_values(&["disk"])
                                    .observe(t0.elapsed().as_secs_f64());
                                metrics.messages_total.with_label_values(&["published"]).inc();
                                metrics.messages_bytes_total.with_label_values(&["published"]).inc_by(payload_slice.len() as f64);
                                batch.push(PubEntry {
                                    topic: topic_slice.to_vec(),
                                    payload: payload_slice.to_vec(),
                                    offset,
                                    rec_len,
                                });
                            }
                            Err(crate::store::StoreError::Overflow) => {
                                metrics.messages_total.with_label_values(&["rejected"]).inc();
                                let mut nack = Vec::with_capacity(48);
                                encode_error(&mut nack, topic_slice, "overflow: would overwrite undelivered data (--on-overflow reject)");
                                let _ = tx.send(Arc::new(nack));
                            }
                            Err(e) => return Err(std::io::Error::other(format!("{e:?}"))),
                        }
                    }
                    Op::Subscribe => {
                        let topic = frame.topic.to_vec();
                        let sub = Arc::new(Subscriber {
                            id,
                            tx: tx.clone(),
                            sent: AtomicU64::new(store.write_pos()),
                        });
                        {
                            let mut g = subs.write(&topic);
                            g.entry(topic.clone()).or_default().push(sub);
                        }
                        subs.note_min_sent(store.write_pos());
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
                                } else {
                                    let mut err = Vec::with_capacity(48);
                                    encode_error(&mut err, frame.topic, "not_found");
                                    let _ = tx.send(Arc::new(err));
                                }
                            } else {
                                let mut err = Vec::with_capacity(48);
                                encode_error(&mut err, frame.topic, "not_found");
                                let _ = tx.send(Arc::new(err));
                            }
                        }
                    }
                    Op::Ping => {
                        let mut pong = Vec::new();
                        encode_ack(&mut pong, b"pong", 0, 0);
                        let _ = tx.send(Arc::new(pong));
                    }
                    Op::Auth => {
                        let mut ack = Vec::with_capacity(32);
                        encode_ack(&mut ack, b"auth", 0, 0);
                        let _ = tx.send(Arc::new(ack));
                    }
                    _ => {}
                }
                parser.consume();
            }

            if !batch.is_empty() {
                if durable_acks
                    && let Some(gate) = store.durable_gate()
                {
                    let max_end = batch.iter().map(|e| e.offset + e.rec_len as u64).max().unwrap_or(0);
                    if gate.pos() < max_end {
                        let wait = wait_durable(gate, max_end);
                        if tokio::time::timeout(durable_ack_timeout, wait).await.is_err() {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "durable ack timeout (flusher stalled); closing without ack",
                            ));
                        }
                    }
                }
                for e in batch.drain(..) {
                    let mut ack = Vec::with_capacity(32);
                    encode_ack(&mut ack, &e.topic, e.offset, e.rec_len);
                    let _ = tx.send(Arc::new(ack));
                    metrics.messages_total.with_label_values(&["acked"]).inc();
                    metrics.messages_bytes_total.with_label_values(&["acked"]).inc_by(e.payload.len() as f64);

                    let guard = subs.read(&e.topic);
                    if let Some(list) = guard.get(&e.topic).filter(|l| !l.is_empty()) {
                        let rec_end = e.offset + e.rec_len as u64;
                        let mut min_sent = u64::MAX;
                        let mut data = Vec::with_capacity(13 + e.topic.len() + e.payload.len());
                        encode_data(&mut data, &e.topic, &e.payload);
                        let arc = Arc::new(data);
                        for sub in list.iter() {
                            let _ = sub.tx.send(arc.clone());
                            sub.sent.fetch_max(rec_end, Ordering::Relaxed);
                            let s = sub.sent.load(Ordering::Relaxed);
                            if s < min_sent {
                                min_sent = s;
                            }
                            metrics.messages_total.with_label_values(&["delivered"]).inc();
                            metrics.messages_bytes_total.with_label_values(&["delivered"]).inc_by(e.payload.len() as f64);
                        }
                        subs.note_min_sent(min_sent);
                    }
                    debug!(connection_id = id, topic = %String::from_utf8_lossy(&e.topic), offset = e.offset, "published");
                }
            }
        }
    }
    .await;

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
    subs.recompute_min_sent();
    metrics.connections_total.dec();
    drop(tx);
    let _ = writer.await;
    res
}
