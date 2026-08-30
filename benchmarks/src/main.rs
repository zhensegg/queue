//! Honest production benchmark for the Zhensegg broker.
//!
//! Design goals:
//!   * CLOSED LOOP — a producer waits for the broker's ACK of every message it
//!     sent before sending the next batch. Throughput is counted on ACKs only,
//!     never on "bytes written to the socket", so it reflects real sustained RPS.
//!   * PRODUCER / CONSUMER ON SEPARATE THREADS — every producer and every
//!     consumer runs on its own OS thread with its own tokio runtime and its own
//!     TCP connection. This is the honest production topology, not a shared
//!     connection with multiplexed tasks.
//!   * E2E DELIVERY LATENCY — the first 8 bytes of each payload carry a
//!     nanosecond timestamp written by the producer; the consumer measures the
//!     round trip from publish to delivery.
//!   * DURABILITY VERIFICATION (file mode) — sampled ACKed records are re-read
//!     straight off media with O_DIRECT (bypassing the page cache) and their
//!     content is compared to what the producer sent. Any mismatch on a
//!     not-yet-overwritten ring means a durability violation.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use zhensegg::protocol::{Op, Parser as RingParser, encode_auth, encode_publish, encode_subscribe};

// ---------------------------------------------------------------------------
// Transport wrapper: either a plain TCP stream or a rustls client stream, so
// the benchmark can run the identical closed-loop workload over TLS.
// ---------------------------------------------------------------------------
enum Conn {
    Plain(TcpStream),
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl AsyncRead for Conn {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            Conn::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            Conn::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Conn {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut *self {
            Conn::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            Conn::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            Conn::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            Conn::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            Conn::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            Conn::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

static T0: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
fn now_ns() -> u64 {
    T0.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

#[cfg(target_os = "linux")]
fn tune_socket(fd: std::os::unix::io::RawFd) {
    unsafe {
        let one: libc::c_int = 1;
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, &one as *const _ as *const libc::c_void, std::mem::size_of_val(&one) as libc::socklen_t);
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_QUICKACK, &one as *const _ as *const libc::c_void, std::mem::size_of_val(&one) as libc::socklen_t);
        let bufsz: libc::c_int = 1 << 20;
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, &bufsz as *const _ as *const libc::c_void, std::mem::size_of_val(&bufsz) as libc::socklen_t);
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, &bufsz as *const _ as *const libc::c_void, std::mem::size_of_val(&bufsz) as libc::socklen_t);
    }
}

async fn tune(stream: &TcpStream) {
    #[cfg(target_os = "linux")]
    tune_socket(std::os::unix::io::AsRawFd::as_raw_fd(stream));
    #[cfg(not(target_os = "linux"))]
    let _ = stream;
}

/// Build a TLS client connector trusting the given CA/self-signed PEM file.
fn build_connector(cafile: &str) -> std::io::Result<Arc<tokio_rustls::TlsConnector>> {
    use rustls::RootCertStore;
    rustls::crypto::ring::default_provider().install_default().ok();
    let mut roots = RootCertStore::empty();
    let f = std::fs::File::open(cafile)?;
    let mut rd = std::io::BufReader::new(f);
    for cert in rustls_pemfile::certs(&mut rd) {
        roots.add(cert.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    }
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(tokio_rustls::TlsConnector::from(Arc::new(config))))
}

/// Establish a transport connection and, if a token is given, authenticate.
async fn connect_conn(
    addr: &str,
    tls: bool,
    connector: Option<Arc<tokio_rustls::TlsConnector>>,
    token: Option<&[u8]>,
) -> std::io::Result<Conn> {
    let tcp = TcpStream::connect(addr).await?;
    tcp.set_nodelay(true)?;
    tune(&tcp).await;

    let mut conn = if tls {
        let connector = connector.expect("--tls requires --cafile");
        let name = rustls::pki_types::ServerName::try_from(addr.split(':').next().unwrap_or("localhost").to_string())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "bad server name"))?;
        let tls_stream = connector.connect(name, tcp).await.map_err(|e| {
            eprintln!("[conn] TLS handshake failed: {e:?}");
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "tls handshake")
        })?;
        Conn::Tls(tls_stream)
    } else {
        Conn::Plain(tcp)
    };

    if let Some(tok) = token {
        let mut auth = Vec::new();
        encode_auth(&mut auth, tok);
        conn.write_all(&auth).await?;
        // read the auth ack before proceeding
        let mut parser = RingParser::new(64 * 1024);
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            if let Some(f) = parser.try_parse() {
                if f.op != Op::Ack {
                    return Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "auth handshake failed"));
                }
                break;
            }
            let n = conn.read(&mut buf).await?;
            if n == 0 {
                return Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "broker closed during auth"));
            }
            parser.feed(&buf[..n]);
        }
    }
    Ok(conn)
}

#[derive(Parser, Debug)]
#[command(name = "zhensegg-bench")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9090")]
    addr: String,

    #[arg(long, default_value = "bench")]
    topic: String,

    /// Total messages to publish across all producers.
    #[arg(long, default_value = "4000000")]
    msgs: usize,

    #[arg(long, default_value = "32")]
    payload_size: usize,

    /// Number of producer OS threads (one TCP connection each).
    #[arg(long, default_value = "8")]
    producers: usize,

    /// Number of consumer OS threads (one TCP connection each).
    #[arg(long, default_value = "0")]
    consumers: usize,

    /// Batch (in-flight messages per producer round trip).
    #[arg(long, default_value = "256")]
    batch: usize,

    /// Run for N seconds instead of a fixed message count.
    #[arg(long, default_value = "0")]
    secs: u64,

    /// Ring file path; when set, sampled ACKed records are verified on media (O_DIRECT).
    #[arg(long)]
    verify_file: Option<String>,

    /// Max e2e latency samples collected per consumer.
    #[arg(long, default_value = "100000")]
    samples: usize,

    /// Connect over TLS. Requires --cafile to verify the broker cert.
    #[arg(long)]
    tls: bool,

    /// PEM file of the CA/self-signed cert used by the broker's TLS endpoint.
    #[arg(long)]
    cafile: Option<String>,

    /// Shared auth token sent as the first Auth frame (works with or without --tls).
    #[arg(long)]
    auth_token: Option<String>,
}

// ---------------------------------------------------------------------------
// O_DIRECT durability verification of the persistent ring file (Linux only).
// ---------------------------------------------------------------------------
#[cfg(target_os = "linux")]
fn verify_on_disk(path: &str, samples: &[(u64, u64, u64)], payload_size: usize, topic_len: usize) -> (usize, usize, usize) {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;
    let f: std::fs::File = match std::fs::OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(path) {
        Ok(f) => f,
        Err(e) => {
            println!("[verify] cannot open {} with O_DIRECT: {} — SKIPPED (not verified)", path, e);
            return (0, 0, 0);
        }
    };
    let fd = f.as_raw_fd();
    let fsize = match f.metadata() { Ok(m) => m.len() as usize, Err(_) => return (0, 0, 0) };
    let align = 4096;
    let mut ok = 0usize;
    let mut checked = 0usize;
    let mut mismatch = 0usize;
    for &(off, len, ts_sent) in samples {
        let file_off = (off as usize) % fsize;
        if file_off + len as usize > fsize { continue; } // straddles ring boundary
        let start = file_off & !(align - 1);
        let end = (file_off + len as usize + align - 1) & !(align - 1);
        let sz = end - start;
        let mut buf: *mut libc::c_void = std::ptr::null_mut();
        if unsafe { libc::posix_memalign(&mut buf, 4096, sz) } != 0 { continue; }
        let slice = unsafe { std::slice::from_raw_parts_mut(buf as *mut u8, sz) };
        let n = unsafe { libc::pread(fd, buf as *mut libc::c_void, sz, start as i64) };
        if n as usize >= (file_off - start) + len as usize {
            let rec = &slice[(file_off - start)..(file_off - start) + len as usize];
            let need = 8 + topic_len + payload_size;
            if rec.len() >= need {
                let tl = u32::from_be_bytes([rec[0], rec[1], rec[2], rec[3]]) as usize;
                let pl = u32::from_be_bytes([rec[4], rec[5], rec[6], rec[7]]) as usize;
                if tl == topic_len && pl == payload_size {
                    let p = &rec[8 + tl..];
                    let ts_disk = u64::from_be_bytes([p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]]);
                    if ts_disk == ts_sent && p[8..].iter().all(|&b| b == b'x') {
                        ok += 1;
                    } else {
                        mismatch += 1; // region overwritten by a newer record (ring wrap) or corrupted
                    }
                } else {
                    mismatch += 1; // header of a different record => overwritten (ring wrap)
                }
            } else {
                mismatch += 1;
            }
            checked += 1;
        }
        unsafe { libc::free(buf) };
    }
    (ok, checked, mismatch)
}

fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() { return 0; }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn fmt_pcts(v: &mut Vec<u64>, unit: f64) -> String {
    v.sort_unstable();
    let f = |x: u64| format!("{:.1}", x as f64 / unit);
    format!(
        "p50={}us p90={}us p99={}us p99.9={}us max={}us",
        f(pct(v, 50.0)), f(pct(v, 90.0)), f(pct(v, 99.0)), f(pct(v, 99.9)), f(*v.last().unwrap_or(&0))
    )
}

fn main() {
    let args = Args::parse();
    assert!(args.payload_size >= 8, "--payload-size must be >= 8 (first 8 bytes carry the latency timestamp)");
    assert!(args.producers > 0, "--producers must be >= 1");
    println!(
        "[bench] HONEST mode: counted on broker ACK only (closed loop), producer/consumer on separate OS threads | addr={} topic={} payload={}B producers={} consumers={} batch={} msgs={} secs={}",
        args.addr, args.topic, args.payload_size, args.producers, args.consumers, args.batch, args.msgs, args.secs
    );

    let acked = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));
    let batch_rtts: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let e2e_lat: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let disk_samples: Arc<Mutex<Vec<(u64, u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));

    if args.tls && args.cafile.is_none() {
        eprintln!("[bench] --tls requires --cafile");
        std::process::exit(2);
    }
    let connector: Option<Arc<tokio_rustls::TlsConnector>> = if args.tls {
        Some(build_connector(args.cafile.as_ref().unwrap()).unwrap_or_else(|e| {
            eprintln!("[bench] cannot build TLS connector: {e}");
            std::process::exit(2)
        }))
    } else {
        None
    };
    let token: Option<Vec<u8>> = args.auth_token.as_ref().map(|t| t.as_bytes().to_vec());
    if args.tls || args.auth_token.is_some() {
        println!("[bench] transport=secure (tls={} auth={})", args.tls, args.auth_token.is_some());
    }

    // ---- start consumers FIRST so they can subscribe before loads begin ----
    let mut consumer_handles = Vec::new();
    for cid in 0..args.consumers {
        let (addr, topic) = (args.addr.clone(), args.topic.clone());
        let consumed = consumed.clone();
        let e2e = e2e_lat.clone();
        let samples = args.samples;
        let tls = args.tls;
        let connector = connector.clone();
        let token = token.clone();
        let handle = std::thread::Builder::new()
            .name(format!("consumer-{cid}"))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                let _ = rt.block_on(consumer_task(cid, addr, topic, consumed, e2e, samples, tls, connector, token));
            })
            .expect("spawn consumer");
        consumer_handles.push(handle);
    }
    if args.consumers > 0 { std::thread::sleep(Duration::from_millis(500)); }

    let t_start = Instant::now();
    let target_per = if args.secs > 0 { usize::MAX } else { args.msgs / args.producers.max(1) };
    let deadline = if args.secs > 0 { Some(Instant::now() + Duration::from_secs(args.secs)) } else { None };

    // ---- start producers, each on its own OS thread with its own runtime ----
    let mut producer_handles = Vec::new();
    for pid in 0..args.producers {
        let (addr, topic) = (args.addr.clone(), args.topic.clone());
        let acked = acked.clone();
        let rtts = batch_rtts.clone();
        let ds = disk_samples.clone();
        let batch = args.batch;
        let payload_size = args.payload_size;
        let deadline = deadline;
        let tls = args.tls;
        let connector = connector.clone();
        let token = token.clone();
        let handle = std::thread::Builder::new()
            .name(format!("producer-{pid}"))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                let r = rt.block_on(producer_task(pid, addr, topic, payload_size, batch, target_per, deadline, acked, rtts, ds, tls, connector, token));
                if let Err(e) = r { eprintln!("[producer {pid}] err: {e:?}"); }
            })
            .expect("spawn producer");
        producer_handles.push(handle);
    }

    // ---- live progress ----
    let mut last_a = 0u64;
    let mut last_c = 0u64;
    let mut last_t = t_start;
    loop {
        std::thread::sleep(Duration::from_secs(1));
        let a = acked.load(Ordering::Relaxed);
        let c = consumed.load(Ordering::Relaxed);
        let dt = last_t.elapsed().as_secs_f64().max(1e-9);
        println!(
            "[{:>5.1}s] acked {} ({:.0}/s) consumed {} ({:.0}/s)",
            t_start.elapsed().as_secs_f64(), a, (a - last_a) as f64 / dt, c, (c - last_c) as f64 / dt
        );
        last_a = a; last_c = c; last_t = Instant::now();
        let all_done = producer_handles.iter().all(|h| h.is_finished());
        if all_done { break; }
    }
    for h in producer_handles { let _ = h.join(); }
    let t_pub_end = t_start.elapsed().as_secs_f64();

    // ---- drain window: measure backlog the consumer burns off ----
    let drain_until = Instant::now() + Duration::from_secs(5);
    while Instant::now() < drain_until {
        if args.consumers == 0 || consumed.load(Ordering::Relaxed) >= acked.load(Ordering::Relaxed) { break; }
        std::thread::sleep(Duration::from_millis(50));
    }
    // NOTE: we intentionally do NOT join the consumer threads here. A consumer
    // only ends when the broker closes its socket, which the broker does not do
    // for idle connections; joining would hang forever. All e2e samples were
    // already flushed into the shared vec during the drain window. Dropping the
    // handles lets the process exit normally and the OS reclaim the threads.
    drop(consumer_handles);

    let t_total = t_start.elapsed().as_secs_f64();
    let a = acked.load(Ordering::Relaxed);
    let c = consumed.load(Ordering::Relaxed);
    println!("=== HONEST RESULT (acked-only, closed loop, {} producers x {} consumers on separate threads) ===", args.producers, args.consumers);
    println!("publish phase {:.3}s | acked {} => {:.0} msg/s | payload {:.1} MB/s",
        t_pub_end, a, a as f64 / t_pub_end.max(1e-9), a as f64 * args.payload_size as f64 / t_pub_end.max(1e-9) / 1e6);
    if args.consumers > 0 {
        let backlog = a.saturating_sub(c);
        println!("delivery phase {:.3}s total | consumed {} => {:.0} msg/s | backlog {} ({:.2}% of acked)",
            t_total, c, c as f64 / t_total.max(1e-9), backlog, backlog as f64 / a.max(1) as f64 * 100.0);
    }
    let mut br = batch_rtts.lock().unwrap().clone();
    println!("pub->ack batch RTT ({} batches, closed loop): {}", br.len(), fmt_pcts(&mut br, 1000.0));
    if args.consumers > 0 {
        let mut e = e2e_lat.lock().unwrap().clone();
        println!("e2e delivery latency ({} samples): {}", e.len(), fmt_pcts(&mut e, 1000.0));
    }

    // ---- MEGA-HONEST: verify sampled ACKed records are actually on media ----
    if let Some(path) = &args.verify_file {
        let s = disk_samples.lock().unwrap().clone();
        if s.is_empty() {
            println!("[verify] no samples collected — NOT VERIFIED");
        } else {
            #[cfg(target_os = "linux")]
            {
                let (ok, checked, mismatch) = verify_on_disk(path, &s, args.payload_size, args.topic.len());
                println!("disk verification (O_DIRECT, page cache bypassed): {}/{} records content-verified on media, {} mismatch (ring-wrap overwrite or corruption)", ok, checked, mismatch);
                if mismatch > 0 {
                    println!("!!! WARNING: {} sampled records mismatch — if total written volume < ring size this is a DURABILITY VIOLATION", mismatch);
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                let _ = path;
                println!("[verify] O_DIRECT not available on this OS — SKIPPED");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Producer: one OS thread, one TCP connection, closed-loop batched publishing.
// ---------------------------------------------------------------------------
async fn producer_task(
    pid: usize,
    addr: String,
    topic: String,
    payload_size: usize,
    batch: usize,
    target: usize,
    deadline: Option<Instant>,
    acked: Arc<AtomicU64>,
    rtts: Arc<Mutex<Vec<u64>>>,
    disk_samples: Arc<Mutex<Vec<(u64, u64, u64)>>>,
    tls: bool,
    connector: Option<Arc<tokio_rustls::TlsConnector>>,
    token: Option<Vec<u8>>,
) -> std::io::Result<()> {
    let mut stream = connect_conn(&addr, tls, connector, token.as_deref()).await?;

    let topic_b = topic.as_bytes();
    let frame_sz = 4 + 13 + topic_b.len() + payload_size;
    let mut batch_buf = Vec::with_capacity(batch * frame_sz);
    let mut payload = vec![b'x'; payload_size];
    let mut parser = RingParser::new(256 * 1024);
    let mut rbuf = vec![0u8; 256 * 1024];

    let mut done = 0usize;
    let mut sample_cnt = 0u64;
    let mut ts_batch: Vec<u64> = Vec::with_capacity(batch);
    loop {
        if let Some(d) = deadline { if Instant::now() >= d { break; } }
        let b = if deadline.is_none() { (target - done).min(batch) } else { batch };
        if b == 0 { break; }
        batch_buf.clear();
        ts_batch.clear();
        for _ in 0..b {
            let ts = now_ns();
            payload[0..8].copy_from_slice(&ts.to_be_bytes());
            ts_batch.push(ts);
            encode_publish(&mut batch_buf, topic_b, &payload);
        }
        let t_send = Instant::now();
        stream.write_all(&batch_buf).await?;
        // closed loop: wait for ALL b ACKs before sending the next batch
        let mut got = 0usize;
        let mut ack_index = 0usize;
        while got < b {
            let n = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut rbuf)).await
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "ack timeout"))??;
            if n == 0 { return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "broker closed")); }
            parser.feed(&rbuf[..n]);
            while let Some(f) = parser.try_parse() {
                if f.op == Op::Ack {
                    got += 1;
                    ack_index += 1;
                    acked.fetch_add(1, Ordering::Relaxed);
                    if ack_index % 64 == 0 {
                        let ts = ts_batch.get(got - 1).copied().unwrap_or(0);
                        let mut s = disk_samples.lock().unwrap();
                        if s.len() < 4096 {
                            if let (Some(off), Some(l)) = (f.offset, f.len) { s.push((off, l as u64, ts)); }
                        } else {
                            let i = (sample_cnt % 4096) as usize;
                            if let (Some(off), Some(l)) = (f.offset, f.len) { s[i] = (off, l as u64, ts); }
                        }
                        sample_cnt += 1;
                    }
                }
                parser.consume();
            }
        }
        rtts.lock().unwrap().push(t_send.elapsed().as_nanos() as u64);
        done += b;
        let _ = pid;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Consumer: one OS thread, one TCP connection, subscribes and measures e2e.
// ---------------------------------------------------------------------------
async fn consumer_task(
    cid: usize,
    addr: String,
    topic: String,
    consumed: Arc<AtomicU64>,
    e2e: Arc<Mutex<Vec<u64>>>,
    max_samples: usize,
    tls: bool,
    connector: Option<Arc<tokio_rustls::TlsConnector>>,
    token: Option<Vec<u8>>,
) -> std::io::Result<()> {
    let mut stream = connect_conn(&addr, tls, connector, token.as_deref()).await?;
    let mut sub = Vec::new();
    encode_subscribe(&mut sub, topic.as_bytes());
    stream.write_all(&sub).await?;
    let mut parser = RingParser::new(256 * 1024);
    let mut rbuf = vec![0u8; 256 * 1024];
    let mut local: Vec<u64> = Vec::new();
    loop {
        let n = stream.read(&mut rbuf).await?;
        if n == 0 { break; }
        parser.feed(&rbuf[..n]);
        let mut cnt = 0u64;
        while let Some(f) = parser.try_parse() {
            if f.op == Op::Data && f.payload.len() >= 8 {
                cnt += 1;
                if local.len() < max_samples {
                    let ts = u64::from_be_bytes(f.payload[0..8].try_into().unwrap());
                    let d = now_ns().saturating_sub(ts);
                    if d < 60_000_000_000 { local.push(d); } // drop >60s outliers
                }
            }
            parser.consume();
        }
        if cnt > 0 { consumed.fetch_add(cnt, Ordering::Relaxed); }
        if !local.is_empty() {
            e2e.lock().unwrap().extend(local.drain(..));
        }
    }
    e2e.lock().unwrap().extend(local);
    let _ = cid;
    Ok(())
}
