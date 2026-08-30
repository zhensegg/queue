use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use zhensegg::protocol::{Op, Parser as RingParser, encode_auth, encode_publish, encode_subscribe};
use zhensegg::store::file::HEADER_SIZE;

enum Conn {
    Plain(TcpStream),
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

const RING_MAGIC: [u8; 4] = *b"ZSR2";

struct AlignedBuf {
    ptr: *mut libc::c_void,
    len: usize,
}

impl AlignedBuf {
    fn new(len: usize) -> Self {
        let mut ptr: *mut libc::c_void = std::ptr::null_mut();
        let rc = unsafe { libc::posix_memalign(&mut ptr, 4096, len.max(4096)) };
        if rc != 0 {
            panic!("posix_memalign failed");
        }
        AlignedBuf { ptr, len: len.max(4096) }
    }

    fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut u8, self.len) }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len) }
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { libc::free(self.ptr) };
    }
}

struct RingReader {
    f: std::fs::File,
    cap: usize,
    fsize: usize,
    win: AlignedBuf,
    win_valid: usize,
    win_lo: u64,
}

fn pread_exact_od(f: &std::fs::File, fsize: usize, phys: u64, dst: &mut [u8]) -> std::io::Result<()> {
    let align = 4096usize;
    let block_start = (phys as usize / align) * align;
    let end = phys as usize + dst.len();
    let blocks = ((end - block_start) + align - 1) / align * align;
    let bounce = AlignedBuf::new(blocks);
    let mut got = 0usize;
    while got < blocks {
        let n = unsafe {
            libc::pread(
                std::os::unix::io::AsRawFd::as_raw_fd(f),
                (bounce.ptr as *mut u8).add(got) as *mut libc::c_void,
                blocks - got,
                (block_start + got) as libc::off_t,
            )
        };
        if n < 0 {
            return Err(std::io::Error::last_os_error());
        }
        if n == 0 {
            break;
        }
        got += n as usize;
    }
    let skip = phys as usize - block_start;
    if got < skip + dst.len() {
        return Err(std::io::Error::other("read past end of ring file"));
    }
    dst.copy_from_slice(&bounce.as_slice()[skip..skip + dst.len()]);
    Ok(())
}

impl RingReader {
    fn open(path: &str) -> std::io::Result<Self> {
        use std::os::unix::fs::OpenOptionsExt;
        let f = std::fs::OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(path)?;
        let fsize = f.metadata()?.len() as usize;
        if fsize < HEADER_SIZE as usize + 4096 {
            return Err(std::io::Error::other("ring file too small"));
        }
        let cap = fsize - HEADER_SIZE as usize;
        Ok(RingReader { f, cap, fsize, win: AlignedBuf::new(4 * 1024 * 1024), win_valid: 0, win_lo: 0 })
    }

    fn ensure(&mut self, logical: u64, need: usize) -> std::io::Result<&[u8]> {
        if need > self.win.len() {
            return Err(std::io::Error::other("record larger than read window"));
        }
        if logical >= self.win_lo
            && (logical - self.win_lo) as usize + need <= self.win_valid
        {
            let off = (logical - self.win_lo) as usize;
            return Ok(&self.win.as_slice()[off..off + need]);
        }
        let total = need.max(4 * 1024 * 1024).min(self.win.len());
        let f = &self.f;
        let cap = self.cap;
        let fsize = self.fsize;
        let buf = self.win.as_slice_mut();
        let mut done = 0usize;
        while done < total {
            let lp = logical + done as u64;
            let off_in_area = (lp as usize) % cap;
            let phys = HEADER_SIZE as usize + off_in_area;
            let can = (cap - off_in_area).min(total - done);
            let readable = fsize.saturating_sub(phys).min(can);
            if readable < can {
                return Err(std::io::Error::other("logical range beyond ring file"));
            }
            pread_exact_od(f, fsize, phys as u64, &mut buf[done..done + can])?;
            done += can;
        }
        self.win_lo = logical;
        self.win_valid = total;
        Ok(&self.win.as_slice()[..total])
    }
}

fn verify_ring_mode(path: &str) -> i32 {
    let mut r = match RingReader::open(path) {
        Ok(r) => r,
        Err(e) => {
            println!("[verify-ring] cannot open {path} with O_DIRECT: {e}");
            return 2;
        }
    };
    let mut hdr = [0u8; 512];
    if let Err(e) = pread_exact_od(&r.f, r.fsize, 0, &mut hdr) {
        println!("[verify-ring] header read failed: {e}");
        return 2;
    }
    if hdr[0..4] != RING_MAGIC {
        println!("[verify-ring] bad magic (expected ZSR2, got {:?})", &hdr[0..4]);
        return 1;
    }
    let slot = |i: usize| -> (u64, u64) {
        let base = i * 64;
        let wp = u64::from_be_bytes(hdr[base + 8..base + 16].try_into().unwrap());
        let cm = u64::from_be_bytes(hdr[base + 16..base + 24].try_into().unwrap());
        (wp, cm)
    };
    let (wp1, cm1) = slot(0);
    let (wp2, cm2) = slot(1);
    if cm1 != cm2 || wp1 != wp2 {
        println!("[verify-ring] header slots diverge (wp {wp1}/{wp2}, committed {cm1}/{cm2}) — torn header");
        return 1;
    }
    let (write_pos, committed) = (wp1, cm1);
    if committed > write_pos {
        println!("[verify-ring] committed > write_pos in header");
        return 1;
    }
    let cap = r.cap as u64;
    let start = committed.saturating_sub(cap);

    let find_boundary = |r: &mut RingReader, from: u64, limit: u64| -> Option<u64> {
        let scan_len = 64 * 1024usize;
        let mut probe = from;
        while probe < limit {
            let win = match r.ensure(probe, scan_len) {
                Ok(w) => w,
                Err(e) => {
                    println!("[verify-ring] boundary scan ensure error at probe {probe}: {e}");
                    return None;
                }
            };
            let bytes = scan_len.min((limit - probe) as usize);
            for o in 0..bytes.saturating_sub(8) {
                let b = &win[o..];
                let tl = u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as u64;
                let pl = u32::from_be_bytes([b[4], b[5], b[6], b[7]]) as u64;
                let rl = 8 + tl + pl;
                if rl < 8 || tl > 64 * 1024 || pl > 64 * 1024 {
                    continue;
                }
                let mut p = o as u64;
                let mut chain = 0u32;
                while chain < 16 {
                    if p + 8 > bytes as u64 {
                        break;
                    }
                    let q = &win[p as usize..];
                    let t2 = u32::from_be_bytes([q[0], q[1], q[2], q[3]]) as u64;
                    let l2 = u32::from_be_bytes([q[4], q[5], q[6], q[7]]) as u64;
                    let r2 = 8 + t2 + l2;
                    if r2 < 8 || t2 > 64 * 1024 || l2 > 64 * 1024 {
                        break;
                    }
                    p += r2;
                    chain += 1;
                }
                if chain >= 8 {
                    return Some(probe + o as u64);
                }
            }
            probe += (scan_len as u64) - 64;
        }
        None
    };

    let mut pos = match find_boundary(&mut r, start, committed) {
        Some(p) => p,
        None => {
            println!("[verify-ring] no record boundary found anywhere in window");
            return 1;
        }
    };
    let mut records = 0u64;
    let mut seams = 0u64;
    let mut hard = 0u64;
    while pos < committed {
        let win = match r.ensure(pos, 8) {
            Ok(w) => w,
            Err(e) => {
                hard += 1;
                println!("[verify-ring] walk ensure error at logical {pos} (phys {}): {e}", HEADER_SIZE as u64 + pos % (r.cap as u64));
                break;
            }
        };
        let tl = u32::from_be_bytes([win[0], win[1], win[2], win[3]]) as u64;
        let pl = u32::from_be_bytes([win[4], win[5], win[6], win[7]]) as u64;
        let rec_len = 8 + tl + pl;
        if rec_len >= 8 && rec_len <= cap / 2 && tl <= 1024 * 1024 && pos + rec_len <= committed {
            pos += rec_len;
            records += 1;
            continue;
        }
        match find_boundary(&mut r, pos + 1, (pos + 4 * 1024 * 1024).min(committed)) {
            Some(np) => {
                pos = np;
                seams += 1;
            }
            None => {
                hard += 1;
                let dbg_len = 96usize;
                match r.ensure(pos, dbg_len) {
                    Ok(dbg) => {
                        let hex: Vec<String> = dbg[..dbg_len].iter().map(|b| format!("{b:02x}")).collect();
                        println!("[verify-ring] hexdump at logical {pos} (phys {}): {}", HEADER_SIZE as u64 + pos % cap, hex.join(" "));
                    }
                    Err(e) => println!("[verify-ring] hexdump ensure failed at logical {pos}: {e}"),
                }
                println!("[verify-ring] hard structural break at logical {pos} (tl={tl} pl={pl}), no boundary within 4 MiB ahead");
                break;
            }
        }
    }
    let window_bytes = (committed - start).min(cap);
    let ok = if hard == 0 && pos == committed {
        println!(
            "[verify-ring] OK: header committed={committed} write_pos={write_pos}; chain reaches committed exactly: {records} records walked, {seams} generation seams re-synced, window {:.1} MiB (O_DIRECT)",
            window_bytes as f64 / (1024.0 * 1024.0)
        );
        0
    } else {
        println!("[verify-ring] FAILED: hard breaks={hard}, stopped at {pos}/{committed}, records={records}, seams={seams}");
        1
    };
    ok
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

    #[arg(long, default_value = "4000000")]
    msgs: usize,

    #[arg(long, default_value = "32")]
    payload_size: usize,

    #[arg(long, default_value = "8")]
    producers: usize,

    #[arg(long, default_value = "0")]
    consumers: usize,

    #[arg(long, default_value = "256")]
    batch: usize,

    #[arg(long, default_value = "0")]
    secs: u64,

    #[arg(long)]
    verify_file: Option<String>,

    #[arg(long)]
    verify_ring: Option<String>,

    #[arg(long, default_value = "500000")]
    samples: usize,

    #[arg(long)]
    tls: bool,

    #[arg(long)]
    cafile: Option<String>,

    #[arg(long)]
    auth_token: Option<String>,
}

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
        let data_cap = fsize as u64 - HEADER_SIZE;
        let file_off = (HEADER_SIZE + off % data_cap) as usize;
        if file_off + len as usize > fsize { continue; } 
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
                        mismatch += 1; 
                    }
                } else {
                    mismatch += 1; 
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
    if let Some(path) = &args.verify_ring {
        std::process::exit(verify_ring_mode(path));
    }
    assert!(args.payload_size >= 8, "--payload-size must be >= 8 (first 8 bytes carry the latency timestamp)");
    assert!(args.producers > 0, "--producers must be >= 1");
    println!(
        "[bench] HONEST mode: counted on broker ACK only (closed loop), producer/consumer on separate OS threads | addr={} topic={} payload={}B producers={} consumers={} batch={} msgs={} secs={}",
        args.addr, args.topic, args.payload_size, args.producers, args.consumers, args.batch, args.msgs, args.secs
    );

    let acked = Arc::new(AtomicU64::new(0));
    let rejected = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));
    let batch_rtts: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let e2e_slots: Vec<Arc<Mutex<Vec<u64>>>> = (0..args.consumers).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
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

    let mut consumer_handles = Vec::new();
    for cid in 0..args.consumers {
        let (addr, topic) = (args.addr.clone(), args.topic.clone());
        let consumed = consumed.clone();
        let e2e = e2e_slots[cid].clone();
        let samples = args.samples;
        let tls = args.tls;
        let connector = connector.clone();
        let token = token.clone();
        let handle = std::thread::Builder::new()
            .name(format!("consumer-{cid}"))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                if let Err(e) = rt.block_on(consumer_task(cid, addr, topic, consumed, e2e, samples, tls, connector, token)) {
                    eprintln!("[consumer {cid}] finished with error: {e:?}");
                }
            })
            .expect("spawn consumer");
        consumer_handles.push(handle);
    }
    if args.consumers > 0 { std::thread::sleep(Duration::from_millis(500)); }

    let t_start = Instant::now();
    let target_per = if args.secs > 0 { usize::MAX } else { args.msgs / args.producers.max(1) };
    let deadline = if args.secs > 0 { Some(Instant::now() + Duration::from_secs(args.secs)) } else { None };

    let mut producer_handles = Vec::new();
    for pid in 0..args.producers {
        let (addr, topic) = (args.addr.clone(), args.topic.clone());
        let acked = acked.clone();
        let rejected = rejected.clone();
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
                let r = rt.block_on(producer_task(pid, addr, topic, payload_size, batch, target_per, deadline, acked, rejected, rtts, ds, tls, connector, token));
                if let Err(e) = r { eprintln!("[producer {pid}] err: {e:?}"); }
            })
            .expect("spawn producer");
        producer_handles.push(handle);
    }

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

    let drain_until = Instant::now() + Duration::from_secs(5);
    while Instant::now() < drain_until {
        if args.consumers == 0 || consumed.load(Ordering::Relaxed) >= acked.load(Ordering::Relaxed) { break; }
        std::thread::sleep(Duration::from_millis(50));
    }
    
    drop(consumer_handles);

    let t_total = t_start.elapsed().as_secs_f64();
    let a = acked.load(Ordering::Relaxed);
    let r = rejected.load(Ordering::Relaxed);
    let c = consumed.load(Ordering::Relaxed);
    println!("=== HONEST RESULT (acked-only, closed loop, {} producers x {} consumers on separate threads) ===", args.producers, args.consumers);
    println!("publish phase {:.3}s | acked {} => {:.0} msg/s | payload {:.1} MB/s",
        t_pub_end, a, a as f64 / t_pub_end.max(1e-9), a as f64 * args.payload_size as f64 / t_pub_end.max(1e-9) / 1e6);
    if r > 0 {
        println!("publish NACKed (overflow reject): {} => {:.0}/s", r, r as f64 / t_pub_end.max(1e-9));
    }
    if args.consumers > 0 {
        let backlog = a.saturating_sub(c);
        println!("delivery phase {:.3}s total | consumed {} => {:.0} msg/s | backlog {} ({:.2}% of acked)",
            t_total, c, c as f64 / t_total.max(1e-9), backlog, backlog as f64 / a.max(1) as f64 * 100.0);
    }
    let mut br = batch_rtts.lock().unwrap().clone();
    println!("pub->ack batch RTT ({} batches, closed loop): {}", br.len(), fmt_pcts(&mut br, 1000.0));
    if args.consumers > 0 {
        let mut e: Vec<u64> = Vec::new();
        for slot in &e2e_slots {
            e.extend(slot.lock().unwrap().iter().copied());
        }
        println!("e2e delivery latency ({} samples): {}", e.len(), fmt_pcts(&mut e, 1000.0));
    }

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

async fn producer_task(
    pid: usize,
    addr: String,
    topic: String,
    payload_size: usize,
    batch: usize,
    target: usize,
    deadline: Option<Instant>,
    acked: Arc<AtomicU64>,
    rejected: Arc<AtomicU64>,
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
                } else if f.op == Op::Error {
                    got += 1;
                    rejected.fetch_add(1, Ordering::Relaxed);
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
    let mut local: Vec<u64> = Vec::with_capacity(max_samples.min(1 << 20));
    let mut local_cnt: u64 = 0;
    let mut last_flush = Instant::now();
    loop {
        let n = stream.read(&mut rbuf).await?;
        if n == 0 { break; }
        parser.feed(&rbuf[..n]);
        let mut cnt = 0u64;
        while let Some(f) = parser.try_parse() {
            if f.op == Op::Data && f.payload.len() >= 8 {
                cnt += 1;
                let ts = u64::from_be_bytes(f.payload[0..8].try_into().unwrap());
                let d = now_ns().saturating_sub(ts);
                if d < 60_000_000_000 {
                    if local.len() < max_samples {
                        local.push(d);
                    } else {
                        local[(local_cnt % max_samples as u64) as usize] = d;
                    }
                    local_cnt += 1;
                }
            }
            parser.consume();
        }
        if cnt > 0 { consumed.fetch_add(cnt, Ordering::Relaxed); }
        if last_flush.elapsed() >= Duration::from_secs(2) {
            let mut g = e2e.lock().unwrap();
            g.clear();
            g.extend_from_slice(&local);
            drop(g);
            last_flush = Instant::now();
        }
    }
    {
        let mut g = e2e.lock().unwrap();
        g.clear();
        g.extend_from_slice(&local);
    }
    let _ = cid;
    Ok(())
}
