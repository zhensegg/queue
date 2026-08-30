use clap::Parser as ClapParser;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use zhensegg::protocol::{encode_ack, encode_data, Op, Parser as ZParser};

// ===== provided-buffers style slab (step3): per-thread free-list, zero alloc on hot path =====
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

#[cfg(target_os = "linux")]
fn bind_reuse_port(addr: &str) -> std::io::Result<std::net::TcpListener> {
    use socket2::{Domain, SockAddr, Socket, Type};
    use std::os::unix::io::AsRawFd;
    let std_addr: std::net::SocketAddr = addr.parse().expect("invalid addr");
    let domain = Domain::for_address(std_addr);
    let socket = Socket::new(domain, Type::STREAM, None)?;
    socket.set_reuse_address(true)?;
    // SO_REUSEPORT via libc (socket2 0.5 set_reuse_port may not be available)
    let optval: libc::c_int = 1;
    let ret = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_REUSEPORT,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of_val(&optval) as libc::socklen_t,
        )
    };
    if ret != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // step7: buffers on listener (accepted sockets inherit) + busy poll best-effort
    let bufsz: libc::c_int = 1 << 20;
    unsafe {
        let _ = libc::setsockopt(socket.as_raw_fd(), libc::SOL_SOCKET, libc::SO_RCVBUF, &bufsz as *const _ as *const libc::c_void, std::mem::size_of_val(&bufsz) as libc::socklen_t);
        let _ = libc::setsockopt(socket.as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF, &bufsz as *const _ as *const libc::c_void, std::mem::size_of_val(&bufsz) as libc::socklen_t);
        let busy: libc::c_int = 50;
        let _ = libc::setsockopt(socket.as_raw_fd(), libc::SOL_SOCKET, libc::SO_BUSY_POLL, &busy as *const _ as *const libc::c_void, std::mem::size_of_val(&busy) as libc::socklen_t);
    }
    socket.set_nonblocking(true)?;
    socket.bind(&SockAddr::from(std_addr))?;
    socket.listen(4096)?;
    Ok(socket.into())
}

// step7: per-connection socket tuning (NODELAY, QUICKACK, buffers, busy poll)
#[cfg(target_os = "linux")]
fn tune_socket(fd: std::os::unix::io::RawFd) {
    unsafe {
        let one: libc::c_int = 1;
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, &one as *const _ as *const libc::c_void, std::mem::size_of_val(&one) as libc::socklen_t);
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_QUICKACK, &one as *const _ as *const libc::c_void, std::mem::size_of_val(&one) as libc::socklen_t);
        let bufsz: libc::c_int = 1 << 20;
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, &bufsz as *const _ as *const libc::c_void, std::mem::size_of_val(&bufsz) as libc::socklen_t);
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, &bufsz as *const _ as *const libc::c_void, std::mem::size_of_val(&bufsz) as libc::socklen_t);
        let busy: libc::c_int = 50;
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_BUSY_POLL, &busy as *const _ as *const libc::c_void, std::mem::size_of_val(&busy) as libc::socklen_t);
    }
}

#[derive(ClapParser, Debug, Clone)]
#[command(name="zhensegg-broker")]
struct Args {
    #[arg(long, default_value="0.0.0.0:9090")]
    addr: String,
    #[arg(long, default_value="1")]
    cores: usize,
    #[arg(long, default_value="mem")]
    mode: String, // mem or file
    #[arg(long, default_value="256")]
    mem_mb: usize,
    #[arg(long, default_value="/tmp/zhensegg.ring")]
    file: String,
    #[arg(long, default_value="1000000")]
    ring_capacity_mb: usize,
}

type SubscriberMap = std::sync::Arc<SubMap>;

// ===== step5: sharded subscriber map (FNV-1a, 64 shards) — hot path takes 1 shard read lock, no global contention =====
struct SubMap {
    shards: Vec<parking_lot::RwLock<HashMap<Vec<u8>, Vec<Arc<Subscriber>>>>>,
    mask: usize,
}

impl SubMap {
    fn new(n: usize) -> Self {
        let n = n.next_power_of_two();
        Self {
            shards: (0..n).map(|_| parking_lot::RwLock::new(HashMap::new())).collect(),
            mask: n - 1,
        }
    }

    #[inline]
    fn idx(&self, topic: &[u8]) -> usize {
        // FNV-1a 64
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in topic {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        (h as usize) & self.mask
    }

    #[inline]
    fn read(&self, topic: &[u8]) -> parking_lot::RwLockReadGuard<'_, HashMap<Vec<u8>, Vec<Arc<Subscriber>>>> {
        self.shards[self.idx(topic)].read()
    }

    #[inline]
    fn write(&self, topic: &[u8]) -> parking_lot::RwLockWriteGuard<'_, HashMap<Vec<u8>, Vec<Arc<Subscriber>>>> {
        self.shards[self.idx(topic)].write()
    }
}

struct Subscriber {
    id: u64,
    // channel to send data frames (already encoded) - tokio async, Arc to avoid per-subscriber clone copy
    tx: tokio::sync::mpsc::UnboundedSender<Arc<Vec<u8>>>,
}

fn main() {
    let args = Args::parse();
    println!("[zhensegg] start addr={} cores={} mode={} mem={}MB", args.addr, args.cores, args.mode, args.mem_mb);
    println!("[zhensegg] target: >11M in-mem, >2M persisted");

    #[cfg(all(target_os="linux", feature="uring"))]
    {
        if args.cores == 1 {
            run_monoio_single(args);
        } else {
            run_monoio_multicore(args);
        }
        return;
    }
    // fallback tokio (also on linux without uring feature)
    #[cfg(not(all(target_os="linux", feature="uring")))]
    {
        if cfg!(target_os="linux") {
            println!("[zhensegg] running tokio fallback (build with --features uring for monoio/io_uring)");
        } else {
            println!("[zhensegg] monoio not available on this OS, fallback to tokio");
        }
        run_tokio_fallback(args);
    }
}

#[cfg(all(target_os="linux", feature="uring"))]
type MonoioRt = monoio::Runtime<monoio::time::TimeDriver<monoio::IoUringDriver>>;

#[cfg(all(target_os="linux", feature="uring"))]
fn build_monoio_rt(cid: usize, sqpoll_cpu: Option<u32>) -> MonoioRt {
    // attempt 1: SQPOLL (kernel poller thread, no submit syscall) + COOP_TASKRUN
    // requires CAP_SYS_NICE or kernel.io_uring unprivileged sqpoll allowed
    {
        let mut urb = io_uring::IoUring::builder();
        urb.setup_sqpoll(1000);
        if let Some(cpu) = sqpoll_cpu {
            urb.setup_sqpoll_cpu(cpu);
        }
        urb.setup_coop_taskrun().setup_single_issuer();
        if let Ok(rt) = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .with_entries(4096)
            .uring_builder(urb)
            .enable_timer()
            .build()
        {
            eprintln!("[rt {}] io_uring: SQPOLL+COOP_TASKRUN+SQ_AFF", cid);
            return rt;
        }
    }
    // attempt 2: COOP_TASKRUN only (no kernel poller thread)
    {
        let mut urb = io_uring::IoUring::builder();
        urb.setup_coop_taskrun();
        urb.setup_single_issuer();
        if let Ok(rt) = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .with_entries(4096)
            .uring_builder(urb)
            .enable_timer()
            .build()
        {
            eprintln!("[rt {}] io_uring: COOP_TASKRUN+SINGLE_ISSUER", cid);
            return rt;
        }
    }
    // attempt 3: plain io_uring
    eprintln!("[rt {}] io_uring: default flags", cid);
    monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .with_entries(4096)
        .enable_timer()
        .build()
        .expect("monoio rt")
}

#[cfg(all(target_os="linux", feature="uring"))]
fn run_monoio_single(args: Args) {
    // shared store
    let store: Arc<dyn zhensegg::Store> = if args.mode == "file" {
        let cap = args.ring_capacity_mb * 1024 * 1024;
        let fr = zhensegg::FileRing::new(&args.file, cap).expect("file ring");
        Arc::new(fr)
    } else {
        let cap = args.mem_mb * 1024 * 1024;
        Arc::new(zhensegg::MemRing::new(cap))
    };
    let subs: SubscriberMap = Arc::new(SubMap::new(64));

    let mut rt = build_monoio_rt(0, Some(0));
    rt.block_on(async move {
        monoio_broker_loop(args.addr, store, subs).await;
    });
}

#[cfg(all(target_os="linux", feature="uring"))]
fn run_monoio_multicore(args: Args) {
    let cores = args.cores;
    let addr = args.addr.clone();
    let mode = args.mode.clone();
    let mem_mb = args.mem_mb;
    let file = args.file.clone();
    let ring_cap = args.ring_capacity_mb;

    // shared store across cores (Arc)
    let store: Arc<dyn zhensegg::Store> = if mode == "file" {
        let fr = zhensegg::FileRing::new(&file, ring_cap*1024*1024).expect("file ring");
        Arc::new(fr)
    } else {
        Arc::new(zhensegg::MemRing::new(mem_mb*1024*1024))
    };
    let subs: SubscriberMap = Arc::new(SubMap::new(64));

    let mut handles = Vec::new();
    for cid in 0..cores {
        let addr_c = addr.clone();
        let store_c = store.clone();
        let subs_c = subs.clone();
        let h = std::thread::Builder::new()
            .name(format!("zhensegg-{}", cid))
            .spawn(move || {
                // pin to core if possible
                #[cfg(target_os="linux")]
                {
                    let _ = core_affinity_attempt(cid);
                }
                let mut rt = build_monoio_rt(cid, Some(cid as u32));
                rt.block_on(async move {
                    println!("[core {}] listening {}", cid, addr_c);
                    monoio_broker_loop(addr_c, store_c, subs_c).await;
                });
            }).expect("spawn");
        handles.push(h);
    }
    for h in handles { let _ = h.join(); }
}

#[cfg(target_os = "linux")]
fn core_affinity_attempt(core_id: usize) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::mem;
        unsafe {
            let mut set: libc::cpu_set_t = mem::zeroed();
            libc::CPU_ZERO(&mut set);
            libc::CPU_SET(core_id, &mut set);
            let ret = libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &set);
            if ret != 0 { return Err(std::io::Error::last_os_error()); }
        }
    }
    Ok(())
}

#[cfg(all(target_os="linux", feature="uring"))]
async fn monoio_broker_loop(addr: String, store: Arc<dyn zhensegg::Store>, subs: SubscriberMap) {
    use monoio::net::TcpListener;

    let listener = {
        #[cfg(target_os = "linux")]
        {
            match bind_reuse_port(&addr) {
                Ok(std_listener) => TcpListener::from_std(std_listener).expect("from_std"),
                Err(e) => {
                    eprintln!("[monoio] reuse_port bind failed {e}, fallback to bind");
                    TcpListener::bind(&addr).expect("bind")
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            TcpListener::bind(&addr).expect("bind")
        }
    };
    println!("[monoio] listening on {}", addr);
    let mut next_id: u64 = 0;
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                next_id += 1;
                let sid = next_id;
                let store_c = store.clone();
                let subs_c = subs.clone();
                monoio::spawn(async move {
                    if let Err(e) = handle_monoio_conn(stream, peer, sid, store_c, subs_c).await {
                        // eprintln!("[conn {}] err: {:?}", sid, e);
                    }
                });
            }
            Err(e) => {
                eprintln!("[listener] accept err: {:?}", e);
                monoio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }
}

#[cfg(all(target_os="linux", feature="uring"))]
async fn handle_monoio_conn(
    mut stream: monoio::net::TcpStream,
    _peer: std::net::SocketAddr,
    id: u64,
    store: Arc<dyn zhensegg::Store>,
    subs: SubscriberMap,
) -> std::io::Result<()> {
    use monoio::io::{AsyncReadRent, AsyncWriteRentExt, Splitable};

    // step7: socket tuning on accepted connection
    #[cfg(target_os = "linux")]
    tune_socket(std::os::unix::io::AsRawFd::as_raw_fd(&stream));
    let _ = stream.set_nodelay(true);

    // per-connection state
    let mut parser = ZParser::new(64 * 1024);
    let mut read_buf = vec![0u8; 64 * 1024];
    let mut write_buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    // subscribe tracking for cleanup
    let mut my_topics: Vec<Vec<u8>> = Vec::new();
    // outbound channel for fan-out messages destined to this conn (if this conn is a subscriber)
    // We use unbounded tokio mpsc channel that writer task drains with batching (works also for monoio)
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Arc<Vec<u8>>>();
    // We need to handle both reading and writing concurrently.
    // Monoio: we can use futures::join? For MVP, we handle read loop and poll rx with timeout.
    // Simplified: we interleave non-blocking check of rx after each read.
    // Better: spawn a writer task that owns the write half. But monoio TcpStream is not split easily without Arc.
    // For MVP, we use single task that selects between reading and forwarding.
    // We'll implement a loop that tries to read with timeout 1ms, and also flushes pending outbound.

    // Instead, we will share the Tx in subscriber map, and this loop will also poll rx and write out.

    // To allow concurrent writes from publishers, we need to store tx clone in subs map.
    // We'll do that on SUBSCRIBE op.

    // For outbound batching we accumulate Vec<u8> buffers and flush via write_all once per iteration.

    // split stream for per-connection concurrent read/write (true thread-per-core)
    let (mut read_half, mut write_half) = stream.into_split();
    // writer task: batched io_uring write
    let mut rx_writer = rx;
    let writer = monoio::spawn(async move {
        let mut pending: Vec<Arc<Vec<u8>>> = Vec::with_capacity(256);
        loop {
            let first = rx_writer.recv().await;
            if first.is_none() {
                break;
            }
            pending.push(first.unwrap());
            while pending.len() < 256 {
                match rx_writer.try_recv() {
                    Ok(m) => pending.push(m),
                    Err(_) => break,
                }
            }
            let total: usize = pending.iter().map(|v| v.len()).sum();
            let mut out = take_buf(total);
            for m in pending.drain(..) {
                // recycle buffer backing if this is the last Arc holder (provided-buffers style slab)
                if Arc::strong_count(&m) == 1 {
                    let raw = Arc::try_unwrap(m).unwrap_or_default();
                    out.extend_from_slice(&raw);
                    give_buf(raw);
                } else {
                    out.extend_from_slice(&m);
                }
            }
            let (res, ret): (std::io::Result<usize>, Vec<u8>) = monoio::io::AsyncWriteRentExt::write_all(&mut write_half, out).await;
            give_buf(ret);
            if res.is_err() {
                break;
            }
        }
    });
    loop {
        let (res, buf): (std::io::Result<usize>, Vec<u8>) = monoio::io::AsyncReadRent::read(&mut read_half, read_buf).await;
        read_buf = buf;
        let n = match res {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        parser.feed(&read_buf[..n]);

        // drain complete frames zero-alloc
        while let Some(frame) = parser.try_parse() {
            match frame.op {
                Op::Publish => {
                    let topic_slice = frame.topic;
                    let payload_slice = frame.payload;
                    let (offset, rec_len) = store.append(topic_slice, payload_slice).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
                    let mut ack = take_buf(32);
                    encode_ack(&mut ack, topic_slice, offset, rec_len);
                    let _ = tx.send(Arc::new(ack));
                    let subs_guard = subs.read(topic_slice);
                    if let Some(list) = subs_guard.get(topic_slice) {
                        if !list.is_empty() {
                            let mut data = take_buf(13 + topic_slice.len() + payload_slice.len());
                            encode_data(&mut data, topic_slice, payload_slice);
                            let arc = Arc::new(data);
                            for sub in list.iter() {
                                let _ = sub.tx.send(arc.clone());
                            }
                        }
                    }
                    // Also try NOTIFY path for offset subscribers: if we had separate list, but for now same.

                    // metrics could be incremented here
                }
                Op::Subscribe => {
                    let topic = frame.topic.to_vec();
                    let sub = Arc::new(Subscriber { id, tx: tx.clone() });
                    {
                        let mut g = subs.write(&topic);
                        g.entry(topic.clone()).or_default().push(sub);
                    }
                    my_topics.push(topic.clone());
                    let mut ack = take_buf(32);
                    encode_ack(&mut ack, &topic, 0, 0);
                    let _ = tx.send(Arc::new(ack));
                }
                Op::Fetch => {
                    if frame.payload.len() >= 12 {
                        let off = u64::from_be_bytes(frame.payload[0..8].try_into().unwrap());
                        let len = u32::from_be_bytes(frame.payload[8..12].try_into().unwrap());
                        let topic = frame.topic.to_vec();
                        let mut raw = Vec::new();
                        match store.read(off, len, &mut raw) {
                            Ok(()) => {
                                if raw.len() >= 8 {
                                    let tl = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
                                    let pl = u32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]) as usize;
                                    if raw.len() >= 8+tl+pl {
                                        let stored_topic = &raw[8..8+tl];
                                        let stored_payload = &raw[8+tl..8+tl+pl];
                                        let mut data = Vec::with_capacity(13+stored_topic.len()+stored_payload.len());
                                        encode_data(&mut data, stored_topic, stored_payload);
                                        let _ = tx.send(Arc::new(data));
                                    }
                                }
                            }
                            Err(_) => {
                                let mut err = Vec::new();
                                encode_ack(&mut err, &topic, 0, 0);
                                let _ = tx.send(Arc::new(err));
                            }
                        }
                    }
                }
                Op::Ping => {
                    let mut pong = Vec::new();
                    encode_ack(&mut pong, b"pong", 0, 0);
                    let _ = tx.send(Arc::new(pong));
                }
                _ => {}
            }
            parser.consume();
        }
    }
    drop(tx);
    let _ = writer.await;
    // cleanup subscriptions (per-topic shard lock)
    for t in my_topics {
        let mut g = subs.write(&t);
        if let Some(list) = g.get_mut(&t) {
            list.retain(|s| s.id != id);
            if list.is_empty() { g.remove(&t); }
        }
    }
    let _ = Ok::<(), std::io::Error>(());
    Ok(())
}

// Fallback tokio with optional per-core SO_REUSEPORT sharding
fn run_tokio_fallback(args: Args) {
    if args.cores <= 1 {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .expect("tokio rt");
        rt.block_on(async move {
            tokio_broker_loop(args).await;
        });
    } else {
        // per-core sharding with SO_REUSEPORT (concept linear scalability)
        let cores = args.cores;
        let store: Arc<dyn zhensegg::Store> = if args.mode == "file" {
            let cap = args.ring_capacity_mb * 1024 * 1024;
            let fr = zhensegg::FileRing::new(&args.file, cap).expect("file ring");
            Arc::new(fr)
        } else {
            let cap = args.mem_mb * 1024 * 1024;
            Arc::new(zhensegg::MemRing::new(cap))
        };
        let subs: SubscriberMap = Arc::new(SubMap::new(64));
        let mut handles = Vec::new();
        for cid in 0..cores {
            let addr_c = args.addr.clone();
            let store_c = store.clone();
            let subs_c = subs.clone();
            let mode_c = args.mode.clone();
            let h = std::thread::Builder::new()
                .name(format!("tokio-{}", cid))
                .spawn(move || {
                    #[cfg(target_os = "linux")]
                    let _ = core_affinity_attempt(cid);
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("tokio rt current_thread");
                    rt.block_on(async move {
                        println!("[tokio core {}] listening {} mode={} (reuse_port)", cid, addr_c, mode_c);
                        tokio_broker_loop_shared(addr_c, store_c, subs_c).await;
                    });
                })
                .expect("spawn");
            handles.push(h);
        }
        for h in handles {
            let _ = h.join();
        }
    }
}

async fn tokio_broker_loop_shared(addr: String, store: Arc<dyn zhensegg::Store>, subs: SubscriberMap) {
    use tokio::net::TcpListener;
    let listener = {
        #[cfg(target_os = "linux")]
        {
            match bind_reuse_port(&addr) {
                Ok(std_listener) => TcpListener::from_std(std_listener).expect("from_std"),
                Err(e) => {
                    eprintln!("[tokio] reuse_port bind failed {e}, fallback to bind");
                    TcpListener::bind(&addr).await.expect("bind tokio")
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            TcpListener::bind(&addr).await.expect("bind tokio")
        }
    };
    let mut next_id: u64 = 0;
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                next_id += 1;
                let id = next_id;
                let store_c = store.clone();
                let subs_c = subs.clone();
                tokio::spawn(async move {
                    if let Err(_e) = handle_tokio_conn(socket, id, store_c, subs_c).await {}
                });
            }
            Err(e) => eprintln!("accept err {:?}", e),
        }
    }
}

async fn tokio_broker_loop(args: Args) {
    use tokio::net::TcpListener;
    use tokio::io::{AsyncReadExt, AsyncWriteExt as TokioWriteExt};

    let store: Arc<dyn zhensegg::Store> = if args.mode == "file" {
        let cap = args.ring_capacity_mb * 1024 * 1024;
        let fr = zhensegg::FileRing::new(&args.file, cap).expect("file ring");
        Arc::new(fr)
    } else {
        let cap = args.mem_mb * 1024 * 1024;
        Arc::new(zhensegg::MemRing::new(cap))
    };
    let subs: SubscriberMap = Arc::new(SubMap::new(64));
    let listener = {
        #[cfg(target_os = "linux")]
        {
            match bind_reuse_port(&args.addr) {
                Ok(std_listener) => TcpListener::from_std(std_listener).expect("from_std"),
                Err(e) => {
                    eprintln!("[tokio] reuse_port bind failed {e}, fallback to bind");
                    TcpListener::bind(&args.addr).await.expect("bind tokio")
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            TcpListener::bind(&args.addr).await.expect("bind tokio")
        }
    };
    println!("[tokio] listening {} mode={} (reuse_port, fallback lower than monoio io_uring)", args.addr, args.mode);
    let mut next_id: u64 = 0;
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                next_id += 1;
                let id = next_id;
                let store_c = store.clone();
                let subs_c = subs.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_tokio_conn(socket, id, store_c, subs_c).await {
                        // eprintln!("conn {} err {:?}", id, e);
                    }
                });
            }
            Err(e) => eprintln!("accept err {:?}", e),
        }
    }
}

async fn handle_tokio_conn(
    stream: tokio::net::TcpStream,
    id: u64,
    store: Arc<dyn zhensegg::Store>,
    subs: SubscriberMap,
) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (mut read_half, mut write_half) = stream.into_split();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Arc<Vec<u8>>>();

    // writer task: batched zero-copy flush (single write per batch) - concept.md:19
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
            let mut out = Vec::with_capacity(total);
            for m in pending.drain(..) {
                out.extend_from_slice(&m);
            }
            if write_half.write_all(&out).await.is_err() {
                break;
            }
        }
    });

    let mut parser = ZParser::new(64*1024);
    let mut read_buf = vec![0u8; 64*1024];
    let mut my_topics: Vec<Vec<u8>> = Vec::new();

    let res: std::io::Result<()> = async {
        loop {
            let n = read_half.read(&mut read_buf).await?;
            if n == 0 {
                break Ok(());
            }
            parser.feed(&read_buf[..n]);
            while let Some(frame) = parser.try_parse() {
                match frame.op {
                    Op::Publish => {
                        let topic_slice = frame.topic;
                        let payload_slice = frame.payload;
                        let (offset, rec_len) = store.append(topic_slice, payload_slice).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
                        let mut ack = Vec::with_capacity(32);
                        encode_ack(&mut ack, topic_slice, offset, rec_len);
                        let _ = tx.send(Arc::new(ack));
                        let guard = subs.read(topic_slice);
                        if let Some(list) = guard.get(topic_slice) {
                            if !list.is_empty() {
                                let mut data = Vec::with_capacity(13 + topic_slice.len() + payload_slice.len());
                                encode_data(&mut data, topic_slice, payload_slice);
                                let arc = Arc::new(data);
                                for sub in list.iter() {
                                    let _ = sub.tx.send(arc.clone());
                                }
                            }
                        }
                    }
                    Op::Subscribe => {
                        let topic = frame.topic.to_vec();
                        let sub = Arc::new(Subscriber { id, tx: tx.clone() });
                        {
                            let mut g = subs.write(&topic);
                            g.entry(topic.clone()).or_default().push(sub);
                        }
                        my_topics.push(topic.clone());
                        let mut ack = Vec::with_capacity(32);
                        encode_ack(&mut ack, &topic, 0, 0);
                        let _ = tx.send(Arc::new(ack));
                    }
                    Op::Fetch => {
                        if frame.payload.len() >= 12 {
                            let off = u64::from_be_bytes(frame.payload[0..8].try_into().unwrap());
                            let len = u32::from_be_bytes(frame.payload[8..12].try_into().unwrap());
                            let mut raw = Vec::new();
                            if store.read(off, len, &mut raw).is_ok() && raw.len() >= 8 {
                                let tl = u32::from_be_bytes([raw[0],raw[1],raw[2],raw[3]]) as usize;
                                let pl = u32::from_be_bytes([raw[4],raw[5],raw[6],raw[7]]) as usize;
                                if raw.len() >= 8+tl+pl {
                                    let st = &raw[8..8+tl];
                                    let sp = &raw[8+tl..8+tl+pl];
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
    }
    // close writer channel to terminate writer task
    drop(tx);
    let _ = writer.await;
    res
}
