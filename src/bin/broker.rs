use clap::Parser as ClapParser;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use zhensegg::protocol::{encode_ack, encode_data, Op, Parser as ZParser};

#[derive(ClapParser, Debug)]
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

type SubscriberMap = Arc<RwLock<HashMap<Vec<u8>, Vec<Arc<Subscriber>>>>>;

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
    let subs: SubscriberMap = Arc::new(RwLock::new(HashMap::new()));

    // monoio runtime single thread
    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_timer()
        .build()
        .expect("monoio rt");
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
    let subs: SubscriberMap = Arc::new(RwLock::new(HashMap::new()));

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
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("rt");
                rt.block_on(async move {
                    println!("[core {}] listening {}", cid, addr_c);
                    monoio_broker_loop(addr_c, store_c, subs_c).await;
                });
            }).expect("spawn");
        handles.push(h);
    }
    for h in handles { let _ = h.join(); }
}

#[cfg(all(target_os="linux", feature="uring"))]
fn core_affinity_attempt(core_id: usize) -> std::io::Result<()> {
    #[cfg(all(target_os="linux", feature="uring"))]
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
    use monoio::io::{AsyncReadRent, AsyncWriteRentExt};

    let listener = TcpListener::bind(&addr).expect("bind");
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
    use monoio::io::{AsyncReadRent, AsyncWriteRentExt};

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

    let mut pending_out: Vec<Vec<u8>> = Vec::new();

    loop {
        // check if there is pending fan-out data for this connection (we are subscriber)
        while let Ok(msg) = rx.try_recv() {
            pending_out.push(msg);
            // batch threshold 64KB or 128 messages
            if pending_out.len() >= 128 {
                break;
            }
        }
        if !pending_out.is_empty() {
            // coalesce into one write syscall (Batching concept.md:19)
            write_buf.clear();
            for m in pending_out.drain(..) {
                write_buf.extend_from_slice(&m);
            }
            // zero-copy batched write: single write() call
            let (res, _) = stream.write_all(write_buf).await;
            res?;
            write_buf = Vec::with_capacity(64*1024);
        }

        // try read with timeout to avoid blocking forever while we need to flush pending
        // Use monoio time timeout
        let read_fut = stream.read(read_buf);
        let timeout = monoio::time::sleep(std::time::Duration::from_millis(1));
        // Simple: race read vs timeout using select
        // monoio doesn't have tokio::select, we can use futures with monoio compat? Use tokio::select via monoio-compat? Simpler: just do read with small timeout via `monoio::time::timeout`
        let read_res = monoio::time::timeout(std::time::Duration::from_millis(2), read_fut).await;
        let n = match read_res {
            Ok((Ok(n), buf)) => { read_buf = buf; n },
            Ok((Err(e), _)) => { break Err(e); },
            Err(_) => {
                // timeout, loop again to flush pending
                if pending_out.is_empty() {
                    // check if any new pending arrived after timeout
                    continue;
                } else {
                    continue;
                }
            }
        };
        if n == 0 {
            break Ok(());
        }
        parser.feed(&read_buf[..n]);

        // drain complete frames zero-alloc
        while let Some(frame) = parser.try_parse() {
            match frame.op {
                Op::Publish => {
                    // copy topic/payload needed before consume (zero-alloc slice)
                    let topic = frame.topic.to_vec();
                    let payload = frame.payload.to_vec();
                    let t_len = topic.len();
                    let p_len = payload.len();
                    // append to store -> offset
                    let (offset, rec_len) = store.append(&topic, &payload).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
                    // ack to producer: encode ack with offset
                    let mut ack = Vec::with_capacity(64);
                    encode_ack(&mut ack, &topic, offset, rec_len);
                    // batch ack with pending_out? For now immediate write
                    let (res, _) = stream.write_all(ack).await;
                    res?;

                    // fan-out to subscribers: either NOTIFY or DATA
                    // We send DATA for in-mem fast path, NOTIFY for offset mode subscribers.
                    // For MVP we fan-out DATA (full payload) to all subs of topic.
                    // To respect zero-copy concept.md:18, we reuse slice bytes without extra alloc per subscriber beyond header copy.
                    // We encode DATA once and clone buffer for each subscriber (still one copy per fan-out, but batched).

                    let subs_guard = subs.read();
                    if let Some(list) = subs_guard.get(&topic) {
                        // encode data frame once
                        let mut data_frame = Vec::with_capacity(13 + t_len + p_len);
                        encode_data(&mut data_frame, &topic, &payload);
                        // also encode notify variant (offset) for offset-mode? We'll send data for now.
                        // Clone for each subscriber via channel (avoids holding lock while writing)
                        for sub in list.iter() {
                            if sub.id == id {
                                // optionally skip self? but publish may also be subscriber; deliver anyway
                            }
                            let _ = sub.tx.send(data_frame.clone());
                        }
                    } else {
                        // Also check wildcard? MVP exact match only
                    }
                    // Also try NOTIFY path for offset subscribers: if we had separate list, but for now same.

                    // metrics could be incremented here
                }
                Op::Subscribe => {
                    let topic = frame.topic.to_vec();
                    // register this connection as subscriber for topic
                    let sub = Arc::new(Subscriber { id, tx: tx.clone() });
                    {
                        let mut g = subs.write();
                        g.entry(topic.clone()).or_default().push(sub);
                    }
                    my_topics.push(topic.clone());
                    // optional ack subscribe
                    let mut ack = Vec::with_capacity(32);
                    // we reuse Ack with offset 0 as subscribe ack
                    encode_ack(&mut ack, &topic, 0, 0);
                    let (res, _) = stream.write_all(ack).await;
                    res?;
                }
                Op::Fetch => {
                    // payload contains offset+len, topic as key
                    if frame.payload.len() >= 12 {
                        let off = u64::from_be_bytes(frame.payload[0..8].try_into().unwrap());
                        let len = u32::from_be_bytes(frame.payload[8..12].try_into().unwrap());
                        let topic = frame.topic.to_vec();
                        let mut out = Vec::with_capacity(len as usize + 32);
                        // read from store
                        let mut raw = Vec::new();
                        match store.read(off, len, &mut raw) {
                            Ok(()) => {
                                // raw contains [4 topic_len][4 payload_len][topic][payload]
                                // decode to extract payload slice
                                if raw.len() >= 8 {
                                    let tl = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
                                    let pl = u32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]) as usize;
                                    if raw.len() >= 8+tl+pl {
                                        let stored_topic = &raw[8..8+tl];
                                        let stored_payload = &raw[8+tl..8+tl+pl];
                                        // send DATA frame containing fetched payload
                                        let mut data = Vec::with_capacity(13+stored_topic.len()+stored_payload.len());
                                        encode_data(&mut data, stored_topic, stored_payload);
                                        let (res, _) = stream.write_all(data).await;
                                        res?;
                                    }
                                }
                            }
                            Err(e) => {
                                // send error ack
                                let mut err = Vec::new();
                                encode_ack(&mut err, &topic, 0, 0);
                                let (res, _) = stream.write_all(err).await;
                                res?;
                            }
                        }
                    }
                }
                Op::Ping => {
                    let mut pong = Vec::new();
                    encode_ack(&mut pong, b"pong", 0, 0);
                    let (res, _) = stream.write_all(pong).await;
                    res?;
                }
                _ => {}
            }
            parser.consume();
        }
    }
    // cleanup subscriptions
    {
        let mut g = subs.write();
        for t in my_topics {
            if let Some(list) = g.get_mut(&t) {
                list.retain(|s| s.id != id);
                if list.is_empty() { g.remove(&t); }
            }
        }
    }
    let _ = Ok::<(), std::io::Error>(());
    Ok(())
}

// Fallback tokio implementation for Windows / non-linux
fn run_tokio_fallback(args: Args) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(args.cores)
        .build().expect("tokio rt");
    rt.block_on(async move {
        tokio_broker_loop(args).await;
    });
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
    let subs: SubscriberMap = Arc::new(RwLock::new(HashMap::new()));
    let listener = TcpListener::bind(&args.addr).await.expect("bind tokio");
    println!("[tokio] listening {} mode={} (fallback, perf lower than monoio io_uring)", args.addr, args.mode);
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
                        let topic = frame.topic.to_vec();
                        let payload = frame.payload.to_vec();
                        let (offset, rec_len) = store.append(&topic, &payload).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
                        // ack to producer via writer channel (batched)
                        let mut ack = Vec::with_capacity(32);
                        encode_ack(&mut ack, &topic, offset, rec_len);
                        let _ = tx.send(Arc::new(ack));

                        // fan-out to subscribers: single encode + Arc clone per subscriber (zero alloc per sub beyond refcount)
                        let guard = subs.read();
                        if let Some(list) = guard.get(&topic) {
                            if !list.is_empty() {
                                let mut data = Vec::with_capacity(13 + topic.len() + payload.len());
                                encode_data(&mut data, &topic, &payload);
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
                            let mut g = subs.write();
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

    // cleanup subscriptions
    {
        let mut g = subs.write();
        for t in my_topics {
            if let Some(list) = g.get_mut(&t) {
                list.retain(|s| s.id != id);
                if list.is_empty() {
                    g.remove(&t);
                }
            }
        }
    }
    // close writer channel to terminate writer task
    drop(tx);
    let _ = writer.await;
    res
}
