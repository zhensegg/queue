use clap::Parser;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zhensegg::protocol::{encode_publish, encode_subscribe, Op, Parser as ZParser};

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

async fn tune(stream: &TcpStream) {
    #[cfg(target_os = "linux")]
    tune_socket(std::os::unix::io::AsRawFd::as_raw_fd(stream));
    #[cfg(not(target_os = "linux"))]
    let _ = stream;
}

#[derive(Parser, Debug)]
#[command(name="zhensegg-bench")]
struct Args {
    #[arg(long, default_value="127.0.0.1:9090")]
    addr: String,
    #[arg(long, default_value="bench")]
    topic: String,
    #[arg(long, default_value="1000000")]
    msgs: usize,
    #[arg(long, default_value="256")]
    payload_size: usize,
    #[arg(long, default_value="4")]
    producers: usize,
    #[arg(long, default_value="4")]
    consumers: usize,
    #[arg(long, default_value="64")]
    batch: usize,
    #[arg(long, default_value="false")]
    wait_ack: bool,
    #[arg(long, default_value="10")]
    secs: u64,
}

fn main() {
    let args = Args::parse();
    println!("[bench] addr={} topic={} msgs={} payload={} prod={} cons={} batch={} wait_ack={}", args.addr, args.topic, args.msgs, args.payload_size, args.producers, args.consumers, args.batch, args.wait_ack);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads((args.producers + args.consumers + 2).max(8))
        .build().unwrap();
    rt.block_on(async move {
        run_bench(args).await;
    });
}

async fn run_bench(args: Args) {
    let total_produced = Arc::new(AtomicU64::new(0));
    let total_consumed = Arc::new(AtomicU64::new(0));
    let total_acks = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    // start consumers first
    let mut cons_handles = Vec::new();
    for cid in 0..args.consumers {
        let addr = args.addr.clone();
        let topic = args.topic.clone();
        let consumed = total_consumed.clone();
        let h = tokio::spawn(async move {
            if let Err(e) = consumer_task(cid, addr, topic, consumed).await {
                eprintln!("[consumer {}] err {:?}", cid, e);
            }
        });
        cons_handles.push(h);
    }
    // give consumers time to subscribe
    tokio::time::sleep(Duration::from_millis(500)).await;

    let bench_start = Instant::now();
    let mut prod_handles = Vec::new();
    let msgs_per_prod = args.msgs / args.producers.max(1);
    for pid in 0..args.producers {
        let addr = args.addr.clone();
        let topic = args.topic.clone();
        let produced = total_produced.clone();
        let acks = total_acks.clone();
        let payload_size = args.payload_size;
        let batch = args.batch;
        let wait_ack = args.wait_ack;
        let msgs = msgs_per_prod;
        let h = tokio::spawn(async move {
            if let Err(e) = producer_task(pid, addr, topic, payload_size, msgs, batch, wait_ack, produced, acks).await {
                eprintln!("[producer {}] err {:?}", pid, e);
            }
        });
        prod_handles.push(h);
    }

    // monitor
    let monitor_consumed = total_consumed.clone();
    let monitor_produced = total_produced.clone();
    let monitor_acks = total_acks.clone();
    let mon = tokio::spawn(async move {
        let mut last_p = 0u64;
        let mut last_c = 0u64;
        let mut last_t = Instant::now();
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let p = monitor_produced.load(Ordering::Relaxed);
            let c = monitor_consumed.load(Ordering::Relaxed);
            let a = monitor_acks.load(Ordering::Relaxed);
            let now = Instant::now();
            let dt = now.duration_since(last_t).as_secs_f64();
            let dp = p - last_p;
            let dc = c - last_c;
            println!("[monitor] produced {} ({:.0} /s) consumed {} ({:.0} /s) acks {} elapsed {:.1}s", p, dp as f64 / dt, c, dc as f64 / dt, a, now.duration_since(bench_start).as_secs_f64());
            last_p = p;
            last_c = c;
            last_t = now;
            if now.duration_since(bench_start).as_secs() >= 30 {
                break;
            }
            if p as usize >= args.msgs && c as usize >= args.msgs {
                // give a bit extra
                tokio::time::sleep(Duration::from_millis(500)).await;
                break;
            }
        }
    });

    for h in prod_handles { let _ = h.await; }
    let prod_elapsed = bench_start.elapsed();
    let p = total_produced.load(Ordering::Relaxed);
    println!("[bench] producers done {} msgs in {:.3}s => {:.0} msg/s ({:.2} MB/s)", p, prod_elapsed.as_secs_f64(), p as f64 / prod_elapsed.as_secs_f64(), p as f64 * args.payload_size as f64 / prod_elapsed.as_secs_f64() / 1024.0/1024.0);

    // wait for consumers to catch up a bit
    let wait_until = Instant::now() + Duration::from_secs(args.secs.min(5));
    while Instant::now() < wait_until {
        let c = total_consumed.load(Ordering::Relaxed);
        if c >= p && c > 0 { break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // abort consumers
    for h in cons_handles { h.abort(); }
    let _ = mon.await;
    let total_elapsed = bench_start.elapsed();
    let c = total_consumed.load(Ordering::Relaxed);
    let p = total_produced.load(Ordering::Relaxed);
    println!("=== RESULT ===");
    println!("produced: {} in {:.3}s => {:.0} RPS", p, total_elapsed.as_secs_f64(), p as f64 / total_elapsed.as_secs_f64());
    println!("consumed: {} => {:.0} RPS", c, c as f64 / total_elapsed.as_secs_f64());
    if c > 0 && p > 0 {
        let loss = if p > c { (p - c) as f64 / p as f64 * 100.0 } else { 0.0 };
        println!("loss: {:.2}%", loss);
        // Compare to NATS targets
        let target_mem = 11_000_000.0;
        let target_persist = 2_000_000.0;
        println!("vs NATS core target 11M: {:.2}x ({:.0}%)", p as f64 / target_mem, p as f64 / target_mem * 100.0);
        println!("vs NATS persist 2M: {:.2}x", p as f64 / target_persist);
        if p as f64 / total_elapsed.as_secs_f64() > target_mem {
            println!("✅ BEATS NATS CORE (>11M)");
        } else if p as f64 / total_elapsed.as_secs_f64() > target_persist {
            println!("✅ BEATS NATS PERSIST (>2M) need >11M for core");
        }
    }
    let overall = start.elapsed();
    println!("overall wall: {:.3}s", overall.as_secs_f64());
}

async fn producer_task(
    pid: usize,
    addr: String,
    topic: String,
    payload_size: usize,
    msgs: usize,
    batch: usize,
    wait_ack: bool,
    produced: Arc<AtomicU64>,
    acks: Arc<AtomicU64>,
) -> std::io::Result<()> {
    let mut stream = TcpStream::connect(&addr).await?;
    stream.set_nodelay(true)?;
    tune(&stream).await;
    // payload pattern
    let payload = vec![b'x'; payload_size];
    let topic_bytes = topic.as_bytes();

    let mut parser = ZParser::new(64*1024);
    let mut read_buf = vec![0u8; 64*1024];
    let mut pending_acks: usize = 0;

    let mut sent = 0usize;
    let mut batch_buf = Vec::with_capacity( batch * (13 + topic_bytes.len() + payload_size) );

    while sent < msgs {
        batch_buf.clear();
        let mut cur_batch = 0;
        while cur_batch < batch && sent + cur_batch < msgs {
            encode_publish(&mut batch_buf, topic_bytes, &payload);
            cur_batch += 1;
        }
        // single syscall batched write (concept batching)
        stream.write_all(&batch_buf).await?;
        sent += cur_batch;
        produced.fetch_add(cur_batch as u64, Ordering::Relaxed);
        pending_acks += cur_batch;

        if wait_ack {
            // drain acks - expect one ack per publish (offset)
            // To avoid blocking, read all available acks with timeout
            // For strict throughput, we need to consume acks otherwise TCP backpressure
            let mut need = cur_batch;
            while need > 0 {
                // try to read ack frames
                let n = match tokio::time::timeout(Duration::from_millis(500), stream.read(&mut read_buf)).await {
                    Ok(Ok(0)) => break,
                    Ok(Ok(n)) => n,
                    Ok(Err(e)) => return Err(e),
                    Err(_) => {
                        eprintln!("[prod {}] ack timeout need {} pending {}", pid, need, pending_acks);
                        break;
                    }
                };
                parser.feed(&read_buf[..n]);
                while let Some(frame) = parser.try_parse() {
                    if frame.op == Op::Ack {
                        acks.fetch_add(1, Ordering::Relaxed);
                        if need > 0 { need -= 1; pending_acks -= 1; }
                    }
                    parser.consume();
                }
            }
        } else {
            // without wait_ack, we still need to drain acks in background to prevent buffer bloat
            // try non-blocking read
            if pending_acks > 1000 {
                // drain
                match tokio::time::timeout(Duration::from_millis(1), stream.read(&mut read_buf)).await {
                    Ok(Ok(n)) if n > 0 => {
                        parser.feed(&read_buf[..n]);
                        while let Some(f) = parser.try_parse() {
                            if f.op == Op::Ack { acks.fetch_add(1, Ordering::Relaxed); pending_acks -= 1; }
                            parser.consume();
                        }
                    }
                    _ => {}
                }
            }
        }
        // Optional: yield to avoid starving consumers in same runtime? Not needed.
    }
    // flush remaining acks if wait
    if wait_ack {
        let deadline = Instant::now() + Duration::from_secs(2);
        while pending_acks > 0 && Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), stream.read(&mut read_buf)).await {
                Ok(Ok(n)) if n > 0 => {
                    parser.feed(&read_buf[..n]);
                    while let Some(f) = parser.try_parse() {
                        if f.op == Op::Ack { acks.fetch_add(1, Ordering::Relaxed); pending_acks = pending_acks.saturating_sub(1); }
                        parser.consume();
                    }
                }
                _ => break,
            }
        }
    }
    println!("[producer {}] done sent {}", pid, sent);
    Ok(())
}

async fn consumer_task(
    cid: usize,
    addr: String,
    topic: String,
    consumed: Arc<AtomicU64>,
) -> std::io::Result<()> {
    let mut stream = TcpStream::connect(&addr).await?;
    stream.set_nodelay(true)?;
    tune(&stream).await;
    // subscribe
    let mut sub_buf = Vec::new();
    encode_subscribe(&mut sub_buf, topic.as_bytes());
    stream.write_all(&sub_buf).await?;
    // wait for sub ack
    let mut parser = ZParser::new(128*1024);
    let mut read_buf = vec![0u8; 128*1024];
    // read subscribe ack (optional)
    match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut read_buf)).await {
        Ok(Ok(n)) if n > 0 => {
            parser.feed(&read_buf[..n]);
            while let Some(f) = parser.try_parse() {
                parser.consume();
            }
        }
        _ => {}
    }
    println!("[consumer {}] subscribed {}", cid, topic);
    let mut total: u64 = 0;
    let mut last_log = Instant::now();
    loop {
        let n = stream.read(&mut read_buf).await?;
        if n == 0 { break; }
        parser.feed(&read_buf[..n]);
        let mut batch_cnt = 0;
        while let Some(frame) = parser.try_parse() {
            match frame.op {
                Op::Data | Op::Notify | Op::Ack => {
                    // count Data as delivered message; also Ack for sub ack
                    if frame.op == Op::Data {
                        batch_cnt += 1;
                        total += 1;
                    }
                }
                _ => {}
            }
            parser.consume();
        }
        if batch_cnt > 0 {
            consumed.fetch_add(batch_cnt, Ordering::Relaxed);
        }
        if last_log.elapsed() > Duration::from_secs(2) {
            // println!("[consumer {}] total {}", cid, total);
            last_log = Instant::now();
        }
    }
    Ok(())
}
