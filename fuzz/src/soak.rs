//! Soak harness: hammer a *running* broker binary over real TCP.
//!
//! This is the "fuzz the binary over the wire" layer. It opens many parallel
//! connections that interleave malformed byte garbage and well-formed frames
//! (publish/subscribe/fetch/auth), so the length-prefix parser is forced to
//! resync and handle hostile framing against a live process. Each flooder then
//! closes cleanly.
//!
//! A soak run is green only if, after the whole flood, a *fresh* connection can
//! still authenticate (when a token is configured) and complete a Ping/ack
//! round-trip. Truncated mid-stream frames legitimately desync a single
//! length-prefix stream (the broker faithfully waits for the declared body), so
//! liveness is asserted on a brand-new connection rather than on stream
//! alignment.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use rand::Rng;

use zhensegg::protocol::{Op, encode_auth, encode_fetch, encode_frame, encode_publish, encode_subscribe};

/// Flood one connection with mixed good/bad frames, then close it. Returns the
/// number of response bytes drained (best-effort) and whether the broker
/// accepted the connection at all.
fn flood_connection(addr: &str, auth: Option<&[u8]>, bytes: usize) -> anyhow::Result<u64> {
    let mut stream = TcpStream::connect(addr).map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_millis(150))).ok();
    let mut rng = rand::thread_rng();
    let mut sent: usize = 0;
    let mut drained: u64 = 0;
    let mut readbuf = [0u8; 8192];

    if let Some(tok) = auth {
        let mut f = Vec::new();
        encode_auth(&mut f, tok);
        stream.write_all(&f)?;
    }

    while sent < bytes {
        let kind = rng.gen_range(0..100u32);
        let mut frame = Vec::new();
        match kind {
            0..=34 => {
                let t = format!("topic{}", rng.gen_range(0..16));
                let p = vec![b'x'; rng.gen_range(0..64)];
                encode_publish(&mut frame, t.as_bytes(), &p);
            }
            35..=49 => {
                let t = format!("topic{}.sub", rng.gen_range(0..16));
                encode_subscribe(&mut frame, t.as_bytes());
            }
            50..=61 => {
                encode_fetch(&mut frame, b"topic0", rng.gen_range(0..1_000_000), rng.gen_range(0..256));
            }
            62..=74 => {
                // Pure garbage bytes the parser must reject gracefully.
                let g = vec![rng.gen(); rng.gen_range(1..64)];
                frame.extend_from_slice(&g);
            }
            75..=89 => {
                // A length prefix claiming far more than we actually send
                // (forces the broker to desync and buffer a phantom body).
                let big = rng.gen_range(2u32..1_000_000);
                frame.extend_from_slice(&big.to_be_bytes());
                frame.extend_from_slice(&[rng.gen(); 4]);
            }
            _ => {
                // A sub-MINIMAL total_len (0..8) is already enough to have
                // reached into the header; make sure hostile tiny lengths flow
                // too. This exercises the parser's length-validation guard.
                let tiny = rng.gen_range(0u32..9);
                frame.extend_from_slice(&tiny.to_be_bytes());
                frame.extend_from_slice(&[rng.gen(); 2]);
            }
        }
        sent += frame.len();
        if stream.write_all(&frame).is_err() {
            break;
        }
        // Best-effort drain so the socket never fills.
        let started = Instant::now();
        while started.elapsed() < Duration::from_millis(1) {
            match stream.set_nonblocking(true).and_then(|_| stream.read(&mut readbuf)) {
                Ok(0) => return Ok(drained), // broker closed us: still handled
                Ok(n) => drained += n as u64,
                Err(_) => break,
            }
        }
        stream.set_nonblocking(false).ok();
    }
    Ok(drained)
}

/// Open a fresh connection and complete a Ping/ack round-trip (after optional
/// auth). Returns Ok(()) only if the server answered.
fn probe_liveness(addr: &str, auth: Option<&[u8]>) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(addr).map_err(|e| anyhow::anyhow!("probe connect: {e}"))?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    if let Some(tok) = auth {
        let mut f = Vec::new();
        encode_auth(&mut f, tok);
        stream.write_all(&f)?;
        stream.flush()?;
    }
    let mut ping = Vec::new();
    encode_frame(&mut ping, Op::Ping, &[], &[]);
    stream.write_all(&ping)?;
    stream.flush()?;
    let mut readbuf = [0u8; 4096];
    // The socket has a 2s read timeout, so a single blocking read is enough to
    // bound the wait: any bytes from the server mean it parsed our Ping and
    // answered; a timeout or close means it did not.
    match stream.read(&mut readbuf) {
        Ok(n) if n > 0 => return Ok(()),
        _ => {}
    }
    anyhow::bail!("broker did not answer Ping probe on fresh connection to {addr}")
}

/// Run the soak: `conns` parallel flooders, each sending `seconds` of traffic,
/// then a fresh liveness probe. Green iff the fresh probe succeeds for every
/// connection's worth of abuse (we probe once more at the end).
pub fn run(
    addr: &str,
    auth: Option<&[u8]>,
    conns: usize,
    seconds: u64,
) -> anyhow::Result<()> {
    let per_conn_bytes = (seconds as usize).saturating_mul(200_000).max(100_000);
    let threads = if conns == 0 { 1 } else { conns };
    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        let a = addr.to_string();
        let tok = auth.map(|t| t.to_vec());
        handles.push(std::thread::spawn(move || {
            flood_connection(&a, tok.as_deref(), per_conn_bytes)
        }));
    }
    let mut total_drained = 0u64;
    for h in handles {
        total_drained += h
            .join()
            .map_err(|_| anyhow::anyhow!("soak thread panicked"))??;
    }

    // The real assertion: after all that abuse the broker still accepts a fresh
    // connection and answers a Ping.
    probe_liveness(addr, auth)?;

    eprintln!(
        "soak: {threads} conns x ~{per_conn_bytes}B flooded, broker alive, drained ≈ {total_drained}B"
    );
    Ok(())
}
