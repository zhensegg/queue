//! Coverage-guided mutation fuzzer for the Zhensegg wire protocol parser.
//!
//! Runs in-process against the *real* `zhensegg::protocol::Parser` — the
//! zero-copy, `unsafe`-containing entry point every other protocol path feeds
//! from — plus a lightweight structural "trace" (the set of `(op, topic_len,
//! payload_len, boundary)` tuples the parser produced) used as a coverage
//! signal: inputs that produce a fresh trace tuple are kept in the corpus so
//! the mutator spends its budget exploring new parser behaviour instead of
//! rediscovering the same prefixes.
//!
//! Any panic inside the parser unwinds to us; we catch it, dump the failing
//! byte-string to the crash directory, and exit non-zero so a supervisor
//! immediately sees a defect.

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use zhensegg::protocol::Parser;

use crate::proto;

/// A per-input structural signature. We don't need full edge coverage; a cheap
/// tuple that distinguishes real parser outcomes is enough to guide mutation.
struct Sig {
    parts: Vec<(u8, u32, u32)>,
}

impl Sig {
    fn key(&self) -> String {
        let mut s = String::new();
        for (op, tl, pl) in &self.parts {
            s.push_str(&format!("{op}@{tl},{pl};"));
        }
        s
    }
}

fn sig_of(buf: &[u8]) -> Sig {
    let mut parser = Parser::new(64 * 1024);
    let mut parts = Vec::new();
    parser.feed(buf);
    // `drain` runs try_parse + consume until no complete frame remains, which
    // is exactly how the connection loop consumes a read buffer.
    parser.drain(|f| {
        parts.push((
            f.op as u8,
            f.topic.len() as u32,
            f.payload.len() as u32,
        ));
    });
    Sig { parts }
}

/// Catch-unwind wrapper. Parsing that panics should never happen; if it does
/// we surface it as a crash rather than let the fuzzer keep going.
fn exercise(buf: &[u8]) -> Result<Sig, String> {
    let s = std::panic::catch_unwind(|| sig_of(buf)).map_err(|_| "parser panicked")?;
    Ok(s)
}

/// One pass of the core loop: take `input`, mutate it, exercise, and decide
/// whether to promote the mutated copy back into the corpus.
pub fn mutate_and_run(
    input: &[u8],
    seen: &mut HashSet<String>,
    budget: &mut u64,
) -> Result<Vec<u8>, String> {
    let mut candidate = input.to_vec();
    mutate(&mut candidate);
    let sig = exercise(&candidate)?;
    if seen.insert(sig.key()) {
        // New coverage -> keep the mutation.
        *budget = budget.saturating_sub(1);
        Ok(candidate)
    } else {
        *budget = budget.saturating_sub(1);
        Ok(vec![])
    }
}

/// Apply a random mutation to `buf`.
pub fn mutate(buf: &mut Vec<u8>) {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    match rng.gen_range(0..6u32) {
        0 => {
            // Bit flip somewhere.
            if !buf.is_empty() {
                let i = rng.gen_range(0..buf.len());
                buf[i] ^= 1 << rng.gen_range(0..8);
            }
        }
        1 => {
            // Inject a random byte at a random position (shifts lengths).
            let i = rng.gen_range(0..=buf.len());
            buf.insert(i, rng.gen());
        }
        2 => {
            // Delete a random byte (shifts lengths).
            if !buf.is_empty() {
                let i = rng.gen_range(0..buf.len());
                buf.remove(i);
            }
        }
        3 => {
            // Overwrite a length field (in the first 13 bytes when possible).
            if buf.len() >= 4 {
                let base = rng.gen_range(0..buf.len().min(13)).min(buf.len() - 4);
                let v: u32 = match rng.gen_range(0..5) {
                    0 => 0,
                    1 => 1,
                    2 => rng.gen_range(0..100),
                    3 => rng.gen::<u32>() % (16 * 1024 * 1024),
                    _ => 0xFFFFFFFF,
                };
                buf[base..base + 4].copy_from_slice(&v.to_be_bytes());
            }
        }
        4 => {
            // Duplicate a random chunk.
            if !buf.is_empty() {
                let i = rng.gen_range(0..buf.len());
                let n = rng.gen_range(1..=buf.len().saturating_sub(i).max(1));
                let chunk = buf[i..i + n].to_vec();
                let at = rng.gen_range(0..=buf.len());
                let k = rng.gen_range(1..4);
                let mut tail = chunk.clone();
                for _ in 1..k {
                    tail.extend_from_slice(&chunk);
                }
                buf.splice(at..at, tail);
            }
        }
        _ => {
            // Append a fully fresh, valid frame (keeps reaching dispatch).
            let mut rng2 = rand::thread_rng();
            proto::push_frame(buf, &mut rng2);
        }
    }
}

pub struct Stats {
    pub iterations: u64,
    pub corpus: usize,
    pub unique_sigs: usize,
    pub rate: f64,
}

/// Run a fixed budget of mutation iterations (no wall-clock bound). Used by the
/// bounded CI fixture so a fuzz pass has a deterministic iteration count.
pub fn run_bounded(
    seed_corpus: Vec<Vec<u8>>,
    corpus_dir: &Path,
    crash_dir: &Path,
    max_iters: u64,
) -> Result<Stats, String> {
    run(seed_corpus, corpus_dir, crash_dir, u64::MAX, max_iters)
}

/// Run the fuzzer for `seconds` OR `max_iters` iterations, whichever comes
/// first, starting from the seed corpus plus any seeds under `corpus_dir`.
pub fn run(
    seed_corpus: Vec<Vec<u8>>,
    corpus_dir: &Path,
    crash_dir: &Path,
    seconds: u64,
    max_iters: u64,
) -> Result<Stats, String> {
    let base_seeds = seed_corpus.clone();
    let mut corpus = seed_corpus;
    // Load any persisted seeds from the corpus dir.
    if corpus_dir.is_dir() {
        if let Ok(rd) = fs::read_dir(corpus_dir) {
            for ent in rd.flatten() {
                if let Some(name) = ent.file_name().to_str() {
                    if name.starts_with("seed-") {
                        if let Ok(b) = fs::read(ent.path()) {
                            corpus.push(b);
                        }
                    }
                }
            }
        }
    } else {
        let _ = fs::create_dir_all(corpus_dir);
    }
    let _ = fs::create_dir_all(crash_dir);

    let mut seen: HashSet<String> = HashSet::new();
    // Seed the seen set so we don't re-explore the initial corpus signatures.
    let mut budget = max_iters;
    for c in &corpus {
        let _ = exercise(c);
        let k = sig_of(c).key();
        seen.insert(k);
    }

    let start = Instant::now();
    let mut iters: u64 = 0;
    let mut replay = HashSet::new();
    let deadline = Duration::from_secs(seconds);

    while budget > 0 && start.elapsed() < deadline {
        // Cycle through the corpus; evolve it as new signatures appear.
        if corpus.is_empty() {
            corpus = base_seeds.clone();
        }
        let idx = (iters as usize) % corpus.len();
        let input = corpus[idx].clone();

        let mut event_budget = 1u64;
        for _ in 0..256 {
            if budget == 0 || start.elapsed() >= deadline {
                break;
            }
            match mutate_and_run(&input, &mut seen, &mut event_budget) {
                Ok(kept) if !kept.is_empty() => {
                    // New coverage: add to corpus, dump to disk as a seed.
                    corpus.push(kept.clone());
                    if corpus.len() > 8192 {
                        corpus.drain(..512);
                    }
                    let name = format!("seed-{:016x}.bin", replay.len());
                    let _ = fs::write(corpus_dir.join(name), &kept);
                    replay.insert(sig_of(&kept).key());
                    budget = budget.saturating_sub(1);
                }
                Ok(_) => {}
                Err(msg) => {
                    // A crash: dump the input and bail with an error.
                    let name = format!("crash-run-{:016x}.bin", iters);
                    let _ = fs::write(crash_dir.join(name), &input);
                    return Err(msg);
                }
            }
            iters += 1;
            budget = budget.saturating_sub(1);
        }
    }

    let secs = start.elapsed().as_secs_f64().max(1e-9);
    Ok(Stats {
        iterations: iters,
        corpus: corpus.len(),
        unique_sigs: seen.len(),
        rate: iters as f64 / secs,
    })
}
