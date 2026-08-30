use std::path::PathBuf;
use std::process::ExitCode;

use zhensegg_fuzz::{fuzzer, proto, soak};

fn usage() -> ! {
    eprintln!(
        "usage: zhensegg-fuzz fuzz|soak|check [options]\n\
         \n\
         fuzz:  --seconds N (0=unlimited) --iters N --corpus DIR --crash DIR\n\
         soak:  --addr HOST:PORT --auth-token FILE --conns N --seconds N\n\
         check: same flags as fuzz (bounded, for CI)"
    );
    std::process::exit(64);
}

fn parse_common(args: &[String]) -> (u64, u64, PathBuf, PathBuf) {
    let mut seconds = 30u64;
    let mut iters = u64::MAX;
    let mut corpus = PathBuf::from("./fuzz-corpus");
    let mut crash = PathBuf::from("./fuzz-crash");
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--seconds" => {
                i += 1;
                seconds = args[i].parse().unwrap_or(30);
            }
            "--iters" => {
                i += 1;
                iters = args[i].parse().unwrap_or(u64::MAX);
            }
            "--corpus" => {
                i += 1;
                corpus = PathBuf::from(&args[i]);
            }
            "--crash" => {
                i += 1;
                crash = PathBuf::from(&args[i]);
            }
            other => {
                eprintln!("unknown flag: {other}");
                usage();
            }
        }
        i += 1;
    }
    (seconds, iters, corpus, crash)
}

fn run_fuzz(args: &[String]) -> ExitCode {
    let (seconds, iters, corpus, crash) = parse_common(args);
    if seconds == 0 && iters == u64::MAX {
        eprintln!("specify --seconds and/or --iters so the run terminates");
        return ExitCode::from(2);
    }
    let seeds = proto::seed_corpus();
    match fuzzer::run(seeds, &corpus, &crash, seconds, iters) {
        Ok(stats) => {
            eprintln!(
                "fuzz: ok  iters={} corpus={} unique_sigs={} rate={:.0}/s (corpus_dir={}, crash_dir={})",
                stats.iterations,
                stats.corpus,
                stats.unique_sigs,
                stats.rate,
                corpus.display(),
                crash.display()
            );
            ExitCode::SUCCESS
        }
        Err(msg) => {
            eprintln!("FUZZ FAILURE: {msg}; input saved under {}", crash.display());
            ExitCode::from(1)
        }
    }
}

fn run_soak(args: &[String]) -> ExitCode {
    let mut addr = String::new();
    let mut auth_file: Option<String> = None;
    let mut conns = 8usize;
    let mut seconds = 20u64;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--addr" => {
                i += 1;
                addr = args[i].clone();
            }
            "--auth-token" => {
                i += 1;
                auth_file = Some(args[i].clone());
            }
            "--conns" => {
                i += 1;
                conns = args[i].parse().unwrap_or(8);
            }
            "--seconds" => {
                i += 1;
                seconds = args[i].parse().unwrap_or(20);
            }
            other => {
                eprintln!("unknown soak flag: {other}");
                usage();
            }
        }
        i += 1;
    }
    if addr.is_empty() {
        eprintln!("soak requires --addr HOST:PORT");
        return ExitCode::from(2);
    }
    let auth = auth_file
        .and_then(|p| std::fs::read_to_string(&p).ok())
        .map(|s| s.trim().to_string());
    match soak::run(&addr, auth.as_deref().map(|s| s.as_bytes()), conns, seconds) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("SOAK FAILURE: {e:#}");
            ExitCode::from(1)
        }
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        usage();
    }
    match args[0].as_str() {
        "fuzz" => run_fuzz(&args[1..]),
        "soak" => run_soak(&args[1..]),
        "check" => run_fuzz(&args[1..]),
        _ => usage(),
    }
}
