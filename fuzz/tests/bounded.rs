use std::time::Instant;

use zhensegg_fuzz::{fuzzer, proto};

#[test]
fn parser_fuzz_smoke_no_crash() {
    let t = std::env::temp_dir().join(format!("zs-fuzz-t-{}", std::process::id()));
    let corpus = t.join("corpus");
    let crash = t.join("crash");
    let _ = std::fs::remove_dir_all(&t);
    std::fs::create_dir_all(&corpus).unwrap();

    let seeds = proto::seed_corpus();
    
    let deadline = Instant::now() + std::time::Duration::from_secs(1);
    let mut remaining = 120_000u64;
    while remaining > 0 && Instant::now() < deadline {
        let budget = 2000.min(remaining);
        if let Err(msg) = fuzzer::run_bounded(seeds.clone(), &corpus, &crash, budget) {
            panic!(
                "parser fuzz found a crashing input: {msg}; saved under {}",
                crash.display()
            );
        }
        remaining -= budget;
    }
    let _ = std::fs::remove_dir_all(&t);
}
