//! Structure-aware generation of protocol byte-strings.
//!
//! The public wire format (see `src/protocol/parser.rs`) is:
//!
//! ```text
//! [0..4 ] u32 total_len          = 1 + 4 + 4 + topic.len + payload.len
//! [4    ] u8  op                 (0x01 Publish .. 0x08 Auth)
//! [5..9 ] u32 topic_len
//! [9..13] u32 payload_len
//! [13..  ] topic bytes
//! [     ] payload bytes
//! ```
//!
//! `total_len` does not include the leading 4-byte prefix. The generator emits
//! mostly *valid* frames (so the fuzzer reaches the deep dispatch code paths)
//! plus a tail of *hostile* variants: truncated headers, huge lengths, length
//! mismatches, and ops that don't exist. Both classes get carried through the
//! coverage-guided mutator, which then blends them with raw bit flips.

use zhensegg::protocol::Op;

pub fn rand_op(rng: &mut rand::rngs::ThreadRng) -> Op {
    use rand::Rng;
    // Lean on the real ops so dispatch code gets exercised; 1/16 of the time
    // emit a garbage op byte (which the parser must reject without panicking).
    if rng.gen_ratio(1, 16) {
        Op::from_u8(rng.gen()).unwrap_or(Op::Ping)
    } else {
        match rng.gen_range(0..8) {
            0 => Op::Publish,
            1 => Op::Subscribe,
            2 => Op::Fetch,
            3 => Op::Ping,
            4 => Op::Ack,
            5 => Op::Notify,
            6 => Op::Data,
            _ => Op::Auth,
        }
    }
}

/// Append a single plausible frame to `out`.
pub fn push_frame(out: &mut Vec<u8>, rng: &mut rand::rngs::ThreadRng) {
    use rand::Rng;
    let op = rand_op(rng);
    let topic_len = rng.gen_range(0..64);
    let payload_len = rng.gen_range(0..256);

    let mut topic = vec![0u8; topic_len];
    rng.fill(&mut topic[..]);
    let mut payload = vec![0u8; payload_len];
    rng.fill(&mut payload[..]);

    // For Ack/Notify the payload overlays offset+len semantics; keep that shape
    // sometimes so the offset-decoding path is reached.
    if payload_len >= 12 && rng.gen_ratio(1, 2) {
        let off = rng.gen::<u64>();
        let l = rng.gen::<u32>();
        payload[..8].copy_from_slice(&off.to_be_bytes());
        payload[8..12].copy_from_slice(&l.to_be_bytes());
    }

    let total = (1 + 4 + 4 + topic_len + payload_len) as u32;
    out.extend_from_slice(&total.to_be_bytes());
    out.push(op as u8);
    out.extend_from_slice(&(topic_len as u32).to_be_bytes());
    out.extend_from_slice(&(payload_len as u32).to_be_bytes());
    out.extend_from_slice(&topic);
    out.extend_from_slice(&payload);
}

/// Seed the initial corpus: a handful of valid multi-frame streams, plus
/// hand-crafted hostile frames targeting parser invariants.
pub fn seed_corpus() -> Vec<Vec<u8>> {
    let mut seeds: Vec<Vec<u8>> = Vec::new();
    let mut rng = rand::thread_rng();
    use rand::Rng;

    // Streams of 1..8 random valid frames.
    for _ in 0..48 {
        let mut s = Vec::new();
        let n = 1 + (rng.gen::<u32>() % 8) as usize;
        for _ in 0..n {
            push_frame(&mut s, &mut rng);
        }
        seeds.push(s);
    }

    // Hostile single frames.
    seeds.push(vec![0u8, 0, 0, 0]); // zero-length (only prefix, len=0)
    seeds.push(vec![0xFF, 0xFF, 0xFF, 0xFF]); // absurd total_len
    seeds.push(vec![0, 0, 0, 17, 0xFF]); // valid prefix, garbage op byte
    seeds.push(vec![0, 0, 0, 13]); // header but no body (truncated)
    seeds.push(vec![0, 0, 0, 13, 0x01, 0, 0, 0, 0, 0, 0, 0, 0]); // publish, empty
    seeds.push(vec![0, 0, 0, 14, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0]); // subscribe + 1 byte
    seeds.push(vec![0, 0, 0, 12]); // total_len smaller than fixed header
    seeds.push(vec![0x7F, 0xFF, 0xFF, 0xFF, 0x01, 0, 0, 0, 0, 0, 0, 0, 0]); // near 16MB cap
    seeds.push(vec![0x00, 0x01, 0x00, 0x00, 0x03]); // total_len=65536 publish
    seeds.push(vec![0, 0, 0, 17, 0x08, 0, 0, 0, 0, 0, 0, 0, 0, 98, 97, 100, 0]); // auth "bad\0"
    seeds.push(vec![0, 0, 0, 14, 0x03, 0, 0, 0, 0, 0, 0, 0, 0, 0]); // fetch w/ short payload
    seeds.push(vec![0, 0, 0, 25, 0x03, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]); // fetch 12B payload
    seeds
}
