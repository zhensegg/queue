use zhensegg::protocol::Op;

pub fn rand_op(rng: &mut rand::rngs::ThreadRng) -> Op {
    use rand::Rng;
    
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

pub fn push_frame(out: &mut Vec<u8>, rng: &mut rand::rngs::ThreadRng) {
    use rand::Rng;
    let op = rand_op(rng);
    let topic_len = rng.gen_range(0..64);
    let payload_len = rng.gen_range(0..256);

    let mut topic = vec![0u8; topic_len];
    rng.fill(&mut topic[..]);
    let mut payload = vec![0u8; payload_len];
    rng.fill(&mut payload[..]);

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

pub fn seed_corpus() -> Vec<Vec<u8>> {
    let mut seeds: Vec<Vec<u8>> = Vec::new();
    let mut rng = rand::thread_rng();
    use rand::Rng;

    for _ in 0..48 {
        let mut s = Vec::new();
        let n = 1 + (rng.gen::<u32>() % 8) as usize;
        for _ in 0..n {
            push_frame(&mut s, &mut rng);
        }
        seeds.push(s);
    }

    seeds.push(vec![0u8, 0, 0, 0]); 
    seeds.push(vec![0xFF, 0xFF, 0xFF, 0xFF]); 
    seeds.push(vec![0, 0, 0, 17, 0xFF]); 
    seeds.push(vec![0, 0, 0, 13]); 
    seeds.push(vec![0, 0, 0, 13, 0x01, 0, 0, 0, 0, 0, 0, 0, 0]); 
    seeds.push(vec![0, 0, 0, 14, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0]); 
    seeds.push(vec![0, 0, 0, 12]); 
    seeds.push(vec![0x7F, 0xFF, 0xFF, 0xFF, 0x01, 0, 0, 0, 0, 0, 0, 0, 0]); 
    seeds.push(vec![0x00, 0x01, 0x00, 0x00, 0x03]); 
    seeds.push(vec![0, 0, 0, 17, 0x08, 0, 0, 0, 0, 0, 0, 0, 0, 98, 97, 100, 0]); 
    seeds.push(vec![0, 0, 0, 14, 0x03, 0, 0, 0, 0, 0, 0, 0, 0, 0]); 
    seeds.push(vec![0, 0, 0, 25, 0x03, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]); 
    seeds
}
