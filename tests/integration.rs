use zhensegg::store::{MemRing, Store};

#[test]
fn test_memring_append_read_roundtrip() {
    let ring = MemRing::new(1024 * 1024);
    let (off, len) = ring.append(b"topic", b"hello").unwrap();
    let mut out = Vec::new();
    ring.read(off, len, &mut out).unwrap();
    
    assert!(out.len() >= 8 + 5 + 5);
    let tl = u32::from_be_bytes([out[0], out[1], out[2], out[3]]) as usize;
    let pl = u32::from_be_bytes([out[4], out[5], out[6], out[7]]) as usize;
    assert_eq!(tl, 5);
    assert_eq!(pl, 5);
    assert_eq!(&out[8..13], b"topic");
    assert_eq!(&out[13..18], b"hello");
}

#[test]
fn test_memring_multiple_appends() {
    let ring = MemRing::new(1024 * 1024);
    let mut last_off = 0u64;
    for i in 0..100 {
        let payload = format!("msg-{i}");
        let (off, _) = ring.append(b"topic", payload.as_bytes()).unwrap();
        assert!(off >= last_off);
        last_off = off;
    }
    assert!(ring.write_pos() > 0);
}

#[test]
fn test_memring_append_and_fetch_payload() {
    let ring = MemRing::new(1024 * 1024);
    let (off, len) = ring.append(b"orders", b"payload-123").unwrap();
    let mut raw = Vec::new();
    ring.read(off, len, &mut raw).unwrap();
    let tl = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let pl = u32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]) as usize;
    let topic = &raw[8..8 + tl];
    let payload = &raw[8 + tl..8 + tl + pl];
    assert_eq!(topic, b"orders");
    assert_eq!(payload, b"payload-123");
}
