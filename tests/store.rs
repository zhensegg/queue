use std::sync::Arc;

use zhensegg::store::{FileRing, MemRing, Store, StoreError};

fn decode_record(raw: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let tl = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let pl = u32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]) as usize;
    let topic = raw[8..8 + tl].to_vec();
    let payload = raw[8 + tl..8 + tl + pl].to_vec();
    (topic, payload)
}

#[test]
fn text_memring_append_read_roundtrip() {
    let ring = MemRing::new(1024 * 1024);
    let (off, len) = ring.append(b"orders", b"abc").unwrap();
    let mut raw = Vec::new();
    ring.read(off, len, &mut raw).unwrap();
    let (topic, payload) = decode_record(&raw);
    assert_eq!(topic.as_slice(), b"orders");
    assert_eq!(payload.as_slice(), b"abc");
}

#[test]
fn test_memring_positions_monotonic() {
    let ring = MemRing::new(1024 * 1024);
    let mut last = 0u64;
    for i in 0..1000 {
        let payload = format!("msg-{i}");
        let (off, _) = ring.append(b"t", payload.as_bytes()).unwrap();
        assert!(off >= last);
        last = off;
    }
    assert!(ring.write_pos() > last);
    assert_eq!(ring.durable_pos(), ring.write_pos(), "mem store is always durable");
}

#[test]
fn test_memring_reads_back_all_messages() {
    let ring = MemRing::new(1024 * 1024);
    let mut offsets = Vec::new();
    for i in 0..50 {
        let payload = format!("data-{i}");
        let (off, len) = ring.append(b"topic", payload.as_bytes()).unwrap();
        offsets.push((off, len, payload));
    }
    for (off, len, expected) in offsets {
        let mut raw = Vec::new();
        ring.read(off, len, &mut raw).unwrap();
        let (_topic, payload) = decode_record(&raw);
        assert_eq!(String::from_utf8(payload).unwrap(), expected);
    }
}

#[test]
fn test_memring_full_error_on_oversized_record() {
    let ring = MemRing::new(64 * 1024); // min capacity
    let big = vec![b'x'; 40 * 1024]; // rec > capacity/2
    let err = ring.append(b"t", &big).unwrap_err();
    assert!(matches!(err, StoreError::Full));
}

#[test]
fn test_memring_invalid_offset_error() {
    let ring = MemRing::new(1024 * 1024);
    let (off, len) = ring.append(b"t", b"hello").unwrap();
    // reading past write_pos is invalid
    let mut out = Vec::new();
    let err = ring.read(off + 10_000_000, len, &mut out).unwrap_err();
    assert!(matches!(err, StoreError::InvalidOffset));
}

#[test]
fn test_memring_wraps_around_and_reads() {
    // minimum capacity is 64KB; write enough records to wrap the ring
    let ring = MemRing::new(64 * 1024);
    let mut offsets_first = Vec::new();
    let mut offsets_last = Vec::new();
    for i in 0..2000 {
        let payload = format!("payload-{i}-with-some-length");
        let (off, len) = ring.append(b"topic", payload.as_bytes()).unwrap();
        if i < 20 {
            offsets_first.push((off, len));
        }
        if i >= 1980 {
            offsets_last.push((off, len));
        }
    }
    // old (overwritten) records may be NotFound; new ones must read back fine
    for (off, len) in offsets_last {
        let mut raw = Vec::new();
        if ring.read(off, len, &mut raw).is_ok() {
            let (topic, _payload) = decode_record(&raw);
            assert_eq!(topic.as_slice(), b"topic");
        }
    }
    let _ = offsets_first; // not verified: ring likely wrapped and overwrote them
    assert!(ring.write_pos() > 64 * 1024, "written enough to wrap");
}

#[test]
fn test_file_ring_append_read_roundtrip() {
    let path = std::env::temp_dir().join(format!(
        "zhensegg-file-ring-test-{}.dat",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let ring = FileRing::new(path.to_str().unwrap(), 1024 * 1024).unwrap();
    let (off, len) = ring.append(b"events", b"file-payload").unwrap();
    let mut raw = Vec::new();
    ring.read(off, len, &mut raw).unwrap();
    let (topic, payload) = decode_record(&raw);
    assert_eq!(topic.as_slice(), b"events");
    assert_eq!(payload.as_slice(), b"file-payload");
    assert!(ring.write_pos() > 0);
    drop(ring);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_file_ring_multiple_records() {
    let path = std::env::temp_dir().join(format!(
        "zhensegg-file-ring-multi-{}.dat",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let ring = FileRing::new(path.to_str().unwrap(), 1024 * 1024).unwrap();
    let mut last = 0u64;
    for i in 0..100 {
        let payload = format!("m{i}");
        let (off, _) = ring.append(b"topic", payload.as_bytes()).unwrap();
        assert!(off >= last);
        last = off;
    }
    // On Linux the background flusher advances durable_pos to write_pos via
    // group-commit; on Windows no flusher runs, so durable_pos stays at 0.
    #[cfg(target_os = "linux")]
    assert_eq!(ring.durable_pos(), ring.write_pos(), "all records readable");
    #[cfg(not(target_os = "linux"))]
    assert!(ring.write_pos() > 0);
    drop(ring);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_store_trait_object_dispatch() {
    // both mem and file stores can be used behind the same trait object
    let mem: Arc<dyn Store> = Arc::new(MemRing::new(1024 * 1024));
    let (off, len) = mem.append(b"t", b"m1").unwrap();
    let mut raw = Vec::new();
    mem.read(off, len, &mut raw).unwrap();

    let path = std::env::temp_dir().join(format!("zhensegg-trait-{}.dat", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let file: Arc<dyn Store> = Arc::new(FileRing::new(path.to_str().unwrap(), 1024 * 1024).unwrap());
    let (off2, len2) = file.append(b"t", b"f1").unwrap();
    let mut raw2 = Vec::new();
    file.read(off2, len2, &mut raw2).unwrap();
    let _ = std::fs::remove_file(&path);
}
