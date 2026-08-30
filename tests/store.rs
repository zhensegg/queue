use std::sync::Arc;

use zhensegg::store::{wait_durable, FileRing, MemRing, Store, StoreError};

fn decode_record(raw: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let tl = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let pl = u32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]) as usize;
    let topic = raw[8..8 + tl].to_vec();
    let payload = raw[8 + tl..8 + tl + pl].to_vec();
    (topic, payload)
}

fn scratch(name: &str) -> (std::path::PathBuf, String) {
    let p = std::env::temp_dir().join(format!("zhensegg-{name}-{}.dat", std::process::id()));
    let _ = std::fs::remove_file(&p);
    (p.clone(), p.to_str().unwrap().to_string())
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
    let ring = MemRing::new(64 * 1024);
    let big = vec![b'x'; 40 * 1024];
    let err = ring.append(b"t", &big).unwrap_err();
    assert!(matches!(err, StoreError::Full));
}

#[test]
fn test_memring_invalid_offset_error() {
    let ring = MemRing::new(1024 * 1024);
    let (off, len) = ring.append(b"t", b"hello").unwrap();
    let mut out = Vec::new();
    let err = ring.read(off + 10_000_000, len, &mut out).unwrap_err();
    assert!(matches!(err, StoreError::InvalidOffset));
}

#[test]
fn test_memring_wraps_around_and_reads() {
    let ring = MemRing::new(64 * 1024);
    let mut offsets_last = Vec::new();
    for i in 0..2000 {
        let payload = format!("payload-{i}-with-some-length");
        let (off, len) = ring.append(b"topic", payload.as_bytes()).unwrap();
        if i >= 1980 {
            offsets_last.push((off, len));
        }
    }
    for (off, len) in offsets_last {
        let mut raw = Vec::new();
        if ring.read(off, len, &mut raw).is_ok() {
            let (topic, _payload) = decode_record(&raw);
            assert_eq!(topic.as_slice(), b"topic");
        }
    }
    assert!(ring.write_pos() > 64 * 1024, "written enough to wrap");
}

#[test]
fn test_file_ring_append_read_roundtrip() {
    let (path, p) = scratch("ring-rt");
    let ring = FileRing::new(&p, 1024 * 1024).unwrap();
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
    let (path, p) = scratch("ring-multi");
    let ring = FileRing::new(&p, 1024 * 1024).unwrap();
    let mut last = 0u64;
    for i in 0..100 {
        let payload = format!("m{i}");
        let (off, _) = ring.append(b"topic", payload.as_bytes()).unwrap();
        assert!(off >= last);
        last = off;
    }
    let target = ring.write_pos();
    let durable = ring.sync_pending(std::time::Duration::from_secs(5));
    assert_eq!(durable, target, "all records flushted on demand");
    drop(ring);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_store_trait_object_dispatch() {
    let mem: Arc<dyn Store> = Arc::new(MemRing::new(1024 * 1024));
    let (off, len) = mem.append(b"t", b"m1").unwrap();
    let mut raw = Vec::new();
    mem.read(off, len, &mut raw).unwrap();

    let (_path, p) = scratch("trait");
    let file: Arc<dyn Store> = Arc::new(FileRing::new(&p, 1024 * 1024).unwrap());
    let (off2, len2) = file.append(b"t", b"f1").unwrap();
    let mut raw2 = Vec::new();
    file.read(off2, len2, &mut raw2).unwrap();
    let _ = std::fs::remove_file(&_path);
}

#[test]
fn test_file_ring_survives_kill9_and_reopens() {
    let (path, p) = scratch("kill9");
    {
        let ring = FileRing::new(&p, 1024 * 1024).unwrap();
        for i in 0..500 {
            let payload = format!("m{i}");
            let _ = ring.append(b"t", payload.as_bytes()).unwrap();
        }
        let _ = ring.sync_pending(std::time::Duration::from_secs(5));
        let target = ring.durable_pos();
        assert!(target > 0, "flusher must reach at least one group commit");
        std::mem::forget(ring);
        let _ = target;
    }
    let ring = FileRing::new(&p, 1024 * 1024).unwrap();
    assert_eq!(ring.durable_pos(), ring.write_pos());
    drop(ring);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_file_ring_corrupt_slot_falls_back_to_replica() {
    let (path, p) = scratch("corrupt-slot");
    {
        let ring = FileRing::new(&p, 1024 * 1024).unwrap();
        let _ = ring.append(b"t", b"x").unwrap();
        let _ = ring.sync_pending(std::time::Duration::from_secs(5));
        std::mem::forget(ring);
    }
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&[0xFF; 64]).unwrap();
    }
    let ring = FileRing::new(&p, 1024 * 1024).unwrap();
    assert!(ring.durable_pos() >= 1, "replica slot must carry the committed position");
    drop(ring);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_file_ring_corrupt_header_refuses_startup() {
    let (path, p) = scratch("corrupt-header");
    {
        let ring = FileRing::new(&p, 1024 * 1024).unwrap();
        let _ = ring.append(b"t", b"x").unwrap();
        let _ = ring.sync_pending(std::time::Duration::from_secs(5));
        std::mem::forget(ring);
    }
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&[0xFF; 128]).unwrap();
    }
    let err = FileRing::new(&p, 1024 * 1024).err().expect("corrupt header must refuse startup");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_file_ring_torn_tail_is_ignored() {
    let (path, p) = scratch("torn");
    let durable: u64;
    {
        let ring = FileRing::new(&p, 4 * 1024 * 1024).unwrap();
for i in 0..1000 {
            let payload = format!("torn-{i}");
            let _ = ring.append(b"t", payload.as_bytes()).unwrap();
        }
        let _ = ring.sync_pending(std::time::Duration::from_secs(5));
        durable = ring.durable_pos();
        assert!(durable > 0);
        std::mem::forget(ring);
    }
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(durable + 8192)).unwrap();
        f.write_all(&[0xAA; 256]).unwrap();
    }
    let ring = FileRing::new(&p, 4 * 1024 * 1024).unwrap();
    assert_eq!(ring.durable_pos(), durable, "torn tail beyond committed is ignored");
    drop(ring);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_file_ring_reopen_after_immediate_crash() {
    let (_path, p) = scratch("immediate");
    {
        let ring = FileRing::new(&p, 1024 * 1024).unwrap();
        let _ = ring.append(b"t", b"boom").unwrap();
        std::mem::forget(ring);
    }
    let ring = FileRing::new(&p, 1024 * 1024).unwrap();
    assert_eq!(ring.write_pos(), ring.durable_pos());
    drop(ring);
    let _ = std::fs::remove_file(&_path);
}

#[test]
fn test_file_ring_durable_gate_waits_for_fsync() {
    let (_path, p) = scratch("gate");
    let ring = FileRing::new(&p, 1024 * 1024).unwrap();
    let (off, len) = ring.append(b"t", b"durable").unwrap();
    let need = off + len as u64;
    let gate = ring.durable_gate().unwrap().clone();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = rt.block_on(async {
        let wait = wait_durable(gate, need);
        tokio::time::timeout(std::time::Duration::from_secs(5), wait).await
    });
    assert!(result.is_ok(), "durable gate must reach the acked position");
    assert!(ring.durable_pos() >= need);
    drop(ring);
    let _ = std::fs::remove_file(&_path);
}

#[cfg(unix)]
#[test]
fn test_file_ring_second_broker_rejected_by_flock() {
    let (_path, p) = scratch("flock");
    let ring = FileRing::new(&p, 1024 * 1024).unwrap();
    let err = FileRing::new(&p, 1024 * 1024).err().expect("second broker must be rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
    drop(ring);
    let _ = std::fs::remove_file(&_path);
}

#[test]
fn test_overflow_reject_policy_blocks_wrap_of_undelivered() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let ring = MemRing::new(64 * 1024);
    ring.set_reject_overflow(true);
    let wm = Arc::new(AtomicU64::new(0));
    ring.attach_watermark(wm.clone());
    let payload = vec![b'x'; 100];
    let mut last = (0u64, 0u32);
    for _ in 0..100000 {
        match ring.append(b"t", &payload) {
            Ok(v) => last = v,
            Err(StoreError::Overflow) => break,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
    assert!(
        last.0 + last.1 as u64 > 60000,
        "ring must fill up before the first overflow rejection"
    );
    assert!(matches!(ring.append(b"t", &payload), Err(StoreError::Overflow)));
    wm.store(u64::MAX, Ordering::Release);
    assert!(ring.append(b"t", &payload).is_ok());
}

#[test]
fn test_overflow_overwrite_default_wraps_silently() {
    let ring = MemRing::new(64 * 1024);
    let payload = vec![b'x'; 100];
    for _ in 0..2000 {
        ring.append(b"t", &payload).expect("overwrite policy must never reject");
    }
}
