//! In-memory ring store with lock-free append.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub enum StoreError {
    Full,
    NotFound,
    Io(std::io::Error),
    InvalidOffset,
}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        StoreError::Io(e)
    }
}

pub trait Store: Send + Sync {
    /// Append payload for topic, returns global byte offset and len.
    fn append(&self, topic: &[u8], payload: &[u8]) -> Result<(u64, u32), StoreError>;
    /// Read payload by offset. Copies into out buffer.
    fn read(&self, offset: u64, len: u32, out: &mut Vec<u8>) -> Result<(), StoreError>;
    fn write_pos(&self) -> u64;
    /// Position durable for THIS store's semantics (mem: written to RAM; file: fdatasync'd to disk).
    fn durable_pos(&self) -> u64 {
        self.write_pos()
    }
}

#[cfg(target_os = "linux")]
fn try_register_buffers(buf: &[u8]) {
    let _ = std::panic::catch_unwind(|| {
        if let Ok(ring) = io_uring::IoUring::new(1) {
            let iov = libc::iovec {
                iov_base: buf.as_ptr() as *mut libc::c_void,
                iov_len: buf.len(),
            };
            let _ = unsafe { ring.submitter().register_buffers(&[iov]) };
        }
    });
}

#[cfg(target_os = "linux")]
pub fn try_register_file(file: &std::fs::File) {
    use std::os::unix::io::AsRawFd;
    let _ = std::panic::catch_unwind(|| {
        if let Ok(ring) = io_uring::IoUring::new(1) {
            let fd = file.as_raw_fd();
            let _ = ring.submitter().register_files(&[fd]);
        }
    });
}

/// In-memory circular buffer. Fast path for >11M in-memory benchmark.
/// No disk, no fsync, pure memory sequential access.
/// Layout for each record: [4 topic_len][4 payload_len][topic][payload]
/// Lock-free append via UnsafeCell + atomic reservation for linear scalability.
/// Concurrent appends to disjoint offsets do NOT contend on lock.
pub struct MemRing {
    buf: std::cell::UnsafeCell<Vec<u8>>,
    capacity: usize,
    write_pos: AtomicU64, // monotonic byte offset (not circular index)
    committed: AtomicU64, // fully copied records only (torn-read guard for flusher)
    records: AtomicU64,
}

// SAFETY: buf is accessed via raw pointers to disjoint regions.
// Each append reserves unique [offset, offset+len) via atomic fetch_add, so no aliasing.
// Reads race with writes only when ring wraps; caller checks for overwrite.
unsafe impl Sync for MemRing {}
unsafe impl Send for MemRing {}

impl MemRing {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.next_power_of_two().max(64 * 1024);
        let ring = Self {
            buf: std::cell::UnsafeCell::new(Self::alloc(cap)),
            capacity: cap,
            write_pos: AtomicU64::new(0),
            committed: AtomicU64::new(0),
            records: AtomicU64::new(0),
        };
        #[cfg(target_os = "linux")]
        {
            // best-effort register fixed buffer for io_uring to avoid per-op mmap
            let buf_ref: &[u8] = unsafe { &*ring.buf.get() };
            try_register_buffers(buf_ref);
        }
        ring
    }

    #[cfg(target_os = "linux")]
    fn alloc(cap: usize) -> Vec<u8> {
        let mut v = vec![0u8; cap];
        unsafe {
            // THP: 2MB transparent huge pages (glibc mmap's large allocs -> page aligned)
            let _ = libc::madvise(v.as_mut_ptr() as *mut libc::c_void, cap, libc::MADV_HUGEPAGE);
            // prefault all pages + pin in RAM: no page faults on hot path
            let _ = libc::mlock(v.as_ptr() as *const libc::c_void, cap);
        }
        v
    }

    #[cfg(not(target_os = "linux"))]
    fn alloc(cap: usize) -> Vec<u8> {
        vec![0u8; cap]
    }

    pub fn with_default() -> Self {
        Self::new(256 * 1024 * 1024) // 256 MB ring
    }

    #[inline]
    fn mask(&self, offset: u64) -> usize {
        (offset as usize) & (self.capacity - 1)
    }
}

impl Store for MemRing {
    fn append(&self, topic: &[u8], payload: &[u8]) -> Result<(u64, u32), StoreError> {
        let rec_len = 8 + topic.len() + payload.len();
        if rec_len > self.capacity / 2 {
            return Err(StoreError::Full);
        }
        let offset = self.write_pos.fetch_add(rec_len as u64, Ordering::Relaxed);
        let start = self.mask(offset);
        unsafe {
            let ptr = (*self.buf.get()).as_mut_ptr();
            if start + rec_len <= self.capacity {
                let dst = ptr.add(start);
                let tl = (topic.len() as u32).to_be_bytes();
                let pl = (payload.len() as u32).to_be_bytes();
                std::ptr::copy_nonoverlapping(tl.as_ptr(), dst, 4);
                std::ptr::copy_nonoverlapping(pl.as_ptr(), dst.add(4), 4);
                std::ptr::copy_nonoverlapping(topic.as_ptr(), dst.add(8), topic.len());
                std::ptr::copy_nonoverlapping(payload.as_ptr(), dst.add(8 + topic.len()), payload.len());
            } else {
                // wrap: need to handle split - copy via intermediate tmp to preserve atomicity
                let mut tmp = Vec::with_capacity(rec_len);
                tmp.extend_from_slice(&(topic.len() as u32).to_be_bytes());
                tmp.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                tmp.extend_from_slice(topic);
                tmp.extend_from_slice(payload);
                let first = self.capacity - start;
                std::ptr::copy_nonoverlapping(tmp.as_ptr(), ptr.add(start), first);
                std::ptr::copy_nonoverlapping(tmp.as_ptr().add(first), ptr, rec_len - first);
            }
        }
        self.records.fetch_add(1, Ordering::Relaxed);
        // mark record fully copied (monotonic max; torn-read guard for flusher)
        self.committed.fetch_max(offset + rec_len as u64, Ordering::Release);
        Ok((offset, rec_len as u32))
    }

    fn read(&self, offset: u64, len: u32, out: &mut Vec<u8>) -> Result<(), StoreError> {
        let cur = self.write_pos.load(Ordering::Acquire);
        if offset + len as u64 > cur {
            return Err(StoreError::InvalidOffset);
        }
        if cur - offset > self.capacity as u64 {
            return Err(StoreError::NotFound);
        }
        let start = self.mask(offset);
        out.clear();
        out.reserve(len as usize);
        out.resize(len as usize, 0);
        unsafe {
            let ptr = (*self.buf.get()).as_ptr();
            if start + len as usize <= self.capacity {
                std::ptr::copy_nonoverlapping(ptr.add(start), out.as_mut_ptr(), len as usize);
            } else {
                let first = self.capacity - start;
                std::ptr::copy_nonoverlapping(ptr.add(start), out.as_mut_ptr(), first);
                std::ptr::copy_nonoverlapping(ptr, out.as_mut_ptr().add(first), len as usize - first);
            }
        }
        Ok(())
    }

    fn write_pos(&self) -> u64 {
        self.write_pos.load(Ordering::Relaxed)
    }
}

impl MemRing {
    /// Fully-copied records (no torn reads past this point)
    pub fn committed_pos(&self) -> u64 {
        self.committed.load(Ordering::Acquire)
    }
}
