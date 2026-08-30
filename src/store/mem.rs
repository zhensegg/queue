use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
pub enum StoreError {
    Full,
    NotFound,
    Io(std::io::Error),
    InvalidOffset,
    Overflow,
}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        StoreError::Io(e)
    }
}

pub trait Store: Send + Sync {
    fn append(&self, topic: &[u8], payload: &[u8]) -> Result<(u64, u32), StoreError>;
    fn read(&self, offset: u64, len: u32, out: &mut Vec<u8>) -> Result<(), StoreError>;
    fn write_pos(&self) -> u64;
    fn durable_pos(&self) -> u64 {
        self.write_pos()
    }
    fn durable_gate(&self) -> Option<std::sync::Arc<super::durable::DurableGate>> {
        None
    }
    fn sync_pending(&self, _timeout: std::time::Duration) -> u64 {
        self.write_pos()
    }
    fn set_reject_overflow(&self, _on: bool) {}
    fn attach_watermark(&self, _wm: Arc<AtomicU64>) {}
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

pub struct MemRing {
    buf: std::cell::UnsafeCell<Vec<u8>>,
    capacity: usize,
    write_pos: AtomicU64,
    committed: AtomicU64,
    records: AtomicU64,
    reject_overflow: AtomicBool,
    watermark: OnceLock<Arc<AtomicU64>>,
}

unsafe impl Sync for MemRing {}
unsafe impl Send for MemRing {}

#[cfg(target_os = "linux")]
fn harden(v: &mut [u8]) {
    unsafe {
        let _ = libc::madvise(v.as_mut_ptr() as *mut libc::c_void, v.len(), libc::MADV_HUGEPAGE);
        let _ = libc::mlock(v.as_ptr() as *const libc::c_void, v.len());
    }
}

impl MemRing {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.next_power_of_two().max(64 * 1024);
        let ring = Self {
            buf: std::cell::UnsafeCell::new(Self::alloc(cap)),
            capacity: cap,
            write_pos: AtomicU64::new(0),
            committed: AtomicU64::new(0),
            records: AtomicU64::new(0),
            reject_overflow: AtomicBool::new(false),
            watermark: OnceLock::new(),
        };
        #[cfg(target_os = "linux")]
        {
            let buf_ref: &[u8] = unsafe { &*ring.buf.get() };
            try_register_buffers(buf_ref);
        }
        ring
    }

    #[cfg(target_os = "linux")]
    fn alloc(cap: usize) -> Vec<u8> {
        let mut v = vec![0u8; cap];
        harden(&mut v);
        v
    }

    #[cfg(not(target_os = "linux"))]
    fn alloc(cap: usize) -> Vec<u8> {
        vec![0u8; cap]
    }

    pub fn from_buffer(data: Vec<u8>, capacity: usize, write_pos: u64, committed: u64) -> Self {
        assert_eq!(data.len(), capacity, "recovered ring bytes must match capacity");
        debug_assert!(committed <= write_pos);
        #[allow(unused_mut)]
        let mut buf = data;
        #[cfg(target_os = "linux")]
        harden(&mut buf);
        let ring = Self {
            buf: std::cell::UnsafeCell::new(buf),
            capacity,
            write_pos: AtomicU64::new(write_pos),
            committed: AtomicU64::new(committed),
            records: AtomicU64::new(0),
            reject_overflow: AtomicBool::new(false),
            watermark: OnceLock::new(),
        };
        #[cfg(target_os = "linux")]
        {
            let buf_ref: &[u8] = unsafe { &*ring.buf.get() };
            try_register_buffers(buf_ref);
        }
        ring
    }

    pub fn with_default() -> Self {
        Self::new(256 * 1024 * 1024)
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
        let cur = self.write_pos.load(Ordering::Relaxed);
        if self.reject_overflow.load(Ordering::Acquire)
            && let Some(wm) = self.watermark.get()
        {
            let w = wm.load(Ordering::Acquire);
            if w != u64::MAX && cur + rec_len as u64 - w > self.capacity as u64 {
                return Err(StoreError::Overflow);
            }
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
    pub fn committed_pos(&self) -> u64 {
        self.committed.load(Ordering::Acquire)
    }

    pub fn set_reject_overflow(&self, on: bool) {
        self.reject_overflow.store(on, Ordering::Release);
    }

    pub fn attach_watermark(&self, wm: Arc<AtomicU64>) {
        let _ = self.watermark.set(wm);
    }
}
