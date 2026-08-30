//! Persistent file ring with background group-commit flusher (file mode).

use std::fs::{File, OpenOptions};
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

use super::flusher::{self, FlusherHandle};
use super::mem::{MemRing, Store, StoreError};

/// Persistent ring with async group-commit.
/// Append is lock-free to MemRing (RAM). A background flusher drains new
/// records [flushed_pos..write_pos] and does batched pwrite to file,
/// then fdatasync every ~1ms (group commit). File layout == MemRing layout,
/// so file offset = logical offset % capacity.
pub struct FileRing {
    inner: Arc<MemRing>,
    _file: Arc<parking_lot::Mutex<File>>,
    _capacity: usize,
    _flusher: FlusherHandle,
}

impl FileRing {
    pub fn new(path: &str, capacity: usize) -> std::io::Result<Self> {
        let cap = capacity.next_power_of_two().max(64 * 1024);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.set_len(cap as u64)?;
        #[cfg(target_os = "linux")]
        unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL); }
        #[cfg(target_os = "linux")]
        super::mem::try_register_file(&file);
        let inner = Arc::new(MemRing::new(cap));
        let file_arc = Arc::new(parking_lot::Mutex::new(file));
        let _flusher = flusher::spawn_flusher(inner.clone(), file_arc.clone(), cap);
        Ok(Self { inner, _file: file_arc, _capacity: cap, _flusher })
    }
}

impl Store for FileRing {
    fn append(&self, topic: &[u8], payload: &[u8]) -> Result<(u64, u32), StoreError> {
        // Fast path: append to RAM ring (lock-free, 13M+ RPS)
        let (off, len) = self.inner.append(topic, payload)?;
        Ok((off, len))
    }

    fn read(&self, offset: u64, len: u32, out: &mut Vec<u8>) -> Result<(), StoreError> {
        // Read from inner mem ring (fast)
        self.inner.read(offset, len, out)
    }

    fn write_pos(&self) -> u64 {
        self.inner.write_pos()
    }

    fn durable_pos(&self) -> u64 {
        self._flusher.synced.load(Ordering::Acquire)
    }
}

impl Drop for FileRing {
    fn drop(&mut self) {
        // best effort sync on drop
        #[cfg(target_os = "linux")]
        {
            if let Some(f) = self._file.try_lock() {
                let _ = unsafe { libc::fdatasync(f.as_raw_fd()) };
            }
        }
    }
}
