//! Background flusher thread for group-commit persistence (file mode only).

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

#[cfg(target_os = "linux")]
use std::sync::atomic::Ordering;
#[cfg(target_os = "linux")]
use std::time::Instant;

use super::mem::MemRing;
#[cfg(target_os = "linux")]
use super::mem::Store;

/// Opaque handle: holds the shared `synced` position and the flusher thread.
pub struct FlusherHandle {
    pub synced: Arc<AtomicU64>,
    _thread: Option<std::thread::JoinHandle<()>>,
}

#[cfg(target_os = "linux")]
pub fn spawn_flusher(
    inner: Arc<MemRing>,
    file: Arc<parking_lot::Mutex<std::fs::File>>,
    capacity: usize,
) -> FlusherHandle {
    use std::os::unix::io::AsRawFd;
    let synced = Arc::new(AtomicU64::new(0));
    let flush_inner = inner.clone();
    let flush_file = file.clone();
    let flush_synced = synced.clone();
    let thread = std::thread::Builder::new()
        .name("zhensegg-flusher".into())
        .spawn(move || {
            let mut buf: Vec<u8> = Vec::with_capacity(1024 * 1024);
            let mut last_sync = Instant::now();
            let fd = flush_file.lock().as_raw_fd();
            let mut flushed_pos: u64 = 0;
            loop {
                let target = flush_inner.committed_pos();
                let mut fpos = flushed_pos;
                // overwritten before flushed? skip ahead
                if target.saturating_sub(fpos) > capacity as u64 {
                    fpos = target - (capacity as u64) / 2;
                    flushed_pos = fpos;
                }
                if target > fpos {
                    // drain entire backlog in 1MB chunks (bandwidth-bound, no sleeps)
                    while fpos < target {
                        let chunk = ((target - fpos) as usize).min(1024 * 1024);
                        if flush_inner.read(fpos, chunk as u32, &mut buf).is_err() { break; }
                        let file_off = (fpos as usize) % capacity;
                        let mut data = &buf[..chunk];
                        if file_off + chunk <= capacity {
                            let ret = unsafe { libc::pwrite(fd, data.as_ptr() as *const libc::c_void, data.len(), file_off as i64) };
                            if ret < 0 { break; }
                        } else {
                            let first = capacity - file_off;
                            let ret = unsafe { libc::pwrite(fd, data.as_ptr() as *const libc::c_void, first, file_off as i64) };
                            if ret < 0 { break; }
                            data = &data[first..];
                            let ret = unsafe { libc::pwrite(fd, data.as_ptr() as *const libc::c_void, data.len(), 0) };
                            if ret < 0 { break; }
                        }
                        fpos += chunk as u64;
                        flushed_pos = fpos;
                    }
                    // group commit: one fdatasync covers the whole drained range
                    let _ = unsafe { libc::fdatasync(fd) };
                    flush_synced.store(fpos, Ordering::Release);
                    last_sync = Instant::now();
                } else if last_sync.elapsed() >= std::time::Duration::from_millis(1) {
                    let _ = unsafe { libc::fdatasync(fd) };
                    flush_synced.store(flushed_pos, Ordering::Release);
                    last_sync = Instant::now();
                    std::thread::sleep(std::time::Duration::from_micros(300));
                } else {
                    std::thread::sleep(std::time::Duration::from_micros(200));
                }
            }
        })
        .ok();
    FlusherHandle { synced, _thread: thread }
}

/// Non-Linux fallback: no async flusher; durable == write_pos.
#[cfg(not(target_os = "linux"))]
pub fn spawn_flusher(
    _inner: Arc<MemRing>,
    _file: Arc<parking_lot::Mutex<std::fs::File>>,
    _capacity: usize,
) -> FlusherHandle {
    FlusherHandle { synced: Arc::new(AtomicU64::new(0)), _thread: None }
}
