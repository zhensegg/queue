use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::durable::DurableGate;
use super::file::{encode_header, HEADER_SIZE};
use super::mem::{MemRing, Store};

const FLUSH_CHUNK: usize = 1024 * 1024;
const IDLE_SLEEP: Duration = Duration::from_micros(200);

pub struct FlusherHandle {
    pub gate: Arc<DurableGate>,
    failed: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    _thread: Option<std::thread::JoinHandle<()>>,
}

impl FlusherHandle {
    pub fn wait_for(&self, target: u64, timeout: Duration) -> u64 {
        let deadline = Instant::now() + timeout;
        loop {
            let cur = self.gate.pos();
            if cur >= target || self.failed.load(std::sync::atomic::Ordering::Acquire) {
                return cur;
            }
            if Instant::now() >= deadline {
                return cur;
            }
            std::thread::sleep(Duration::from_micros(200));
        }
    }

    pub fn shutdown(&mut self) {
        self.shutdown.store(true, std::sync::atomic::Ordering::Release);
        if let Some(thread) = self._thread.take() {
            let _ = thread.join();
        }
    }

    pub fn is_failed(&self) -> bool {
        self.failed.load(std::sync::atomic::Ordering::Acquire)
    }
}

pub fn spawn_flusher(
    inner: Arc<MemRing>,
    file: Arc<parking_lot::Mutex<std::fs::File>>,
    capacity: usize,
    gate: Arc<DurableGate>,
) -> FlusherHandle {
    let failed = Arc::new(AtomicBool::new(false));
    let shutdown = Arc::new(AtomicBool::new(false));
    let thread = {
        let (gate_c, failed_c, shutdown_c) = (gate.clone(), failed.clone(), shutdown.clone());
        std::thread::Builder::new()
            .name("zhensegg-flusher".into())
            .spawn(move || {
                flusher_loop(inner, file, capacity, gate_c, failed_c, shutdown_c);
            })
            .ok()
    };
    FlusherHandle { gate, failed, shutdown, _thread: thread }
}

fn fail_stop(failed: &AtomicBool, error: &std::io::Error, phase: &'static str) -> ! {
    tracing::error!(error = %error, phase, "flusher I/O error; fail-stop (systemd restarts; recovery resumes from last synced header)");
    failed.store(true, std::sync::atomic::Ordering::Release);
    std::process::exit(70);
}

fn flusher_loop(
    inner: Arc<MemRing>,
    file: Arc<parking_lot::Mutex<std::fs::File>>,
    capacity: usize,
    gate: Arc<DurableGate>,
    failed: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
) {
    let mut hdr: [u8; HEADER_SIZE as usize] = [0u8; HEADER_SIZE as usize];
    let mut flushed: u64 = 0;
    loop {
        if let Err(e) = flush_cycle(&inner, &file, capacity, &mut flushed, &mut hdr, &gate) {
            fail_stop(&failed, &e, "drain");
        }
        if shutdown.load(std::sync::atomic::Ordering::Acquire) {
            if let Err(e) = flush_cycle(&inner, &file, capacity, &mut flushed, &mut hdr, &gate) {
                fail_stop(&failed, &e, "final drain");
            }
            break;
        }
        std::thread::sleep(IDLE_SLEEP);
    }
}

fn flush_cycle(
    inner: &Arc<MemRing>,
    file: &Arc<parking_lot::Mutex<std::fs::File>>,
    capacity: usize,
    flushed: &mut u64,
    hdr: &mut [u8; HEADER_SIZE as usize],
    gate: &Arc<DurableGate>,
) -> std::io::Result<()> {
    let target = inner.committed_pos();
    let mut fpos = *flushed;
    if target.saturating_sub(fpos) > capacity as u64 {
        fpos = target - (capacity as u64) / 2;
        *flushed = fpos;
    }
    if target > fpos {
        let mut buf: Vec<u8> = Vec::with_capacity(FLUSH_CHUNK);
        while fpos < target {
            let chunk = ((target - fpos) as usize).min(FLUSH_CHUNK);
            if let Err(e) = inner.read(fpos, chunk as u32, &mut buf) {
                return Err(std::io::Error::other(format!(
                    "committed-range read failed at {fpos}: {e:?}"
                )));
            }
            let file_off = HEADER_SIZE as usize + (fpos as usize % capacity);
            let data = &buf[..chunk];
            if file_off + chunk <= HEADER_SIZE as usize + capacity {
                write_all_at(file, file_off as u64, data)?;
            } else {
                let first = HEADER_SIZE as usize + capacity - file_off;
                if first == 0 {
                    return Err(std::io::Error::other("flusher wrap position out of range"));
                }
                write_all_at(file, file_off as u64, &data[..first])?;
                write_all_at(file, HEADER_SIZE, &data[first..])?;
            }
            fpos += chunk as u64;
            *flushed = fpos;
        }
        encode_header(hdr, target, target);
        write_all_at(file, 0, hdr)?;
        sync_file(file)?;
        gate.advance(target);
    }
    Ok(())
}

fn write_all_at(
    file: &Arc<parking_lot::Mutex<std::fs::File>>,
    off: u64,
    data: &[u8],
) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let fd = file.lock().as_raw_fd();
        let mut written = 0usize;
        while written < data.len() {
            let ptr = unsafe { data.as_ptr().add(written) };
            let ret = unsafe {
                libc::pwrite(
                    fd,
                    ptr as *const libc::c_void,
                    data.len() - written,
                    (off + written as u64) as libc::off_t,
                )
            };
            if ret < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(err);
            }
            written += ret as usize;
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = file.lock();
        f.seek(SeekFrom::Start(off))?;
        f.write_all(data)
    }
}

fn sync_file(file: &Arc<parking_lot::Mutex<std::fs::File>>) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let fd = file.lock().as_raw_fd();
        let ret = unsafe { libc::fdatasync(fd) };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        file.lock().sync_data()
    }
}
