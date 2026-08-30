use std::fs::{File, OpenOptions};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

use super::durable::DurableGate;
use super::flusher::{self, FlusherHandle};
use super::mem::{MemRing, Store, StoreError};

pub const HEADER_SIZE: u64 = 128;
pub const SLOT_SIZE: usize = 64;
const MAGIC: [u8; 4] = *b"ZSR2";
const VERSION: u32 = 2;

const fn make_crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

static CRC_TABLE: [u32; 256] = make_crc_table();

fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc = CRC_TABLE[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

fn encode_slot(buf: &mut [u8], write_pos: u64, committed: u64) {
    debug_assert!(committed <= write_pos);
    buf[..4].copy_from_slice(&MAGIC);
    buf[4..8].copy_from_slice(&VERSION.to_be_bytes());
    buf[8..16].copy_from_slice(&write_pos.to_be_bytes());
    buf[16..24].copy_from_slice(&committed.to_be_bytes());
    let crc = crc32(&buf[..24]);
    buf[24..28].copy_from_slice(&crc.to_be_bytes());
    for b in buf[28..SLOT_SIZE].iter_mut() {
        *b = 0;
    }
}

fn decode_slot(buf: &[u8]) -> Option<(u64, u64)> {
    if buf.len() < SLOT_SIZE {
        return None;
    }
    if buf[..4] != MAGIC {
        return None;
    }
    let version = u32::from_be_bytes(buf[4..8].try_into().ok()?);
    if version != VERSION {
        return None;
    }
    let stored_crc = u32::from_be_bytes(buf[24..28].try_into().ok()?);
    if crc32(&buf[..24]) != stored_crc {
        return None;
    }
    let write_pos = u64::from_be_bytes(buf[8..16].try_into().ok()?);
    let committed = u64::from_be_bytes(buf[16..24].try_into().ok()?);
    if committed > write_pos {
        return None;
    }
    Some((write_pos, committed))
}

pub fn encode_header(buf: &mut [u8], write_pos: u64, committed: u64) {
    debug_assert_eq!(buf.len(), HEADER_SIZE as usize);
    encode_slot(&mut buf[..SLOT_SIZE], write_pos, committed);
    encode_slot(&mut buf[SLOT_SIZE..2 * SLOT_SIZE], write_pos, committed);
}

fn decode_header(buf: &[u8]) -> Option<(u64, u64)> {
    if buf.len() < HEADER_SIZE as usize {
        return None;
    }
    let s0 = decode_slot(&buf[..SLOT_SIZE]);
    let s1 = decode_slot(&buf[SLOT_SIZE..]);
    let picked = match (s0, s1) {
        (Some(a), Some(b)) => {
            if a.1 >= b.1 {
                a
            } else {
                b
            }
        }
        (Some(a), None) => a,
        (None, Some(b)) => b,
        (None, None) => return None,
    };
    Some(picked)
}

fn header_all_zero(buf: &[u8]) -> bool {
    buf[..HEADER_SIZE as usize].iter().all(|&b| b == 0)
}

pub struct FileRing {
    inner: Arc<MemRing>,
    file: Arc<parking_lot::Mutex<File>>,
    _capacity: usize,
    flusher: FlusherHandle,
    gate: Arc<DurableGate>,
}

impl FileRing {
    pub fn new(path: &str, capacity: usize) -> std::io::Result<Self> {
        let data_capacity = (capacity.saturating_sub(HEADER_SIZE as usize))
            .next_power_of_two()
            .max(64 * 1024);
        let total_len = HEADER_SIZE + data_capacity as u64;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            let ret = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
            if ret != 0 {
                let err = std::io::Error::last_os_error();
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("ring file `{path}` is locked by another process: {err}"),
                ));
            }
        }

        let meta_len = file.metadata()?.len();
        if meta_len == 0 {
            file.set_len(total_len)?;
        } else if meta_len < HEADER_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("ring file `{path}` is not a zhensegg ring (too small)"),
            ));
        }

        let mut ring_bytes = vec![0u8; total_len as usize];
        use std::io::{Read, Seek, SeekFrom};
        {
            let mut f = &file;
            f.seek(SeekFrom::Start(0))?;
            f.read_exact(&mut ring_bytes)?;
        }

        let (write_pos, committed) = match decode_header(&ring_bytes[..HEADER_SIZE as usize]) {
            Some(h) => h,
            None if header_all_zero(&ring_bytes) => (0, 0),
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "ring file `{path}` has an unrecognized header \
                         (corrupt, or written by an older zhensegg format)"
                    ),
                ))
            }
        };

        if committed <= write_pos && write_pos.saturating_sub(committed) > data_capacity as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("ring file `{path}` has an inconsistent durability header"),
            ));
        }

        if meta_len != 0 && write_pos != 0 && meta_len != total_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("ring file `{path}` capacity does not match the configured ring capacity"),
            ));
        }

        let inner = Arc::new(if meta_len == 0 {
            MemRing::new(data_capacity)
        } else {
            let data = ring_bytes[HEADER_SIZE as usize..].to_vec();
            MemRing::from_buffer(data, data_capacity, write_pos, committed)
        });

        if meta_len == 0 {
            let mut hdr = [0u8; HEADER_SIZE as usize];
            encode_header(&mut hdr, 0, 0);
            {
                use std::io::{Seek, SeekFrom, Write};
                let mut f = &file;
                f.seek(SeekFrom::Start(0))?;
                f.write_all(&hdr)?;
                f.sync_data()?;
            }
        }

        #[cfg(target_os = "linux")]
        unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL); }
        #[cfg(target_os = "linux")]
        super::mem::try_register_file(&file);

        let file_arc = Arc::new(parking_lot::Mutex::new(file));
        let gate = Arc::new(DurableGate::new(inner.committed_pos()));
        let flusher = flusher::spawn_flusher(inner.clone(), file_arc.clone(), data_capacity, gate.clone());
        Ok(Self { inner, file: file_arc, _capacity: data_capacity, flusher, gate })
    }
}

impl Store for FileRing {
    fn append(&self, topic: &[u8], payload: &[u8]) -> Result<(u64, u32), StoreError> {
        self.inner.append(topic, payload)
    }

    fn read(&self, offset: u64, len: u32, out: &mut Vec<u8>) -> Result<(), StoreError> {
        self.inner.read(offset, len, out)
    }

    fn write_pos(&self) -> u64 {
        self.inner.write_pos()
    }

    fn durable_pos(&self) -> u64 {
        self.gate.pos()
    }

    fn durable_gate(&self) -> Option<Arc<super::durable::DurableGate>> {
        Some(self.gate.clone())
    }

    fn sync_pending(&self, timeout: std::time::Duration) -> u64 {
        let target = self.inner.committed_pos();
        self.flusher.wait_for(target, timeout)
    }

    fn set_reject_overflow(&self, on: bool) {
        self.inner.set_reject_overflow(on)
    }

    fn attach_watermark(&self, wm: Arc<AtomicU64>) {
        self.inner.attach_watermark(wm)
    }
}

impl Drop for FileRing {
    fn drop(&mut self) {
        self.flusher.shutdown();
        #[cfg(target_os = "linux")]
        {
            if let Some(f) = self.file.try_lock() {
                let _ = unsafe { libc::fdatasync(f.as_raw_fd()) };
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            if let Some(f) = self.file.try_lock() {
                let _ = f.sync_data();
            }
        }
    }
}
