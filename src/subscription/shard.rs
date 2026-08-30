use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use super::Subscriber;

pub const NO_SUBSCRIBERS: u64 = u64::MAX;

pub struct SubMap {
    shards: Vec<RwLock<HashMap<Vec<u8>, Vec<Arc<Subscriber>>>>>,
    mask: usize,
    pub retention: Arc<AtomicU64>,
}

impl SubMap {
    pub fn new(n: usize) -> Self {
        let n = n.next_power_of_two();
        Self {
            shards: (0..n).map(|_| RwLock::new(HashMap::new())).collect(),
            mask: n - 1,
            retention: Arc::new(AtomicU64::new(NO_SUBSCRIBERS)),
        }
    }

    #[inline]
    pub fn idx(&self, topic: &[u8]) -> usize {
        
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in topic {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        (h as usize) & self.mask
    }

    #[inline]
    pub fn read(&self, topic: &[u8]) -> parking_lot::RwLockReadGuard<'_, HashMap<Vec<u8>, Vec<Arc<Subscriber>>>> {
        self.shards[self.idx(topic)].read()
    }

    #[inline]
    pub fn write(&self, topic: &[u8]) -> parking_lot::RwLockWriteGuard<'_, HashMap<Vec<u8>, Vec<Arc<Subscriber>>>> {
        self.shards[self.idx(topic)].write()
    }

    #[inline]
    pub fn note_min_sent(&self, candidate: u64) {
        let mut cur = self.retention.load(Ordering::Relaxed);
        while candidate < cur {
            match self.retention.compare_exchange_weak(cur, candidate, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }

    pub fn recompute_min_sent(&self) -> u64 {
        let mut min = NO_SUBSCRIBERS;
        for shard in &self.shards {
            let g = shard.read();
            for list in g.values() {
                for s in list {
                    let v = s.sent.load(Ordering::Relaxed);
                    if v < min {
                        min = v;
                    }
                }
            }
        }
        self.retention.store(min, Ordering::Release);
        min
    }
}
