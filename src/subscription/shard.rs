use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use super::Subscriber;

pub struct SubMap {
    shards: Vec<RwLock<HashMap<Vec<u8>, Vec<Arc<Subscriber>>>>>,
    mask: usize,
}

impl SubMap {
    pub fn new(n: usize) -> Self {
        let n = n.next_power_of_two();
        Self {
            shards: (0..n).map(|_| RwLock::new(HashMap::new())).collect(),
            mask: n - 1,
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
}
