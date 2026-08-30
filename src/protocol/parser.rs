#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Publish = 0x01,
    Subscribe = 0x02,
    Fetch = 0x03,
    Ping = 0x04,
    Ack = 0x05,
    Notify = 0x06,
    Data = 0x07,
    Auth = 0x08,
}

impl Op {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Op::Publish),
            0x02 => Some(Op::Subscribe),
            0x03 => Some(Op::Fetch),
            0x04 => Some(Op::Ping),
            0x05 => Some(Op::Ack),
            0x06 => Some(Op::Notify),
            0x07 => Some(Op::Data),
            0x08 => Some(Op::Auth),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct FrameRef<'a> {
    pub op: Op,
    pub topic: &'a [u8],
    pub payload: &'a [u8],
    
    pub offset: Option<u64>,
    pub len: Option<u32>,
}

pub const HEADER_SIZE: usize = 13; 
pub const LEN_PREFIX: usize = 4;
pub const MIN_FRAME: usize = 1 + 4 + 4; 

pub struct Parser {
    buf: Vec<u8>,
    
    start: usize,
}

impl Parser {
    pub fn new(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
            start: 0,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self::new(cap)
    }

    pub fn buffered(&self) -> usize {
        self.buf.len() - self.start
    }

    pub fn feed(&mut self, data: &[u8]) {
        
        if self.start > 0
            
            && (self.start > 4096 || self.buf.len() > 64 * 1024) {
                self.compact();
            }
        self.buf.extend_from_slice(data);
    }

    pub fn spare_mut(&mut self, want: usize) -> &mut [u8] {
        if self.start > 4096 {
            self.compact();
        }
        let needed = self.buf.len() + want;
        if needed > self.buf.capacity() {
            self.buf.reserve(want);
        }
        
        let old_len = self.buf.len();
        self.buf.resize(old_len + want, 0);
        &mut self.buf[old_len..]
    }

    pub fn commit(&mut self, n: usize) {
        
        let total = self.buf.len();
        
        if n == 0 {
            self.buf.truncate(total - (self.buf.len() - self.start - (self.buf.len() - total)));
        }
        
        let _ = n;
    }

    fn compact(&mut self) {
        if self.start == 0 {
            return;
        }
        let remaining = self.buf.len() - self.start;
        if remaining > 0 {
            self.buf.copy_within(self.start.., 0);
        }
        self.buf.truncate(remaining);
        self.start = 0;
    }

    pub fn try_parse(&mut self) -> Option<FrameRef<'_>> {
        let avail = self.buffered();
        if avail < LEN_PREFIX {
            return None;
        }
        let len_bytes = &self.buf[self.start..self.start + 4];
        let total_len = u32::from_be_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
        
        if total_len > 16 * 1024 * 1024 {
            
            self.start += 4;
            return None;
        }
        
        if total_len < MIN_FRAME {
            self.start += LEN_PREFIX + total_len;
            return None;
        }
        if avail < LEN_PREFIX + total_len {
            return None;
        }
        
        let frame_start = self.start + LEN_PREFIX;
        let op_byte = self.buf[frame_start];
        let op = Op::from_u8(op_byte)?;
        
        let topic_len = u32::from_be_bytes([
            self.buf[frame_start + 1],
            self.buf[frame_start + 2],
            self.buf[frame_start + 3],
            self.buf[frame_start + 4],
        ]) as usize;
        let payload_len = u32::from_be_bytes([
            self.buf[frame_start + 5],
            self.buf[frame_start + 6],
            self.buf[frame_start + 7],
            self.buf[frame_start + 8],
        ]) as usize;

        let expected = 1 + 4 + 4 + topic_len + payload_len;
        if expected != total_len {
            
            self.start += LEN_PREFIX + total_len;
            return None;
        }

        let topic_start = frame_start + 9;
        let topic_end = topic_start + topic_len;
        let payload_start = topic_end;
        let payload_end = payload_start + payload_len;

        let topic = &self.buf[topic_start..topic_end];
        let payload = &self.buf[payload_start..payload_end];

        let (offset, len) = if matches!(op, Op::Ack | Op::Notify) && payload_len >= 12 {
            
            let off = u64::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6], payload[7],
            ]);
            let l = u32::from_be_bytes([payload[8], payload[9], payload[10], payload[11]]);
            (Some(off), Some(l))
        } else {
            (None, None)
        };

        let topic_ptr = topic.as_ptr();
        let payload_ptr = payload.as_ptr();
        let topic_ref: &[u8] = unsafe { std::slice::from_raw_parts(topic_ptr, topic_len) };
        let payload_ref: &[u8] = unsafe { std::slice::from_raw_parts(payload_ptr, payload_len) };
        
        let frame = FrameRef {
            op,
            topic: unsafe { std::mem::transmute::<&[u8], &[u8]>(topic_ref) },
            payload: unsafe { std::mem::transmute::<&[u8], &[u8]>(payload_ref) },
            offset,
            len,
        };
        Some(frame)
    }

    pub fn consume(&mut self) {
        let total_len = u32::from_be_bytes([
            self.buf[self.start],
            self.buf[self.start + 1],
            self.buf[self.start + 2],
            self.buf[self.start + 3],
        ]) as usize;
        self.start += LEN_PREFIX + total_len;
        
        if self.start > 32 * 1024 && self.start > self.buf.len() / 2 {
            self.compact();
        }
        if self.start == self.buf.len() {
            
            self.buf.clear();
            self.start = 0;
        }
    }

    pub fn current_frame_raw(&self) -> Option<&[u8]> {
        let avail = self.buffered();
        if avail < LEN_PREFIX {
            return None;
        }
        let total_len = u32::from_be_bytes([
            self.buf[self.start],
            self.buf[self.start + 1],
            self.buf[self.start + 2],
            self.buf[self.start + 3],
        ]) as usize;
        if total_len > 16 * 1024 * 1024 || avail < LEN_PREFIX + total_len {
            return None;
        }
        Some(&self.buf[self.start..self.start + LEN_PREFIX + total_len])
    }

    pub fn drain<F>(&mut self, mut f: F)
    where
        F: FnMut(FrameRef<'_>),
    {
        while let Some(frame) = self.try_parse() {
            f(frame);
            self.consume();
        }
    }
}
