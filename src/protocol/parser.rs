//! Zero-allocation protocol parser.
//! Frame layout (big-endian):
//!   [0..4]  u32 total_len  = 1 + 4 + 4 + topic.len + payload.len  (len of rest)
//!   [4]     u8  op
//!   [5..9]  u32 topic_len
//!   [9..13] u32 payload_len
//!   [13..13+topic_len] topic bytes
//!   [13+topic_len..] payload bytes
//!
//! total_len does NOT include the 4-byte prefix itself.
//! Parser returns slices that borrow from the input buffer - no per-message allocation.

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
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct FrameRef<'a> {
    pub op: Op,
    pub topic: &'a [u8],
    pub payload: &'a [u8],
    /// For Ack/Notify/Data with offset semantics
    pub offset: Option<u64>,
    pub len: Option<u32>,
}

pub const HEADER_SIZE: usize = 13; // 4 +1+4+4, but 4 is prefix outside
pub const LEN_PREFIX: usize = 4;
pub const MIN_FRAME: usize = 1 + 4 + 4; // op + topic_len + payload_len (when both zero)

/// Zero-allocation parser that works on a growing Vec<u8> buffer.
pub struct Parser {
    buf: Vec<u8>,
    // number of bytes already consumed from front (to avoid shift each time)
    // we keep buf as ring-style: start..end valid, when start > 64KB we compact
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

    /// Return number of bytes buffered and not yet parsed
    pub fn buffered(&self) -> usize {
        self.buf.len() - self.start
    }

    /// Feed raw bytes read from socket into buffer.
    /// Caller should have read n bytes into provided slice and call this.
    pub fn feed(&mut self, data: &[u8]) {
        // if we have consumed prefix, compact before extending to avoid unbounded growth
        if self.start > 0 {
            // if buffer is large and start is significant, compact
            if self.start > 4096 || self.buf.len() > 64 * 1024 {
                self.compact();
            }
        }
        self.buf.extend_from_slice(data);
    }

    /// Reserve space and return mutable slice to read directly into.
    /// Used for vectored reads without extra copy.
    pub fn spare_mut(&mut self, want: usize) -> &mut [u8] {
        if self.start > 0 && self.start > 4096 {
            self.compact();
        }
        let needed = self.buf.len() + want;
        if needed > self.buf.capacity() {
            self.buf.reserve(want);
        }
        // extend with zeros then return spare tail
        let old_len = self.buf.len();
        self.buf.resize(old_len + want, 0);
        &mut self.buf[old_len..]
    }

    pub fn commit(&mut self, n: usize) {
        // we already resized, nothing to do if n == want; if short read, truncate
        let total = self.buf.len();
        // the spare region was want bytes at end, we keep only n of them
        if n == 0 {
            self.buf.truncate(total - (self.buf.len() - self.start - (self.buf.len() - total)));
        }
        // actually spare_mut already resized; if we read less than want, shrink
        // Caller passes n read; we need to adjust
        // Simpler: spare_mut resizes by want, if actual n < want, truncate extra
        // But we don't know want here. So caller should use feed() instead for simplicity.
        // This method is not used in MVP; keep for future zero-copy read.
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

    /// Try to parse one complete frame without allocating.
    /// Returns Some(FrameRef) borrowing from internal buffer if a full frame is available.
    /// Caller must copy needed data before calling `consume`.
    pub fn try_parse(&mut self) -> Option<FrameRef<'_>> {
        let avail = self.buffered();
        if avail < LEN_PREFIX {
            return None;
        }
        let len_bytes = &self.buf[self.start..self.start + 4];
        let total_len = u32::from_be_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
        // sanity limit 16MB per frame
        if total_len > 16 * 1024 * 1024 {
            // corrupt - skip? For MVP, panic and drain
            self.start += 4;
            return None;
        }
        if avail < LEN_PREFIX + total_len {
            return None;
        }
        // full frame available
        let frame_start = self.start + LEN_PREFIX;
        let op_byte = self.buf[frame_start];
        let op = Op::from_u8(op_byte)?;
        // topic_len / payload_len at offset 1
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

        // validate lengths against total_len
        let expected = 1 + 4 + 4 + topic_len + payload_len;
        if expected != total_len {
            // allow payload containing offset metadata for Ack/Notify where we overload?
            // For strict mode, drop frame
            // For offset frames, topic_len/payload_len still must match
            // If mismatch, skip
            self.start += LEN_PREFIX + total_len;
            return None;
        }

        let topic_start = frame_start + 9;
        let topic_end = topic_start + topic_len;
        let payload_start = topic_end;
        let payload_end = payload_start + payload_len;

        let topic = &self.buf[topic_start..topic_end];
        let payload = &self.buf[payload_start..payload_end];

        // For Ack/Notify/Data we embed offset in first 8 bytes of payload if needed
        // But for generic frame we return as is; helper to decode offset is separate
        let (offset, len) = if matches!(op, Op::Ack | Op::Notify) && payload_len >= 12 {
            // [8] offset BE u64 + [4] len BE u32 + rest (maybe)
            let off = u64::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6], payload[7],
            ]);
            let l = u32::from_be_bytes([payload[8], payload[9], payload[10], payload[11]]);
            (Some(off), Some(l))
        } else {
            (None, None)
        };

        // SAFETY: we return slices borrowing from self.buf; caller must consume before next mutable borrow
        // This is safe because we don't mutate buf until consume()
        // Need to transmute lifetime to tie to &self
        let topic_ptr = topic.as_ptr();
        let payload_ptr = payload.as_ptr();
        let topic_ref: &[u8] = unsafe { std::slice::from_raw_parts(topic_ptr, topic_len) };
        let payload_ref: &[u8] = unsafe { std::slice::from_raw_parts(payload_ptr, payload_len) };
        // This extends lifetime to &self instead of &mut self, but we are in &mut self;
        // Transmute to allow caller to hold reference while we still have &mut.
        // Caller must not call try_parse again until consume.
        let frame = FrameRef {
            op,
            topic: unsafe { std::mem::transmute::<&[u8], &[u8]>(topic_ref) },
            payload: unsafe { std::mem::transmute::<&[u8], &[u8]>(payload_ref) },
            offset,
            len,
        };
        Some(frame)
    }

    /// Mark last parsed frame as consumed, freeing space for next parse.
    pub fn consume(&mut self) {
        let total_len = u32::from_be_bytes([
            self.buf[self.start],
            self.buf[self.start + 1],
            self.buf[self.start + 2],
            self.buf[self.start + 3],
        ]) as usize;
        self.start += LEN_PREFIX + total_len;
        // compact if start grew too large
        if self.start > 32 * 1024 && self.start > self.buf.len() / 2 {
            self.compact();
        }
        if self.start == self.buf.len() {
            // fully consumed, reset to avoid holding memory
            self.buf.clear();
            self.start = 0;
        }
    }

    /// Returns raw bytes of current frame (including 4-byte len prefix) for zero-copy forward.
    /// Must be called after try_parse() returns Some and before consume().
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

    /// Convenience: parse all complete frames, calling handler for each.
    /// Handler receives FrameRef and should copy if needed.
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
