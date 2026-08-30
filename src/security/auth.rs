//! Shared-token authentication for the data plane.
//!
//! Verification happens *once* per connection, before the connection is
//! allowed to publish. The hot data path never touches this module.

use std::sync::Arc;

/// Single-shot constant-time comparison of two equal-length secrets.
///
/// Two different lengths short-circuit immediately (length is not a secret),
/// and the byte-wise comparison accumulates an XOR diff over a loop whose
/// iteration count and body do not depend on the input bytes, so no early exit
/// leaks where the first difference is.
pub fn secure_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// Access policy for the broker data plane.
#[derive(Clone)]
pub enum AccessControl {
    /// No authentication required — plain open access.
    Open,
    /// The client must present this shared token as its first frame.
    Token(Arc<[u8]>),
}

impl AccessControl {
    pub fn open() -> Self {
        AccessControl::Open
    }

    pub fn token(t: impl Into<Vec<u8>>) -> Self {
        AccessControl::Token(t.into().into())
    }

    /// Whether a connection is already considered authenticated at the start,
    /// without presenting any frame.
    pub fn initially_authenticated(&self) -> bool {
        matches!(self, AccessControl::Open)
    }

    /// Verify a presented token in constant time. Always `true` for `Open`.
    pub fn verify(&self, presented: &[u8]) -> bool {
        match self {
            AccessControl::Open => true,
            AccessControl::Token(expected) => secure_eq(expected, presented),
        }
    }
}
