use std::sync::Arc;

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

#[derive(Clone, Default)]
pub enum AccessControl {
    
    #[default]
    Open,
    
    Token(Arc<[u8]>),
}

impl AccessControl {
    pub fn open() -> Self {
        AccessControl::Open
    }

    pub fn token(t: impl Into<Vec<u8>>) -> Self {
        AccessControl::Token(t.into().into())
    }

    pub fn initially_authenticated(&self) -> bool {
        matches!(self, AccessControl::Open)
    }

    pub fn verify(&self, presented: &[u8]) -> bool {
        match self {
            AccessControl::Open => true,
            AccessControl::Token(expected) => secure_eq(expected, presented),
        }
    }
}
