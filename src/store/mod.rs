//! Ring-buffer store: in-memory and persistent (with background flusher).

mod mem;
pub mod file;
pub mod flusher;

pub use mem::{MemRing, Store, StoreError};
pub use file::FileRing;
