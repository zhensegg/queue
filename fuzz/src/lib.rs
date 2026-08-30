//! Zhensegg protocol fuzzing + soak harness, as a library and a CLI.
//!
//! The fuzz logic is exposed here so the bounded CI fixture in `tests/` can
//! drive it directly, and so the `zhensegg-fuzz` binary is a thin wrapper.

pub mod fuzzer;
pub mod proto;
pub mod soak;
