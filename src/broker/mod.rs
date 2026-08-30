//! Broker module: TCP accept loop, per-connection protocol handling, socket setup,
//! lifecycle orchestration and the HTTP sidecar.

mod accept;
pub mod connection;
mod http;
mod listener;
mod run;

pub use run::run_broker;
