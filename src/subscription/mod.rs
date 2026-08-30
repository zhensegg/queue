//! Subscription types: a per-connection subscriber with an async outbound channel.

pub mod shard;

use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;

pub struct Subscriber {
    pub id: u64,
    // channel to send data frames (already encoded) - tokio async, Arc to avoid per-subscriber clone copy
    pub tx: UnboundedSender<Arc<Vec<u8>>>,
}

pub type SubscriberMap = Arc<shard::SubMap>;

pub use shard::SubMap;
