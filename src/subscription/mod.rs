pub mod shard;

use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;

pub struct Subscriber {
    pub id: u64,
    
    pub tx: UnboundedSender<Arc<Vec<u8>>>,
}

pub type SubscriberMap = Arc<shard::SubMap>;

pub use shard::SubMap;
