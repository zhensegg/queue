//! Health check types.

mod http;

use serde::Serialize;

pub use http::{health_handler, HealthState};

#[derive(Debug, Clone, Serialize)]
pub struct HealthStatus {
    pub status: String,
    pub version: String,
    pub uptime_seconds: f64,
    pub connections: u64,
    pub subscriptions: u64,
    pub store: StoreHealth,
    pub checks: Checks,
}

#[derive(Debug, Clone, Serialize)]
pub struct StoreHealth {
    #[serde(rename = "type")]
    pub store_type: String,
    pub capacity_mb: u64,
    pub used_mb: u64,
    pub durable_pos: u64,
    pub write_pos: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Checks {
    pub store_write: String,
    pub store_read: String,
    pub fsync: String,
}
