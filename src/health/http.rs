use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};

use crate::metrics::Metrics;

use super::{Checks, HealthStatus, StoreHealth};

#[derive(Clone)]
pub struct HealthState {
    pub metrics: Arc<Metrics>,
    pub store_type: String,
    pub store_capacity_mb: u64,
    pub store_usage_bytes: Arc<AtomicU64>,
    pub write_pos: Arc<AtomicU64>,
    pub durable_pos: Arc<AtomicU64>,
}

impl Default for HealthState {
    fn default() -> Self {
        Self {
            metrics: Arc::new(Metrics::new()),
            store_type: "mem".to_string(),
            store_capacity_mb: 0,
            store_usage_bytes: Arc::new(AtomicU64::new(0)),
            write_pos: Arc::new(AtomicU64::new(0)),
            durable_pos: Arc::new(AtomicU64::new(0)),
        }
    }
}

pub async fn health_handler(State(state): State<HealthState>) -> impl IntoResponse {
    let used = state.store_usage_bytes.load(Ordering::Relaxed);
    let write_pos = state.write_pos.load(Ordering::Relaxed);
    let durable = state.durable_pos.load(Ordering::Relaxed);

    let store_write_ok = state.store_type != "file" || durable >= write_pos.saturating_sub(1024 * 1024);
    let status = if store_write_ok {
        "healthy"
    } else if state.store_type == "file" {
        "degraded"
    } else {
        "unhealthy"
    };

    let body = HealthStatus {
        status: status.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: state.metrics.uptime_seconds(),
        connections: state.metrics.connections_total.get() as u64,
        subscriptions: state.metrics.subscriptions_total.get() as u64,
        store: StoreHealth {
            store_type: state.store_type.clone(),
            capacity_mb: state.store_capacity_mb,
            used_mb: used / (1024 * 1024),
            durable_pos: durable,
            write_pos,
        },
        checks: Checks {
            store_write: if store_write_ok { "ok" } else { "fail" }.to_string(),
            store_read: "ok".to_string(),
            fsync: if state.store_type == "file" { "ok" } else { "n/a" }.to_string(),
        },
    };

    let http_code = if status == "healthy" {
        StatusCode::OK
    } else if status == "degraded" {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (http_code, Json(body))
}
