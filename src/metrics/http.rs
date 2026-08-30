//! HTTP handlers for /metrics and /ready.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use super::registry::Metrics;

pub async fn metrics_handler(State(metrics): State<Arc<Metrics>>) -> impl IntoResponse {
    (StatusCode::OK, [("content-type", "text/plain; version=0.0.4")], metrics.render())
}

pub async fn ready_handler(State(metrics): State<Arc<Metrics>>) -> impl IntoResponse {
    // Ready if initialized (uptime > 0) and no extreme backlog
    let status = if metrics.uptime_seconds() > 0.0 && metrics.backlog_size.get() < 10_000_000.0 {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, "ok")
}
