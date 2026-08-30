use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use zhensegg::health::{HealthState, health_handler};
use zhensegg::metrics::Metrics;

fn state(store_type: &str, capacity_mb: u64, used: u64, write: u64, durable: u64) -> HealthState {
    HealthState {
        metrics: Arc::new(Metrics::new()),
        store_type: store_type.to_string(),
        store_capacity_mb: capacity_mb,
        store_usage_bytes: Arc::new(AtomicU64::new(used)),
        write_pos: Arc::new(AtomicU64::new(write)),
        durable_pos: Arc::new(AtomicU64::new(durable)),
    }
}

fn status_code_of(res: axum::response::Response) -> StatusCode {
    res.status()
}

#[tokio::test]
async fn health_mem_mode_is_healthy() {
    let s = state("mem", 256, 1024, 5000, 5000);
    let res = health_handler(State(s)).await.into_response();
    assert_eq!(status_code_of(res), StatusCode::OK);
}

#[tokio::test]
async fn health_file_mode_caught_up_is_healthy() {
    
    let s = state("file", 1024, 2048, 5000, 5000);
    let res = health_handler(State(s)).await.into_response();
    assert_eq!(status_code_of(res), StatusCode::OK);
}

#[tokio::test]
async fn health_file_mode_lagging_is_degraded() {
    
    let s = state("file", 1024, 2048, 2_000_000, 0);
    let res = health_handler(State(s)).await.into_response();
    assert_eq!(status_code_of(res), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn health_body_reflects_state() {
    let s = state("file", 1024, 1024, 100, 100);
    let res = health_handler(State(s)).await.into_response();
    let body = axum::body::to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "healthy");
    assert_eq!(json["store"]["type"], "file");
    assert_eq!(json["store"]["capacity_mb"], 1024);
    assert_eq!(json["store"]["write_pos"], 100);
    assert_eq!(json["store"]["durable_pos"], 100);
    assert_eq!(json["checks"]["store_write"], "ok");
    assert!(!json["version"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn health_body_reflects_degraded_checks() {
    let s = state("file", 1024, 1024, 5_000_000, 0);
    let res = health_handler(State(s)).await.into_response();
    let body = axum::body::to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "degraded");
    assert_eq!(json["checks"]["store_write"], "fail");
}

#[tokio::test]
async fn health_default_state_is_healthy_mem() {
    let s = HealthState::default();
    let res = health_handler(State(s)).await.into_response();
    assert_eq!(status_code_of(res), StatusCode::OK);
}
