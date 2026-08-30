//! HTTP sidecar: /metrics, /health, /ready endpoints (axum) on a separate port.

use std::sync::Arc;

use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{http::StatusCode, Router};
use tracing::{error, info};
use tower_http::cors::CorsLayer;

use crate::config::Config;
use crate::health::HealthState;
use crate::metrics::Metrics;

/// Serve the metrics/health/ready HTTP endpoints until bind/serve error.
pub async fn metrics_http_server(config: Config, health_state: HealthState) {
    #[derive(Clone)]
    struct AppState {
        metrics: Arc<Metrics>,
        health: HealthState,
    }

    async fn metrics_handler(State(s): State<AppState>) -> impl IntoResponse {
        (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4")],
            s.metrics.render(),
        )
    }
    async fn health_handler(State(s): State<AppState>) -> impl IntoResponse {
        crate::health::health_handler(State(s.health)).await
    }
    async fn ready_handler(State(_s): State<AppState>) -> impl IntoResponse {
        (StatusCode::OK, "ok")
    }

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .layer(CorsLayer::permissive())
        .with_state(AppState {
            metrics: health_state.metrics.clone(),
            health: health_state,
        });

    let addr: std::net::SocketAddr = match config.http_addr.parse() {
        Ok(a) => a,
        Err(e) => {
            error!(addr = %config.http_addr, error = %e, "invalid http addr");
            return;
        }
    };
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(%addr, error = %e, "failed to bind http server");
            return;
        }
    };
    info!(%addr, "metrics http server listening");

    if let Err(e) = axum::serve(listener, app).await {
        error!(error = %e, "http server error");
    }
}
