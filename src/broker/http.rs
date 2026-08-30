//! HTTP sidecar: /metrics, /health, /ready endpoints (axum) on a separate port.
//!
//! The admin plane can be protected with an optional token (Basic auth) and/or
//! restricted to loopback. The token is read from [`SharedSecurity`] on each
//! request so a `SIGHUP` rotation applies immediately.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use tracing::{error, info};
use tower_http::cors::CorsLayer;

use crate::config::Config;
use crate::health::HealthState;
use crate::metrics::Metrics;
use crate::security::{SharedSecurity, secure_eq};

/// Check whether an incoming admin-plane request is authorized. Reads the
/// current admin token from `sec` on every call (so a SIGHUP rotation applies
/// immediately), and compares it in constant time against the Basic-auth
/// credentials. With no token configured, every request passes.
pub fn is_admin_authorized(sec: &SharedSecurity, req: &Request) -> bool {
    match sec.http_token() {
        None => true,
        Some(expected) => req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Basic "))
            .and_then(|b64| {
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64).ok()
            })
            .map(|decoded| match decoded.iter().position(|&b| b == b':') {
                Some(idx) => secure_eq(expected.as_ref(), &decoded[idx + 1..]),
                None => false,
            })
            .unwrap_or(false),
    }
}

/// Serve the metrics/health/ready HTTP endpoints until bind/serve error.
pub async fn metrics_http_server(config: Config, health_state: HealthState, sec: Arc<SharedSecurity>) {
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

    // Basic-auth middleware for the admin plane. Reads the current admin token
    // from SharedSecurity on every request (so a SIGHUP rotation applies).
    let auth_layer = axum::middleware::from_fn(move |req: Request, next: Next| {
        let sec = sec.clone();
        async move {
            if is_admin_authorized(&sec, &req) {
                next.run(req).await
            } else {
                let mut resp = Response::new("unauthorized".into());
                *resp.status_mut() = StatusCode::UNAUTHORIZED;
                resp.headers_mut()
                    .insert("WWW-Authenticate", "Basic realm=\"zhensegg\"".parse().unwrap());
                resp
            }
        }
    });

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .layer(CorsLayer::permissive())
        .layer(auth_layer)
        .with_state(AppState {
            metrics: health_state.metrics.clone(),
            health: health_state,
        });

    let mut addr: SocketAddr = match config.http_addr.parse() {
        Ok(a) => a,
        Err(e) => {
            error!(addr = %config.http_addr, error = %e, "invalid http addr");
            return;
        }
    };
    if config.http_loopback_only {
        // Force a loopback bind (keep the configured port).
        addr.set_ip(IpAddr::V4(Ipv4Addr::LOCALHOST));
    }
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(%addr, error = %e, "failed to bind http server");
            return;
        }
    };
    info!(%addr, loopback_only = config.http_loopback_only, "metrics http server listening");

    if let Err(e) = axum::serve(listener, app).await {
        error!(error = %e, "http server error");
    }
}