//! Broker lifecycle: runtime setup, store/subscription creation, accept-loop entry.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use super::accept::accept_loop;
use super::http::metrics_http_server;
use super::listener;
use crate::config::Config;
use crate::health::HealthState;
use crate::metrics::Metrics;
use crate::security::{AccessControl, build_tls_acceptor};
use crate::store::{FileRing, MemRing, Store};
use crate::subscription::{SubMap, SubscriberMap};

/// Run the broker until a shutdown signal is received.
pub fn run_broker(config: Config) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!(addr = %config.addr, http = %config.http_addr, mode = %config.mode, cores = config.cores, "zhensegg broker starting");

    // Shared store
    let store: Arc<dyn Store> = if config.is_file_mode() {
        info!(path = %config.file, mb = config.ring_capacity_mb, "persistent store (file mode)");
        Arc::new(FileRing::new(&config.file, config.ring_capacity_bytes())?)
    } else {
        info!(mb = config.mem_mb, "in-memory store (mem mode)");
        Arc::new(MemRing::new(config.mem_capacity_bytes()))
    };

    let metrics = Arc::new(Metrics::new());
    let subs: SubscriberMap = Arc::new(SubMap::new(64));

    // Health snapshot state, updated by the accept loop
    let store_usage = Arc::new(AtomicU64::new(0));
    let write_pos_atomic = Arc::new(AtomicU64::new(0));
    let durable_pos_atomic = Arc::new(AtomicU64::new(0));
    let health_state = HealthState {
        metrics: metrics.clone(),
        store_type: config.mode.clone(),
        store_capacity_mb: (config.ring_capacity_bytes() as u64 / (1024 * 1024)),
        store_usage_bytes: store_usage.clone(),
        write_pos: write_pos_atomic.clone(),
        durable_pos: durable_pos_atomic.clone(),
    };

    // HTTP sidecar (non-blocking, separate thread + runtime)
    {
        let http_config = config.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("http runtime");
            rt.block_on(metrics_http_server(http_config, health_state));
        });
    }

    // Graceful shutdown flag + signal handler
    let shutting_down = Arc::new(AtomicBool::new(false));
    {
        let shutting_down = shutting_down.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("signal runtime");
            rt.block_on(async move {
                wait_for_shutdown_signal().await;
                info!("shutdown signal received, draining");
                shutting_down.store(true, Ordering::SeqCst);
            });
        });
    }

    // TLS: if cert+key are configured, terminate TLS on the data plane.
    let tls: Option<tokio_rustls::TlsAcceptor> = match (&config.tls_cert, &config.tls_key) {
        (Some(cert), Some(key)) => {
            info!(cert = %cert, "TLS enabled");
            Some(build_tls_acceptor(cert, key)?)
        }
        (None, None) => None,
        _ => anyhow::bail!("--tls-cert and --tls-key must be provided together"),
    };

    // Auth: a shared token makes the data plane require an Auth frame first.
    let auth = match &config.auth_token {
        Some(t) => {
            info!("auth enabled (shared token)");
            AccessControl::token(t.as_bytes())
        }
        None => AccessControl::open(),
    };

    // Start the accept loop(s)
    let broker_store = store.clone();
    if config.cores > 1 {
        run_multicore(&config, broker_store, subs, metrics, shutting_down.clone(), tls, auth)?;
    } else {
        run_singlecore(&config, broker_store, subs, metrics, shutting_down.clone(), tls, auth)?;
    }

    // Graceful drain: give connections time to flush pending writes
    info!("draining pending writes...");
    std::thread::sleep(Duration::from_secs(5));
    info!("final store sync");
    let _ = store.durable_pos();

    info!("shutdown complete");
    Ok(())
}

/// Wait for a termination signal (SIGTERM/SIGINT/ctrl-C).
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("sigterm");
        let mut int = signal(SignalKind::interrupt()).expect("sigint");
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Single-core accept loop (tokio multi_thread with 1 worker).
pub fn run_singlecore(
    config: &Config,
    store: Arc<dyn Store>,
    subs: SubscriberMap,
    metrics: Arc<Metrics>,
    shutting_down: Arc<AtomicBool>,
    tls: Option<tokio_rustls::TlsAcceptor>,
    auth: AccessControl,
) -> anyhow::Result<()> {
    let addr = config.addr.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .expect("broker runtime");
    rt.block_on(async move {
        let listener = listener::bind_listener(&addr).await?;
        info!(%addr, "listening (single core)");
        accept_loop(listener, store, subs, metrics, shutting_down, tls, auth).await
    })
}

/// Multi-core accept loop with SO_REUSEPORT sharding (one runtime/thread per core).
pub fn run_multicore(
    config: &Config,
    store: Arc<dyn Store>,
    subs: SubscriberMap,
    metrics: Arc<Metrics>,
    shutting_down: Arc<AtomicBool>,
    tls: Option<tokio_rustls::TlsAcceptor>,
    auth: AccessControl,
) -> anyhow::Result<()> {
    let addr = config.addr.clone();
    let mut handles = Vec::new();
    for cid in 0..config.cores {
        let addr_c = addr.clone();
        let store_c = store.clone();
        let subs_c = subs.clone();
        let metrics_c = metrics.clone();
        let shutting_down_c = shutting_down.clone();
        let tls_c = tls.clone();
        let auth_c = auth.clone();
        let handle = std::thread::Builder::new()
            .name(format!("zhensegg-{cid}"))
            .spawn(move || {
                #[cfg(target_os = "linux")]
                let _ = listener::core_affinity_attempt(cid);
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("core runtime");
                rt.block_on(async move {
                    let listener = listener::bind_listener(&addr_c).await.unwrap();
                    info!(cid, addr = %addr_c, "listening (core)");
                    let _ = accept_loop(listener, store_c, subs_c, metrics_c, shutting_down_c, tls_c, auth_c).await;
                });
            })
            .expect("spawn core");
        handles.push(handle);
    }
    for h in handles {
        let _ = h.join();
    }
    Ok(())
}
