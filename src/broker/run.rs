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
use crate::security::SharedSecurity;
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

    // Reloadable transport security (TLS + auth tokens), shared by the accept
    // loops and the HTTP admin server so a SIGHUP rotation applies to both.
    //
    // Created as an empty placeholder first: the signal thread (which owns the
    // SIGHUP/reload path) is handed a clone of this Arc, and the slow initial
    // load (TLS certificate parsing) happens only *after* the OS signal handlers
    // are confirmed installed, so an early SIGHUP can never hit the default
    // (terminate) disposition and kill a freshly booted broker.
    let sec = Arc::new(SharedSecurity::default());

    // Graceful shutdown flag + signal handler (SIGTERM/SIGINT/ctrl-C drain, plus
    // SIGHUP on unix to reload TLS certs and auth tokens without restart).
    //
    // The handler thread registers all OS signal handlers *synchronously* and
    // then signals readiness through a channel. `run_broker` blocks on that
    // channel before doing any slow work (store setup, TLS load), so there is no
    // startup window in which a SIGHUP would hit the default (terminate)
    // disposition and kill a freshly booted broker.
    let shutting_down = Arc::new(AtomicBool::new(false));
    let (signal_ready_tx, signal_ready_rx) = std::sync::mpsc::channel::<()>();
    {
        let shutting_down = shutting_down.clone();
        let _sec = sec.clone();
        let _cfg = config.clone();
        let signal_ready_tx = signal_ready_tx;
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("signal runtime");
            rt.block_on(async move {
                #[cfg(unix)]
                {
                    use tokio::signal::unix::{SignalKind, signal};
                    // Register all handlers up front (synchronously) before we
                    // tell the caller we are ready, so no rotation signal can be
                    // lost or mis-handled during startup.
                    let mut hup = signal(SignalKind::hangup()).expect("sighup");
                    let mut term = signal(SignalKind::terminate()).expect("sigterm");
                    let mut int = signal(SignalKind::interrupt()).expect("sigint");
                    let _ = signal_ready_tx.send(());
                    tokio::pin!(hup);
                    tokio::pin!(term);
                    tokio::pin!(int);
                    loop {
                        tokio::select! {
                            _ = hup.recv() => {
                                match _sec.reload(&_cfg) {
                                    Ok(()) => info!("SIGHUP: TLS/auth rotated"),
                                    Err(e) => {
                                        use tracing::error;
                                        error!(error = %e, "SIGHUP reload failed, keeping previous context");
                                    }
                                }
                            }
                            _ = term.recv() => {
                                info!("shutdown signal received (SIGTERM), draining");
                                shutting_down.store(true, Ordering::SeqCst);
                                break;
                            }
                            _ = int.recv() => {
                                info!("shutdown signal received (SIGINT), draining");
                                shutting_down.store(true, Ordering::SeqCst);
                                break;
                            }
                        }
                    }
                }
                #[cfg(not(unix))]
                {
                    let _ = signal_ready_tx.send(());
                    wait_for_shutdown_signal().await;
                    info!("shutdown signal received, draining");
                    shutting_down.store(true, Ordering::SeqCst);
                }
            });
        });
    }
    // Block until the OS signal handlers are guaranteed installed. From here on
    // an early SIGHUP is handled (reload), never a default terminate.
    let _ = signal_ready_rx.recv();

    // Initial security load (TLS cert/key parsing, token files). Same code path
    // as SIGHUP reload; runs only after signal handlers are installed.
    sec.reload(&config)?;
    info!(
        tls = sec.snapshot().0.is_some(),
        auth = !sec.snapshot().1.initially_authenticated(),
        http_auth = sec.http_token().is_some(),
        "security context initialized"
    );

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
        let sec = sec.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("http runtime");
            rt.block_on(metrics_http_server(http_config, health_state, sec));
        });
    }

    // Start the accept loop(s)
    let broker_store = store.clone();
    if config.cores > 1 {
        run_multicore(&config, broker_store, subs, metrics, shutting_down.clone(), sec.clone())?;
    } else {
        run_singlecore(&config, broker_store, subs, metrics, shutting_down.clone(), sec.clone())?;
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
    sec: Arc<SharedSecurity>,
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
        accept_loop(listener, store, subs, metrics, shutting_down, sec, config).await
    })
}

/// Multi-core accept loop with SO_REUSEPORT sharding (one runtime/thread per core).
pub fn run_multicore(
    config: &Config,
    store: Arc<dyn Store>,
    subs: SubscriberMap,
    metrics: Arc<Metrics>,
    shutting_down: Arc<AtomicBool>,
    sec: Arc<SharedSecurity>,
) -> anyhow::Result<()> {
    let addr = config.addr.clone();
    let mut handles = Vec::new();
    for cid in 0..config.cores {
        let addr_c = addr.clone();
        let store_c = store.clone();
        let subs_c = subs.clone();
        let metrics_c = metrics.clone();
        let shutting_down_c = shutting_down.clone();
        let sec_c = sec.clone();
        let cfg_c = config.clone();
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
                    let _ = accept_loop(listener, store_c, subs_c, metrics_c, shutting_down_c, sec_c, &cfg_c).await;
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