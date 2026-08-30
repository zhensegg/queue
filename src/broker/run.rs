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

pub fn run_broker(config: Config) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!(addr = %config.addr, http = %config.http_addr, mode = %config.mode, cores = config.cores, "zhensegg broker starting");

    let sec = Arc::new(SharedSecurity::default());

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
                    
                    let hup = signal(SignalKind::hangup()).expect("sighup");
                    let term = signal(SignalKind::terminate()).expect("sigterm");
                    let int = signal(SignalKind::interrupt()).expect("sigint");
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
    
    let _ = signal_ready_rx.recv();

    sec.reload(&config)?;
    info!(
        tls = sec.snapshot().0.is_some(),
        auth = !sec.snapshot().1.initially_authenticated(),
        http_auth = sec.http_token().is_some(),
        "security context initialized"
    );

    let store: Arc<dyn Store> = if config.is_file_mode() {
        info!(path = %config.file, mb = config.ring_capacity_mb, "persistent store (file mode)");
        Arc::new(FileRing::new(&config.file, config.ring_capacity_bytes())?)
    } else {
        info!(mb = config.mem_mb, "in-memory store (mem mode)");
        Arc::new(MemRing::new(config.mem_capacity_bytes()))
    };

    let metrics = Arc::new(Metrics::new());
    let subs: SubscriberMap = Arc::new(SubMap::new(64));
    let reject_overflow = config.on_overflow.eq_ignore_ascii_case("reject");
    store.set_reject_overflow(reject_overflow);
    store.attach_watermark(subs.retention.clone());
    if reject_overflow {
        info!("overflow policy: reject (publishes NACKed instead of overwriting undelivered data)");
    }

    let store_usage = Arc::new(AtomicU64::new(0));
    let write_pos_atomic = Arc::new(AtomicU64::new(0));
    let durable_pos_atomic = Arc::new(AtomicU64::new(0));
    let seconds_to_wrap_ms = Arc::new(AtomicU64::new(u64::MAX));
    let store_capacity_bytes = if config.is_file_mode() {
        config.ring_capacity_bytes()
    } else {
        config.mem_capacity_bytes()
    };
let health_state = HealthState {
        metrics: metrics.clone(),
        store_type: config.mode.clone(),
        store_capacity_mb: (store_capacity_bytes as u64 / (1024 * 1024)),
        store_usage_bytes: store_usage.clone(),
        write_pos: write_pos_atomic.clone(),
        durable_pos: durable_pos_atomic.clone(),
        seconds_to_wrap_ms: seconds_to_wrap_ms.clone(),
        overflow_reject: reject_overflow,
    };

    {
        let store = store.clone();
        let write_pos_atomic = write_pos_atomic.clone();
        let durable_pos_atomic = durable_pos_atomic.clone();
        let store_usage = store_usage.clone();
        let metrics = metrics.clone();
        let seconds_to_wrap_ms = seconds_to_wrap_ms.clone();
        std::thread::spawn(move || {
            let mut last_wp = store.write_pos();
            let mut last_t = std::time::Instant::now();
            loop {
                std::thread::sleep(Duration::from_millis(200));
                let wp = store.write_pos();
                let now = std::time::Instant::now();
                let dt = now.duration_since(last_t).as_secs_f64();
                let bytes_per_sec = if dt > 0.0 {
                    ((wp.saturating_sub(last_wp)) as f64 / dt) as u64
                } else {
                    0
                };
                last_wp = wp;
                last_t = now;
                let cap = store_capacity_bytes as u64;
                let remaining = cap.saturating_sub(wp % cap);
                let stw = remaining
                    .saturating_mul(1000)
                    .checked_div(bytes_per_sec)
                    .unwrap_or(u64::MAX);
                seconds_to_wrap_ms.store(stw, Ordering::Relaxed);
                durable_pos_atomic.store(store.durable_pos(), Ordering::Relaxed);
                let usage = wp.saturating_sub(store.durable_pos());
                store_usage.store(usage, Ordering::Relaxed);
                metrics.store_usage_bytes.set(usage as f64);
                write_pos_atomic.store(wp, Ordering::Relaxed);
            }
        });
    }

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

    let broker_store = store.clone();
    if config.cores > 1 {
        run_multicore(&config, broker_store, subs, metrics, shutting_down.clone(), sec.clone())?;
    } else {
        run_singlecore(&config, broker_store, subs, metrics, shutting_down.clone(), sec.clone())?;
    }

    info!("draining pending writes...");
    let durable = store.sync_pending(Duration::from_secs(30));
    info!(durable, "shutdown complete");
    Ok(())
}

#[cfg(not(unix))]
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
