//! Accept loop: spawn one connection task per inbound connection.
//!
//! Transport security is snapshot once per accepted connection from the shared
//! [`SharedSecurity`] holder, so a `SIGHUP` rotation only affects new
//! connections. When a TLS acceptor is present, each accepted socket is first
//! driven through the TLS handshake, then handed to the TLS connection driver.
//! The handshake is the only TLS cost and happens once per connection, off the
//! hot path.
//!
//! Resilience: `--max-connections` bounds the number of concurrently handled
//! connections with a semaphore, and `--auth-timeout-secs` bounds both the TLS
//! handshake and how long a connection may sit unauthenticated before it is
//! dropped.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, info_span, warn, Instrument};

use super::connection;
use crate::config::Config;
use crate::metrics::Metrics;
use crate::security::SharedSecurity;
use crate::store::Store;
use crate::subscription::SubscriberMap;

/// Accept connections until the shutdown flag is set.
pub async fn accept_loop(
    listener: tokio::net::TcpListener,
    store: Arc<dyn Store>,
    subs: SubscriberMap,
    metrics: Arc<Metrics>,
    shutting_down: Arc<AtomicBool>,
    sec: Arc<SharedSecurity>,
    cfg: &Config,
) -> anyhow::Result<()> {
    let max_conns = cfg.max_connections;
    let sem = max_conns.map(|n| Arc::new(tokio::sync::Semaphore::new(n)));
    let auth_timeout = Duration::from_secs(cfg.auth_timeout_secs);

    let mut next_id: u64 = 0;
    loop {
        if shutting_down.load(Ordering::Relaxed) {
            info!("shutdown flag set, stopping accept loop");
            break;
        }
        match tokio::time::timeout(Duration::from_millis(200), listener.accept()).await {
            Ok(Ok((socket, peer))) => {
                // Respect the connection cap: if a cap is configured and its
                // semaphore is exhausted, drop the accepted socket immediately.
                let permit = match &sem {
                    None => None,
                    Some(s) => match s.clone().try_acquire_owned() {
                        Ok(p) => Some(p),
                        Err(_) => {
                            warn!(%peer, max_connections = max_conns, "connection limit reached, dropping");
                            continue;
                        }
                    },
                };
                next_id += 1;
                let id = next_id;
                let span = info_span!("conn", id, peer = %peer);
                let store_c = store.clone();
                let subs_c = subs.clone();
                let metrics_c = metrics.clone();
                let (tls_c, auth_c) = sec.snapshot();
                let auth_timeout_c = auth_timeout;
                tokio::spawn(
                    async move {
                        let _permit = permit;                        match tls_c {
                            Some(acceptor) => {
                                let hs_timeout = tokio::time::timeout(auth_timeout_c, acceptor.accept(socket));
                                match hs_timeout.await {
                                    Ok(Ok(tls_stream)) => {
                                        let _ = connection::handle_tls_conn(tls_stream, id, store_c, subs_c, metrics_c, auth_c, Some(auth_timeout_c)).await;
                                    }
                                    Ok(Err(e)) => {
                                        warn!(connection_id = id, error = %e, "tls handshake failed");
                                    }
                                    Err(_) => {
                                        warn!(connection_id = id, "tls handshake timed out");
                                    }
                                }
                            }
                            None => {
                                let _ = connection::handle_tokio_conn(socket, id, store_c, subs_c, metrics_c, auth_c, Some(auth_timeout_c)).await;
                            }
                        }
                    }
                    .instrument(span),
                );
            }
            Ok(Err(e)) => {
                warn!(error = %e, "accept error");
            }
            Err(_) => {
                // poll timeout: re-check shutdown flag
            }
        }
    }
    Ok(())
}
