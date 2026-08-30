//! Accept loop: spawn one connection task per inbound connection.
//!
//! When a TLS acceptor is present, each accepted socket is first driven through
//! the TLS handshake, then handed to the TLS connection driver. The handshake is
//! the only TLS cost and happens once per connection, off the hot path.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, info_span, warn, Instrument};

use super::connection;
use crate::metrics::Metrics;
use crate::security::AccessControl;
use crate::store::Store;
use crate::subscription::SubscriberMap;

/// Accept connections until the shutdown flag is set.
pub async fn accept_loop(
    listener: tokio::net::TcpListener,
    store: Arc<dyn Store>,
    subs: SubscriberMap,
    metrics: Arc<Metrics>,
    shutting_down: Arc<AtomicBool>,
    tls: Option<tokio_rustls::TlsAcceptor>,
    auth: AccessControl,
) -> anyhow::Result<()> {
    let mut next_id: u64 = 0;
    loop {
        if shutting_down.load(Ordering::Relaxed) {
            info!("shutdown flag set, stopping accept loop");
            break;
        }
        match tokio::time::timeout(Duration::from_millis(200), listener.accept()).await {
            Ok(Ok((socket, peer))) => {
                next_id += 1;
                let id = next_id;
                let span = info_span!("conn", id, peer = %peer);
                let store_c = store.clone();
                let subs_c = subs.clone();
                let metrics_c = metrics.clone();
                let auth_c = auth.clone();
                let tls_c = tls.clone();
                tokio::spawn(
                    async move {
                        match tls_c {
                            Some(acceptor) => {
                                match acceptor.accept(socket).await {
                                    Ok(tls_stream) => {
                                        let _ = connection::handle_tls_conn(tls_stream, id, store_c, subs_c, metrics_c, auth_c).await;
                                    }
                                    Err(e) => {
                                        warn!(connection_id = id, error = %e, "tls handshake failed");
                                    }
                                }
                            }
                            None => {
                                let _ = connection::handle_tokio_conn(socket, id, store_c, subs_c, metrics_c, auth_c).await;
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
