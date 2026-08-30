//! Accept loop: spawn one connection task per inbound TCP connection.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, info_span, warn, Instrument};

use super::connection;
use crate::metrics::Metrics;
use crate::store::Store;
use crate::subscription::SubscriberMap;

/// Accept connections until the shutdown flag is set.
pub async fn accept_loop(
    listener: tokio::net::TcpListener,
    store: Arc<dyn Store>,
    subs: SubscriberMap,
    metrics: Arc<Metrics>,
    shutting_down: Arc<AtomicBool>,
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
                tokio::spawn(
                    async move {
                        let _ = connection::handle_tokio_conn(socket, id, store_c, subs_c, metrics_c).await;
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
