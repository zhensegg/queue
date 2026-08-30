//! TLS termination for the data plane.
//!
//! The handshake runs *once* per connection in the accept loop, so steady-state
//! message latency/RPS are unaffected by enabling TLS.

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use rustls_pemfile::{certs, private_key};
use tokio_rustls::TlsAcceptor;

/// Result-code for a connection that arrived on a TLS listener before the
/// handshake completes in the accept loop.
pub use tokio_rustls::server::TlsStream;

/// Load a PEM cert chain plus a PEM private key and build a rustls TLS acceptor.
pub fn build_tls_acceptor(
    cert_pem_path: &str,
    key_pem_path: &str,
) -> anyhow::Result<TlsAcceptor> {
    // Use the `ring` crypto provider (hardware-accelerated AES-GCM on x86)
    // as the process default so server and client stay on the same provider.
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let cert_file = File::open(cert_pem_path).map_err(|e| {
        anyhow::anyhow!("cannot open TLS cert `{cert_pem_path}`: {e}")
    })?;
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        certs(&mut BufReader::new(cert_file))
            .collect::<Result<Vec<_>, _>>()?;

    let key_file = File::open(key_pem_path).map_err(|e| {
        anyhow::anyhow!("cannot open TLS key `{key_pem_path}`: {e}")
    })?;
    let key = private_key(&mut BufReader::new(key_file))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in `{key_pem_path}`"))?;

    let mut config =
        rustls::ServerConfig::builder().with_no_client_auth().with_single_cert(certs, key)?;

    // Prefer TLS 1.3 first, then TLS 1.2 for legacy clients; leave rustls defaults
    // (AES-256/128-GCM, CHACHA20, x25519) untouched so the fast provider is used.
    config.alpn_protocols = vec![b"zhensegg/1".to_vec()];
    config.max_early_data_size = u32::MAX; // allow TLS 1.3 0-RTT for latency

    Ok(TlsAcceptor::from(Arc::new(config)))
}
