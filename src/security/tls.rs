use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use rustls_pemfile::{certs, private_key};
use tokio_rustls::TlsAcceptor;

pub use tokio_rustls::server::TlsStream;

pub fn build_tls_acceptor(
    cert_pem_path: &str,
    key_pem_path: &str,
) -> anyhow::Result<TlsAcceptor> {
    
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

    config.alpn_protocols = vec![b"zhensegg/1".to_vec()];
    config.max_early_data_size = u32::MAX; 

    Ok(TlsAcceptor::from(Arc::new(config)))
}
