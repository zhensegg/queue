use std::fs;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio_rustls::TlsAcceptor;

use super::tls::build_tls_acceptor;
use super::AccessControl;
use crate::config::Config;

#[derive(Clone, Default)]
struct Inner {
    tls: Option<TlsAcceptor>,
    auth: AccessControl,
    http_token: Option<Arc<[u8]>>,
}

#[derive(Clone)]
pub struct SharedSecurity {
    inner: Arc<RwLock<Inner>>,
}

pub fn read_token_file(path: &str) -> anyhow::Result<Vec<u8>> {
    let raw = fs::read(path)
        .map_err(|e| anyhow::anyhow!("cannot read token file `{path}`: {e}"))?;
    let mut v = raw;
    if v.last() == Some(&b'\n') {
        v.pop();
        if v.last() == Some(&b'\r') {
            v.pop();
        }
    }
    Ok(v)
}

impl Default for SharedSecurity {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner::default())),
        }
    }
}

impl SharedSecurity {
    
    pub fn from_config(cfg: &Config) -> anyhow::Result<SharedSecurity> {
        let s = SharedSecurity::default();
        
        let tls = match (&cfg.tls_cert, &cfg.tls_key) {
            (Some(cert), Some(key)) => Some(super::build_tls_acceptor(cert, key)?),
            (None, None) => None,
            _ => anyhow::bail!("--tls-cert and --tls-key must be provided together"),
        };
        let auth = resolve_auth(&cfg.auth_token, &cfg.auth_token_file);
        let http_token = resolve_http_token(&cfg.http_auth_token, &cfg.http_auth_token_file);
        *s.inner.write() = Inner { tls, auth, http_token };
        Ok(s)
    }

    pub fn reload(&self, cfg: &Config) -> anyhow::Result<()> {
        let tls = match (&cfg.tls_cert, &cfg.tls_key) {
            (Some(cert), Some(key)) => Some(build_tls_acceptor(cert, key)?),
            (None, None) => None,
            _ => anyhow::bail!("--tls-cert and --tls-key must be provided together"),
        };
        let auth = resolve_auth(&cfg.auth_token, &cfg.auth_token_file);
        let http_token = resolve_http_token(&cfg.http_auth_token, &cfg.http_auth_token_file);

        let mut guard = self.inner.write();
        guard.tls = tls;
        guard.auth = auth;
        guard.http_token = http_token;
        Ok(())
    }

    pub fn snapshot(&self) -> (Option<TlsAcceptor>, AccessControl) {
        let guard = self.inner.read();
        (guard.tls.clone(), guard.auth.clone())
    }

    pub fn http_token(&self) -> Option<Arc<[u8]>> {
        self.inner.read().http_token.clone()
    }
}

fn read_opt(primary: &Option<String>, file: &Option<String>) -> anyhow::Result<Option<Arc<[u8]>>> {
    if let Some(p) = file.as_ref().filter(|p| !p.is_empty()) {
        let raw = read_token_file(p)?;
        if !raw.is_empty() {
            return Ok(Some(raw.into()));
        }
    }
    Ok(primary.as_deref().map(|s| Arc::<[u8]>::from(s.as_bytes())))
}

fn resolve_auth(primary: &Option<String>, file: &Option<String>) -> AccessControl {
    match read_opt(primary, file) {
        Ok(Some(t)) => AccessControl::Token(t),
        _ => AccessControl::open(),
    }
}

fn resolve_http_token(primary: &Option<String>, file: &Option<String>) -> Option<Arc<[u8]>> {
    read_opt(primary, file).ok().flatten()
}
