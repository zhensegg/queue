//! Reloadable security context (TLS acceptor + auth tokens).
//!
//! The broker holds the security state in an [`Arc<SharedSecurity>`] shared by
//! the accept loops and the HTTP admin server. A `SIGHUP` (unix) triggers
//! [`SharedSecurity::reload`], which re-reads the token files and TLS
//! certificate/key from disk and atomically swaps in the new values.
//!
//! Existing connections keep whatever context they snapshotted at accept time;
//! only new connections observe the rotated material — there is no churn on the
//! steady-state hot path.

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

/// A reloadable snapshot of the broker's transport security material.
#[derive(Clone)]
pub struct SharedSecurity {
    inner: Arc<RwLock<Inner>>,
}

/// Read a token from a file, trimming a single trailing line terminator.
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
    /// Build the initial security context from the configuration.
    pub fn from_config(cfg: &Config) -> anyhow::Result<SharedSecurity> {
        let s = SharedSecurity::default();
        // Build the initial Inner directly so a bad cert/key fails startup.
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

    /// Re-read the configured token files and TLS material from disk and swap
    /// them into place. Any error leaves the current context untouched.
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

    /// Snapshot the current data-plane TLS acceptor + auth policy. The accept
    /// loop calls this once per accepted connection.
    pub fn snapshot(&self) -> (Option<TlsAcceptor>, AccessControl) {
        let guard = self.inner.read();
        (guard.tls.clone(), guard.auth.clone())
    }

    /// The current HTTP admin token (re-read on every request so a SIGHUP
    /// rotation applies immediately to the admin plane).
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
