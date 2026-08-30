//! Tests for runtime security rotation (`SharedSecurity`), the HTTP admin-plane
//! Basic-auth check, and the auth-timeout resilience guard.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::Request;
use axum::http::HeaderValue;
use base64::Engine;
use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use zhensegg::broker::connection::handle_tokio_conn;
use zhensegg::config::Config;
use zhensegg::metrics::Metrics;
use zhensegg::protocol::{encode_auth, encode_publish};
use zhensegg::security::{AccessControl, SharedSecurity};
use zhensegg::store::{MemRing, Store};
use zhensegg::subscription::{SubMap, SubscriberMap};

fn auth_req(token: Option<&str>) -> Request {
    let mut b = Request::builder().uri("/metrics");
    if let Some(t) = token {
        b = b.header("authorization", header_auth(t));
    }
    b.body(axum::body::Body::empty()).unwrap()
}

fn scratch(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("zhensegg-reload-{}-{name}", std::process::id()))
}

fn write_file(path: &std::path::Path, contents: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, contents).unwrap();
}

fn config_with(overrides: impl Fn(&mut Config)) -> Config {
    let mut c = Config::parse_from([
        "zhensegg-broker",
        "--addr",
        "127.0.0.1:0",
        "--http-addr",
        "127.0.0.1:0",
        "--cores",
        "1",
    ]);
    overrides(&mut c);
    c
}

fn header_auth(token: &str) -> HeaderValue {
    let b64 = base64::engine::general_purpose::STANDARD.encode(format!("user:{token}"));
    HeaderValue::from_str(&format!("Basic {b64}")).unwrap()
}

#[test]
fn shared_security_token_rotation_via_file() {
    let dir = scratch("token");
    let token_file = dir.join("auth.token");
    let _ = std::fs::remove_dir_all(&dir);

    write_file(&token_file, "tok-one\n");
    let sec = SharedSecurity::from_config(&config_with(|c| {
        c.auth_token_file = Some(token_file.to_str().unwrap().to_string());
    }))
    .expect("build");

    assert!(sec.snapshot().1.verify(b"tok-one"));
    assert!(!sec.snapshot().1.verify(b"tok-two"));

    // Rotate: rewrite the file and reload. New snapshot must use the new token.
    write_file(&token_file, "tok-two\n");
    sec.reload(&config_with(|c| {
        c.auth_token_file = Some(token_file.to_str().unwrap().to_string());
    }))
    .expect("reload");

    assert!(!sec.snapshot().1.verify(b"tok-one"), "old token must be rotated out");
    assert!(sec.snapshot().1.verify(b"tok-two"), "new token must take effect");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn admin_auth_check_accepts_and_rejects() {
    let dir = scratch("authcheck");
    let _ = std::fs::remove_dir_all(&dir);
    let sec = SharedSecurity::from_config(&config_with(|c| {
        c.http_auth_token = Some("adm1n".to_string());
    }))
    .expect("build");

    assert!(
        !zhensegg::broker::http::is_admin_authorized(&sec, &auth_req(None)),
        "no credentials must be rejected when a token is set"
    );
    assert!(zhensegg::broker::http::is_admin_authorized(&sec, &auth_req(Some("adm1n"))));
    assert!(
        !zhensegg::broker::http::is_admin_authorized(&sec, &auth_req(Some("wrong"))),
        "wrong token must be rejected"
    );

    // No token configured -> everything passes.
    let sec_open = SharedSecurity::from_config(&config_with(|_| {})).expect("build");
    assert!(zhensegg::broker::http::is_admin_authorized(&sec_open, &auth_req(None)));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn admin_auth_token_rotation_is_picked_up_immediately() {
    let dir = scratch("http-rotation");
    let _ = std::fs::remove_dir_all(&dir);
    let token_file = dir.join("http-auth.token");
    write_file(&token_file, "adm1n\n");
    let sec = SharedSecurity::from_config(&config_with(|c| {
        c.http_auth_token_file = Some(token_file.to_str().unwrap().to_string());
    }))
    .expect("build");

    let good_old = auth_req(Some("adm1n"));
    assert!(zhensegg::broker::http::is_admin_authorized(&sec, &good_old));

    write_file(&token_file, "adm2n\n");
    sec.reload(&config_with(|c| {
        c.http_auth_token_file = Some(token_file.to_str().unwrap().to_string());
    }))
    .expect("reload");

    assert!(!zhensegg::broker::http::is_admin_authorized(&sec, &good_old));
    assert!(zhensegg::broker::http::is_admin_authorized(&sec, &auth_req(Some("adm2n"))));

    let _ = std::fs::remove_dir_all(&dir);
}

// ---- auth timeout: an unauthenticated connection is dropped after the timeout ----

#[tokio::test]
async fn auth_timeout_drops_unauthenticated_connection() {
    let store: Arc<dyn Store> = Arc::new(MemRing::new(1024 * 1024));
    let subs: SubscriberMap = Arc::new(SubMap::new(64));
    let metrics = Arc::new(Metrics::new());
    let auth = AccessControl::token("s3cret");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = handle_tokio_conn(
            stream,
            1,
            store,
            subs,
            metrics,
            auth,
            Some(Duration::from_millis(200)),
        )
        .await;
    });

    let mut conn = tokio::net::TcpStream::connect(server_addr).await.unwrap();

    // Do nothing: the server must close us after ~200ms.
    let mut buf = vec![0u8; 16];
    let res = tokio::time::timeout(Duration::from_millis(2000), conn.read(&mut buf)).await;
    match res {
        Ok(Ok(0)) => {}                  // clean EOF — dropped as expected
        Ok(Ok(_)) => panic!("unexpected data on unauthenticated connection"),
        Ok(Err(e)) => {
            // A reset is acceptable (broker closes the fd).
            assert_eq!(e.kind(), std::io::ErrorKind::ConnectionReset);
        }
        Err(_) => panic!("connection was not dropped within the auth timeout"),
    }

    // The handler returns after cleanup.
    let _ = conn;
    tokio::time::timeout(Duration::from_secs(2), server).await.unwrap().unwrap();
}

#[tokio::test]
async fn auth_timeout_does_not_kill_authenticated_connection() {
    let store: Arc<dyn Store> = Arc::new(MemRing::new(1024 * 1024));
    let subs: SubscriberMap = Arc::new(SubMap::new(64));
    let metrics = Arc::new(Metrics::new());
    let auth = AccessControl::token("s3cret");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = handle_tokio_conn(
            stream,
            1,
            store,
            subs,
            metrics,
            auth,
            Some(Duration::from_millis(200)),
        )
        .await;
    });

    let mut conn = tokio::net::TcpStream::connect(server_addr).await.unwrap();
    let mut auth_frame = Vec::new();
    encode_auth(&mut auth_frame, b"s3cret");
    conn.write_all(&auth_frame).await.unwrap();

    // Authenticate; then publish after the original 200ms deadline — a live
    // connection must still work (timeout applies only to THE auth phase).
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut pub_frame = Vec::new();
    encode_publish(&mut pub_frame, b"t", b"ping");
    conn.write_all(&pub_frame).await.unwrap();

    let mut buf = [0u8; 128];
    let n = tokio::time::timeout(Duration::from_secs(2), conn.read(&mut buf)).await.unwrap().unwrap();
    assert!(n > 0, "authenticated connection must survive past the auth deadline");

    // Close the connection so the server-side handler sees EOF and returns.
    drop(conn);
    tokio::time::timeout(Duration::from_secs(2), server).await.unwrap().unwrap();
}

// ---- TLS cert rotation: reload swaps the served certificate ----

/// Try to complete a TLS handshake against `addr`, trusting `trust_pem`.
/// Returns `Ok(())` on success, the error on failure.
async fn try_handshake(addr: std::net::SocketAddr, trust_pem: &str) -> Result<(), String> {
    rustls::crypto::ring::default_provider().install_default().ok();
    let mut roots = rustls::RootCertStore::empty();
    let mut buf = std::io::Cursor::new(trust_pem.as_bytes().to_vec());
    let der = rustls_pemfile::certs(&mut buf)
        .next()
        .ok_or("no trust cert")?
        .unwrap();
    roots.add(der).map_err(|e| e.to_string())?;
    let client_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));

    let tcp = tokio::net::TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
    let name = rustls::pki_types::ServerName::try_from("localhost").map_err(|e| e.to_string())?;
    connector.connect(name, tcp).await.map_err(|e| e.to_string())?;
    Ok(())
}

#[tokio::test]
async fn tls_cert_rotation_replaces_served_certificate() {
    let dir = scratch("tls-rotation");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let cert1 = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert2 = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");

    std::fs::write(&cert_path, cert1.cert.pem()).unwrap();
    std::fs::write(&key_path, cert1.key_pair.serialize_pem()).unwrap();

    // Ignore auth for this test — exercise cert swap only.
    let sec = SharedSecurity::from_config(&config_with(|c| {
        c.tls_cert = Some(cert_path.to_str().unwrap().to_string());
        c.tls_key = Some(key_path.to_str().unwrap().to_string());
    }))
    .expect("build");

    let (acceptor1, _auth) = sec.snapshot();
    let acceptor1 = acceptor1.expect("tls acceptor present");

    // Serve with cert1, client trusting cert1 -> handshake OK.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let _ = acceptor1.accept(socket).await;
    });
    try_handshake(addr, &cert1.cert.pem()).await.expect("handshake with cert1");
    server.await.unwrap();

    // Rotate: write cert2 over the same paths and reload.
    std::fs::write(&cert_path, cert2.cert.pem()).unwrap();
    std::fs::write(&key_path, cert2.key_pair.serialize_pem()).unwrap();
    sec.reload(&config_with(|c| {
        c.tls_cert = Some(cert_path.to_str().unwrap().to_string());
        c.tls_key = Some(key_path.to_str().unwrap().to_string());
    }))
    .expect("reload");

    let (acceptor2, _auth) = sec.snapshot();
    let acceptor2 = acceptor2.expect("tls acceptor present after reload");

    // Serve with the reloaded acceptor: client trusting cert2 must succeed,
    // while trusting the old cert1 must fail (unknown issuer). The server
    // accepts two connections (one for each client attempt).
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = acceptor2.accept(socket).await;
        }
    });
    try_handshake(addr, &cert2.cert.pem()).await.expect("handshake with rotated cert2");
    let old_ok = tokio::time::timeout(Duration::from_secs(5), try_handshake(addr, &cert1.cert.pem()))
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false);
    assert!(!old_ok, "old cert1 must no longer be trusted after rotation");

    server.await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}