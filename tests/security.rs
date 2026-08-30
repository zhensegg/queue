use std::io::Cursor;
use std::sync::Arc;

use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use zhensegg::broker::connection::handle_tls_conn;
use zhensegg::metrics::Metrics;
use zhensegg::protocol::{Op, Parser, encode_auth, encode_publish};
use zhensegg::security::{AccessControl, build_tls_acceptor, secure_eq};
use zhensegg::store::{MemRing, Store};
use zhensegg::subscription::{SubMap, SubscriberMap};

#[test]
fn secure_eq_matches_exact_values() {
    assert!(secure_eq(b"secret-token", b"secret-token"));
    assert!(!secure_eq(b"secret-token", b"secret-tokpn"));
    assert!(!secure_eq(b"secret-token", b"short"));
    assert!(!secure_eq(b"short", b"secret-token"));
    assert!(secure_eq(b"", b""));
}

#[test]
fn access_control_open_accepts_anything() {
    let ac = AccessControl::open();
    assert!(ac.initially_authenticated());
    assert!(ac.verify(b""));
    assert!(ac.verify(b"whatever"));
}

#[test]
fn access_control_token_policy() {
    let ac = AccessControl::token("s3cret");
    assert!(!ac.initially_authenticated());
    assert!(ac.verify(b"s3cret"));
    assert!(!ac.verify(b"s3cret "));
    assert!(!ac.verify(b"secret"));
    assert!(!ac.verify(b""));
}

async fn write_temp(path: &std::path::Path, contents: &str) {
    tokio::fs::write(path, contents).await.unwrap();
}

#[tokio::test]
async fn tls_handshake_and_auth_publish_roundtrip() {
    
    rustls::crypto::ring::default_provider().install_default().ok();

    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("rcgen");
    let cert_pem = certified.cert.pem();
    let key_pem = certified.key_pair.serialize_pem();

    let dir = std::env::temp_dir().join(format!("zhensegg-tls-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");
    write_temp(&cert_path, &cert_pem).await;
    write_temp(&key_path, &key_pem).await;

    let acceptor = build_tls_acceptor(cert_path.to_str().unwrap(), key_path.to_str().unwrap())
        .expect("build acceptor");

    let store: Arc<dyn Store> = Arc::new(MemRing::new(1024 * 1024));
    let subs: SubscriberMap = Arc::new(SubMap::new(64));
    let metrics = Arc::new(Metrics::new());
    let auth = AccessControl::token("tls-token");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let tls_stream = acceptor.accept(socket).await.expect("server tls handshake");
        let _ = handle_tls_conn(tls_stream, 1, store, subs, metrics, auth, None, false, std::time::Duration::from_secs(10)).await;
    });

    let mut roots = RootCertStore::empty();
    let mut buf = Cursor::new(cert_pem.into_bytes());
    let der = rustls_pemfile::certs(&mut buf).next().unwrap().unwrap();
    roots.add(der).unwrap();
    let client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));

    let tcp = TcpStream::connect(server_addr).await.unwrap();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls = connector.connect(name, tcp).await.expect("client tls handshake");

    async fn read_frame<S>(stream: &mut S, parser: &mut Parser, rbuf: &mut [u8]) -> (Op, Vec<u8>, Vec<u8>)
    where
        S: tokio::io::AsyncRead + Unpin,
    {
        loop {
            if let Some(f) = parser.try_parse() {
                let out = (f.op, f.topic.to_vec(), f.payload.to_vec());
                parser.consume();
                return out;
            }
            let n = stream.read(rbuf).await.unwrap();
            assert!(n > 0, "unexpected EOF from broker");
            parser.feed(&rbuf[..n]);
        }
    }

    let mut parser = Parser::new(4096);
    let mut rbuf = vec![0u8; 8192];

    let mut auth_frame = Vec::new();
    encode_auth(&mut auth_frame, b"tls-token");
    tls.write_all(&auth_frame).await.unwrap();
    let (op, topic, _p) = read_frame(&mut tls, &mut parser, &mut rbuf).await;
    assert_eq!(op, Op::Ack);
    assert_eq!(topic, b"auth".as_slice());

    let mut pub_frame = Vec::new();
    encode_publish(&mut pub_frame, b"tls-topic", b"encrypted-payload");
    tls.write_all(&pub_frame).await.unwrap();
    let (op, _topic, _p) = read_frame(&mut tls, &mut parser, &mut rbuf).await;
    assert_eq!(op, Op::Ack);

    drop(tls);
    server.await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
