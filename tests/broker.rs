use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

use zhensegg::broker::connection::handle_tokio_conn;
use zhensegg::metrics::Metrics;
use zhensegg::protocol::{Op, Parser, encode_auth, encode_fetch, encode_publish, encode_subscribe};
use zhensegg::security::AccessControl;
use zhensegg::store::{MemRing, Store};
use zhensegg::subscription::{SubMap, SubscriberMap};

struct OwnedFrame {
    op: Op,
    topic: Vec<u8>,
    payload: Vec<u8>,
    offset: Option<u64>,
    len: Option<u32>,
}

struct FrameReader {
    read: OwnedReadHalf,
    parser: Parser,
    buf: Vec<u8>,
}

impl FrameReader {
    fn new(read: OwnedReadHalf) -> Self {
        Self {
            read,
            parser: Parser::new(64 * 1024),
            buf: vec![0u8; 64 * 1024],
        }
    }

    async fn next(&mut self) -> Option<OwnedFrame> {
        loop {
            if let Some(f) = self.parser.try_parse() {
                let owned = OwnedFrame {
                    op: f.op,
                    topic: f.topic.to_vec(),
                    payload: f.payload.to_vec(),
                    offset: f.offset,
                    len: f.len,
                };
                self.parser.consume();
                return Some(owned);
            }
            let n = self.read.read(&mut self.buf).await.ok()?;
            if n == 0 {
                return None;
            }
            self.parser.feed(&self.buf[..n]);
        }
    }
}

fn encode_ping(buf: &mut Vec<u8>) {
    buf.extend_from_slice(&9u32.to_be_bytes());
    buf.push(0x04); 
    buf.extend_from_slice(&0u32.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes());
}

fn client(stream: TcpStream) -> (OwnedWriteHalf, FrameReader) {
    let (read, write) = stream.into_split();
    (write, FrameReader::new(read))
}

struct Harness {
    store: Arc<dyn Store>,
    subs: SubscriberMap,
    metrics: Arc<Metrics>,
    listener: Arc<TcpListener>,
    auth: AccessControl,
}

impl Harness {
    async fn new() -> Self {
        Self::with_auth(AccessControl::open()).await
    }

    async fn with_auth(auth: AccessControl) -> Self {
        let store: Arc<dyn Store> = Arc::new(MemRing::new(1024 * 1024));
        let subs: SubscriberMap = Arc::new(SubMap::new(64));
        let metrics = Arc::new(Metrics::new());
        let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
        Self { store, subs, metrics, listener, auth }
    }

    async fn connect(&self) -> TcpStream {
        TcpStream::connect(self.listener.local_addr().unwrap()).await.unwrap()
    }

    fn spawn_servers(&self, n: usize) -> tokio::task::JoinHandle<()> {
        let connector = self.listener.clone();
        let store = self.store.clone();
        let subs = self.subs.clone();
        let metrics = self.metrics.clone();
        let auth = self.auth.clone();
        tokio::spawn(async move {
            for i in 0..n {
                let (stream, _) = connector.accept().await.unwrap();
                let store = store.clone();
                let subs = subs.clone();
                let metrics = metrics.clone();
                let auth = auth.clone();
                tokio::spawn(async move {
                    let _ = handle_tokio_conn(stream, i as u64 + 1, store, subs, metrics, auth, None, false, std::time::Duration::from_secs(10)).await;
                });
            }
        })
    }
}

fn decode_record(raw: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let tl = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let pl = u32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]) as usize;
    let topic = raw[8..8 + tl].to_vec();
    let payload = raw[8 + tl..8 + tl + pl].to_vec();
    (topic, payload)
}

#[tokio::test]
async fn broker_rejects_publish_before_auth() {
    
    let h = Harness::with_auth(AccessControl::token("s3cret")).await;
    let _server = h.spawn_servers(1);
    let (mut w, mut r) = client(h.connect().await);

    let mut frame = Vec::new();
    encode_publish(&mut frame, b"orders", b"hack");
    w.write_all(&frame).await.unwrap();

    let next = r.next().await;
    assert!(next.is_none(), "unauthorized publish must close the connection");
    assert_eq!(h.metrics.auth_failures_total.get(), 1.0);
}

#[tokio::test]
async fn broker_rejects_bad_token() {
    let h = Harness::with_auth(AccessControl::token("s3cret")).await;
    let _server = h.spawn_servers(1);
    let (mut w, mut r) = client(h.connect().await);

    let mut auth = Vec::new();
    encode_auth(&mut auth, b"wrong-token");
    w.write_all(&auth).await.unwrap();

    let next = r.next().await;
    assert!(next.is_none(), "bad token must close the connection");
    assert!(h.metrics.auth_failures_total.get() >= 1.0);
}

#[tokio::test]
async fn broker_valid_token_then_publish_works() {
    let h = Harness::with_auth(AccessControl::token("s3cret")).await;
    let _server = h.spawn_servers(1);
    let (mut w, mut r) = client(h.connect().await);

    let mut auth = Vec::new();
    encode_auth(&mut auth, b"s3cret");
    w.write_all(&auth).await.unwrap();
    let ack = r.next().await.expect("auth ack");
    assert_eq!(ack.op, Op::Ack);
    assert_eq!(ack.topic.as_slice(), b"auth");

    let mut p = Vec::new();
    encode_publish(&mut p, b"orders", b"allowed");
    w.write_all(&p).await.unwrap();
    let p_ack = r.next().await.expect("publish ack");
    assert_eq!(p_ack.op, Op::Ack);
    assert_eq!(h.metrics.auth_successes_total.get(), 1.0);
}

#[tokio::test]
async fn broker_publish_acks_and_stores_record() {
    let h = Harness::new().await;
    let _server = h.spawn_servers(1);
    let (mut w, mut r) = client(h.connect().await);

    let mut frame = Vec::new();
    encode_publish(&mut frame, b"orders", b"hello world");
    w.write_all(&frame).await.unwrap();

    let ack = r.next().await.expect("ack frame");
    assert_eq!(ack.op, Op::Ack);
    let off = ack.offset.expect("offset");
    let len = ack.len.expect("len");
    assert!(len > 0);

    let mut raw = Vec::new();
    h.store.read(off, len, &mut raw).unwrap();
    let (topic, payload) = decode_record(&raw);
    assert_eq!(topic.as_slice(), b"orders");
    assert_eq!(payload.as_slice(), b"hello world");
}

#[tokio::test]
async fn broker_subscribe_then_fetch_data() {
    let h = Harness::new().await;
    let _server = h.spawn_servers(1);
    let (mut w, mut r) = client(h.connect().await);

    let mut f = Vec::new();
    encode_subscribe(&mut f, b"news");
    w.write_all(&f).await.unwrap();
    let ack = r.next().await.expect("subscribe ack");
    assert_eq!(ack.op, Op::Ack);

    let mut p = Vec::new();
    encode_publish(&mut p, b"news", b"breaking");
    w.write_all(&p).await.unwrap();
    let p_ack = r.next().await.expect("publish ack");
    assert_eq!(p_ack.op, Op::Ack);
    let off = p_ack.offset.unwrap();
    let len = p_ack.len.unwrap();

    let mut q = Vec::new();
    encode_fetch(&mut q, b"news", off, len);
    w.write_all(&q).await.unwrap();

    let data = r.next().await.expect("data frame");
    assert_eq!(data.op, Op::Data);
    assert_eq!(data.payload.as_slice(), b"breaking");
}

#[tokio::test]
async fn broker_fanout_delivers_to_subscriber() {
    let h = Harness::new().await;
    let _server = h.spawn_servers(2);
    let (mut sub_w, mut sub_r) = client(h.connect().await);
    let (mut pub_w, _pub_r) = client(h.connect().await);

    let mut f = Vec::new();
    encode_subscribe(&mut f, b"news");
    sub_w.write_all(&f).await.unwrap();
    let ack = sub_r.next().await.expect("subscribe ack");
    assert_eq!(ack.op, Op::Ack);

    let mut p = Vec::new();
    encode_publish(&mut p, b"news", b"broadcast");
    pub_w.write_all(&p).await.unwrap();

    let data = sub_r.next().await.expect("data frame");
    assert_eq!(data.op, Op::Data);
    assert_eq!(data.topic.as_slice(), b"news");
    assert_eq!(data.payload.as_slice(), b"broadcast");
}

#[tokio::test]
async fn broker_ping_returns_pong() {
    let h = Harness::new().await;
    let _server = h.spawn_servers(1);
    let (mut w, mut r) = client(h.connect().await);

    let mut ping = Vec::new();
    encode_ping(&mut ping);
    w.write_all(&ping).await.unwrap();

    let pong = r.next().await.expect("pong frame");
    assert_eq!(pong.op, Op::Ack);
    assert_eq!(pong.topic.as_slice(), b"pong");
}

#[tokio::test]
async fn broker_cleans_up_subscription_on_disconnect() {
    let h = Harness::new().await;
    let _server = h.spawn_servers(1);
    let (mut w, mut r) = client(h.connect().await);

    let mut f = Vec::new();
    encode_subscribe(&mut f, b"topic-x");
    w.write_all(&f).await.unwrap();
    let ack = r.next().await.expect("subscribe ack");
    assert_eq!(ack.op, Op::Ack);

    {
        let g = h.subs.read(b"topic-x");
        assert!(g.contains_key(b"topic-x".as_slice()));
    }

    drop(w);
    while r.next().await.is_some() {}

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let g = h.subs.read(b"topic-x");
    assert!(!g.contains_key(b"topic-x".as_slice()), "subscriber removed after disconnect");
}
