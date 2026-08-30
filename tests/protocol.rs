use zhensegg::protocol::{
    Op, Parser, encode_ack, encode_fetch, encode_notify, encode_publish, encode_subscribe,
};

#[test]
fn roundtrip_zero_alloc() {
    let mut buf = Vec::new();
    encode_publish(&mut buf, b"test-topic", b"hello world");
    let mut parser = Parser::new(64 * 1024);
    parser.feed(&buf);
    let frame = parser.try_parse().expect("frame");
    assert_eq!(frame.op, Op::Publish);
    assert_eq!(frame.topic, b"test-topic");
    assert_eq!(frame.payload, b"hello world");
    parser.consume();
    assert_eq!(parser.buffered(), 0);
}

#[test]
fn batch_two_frames() {
    let mut buf = Vec::new();
    encode_publish(&mut buf, b"t1", b"msg1");
    encode_publish(&mut buf, b"t2", b"msg2");
    let mut p = Parser::new(1024);
    p.feed(&buf);
    let mut count = 0;
    p.drain(|f| {
        count += 1;
        assert_eq!(f.op, Op::Publish);
    });
    assert_eq!(count, 2);
}

#[test]
fn partial_feed() {
    let mut buf = Vec::new();
    encode_publish(&mut buf, b"t", b"payload");
    let mut p = Parser::new(1024);
    p.feed(&buf[..5]);
    assert!(p.try_parse().is_none());
    p.feed(&buf[5..]);
    assert!(p.try_parse().is_some());
}

#[test]
fn bench_no_alloc() {
    let mut buf = Vec::new();
    for _ in 0..1000 {
        encode_publish(&mut buf, b"bench", b"x");
    }
    let mut p = Parser::new(1024 * 1024);
    p.feed(&buf);
    let mut n = 0;
    p.drain(|_| n += 1);
    assert_eq!(n, 1000);
    assert_eq!(p.buffered(), 0);
}

#[test]
fn encode_subscribe_frame() {
    let mut buf = Vec::new();
    encode_subscribe(&mut buf, b"test");
    let mut parser = Parser::new(1024);
    parser.feed(&buf);
    let frame = parser.try_parse().unwrap();
    assert_eq!(frame.op, Op::Subscribe);
    assert_eq!(frame.topic, b"test");
    assert_eq!(frame.payload.len(), 0);
}

#[test]
fn encode_ack_and_notify_with_offset() {
    let mut buf = Vec::new();
    encode_ack(&mut buf, b"topic", 999, 42);
    let mut parser = Parser::new(1024);
    parser.feed(&buf);
    let frame = parser.try_parse().unwrap();
    assert_eq!(frame.op, Op::Ack);
    assert_eq!(frame.offset, Some(999));
    assert_eq!(frame.len, Some(42));

    let mut buf2 = Vec::new();
    encode_notify(&mut buf2, b"topic", 111, 7);
    let mut parser2 = Parser::new(1024);
    parser2.feed(&buf2);
    let frame2 = parser2.try_parse().unwrap();
    assert_eq!(frame2.op, Op::Notify);
    assert_eq!(frame2.offset, Some(111));
    assert_eq!(frame2.len, Some(7));
}

#[test]
fn encode_fetch_embeds_offset_in_payload() {
    let mut buf = Vec::new();
    encode_fetch(&mut buf, b"topic", 12345, 100);
    let mut parser = Parser::new(1024);
    parser.feed(&buf);
    let frame = parser.try_parse().unwrap();
    assert_eq!(frame.op, Op::Fetch);
    // Fetch embeds offset+len in the 12-byte payload
    assert_eq!(frame.payload.len(), 12);
    let off = u64::from_be_bytes(frame.payload[0..8].try_into().unwrap());
    let len = u32::from_be_bytes(frame.payload[8..12].try_into().unwrap());
    assert_eq!(off, 12345);
    assert_eq!(len, 100);
}
