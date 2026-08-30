use super::{Op, LEN_PREFIX};

pub fn encode_frame(buf: &mut Vec<u8>, op: Op, topic: &[u8], payload: &[u8]) {
    let total_len = 1 + 4 + 4 + topic.len() + payload.len();
    buf.reserve(LEN_PREFIX + total_len);
    buf.extend_from_slice(&(total_len as u32).to_be_bytes());
    buf.push(op as u8);
    buf.extend_from_slice(&(topic.len() as u32).to_be_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(topic);
    buf.extend_from_slice(payload);
}

pub fn encode_publish(buf: &mut Vec<u8>, topic: &[u8], payload: &[u8]) {
    encode_frame(buf, Op::Publish, topic, payload);
}

pub fn encode_subscribe(buf: &mut Vec<u8>, topic: &[u8]) {
    encode_frame(buf, Op::Subscribe, topic, &[]);
}

pub fn encode_fetch(buf: &mut Vec<u8>, topic: &[u8], offset: u64, len: u32) {
    let mut payload = Vec::with_capacity(12);
    payload.extend_from_slice(&offset.to_be_bytes());
    payload.extend_from_slice(&len.to_be_bytes());
    encode_frame(buf, Op::Fetch, topic, &payload);
}

pub fn encode_ack(buf: &mut Vec<u8>, topic: &[u8], offset: u64, len: u32) {
    let mut payload = Vec::with_capacity(12);
    payload.extend_from_slice(&offset.to_be_bytes());
    payload.extend_from_slice(&len.to_be_bytes());
    encode_frame(buf, Op::Ack, topic, &payload);
}

pub fn encode_notify(buf: &mut Vec<u8>, topic: &[u8], offset: u64, len: u32) {
    let mut payload = Vec::with_capacity(12);
    payload.extend_from_slice(&offset.to_be_bytes());
    payload.extend_from_slice(&len.to_be_bytes());
    encode_frame(buf, Op::Notify, topic, &payload);
}

pub fn encode_data(buf: &mut Vec<u8>, topic: &[u8], payload: &[u8]) {
    encode_frame(buf, Op::Data, topic, payload);
}

pub fn encode_auth(buf: &mut Vec<u8>, token: &[u8]) {
    encode_frame(buf, Op::Auth, b"auth", token);
}

pub fn encode_error(buf: &mut Vec<u8>, topic: &[u8], message: &str) {
    encode_frame(buf, Op::Error, topic, message.as_bytes());
}
