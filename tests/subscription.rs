use std::sync::Arc;

use zhensegg::subscription::{SubMap, Subscriber};

fn make_sub(tx: tokio::sync::mpsc::UnboundedSender<Arc<Vec<u8>>>, id: u64) -> Arc<Subscriber> {
    Arc::new(Subscriber { id, tx })
}

#[test]
fn submap_new_normalizes_shard_count() {
    // n rounds up to next power of two; even a small n must produce a valid map
    let map = SubMap::new(3);
    let _ = map.read(b"topic").get(b"topic".as_slice());
    let map2 = SubMap::new(0);
    let _ = map2.read(b"t").get(b"t".as_slice());
}

#[test]
fn test_shard_hash_is_deterministic() {
    let m1 = SubMap::new(64);
    let m2 = SubMap::new(64);
    assert_eq!(m1.idx(b"orders"), m2.idx(b"orders"));
    assert_eq!(m1.idx(b""), m2.idx(b""));
    // equal topics must map to the same shard index
    assert_eq!(m1.idx(b"abc"), m1.idx(b"abc"));
}

#[test]
fn test_subscribe_adds_and_reads_subscribers() {
    let subs = SubMap::new(64);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Arc<Vec<u8>>>();

    // subscribe one connection to two topics
    {
        let mut g = subs.write(b"orders");
        g.entry(b"orders".to_vec()).or_default().push(make_sub(tx.clone(), 1));
    }
    {
        let mut g = subs.write(b"shipments");
        g.entry(b"shipments".to_vec()).or_default().push(make_sub(tx.clone(), 1));
    }

    // reading back returns the subscriber
    let g = subs.read(b"orders");
    let list = g.get(b"orders".as_slice()).expect("topic present");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, 1);

    // unrelated topic has no subscribers
    let g2 = subs.read(b"none");
    assert!(g2.get(b"none".as_slice()).is_none());
    drop(rx);
}

#[test]
fn test_multiple_subscribers_on_same_topic() {
    let subs = SubMap::new(64);
    let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel::<Arc<Vec<u8>>>();
    let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel::<Arc<Vec<u8>>>();
    let (tx3, _rx3) = tokio::sync::mpsc::unbounded_channel::<Arc<Vec<u8>>>();

    {
        let mut g = subs.write(b"topic");
        let list = g.entry(b"topic".to_vec()).or_default();
        list.push(make_sub(tx1, 1));
        list.push(make_sub(tx2, 2));
        list.push(make_sub(tx3, 3));
    }

    let g = subs.read(b"topic");
    let list = g.get(b"topic".as_slice()).expect("present");
    assert_eq!(list.len(), 3);
    let ids: Vec<u64> = list.iter().map(|s| s.id).collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn test_fanout_delivers_to_all_subscribers() {
    let subs = SubMap::new(64);
    let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel::<Arc<Vec<u8>>>();
    let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel::<Arc<Vec<u8>>>();

    {
        let mut g = subs.write(b"notifications");
        let list = g.entry(b"notifications".to_vec()).or_default();
        list.push(make_sub(tx1, 1));
        list.push(make_sub(tx2, 2));
    }

    // emulate a published frame forwarded to every subscriber
    let frame = Arc::new(b"data".to_vec());
    {
        let g = subs.read(b"notifications");
        if let Some(list) = g.get(b"notifications".as_slice()) {
            for sub in list.iter() {
                let _ = sub.tx.send(frame.clone());
            }
        }
    }

    assert_eq!(rx1.try_recv().unwrap().as_slice(), b"data");
    assert_eq!(rx2.try_recv().unwrap().as_slice(), b"data");
    assert!(rx1.try_recv().is_err()); // exactly one message each
    assert!(rx2.try_recv().is_err());
}

#[test]
fn test_unsubscribe_removes_by_id() {
    let subs = SubMap::new(64);
    let (tx1, _) = tokio::sync::mpsc::unbounded_channel::<Arc<Vec<u8>>>();
    let (tx2, _) = tokio::sync::mpsc::unbounded_channel::<Arc<Vec<u8>>>();

    {
        let mut g = subs.write(b"t");
        let list = g.entry(b"t".to_vec()).or_default();
        list.push(make_sub(tx1, 1));
        list.push(make_sub(tx2, 2));
    }

    // connection 1 disconnects
    {
        let mut g = subs.write(b"t");
        if let Some(list) = g.get_mut(b"t".as_slice()) {
            list.retain(|s| s.id != 1);
            if list.is_empty() {
                g.remove(b"t".as_slice());
            }
        }
    }

    let g = subs.read(b"t");
    let list = g.get(b"t".as_slice()).expect("still present, one left");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, 2);
}

#[test]
fn test_empty_topic_removed_entirely() {
    let subs = SubMap::new(64);
    let (tx, _) = tokio::sync::mpsc::unbounded_channel::<Arc<Vec<u8>>>();

    {
        let mut g = subs.write(b"t");
        g.entry(b"t".to_vec()).or_default().push(make_sub(tx, 7));
    }

    {
        let mut g = subs.write(b"t");
        if let Some(list) = g.get_mut(b"t".as_slice()) {
            list.retain(|s| s.id != 7);
            if list.is_empty() {
                g.remove(b"t".as_slice());
            }
        }
    }

    let g = subs.read(b"t");
    assert!(g.get(b"t".as_slice()).is_none(), "topic entry removed when empty");
}
