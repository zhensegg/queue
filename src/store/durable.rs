use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;

pub struct DurableGate {
    pos: AtomicU64,
    waiters: Mutex<Vec<(u64, Waker)>>,
}

impl DurableGate {
    pub fn new(pos: u64) -> Self {
        Self { pos: AtomicU64::new(pos), waiters: Mutex::new(Vec::new()) }
    }

    #[inline]
    pub fn pos(&self) -> u64 {
        self.pos.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn advance(&self, new_pos: u64) {
        if self.pos() >= new_pos {
            return;
        }
        self.pos.fetch_max(new_pos, std::sync::atomic::Ordering::AcqRel);
        let mut waiters = self.waiters.lock();
        if !waiters.is_empty() {
            waiters.retain(|(target, waker)| {
                if *target <= new_pos {
                    waker.wake_by_ref();
                    false
                } else {
                    true
                }
            });
        }
    }
}

pub struct WaitDurable {
    gate: Arc<DurableGate>,
    target: u64,
    registered: bool,
}

pub fn wait_durable(gate: Arc<DurableGate>, target: u64) -> WaitDurable {
    WaitDurable { gate, target, registered: false }
}

impl std::future::Future for WaitDurable {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        use std::sync::atomic::Ordering;
        let this = &mut *self;
        if this.gate.pos.load(Ordering::Acquire) >= this.target {
            return Poll::Ready(());
        }
        if !this.registered {
            this.gate.waiters.lock().push((this.target, cx.waker().clone()));
            this.registered = true;
            if this.gate.pos.load(Ordering::Acquire) >= this.target {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_wakes_waiters_below_new_pos() {
        let gate = Arc::new(DurableGate::new(0));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let g = gate.clone();
            let waiter = tokio::spawn(async move {
                wait_durable(g, 100).await;
            });
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            assert!(!waiter.is_finished(), "must not be ready before advance");
            gate.advance(99);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            assert!(!waiter.is_finished(), "target above new pos stays pending");
            gate.advance(150);
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), waiter).await.unwrap();
        });
    }
}
