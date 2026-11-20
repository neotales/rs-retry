//! A concurrency limit with a bounded queue. One value, no key map.

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Waker},
};

use crate::{
    cancel::{Cancellation, Registration},
    sync_util::lock,
};

#[derive(Debug)]
struct State {
    in_flight: usize,
    queued: usize,
    waiters: VecDeque<Waiter>,
    next_id: u64,
}

#[derive(Debug)]
struct Waiter {
    id: u64,
    waker: Waker,
}

/// Maximum in-flight calls and a bounded queue of waiters.
///
/// When both are full the call is rejected and the operation is not started.
/// A permit is RAII: dropping it frees the slot and wakes one waiter. The crate
/// does not grow a map of keys. The caller owns partitions.
#[derive(Clone, Debug)]
pub struct Bulkhead {
    max_in_flight: usize,
    max_queue: usize,
    state: Arc<Mutex<State>>,
    available: Arc<Condvar>,
}

impl Bulkhead {
    /// Builds a limiter. Zero in-flight or zero queue is allowed: a call that
    /// cannot take a slot and cannot queue is rejected.
    #[must_use]
    pub fn new(max_in_flight: usize, max_queue: usize) -> Self {
        Self {
            max_in_flight,
            max_queue,
            state: Arc::new(Mutex::new(State {
                in_flight: 0,
                queued: 0,
                waiters: VecDeque::new(),
                next_id: 0,
            })),
            available: Arc::new(Condvar::new()),
        }
    }

    /// Takes an in-flight slot without waiting.
    ///
    /// Returns [`None`] when every in-flight slot is taken. The queue is not used.
    #[must_use]
    pub fn try_acquire(&self) -> Option<Permit> {
        let mut state = lock(&self.state);
        if state.in_flight >= self.max_in_flight {
            return None;
        }
        state.in_flight += 1;
        Some(self.permit())
    }

    /// Takes an in-flight slot, waiting in the bounded queue when every slot is busy.
    ///
    /// Returns [`None`] when the queue is full or `cancel` is set. The wait uses
    /// [`Condvar`] and does not spawn a thread. A test that must not block should
    /// use a zero queue, or [`Bulkhead::try_acquire`].
    #[must_use]
    pub fn acquire(&self, cancel: &Cancellation) -> Option<Permit> {
        let mut state = lock(&self.state);
        loop {
            if cancel.is_cancelled() {
                return None;
            }
            if state.in_flight < self.max_in_flight {
                state.in_flight += 1;
                return Some(self.permit());
            }
            if state.queued >= self.max_queue {
                return None;
            }
            state.queued += 1;
            let shared = Arc::clone(&self.state);
            let available = Arc::clone(&self.available);
            let Some(registration) = cancel.listen(Arc::new(move || {
                let _guard = lock(&shared);
                available.notify_all();
            })) else {
                state.queued -= 1;
                return None;
            };
            state = self
                .available
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.queued = state.queued.saturating_sub(1);
            drop(registration);
        }
    }

    pub(crate) fn acquire_async<'a>(&self, cancel: &'a Cancellation) -> Acquire<'a> {
        let id = {
            let mut state = lock(&self.state);
            let id = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            id
        };
        Acquire {
            bulkhead: self.clone(),
            cancel,
            id,
            queued: false,
            registration: None,
        }
    }

    fn permit(&self) -> Permit {
        Permit {
            state: Arc::clone(&self.state),
            available: Arc::clone(&self.available),
        }
    }
}

/// An in-flight slot. Dropping it releases the slot.
#[derive(Debug)]
#[must_use = "dropping the permit releases the in-flight slot"]
pub struct Permit {
    state: Arc<Mutex<State>>,
    available: Arc<Condvar>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let waker = {
            let mut state = lock(&self.state);
            state.in_flight = state.in_flight.saturating_sub(1);
            state.waiters.pop_front().map(|waiter| waiter.waker)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        self.available.notify_one();
    }
}

pub(crate) struct Acquire<'a> {
    bulkhead: Bulkhead,
    cancel: &'a Cancellation,
    id: u64,
    queued: bool,
    registration: Option<Registration<'a>>,
}

impl Future for Acquire<'_> {
    type Output = Option<Permit>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Permit>> {
        let this = self.get_mut();
        if this.cancel.is_cancelled() {
            this.leave_queue();
            return Poll::Ready(None);
        }
        let waker = cx.waker().clone();
        this.registration = this.cancel.listen(Arc::new(move || waker.wake_by_ref()));
        if this.registration.is_none() {
            this.leave_queue();
            return Poll::Ready(None);
        }
        let mut state = lock(&this.bulkhead.state);
        if state.in_flight < this.bulkhead.max_in_flight {
            if this.queued {
                state.queued = state.queued.saturating_sub(1);
                state.waiters.retain(|waiter| waiter.id != this.id);
                this.queued = false;
            }
            this.registration = None;
            state.in_flight += 1;
            return Poll::Ready(Some(this.bulkhead.permit()));
        }
        if this.queued {
            if let Some(waiter) = state.waiters.iter_mut().find(|waiter| waiter.id == this.id) {
                waiter.waker.clone_from(cx.waker());
            }
            return Poll::Pending;
        }
        if state.queued >= this.bulkhead.max_queue {
            this.registration = None;
            return Poll::Ready(None);
        }
        state.queued += 1;
        state.waiters.push_back(Waiter {
            id: this.id,
            waker: cx.waker().clone(),
        });
        this.queued = true;
        Poll::Pending
    }
}

impl Acquire<'_> {
    fn leave_queue(&mut self) {
        if !self.queued {
            return;
        }
        let mut state = lock(&self.bulkhead.state);
        state.queued = state.queued.saturating_sub(1);
        state.waiters.retain(|waiter| waiter.id != self.id);
        self.queued = false;
    }
}

impl Drop for Acquire<'_> {
    fn drop(&mut self) {
        self.leave_queue();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        task::Wake,
        time::{Duration, Instant},
    };

    #[test]
    fn sync_cancel_wakes_queued_waiter_and_frees_queue() {
        let bulkhead = Bulkhead::new(0, 1);
        let cancel = Arc::new(Cancellation::new());
        let worker_bulkhead = bulkhead.clone();
        let worker_cancel = Arc::clone(&cancel);
        let (send, recv) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            send.send(worker_bulkhead.acquire(&worker_cancel).is_none())
                .unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while lock(&bulkhead.state).queued == 0 {
            assert!(Instant::now() < deadline, "waiter never queued");
            std::thread::yield_now();
        }
        cancel.cancel();
        assert!(
            recv.recv_timeout(Duration::from_secs(2))
                .expect("cancel did not wake waiter")
        );
        worker.join().unwrap();
        assert_eq!(lock(&bulkhead.state).queued, 0);
    }

    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn async_cancel_wakes_queued_waiter_and_frees_queue() {
        let bulkhead = Bulkhead::new(0, 1);
        let cancel = Cancellation::new();
        let mut future = std::pin::pin!(bulkhead.acquire_async(&cancel));
        let counter = Arc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&counter));
        let mut context = Context::from_waker(&waker);
        assert!(future.as_mut().poll(&mut context).is_pending());
        assert_eq!(lock(&bulkhead.state).queued, 1);
        cancel.cancel();
        assert!(counter.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(
            future.as_mut().poll(&mut context),
            Poll::Ready(None)
        ));
        assert_eq!(lock(&bulkhead.state).queued, 0);
    }
}
