//! Clocks policies read so tests can advance time without sleeping.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// A clock a breaker or a rate limiter reads.
///
/// Production code uses [`SystemClock`]. Tests use [`ManualClock`] and advance
/// it. The crate does not sleep.
pub trait Clock: Send + Sync {
    /// Returns the current instant.
    fn now(&self) -> Instant;
}

/// The process clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// A clock whose time moves only when [`ManualClock::advance`] is called.
///
/// Share it with [`ManualClock::handle`] so a policy and a test observe the same
/// instant. Advancing it does not sleep.
#[derive(Debug)]
pub struct ManualClock {
    origin: Instant,
    offset: AtomicU64,
}

impl ManualClock {
    /// Starts at [`Instant::now`] with a zero offset.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
            offset: AtomicU64::new(0),
        }
    }

    /// Moves the clock forward. A backward move is not representable.
    pub fn advance(&self, by: Duration) {
        let add = u64::try_from(by.as_nanos()).unwrap_or(u64::MAX);
        let mut current = self.offset.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_add(add);
            match self.offset.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(seen) => current = seen,
            }
        }
    }

    /// Clones the shared clock as the trait object policies store.
    #[must_use]
    pub fn handle(clock: &Arc<Self>) -> Arc<dyn Clock> {
        Arc::clone(clock) as Arc<dyn Clock>
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        let offset = Duration::from_nanos(self.offset.load(Ordering::Relaxed));
        self.origin.checked_add(offset).unwrap_or(self.origin)
    }
}
