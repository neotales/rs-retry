//! One circuit breaker. The caller owns any map of partitions.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    clock::Clock,
    invalid::InvalidPolicy,
    outcome::{Outcome, Reason},
    sync_util::lock,
};

/// Closed, open, or half-open.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BreakerState {
    /// Calls are admitted. Consecutive handled failures are counted.
    Closed,
    /// Calls are rejected and the operation is not invoked.
    Open,
    /// One probe is in flight or about to be admitted.
    HalfOpen,
}

#[derive(Debug)]
struct Inner {
    phase: Phase,
    consecutive: u32,
    opened_at: Option<Instant>,
    probe: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Closed,
    Open,
    HalfOpen,
}

impl Inner {
    const fn closed() -> Self {
        Self {
            phase: Phase::Closed,
            consecutive: 0,
            opened_at: None,
            probe: false,
        }
    }
}

/// A consecutive-failure circuit breaker.
///
/// Shared state is [`std::sync`]. One value is one breaker. The crate does not
/// grow a map of keys; the caller owns partitions and keeps that map bounded.
///
/// An open breaker returns [`Outcome::Rejected`] and does not call the operation.
/// After the break duration, one half-open probe is admitted. A successful probe
/// closes the breaker. A handled failure on that probe opens it again. Caller
/// cancellation is not a failure and does not open the breaker.
#[derive(Clone)]
pub struct CircuitBreaker {
    inner: Arc<Mutex<Inner>>,
    clock: Arc<dyn Clock>,
    threshold: u32,
    break_for: Duration,
}

impl std::fmt::Debug for CircuitBreaker {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output
            .debug_struct("CircuitBreaker")
            .field("state", &self.state())
            .field("threshold", &self.threshold)
            .field("break_for", &self.break_for)
            .finish_non_exhaustive()
    }
}

impl CircuitBreaker {
    /// Builds a breaker that opens after `threshold` consecutive handled failures
    /// and stays open for `break_for`.
    ///
    /// The clock is [`crate::SystemClock`].
    ///
    /// # Errors
    ///
    /// Returns [`InvalidPolicy::ZeroThreshold`] when `threshold` is zero.
    pub fn new(threshold: u32, break_for: Duration) -> Result<Self, InvalidPolicy> {
        Self::with_clock(threshold, break_for, Arc::new(crate::clock::SystemClock))
    }

    /// Like [`CircuitBreaker::new`], with a caller-supplied clock.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidPolicy::ZeroThreshold`] when `threshold` is zero.
    pub fn with_clock(
        threshold: u32,
        break_for: Duration,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, InvalidPolicy> {
        if threshold == 0 {
            return Err(InvalidPolicy::ZeroThreshold);
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner::closed())),
            clock,
            threshold,
            break_for,
        })
    }

    /// Returns the current phase. A probe in flight is [`BreakerState::HalfOpen`].
    #[must_use]
    pub fn state(&self) -> BreakerState {
        match lock(&self.inner).phase {
            Phase::Closed => BreakerState::Closed,
            Phase::Open => BreakerState::Open,
            Phase::HalfOpen => BreakerState::HalfOpen,
        }
    }

    pub(crate) fn admit(&self) -> Admit {
        let now = self.clock.now();
        let mut inner = lock(&self.inner);
        match inner.phase {
            Phase::Closed => Admit::Allowed(self.guard(false)),
            Phase::Open => {
                let elapsed = inner
                    .opened_at
                    .is_some_and(|opened| now.saturating_duration_since(opened) >= self.break_for);
                if elapsed && !inner.probe {
                    inner.phase = Phase::HalfOpen;
                    inner.probe = true;
                    Admit::Probe(self.guard(true))
                } else {
                    Admit::Rejected
                }
            }
            Phase::HalfOpen => {
                if inner.probe {
                    Admit::Rejected
                } else {
                    inner.probe = true;
                    Admit::Probe(self.guard(true))
                }
            }
        }
    }

    pub(crate) fn settle<T, E>(&self, guard: &mut Guard, outcome: &Outcome<T, E>, handled: bool) {
        guard.settled = true;
        let mut inner = lock(&self.inner);
        if guard.probe {
            inner.probe = false;
        }
        if matches!(outcome, Outcome::Cancelled { .. }) {
            return;
        }
        if matches!(outcome, Outcome::Success { .. }) {
            inner.consecutive = 0;
            inner.phase = Phase::Closed;
            inner.opened_at = None;
            return;
        }
        if !handled {
            return;
        }
        inner.consecutive = inner.consecutive.saturating_add(1);
        if guard.probe || inner.consecutive >= self.threshold {
            inner.phase = Phase::Open;
            inner.opened_at = Some(self.clock.now());
            inner.consecutive = 0;
        }
    }

    fn guard(&self, probe: bool) -> Guard {
        Guard {
            inner: Arc::clone(&self.inner),
            probe,
            settled: false,
        }
    }
}

pub(crate) enum Admit {
    Allowed(Guard),
    Probe(Guard),
    Rejected,
}

pub(crate) struct Guard {
    inner: Arc<Mutex<Inner>>,
    probe: bool,
    settled: bool,
}

impl Drop for Guard {
    fn drop(&mut self) {
        if self.probe && !self.settled {
            lock(&self.inner).probe = false;
        }
    }
}

pub(crate) fn transition_reason(before: BreakerState, after: BreakerState) -> Option<Reason> {
    match (before, after) {
        (BreakerState::Open, BreakerState::HalfOpen) => Some(Reason::BreakerHalfOpen),
        (_, BreakerState::Open) if before != BreakerState::Open => Some(Reason::BreakerOpen),
        (_, BreakerState::Closed) if before != BreakerState::Closed => Some(Reason::BreakerClosed),
        _ => None,
    }
}
