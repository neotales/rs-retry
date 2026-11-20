//! Token-bucket and fixed-window rate limits. One value, no key map.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{clock::Clock, invalid::InvalidPolicy, sync_util::lock};

#[derive(Clone, Copy, Debug)]
enum Kind {
    TokenBucket {
        capacity: u32,
        refill: u32,
        every: Duration,
    },
    FixedWindow {
        limit: u32,
        window: Duration,
    },
}

struct LimitState {
    tokens: u32,
    updated: Instant,
    count: u32,
    window_start: Instant,
}

/// A token bucket or a fixed window.
///
/// No token means the call is rejected and the operation is not started. Time
/// comes from the injected clock, so tests advance a [`crate::ManualClock`]
/// instead of sleeping. One value is one limiter. The crate does not grow a map
/// of keys; the caller owns partitions.
#[derive(Clone)]
pub struct RateLimiter {
    kind: Kind,
    clock: Arc<dyn Clock>,
    state: Arc<Mutex<LimitState>>,
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output
            .debug_struct("RateLimiter")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl RateLimiter {
    /// A bucket that starts full and refills `refill` tokens every `every`.
    ///
    /// A capacity of zero rejects every call. The clock is [`crate::SystemClock`].
    ///
    /// # Errors
    ///
    /// Returns [`InvalidPolicy::ZeroInterval`] when `every` is zero.
    pub fn token_bucket(
        capacity: u32,
        refill: u32,
        every: Duration,
    ) -> Result<Self, InvalidPolicy> {
        Self::token_bucket_with_clock(capacity, refill, every, Arc::new(crate::clock::SystemClock))
    }

    /// Like [`RateLimiter::token_bucket`], with a caller-supplied clock.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidPolicy::ZeroInterval`] when `every` is zero.
    pub fn token_bucket_with_clock(
        capacity: u32,
        refill: u32,
        every: Duration,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, InvalidPolicy> {
        if every.is_zero() {
            return Err(InvalidPolicy::ZeroInterval);
        }
        Ok(Self::new(
            Kind::TokenBucket {
                capacity,
                refill,
                every,
            },
            clock,
        ))
    }

    /// A window that allows `limit` acquisitions, then rejects until `window` elapses.
    ///
    /// A limit of zero rejects every call. The clock is [`crate::SystemClock`].
    ///
    /// # Errors
    ///
    /// Returns [`InvalidPolicy::ZeroInterval`] when `window` is zero.
    pub fn fixed_window(limit: u32, window: Duration) -> Result<Self, InvalidPolicy> {
        Self::fixed_window_with_clock(limit, window, Arc::new(crate::clock::SystemClock))
    }

    /// Like [`RateLimiter::fixed_window`], with a caller-supplied clock.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidPolicy::ZeroInterval`] when `window` is zero.
    pub fn fixed_window_with_clock(
        limit: u32,
        window: Duration,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, InvalidPolicy> {
        if window.is_zero() {
            return Err(InvalidPolicy::ZeroInterval);
        }
        Ok(Self::new(Kind::FixedWindow { limit, window }, clock))
    }

    fn new(kind: Kind, clock: Arc<dyn Clock>) -> Self {
        let now = clock.now();
        let tokens = match kind {
            Kind::TokenBucket { capacity, .. } => capacity,
            Kind::FixedWindow { .. } => 0,
        };
        Self {
            kind,
            clock,
            state: Arc::new(Mutex::new(LimitState {
                tokens,
                updated: now,
                count: 0,
                window_start: now,
            })),
        }
    }

    /// Takes one token. Returns `false` when none is available.
    #[must_use]
    pub fn try_acquire(&self) -> bool {
        let now = self.clock.now();
        let mut state = lock(&self.state);
        match self.kind {
            Kind::TokenBucket {
                capacity,
                refill,
                every,
            } => take_token(&mut state, now, capacity, refill, every),
            Kind::FixedWindow { limit, window } => take_window(&mut state, now, limit, window),
        }
    }
}

fn take_token(
    state: &mut LimitState,
    now: Instant,
    capacity: u32,
    refill: u32,
    every: Duration,
) -> bool {
    refill_bucket(state, now, capacity, refill, every);
    if state.tokens == 0 {
        return false;
    }
    state.tokens -= 1;
    true
}

fn refill_bucket(
    state: &mut LimitState,
    now: Instant,
    capacity: u32,
    refill: u32,
    every: Duration,
) {
    let elapsed = now.saturating_duration_since(state.updated);
    let intervals = elapsed.as_nanos() / every.as_nanos();
    if intervals == 0 {
        return;
    }
    let steps = u32::try_from(intervals).unwrap_or(u32::MAX);
    let added = steps.saturating_mul(refill);
    state.tokens = state.tokens.saturating_add(added).min(capacity);
    let advanced = every.saturating_mul(steps);
    state.updated = state.updated.checked_add(advanced).unwrap_or(now);
}

fn take_window(state: &mut LimitState, now: Instant, limit: u32, window: Duration) -> bool {
    if now.saturating_duration_since(state.window_start) >= window {
        state.window_start = now;
        state.count = 0;
    }
    if state.count >= limit {
        return false;
    }
    state.count += 1;
    true
}
