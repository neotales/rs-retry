//! An ordered list of policies. The first policy is the outermost.

use std::{fmt, future::Future, sync::Arc, time::Duration};

use neotales_retry::RetryPolicy;

use crate::{
    breaker::CircuitBreaker,
    bulkhead::Bulkhead,
    cancel::{CancelFlag, Cancellation},
    clock::{Clock, SystemClock},
    invalid::InvalidPolicy,
    limit::RateLimiter,
    outcome::{Class, Classified, Event, Outcome},
};

/// How an execution may differ from the defaults.
///
/// The default is not idempotent, not cancelled, and uses [`SystemClock`]. Retry
/// and hedging reject a call that is not marked idempotent, without starting the
/// operation.
#[derive(Clone)]
pub struct Exec<'a> {
    idempotent: bool,
    cancellation: Option<&'a Cancellation>,
    clock: Arc<dyn Clock>,
}

impl Exec<'static> {
    /// A non-idempotent call on the system clock, with no caller cancellation.
    #[must_use]
    pub fn new() -> Self {
        Self {
            idempotent: false,
            cancellation: None,
            clock: Arc::new(SystemClock),
        }
    }
}

impl Default for Exec<'static> {
    fn default() -> Self {
        Self::new()
    }
}

impl Exec<'_> {
    /// Reports whether the caller has requested cancellation.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation_flag().is_cancelled()
    }

    /// Opts in to retry and hedging. Without this, those policies reject the call.
    #[must_use]
    pub fn idempotent(mut self) -> Self {
        self.idempotent = true;
        self
    }

    /// Replaces the clock. Breakers and rate limiters built with their own clock
    /// keep that clock; this one is what sync timeouts read.
    #[must_use]
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Attaches a caller cancellation flag.
    ///
    /// A flag that is already set produces [`Outcome::Cancelled`] and does not
    /// start the operation. Cancellation is not a handled failure.
    #[must_use]
    pub fn cancellation(self, cancellation: &Cancellation) -> Exec<'_> {
        Exec {
            idempotent: self.idempotent,
            cancellation: Some(cancellation),
            clock: self.clock,
        }
    }

    pub(crate) fn is_idempotent(&self) -> bool {
        self.idempotent
    }

    pub(crate) fn cancellation_flag(&self) -> &Cancellation {
        self.cancellation.unwrap_or(&NEVER)
    }

    pub(crate) fn clock_ref(&self) -> &dyn Clock {
        self.clock.as_ref()
    }
}

impl fmt::Debug for Exec<'_> {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("Exec")
            .field("idempotent", &self.idempotent)
            .field("cancelled", &self.cancellation_flag().is_cancelled())
            .finish_non_exhaustive()
    }
}

static NEVER: Cancellation = Cancellation::new();

/// One policy in a [`Pipeline`].
///
/// Build values with the constructors. The list order is the execution order:
/// the first policy is the outermost.
#[derive(Clone)]
pub enum Policy<T, E> {
    /// Retry handled failures using a [`RetryPolicy`].
    Retry(RetryPolicy),
    /// A deadline. See [`Pipeline`] for which composition this participates in.
    Timeout(Duration),
    /// A consecutive-failure breaker. One value, not a map of keys.
    Breaker(CircuitBreaker),
    /// A concurrency limit with a bounded queue.
    Bulkhead(Bulkhead),
    /// A token bucket or a fixed window.
    RateLimit(RateLimiter),
    /// Async hedging. The synchronous API rejects this variant.
    Hedge(Hedge),
    /// A factory consulted once for a handled failure.
    Fallback(Fallback<T, E>),
}

/// Maximum number of hedge attempts, including the first, and the delay between them.
///
/// Constructed by [`Policy::hedge`]. The maximum is caller-supplied.
#[derive(Clone, Copy, Debug)]
pub struct Hedge {
    pub(crate) max: u32,
    pub(crate) delay: Duration,
}

type Factory<T, E> = std::sync::Arc<std::sync::Mutex<Box<dyn FnMut() -> Result<T, E> + Send>>>;

/// Runs its factory once for a handled outcome. The factory receives no error;
/// its `Err` is not fed through this fallback again.
pub struct Fallback<T, E> {
    pub(crate) factory: Factory<T, E>,
}

impl<T, E> Clone for Fallback<T, E> {
    fn clone(&self) -> Self {
        Self {
            factory: std::sync::Arc::clone(&self.factory),
        }
    }
}

impl<T, E> fmt::Debug for Fallback<T, E> {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("Fallback")
    }
}

impl<T, E> fmt::Debug for Policy<T, E> {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retry(policy) => output.debug_tuple("Retry").field(policy).finish(),
            Self::Timeout(limit) => output.debug_tuple("Timeout").field(limit).finish(),
            Self::Breaker(breaker) => output.debug_tuple("Breaker").field(breaker).finish(),
            Self::Bulkhead(bulkhead) => output.debug_tuple("Bulkhead").field(bulkhead).finish(),
            Self::RateLimit(limiter) => output.debug_tuple("RateLimit").field(limiter).finish(),
            Self::Hedge(hedge) => output.debug_tuple("Hedge").field(hedge).finish(),
            Self::Fallback(fallback) => output.debug_tuple("Fallback").field(fallback).finish(),
        }
    }
}

impl<T, E> Policy<T, E> {
    /// Retries handled failures with `policy`.
    ///
    /// The call must be marked idempotent. Otherwise the policy rejects it and
    /// does not start the operation. Delays go through the sleeper passed to
    /// [`Pipeline::execute`] or [`Pipeline::execute_async`].
    #[must_use]
    pub fn retry(policy: RetryPolicy) -> Self {
        Self::Retry(policy)
    }

    /// A deadline of `limit`.
    ///
    /// On the async path the caller-supplied sleeper is the timer. When that
    /// sleeper finishes, the operation future is dropped, and that drop is the
    /// cancel. The outcome is [`Outcome::TimedOut`].
    ///
    /// On the sync path the policy does not kill a thread. It passes a
    /// [`CancelFlag`]. A blocking closure that ignores the flag runs to completion.
    /// An enclosing timeout prevents retry attempts after its deadline, but does
    /// not interrupt a caller-supplied retry sleeper.
    #[must_use]
    pub fn timeout(limit: Duration) -> Self {
        Self::Timeout(limit)
    }

    /// Admits calls through `breaker`.
    #[must_use]
    pub fn breaker(breaker: CircuitBreaker) -> Self {
        Self::Breaker(breaker)
    }

    /// Admits calls through `bulkhead`.
    #[must_use]
    pub fn bulkhead(bulkhead: Bulkhead) -> Self {
        Self::Bulkhead(bulkhead)
    }

    /// Admits calls through `limiter`.
    #[must_use]
    pub fn rate_limit(limiter: RateLimiter) -> Self {
        Self::RateLimit(limiter)
    }

    /// Races up to `max_hedges` caller-built futures, including the first.
    ///
    /// Hedging is async only. A synchronous execution rejects it. An async
    /// execution rejects a call that was not marked idempotent, with zero attempts.
    /// The crate does not spawn threads. Losers are dropped when the first success
    /// wins.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidPolicy::ZeroHedges`] when `max_hedges` is zero.
    pub fn hedge(max_hedges: u32, delay: Duration) -> Result<Self, InvalidPolicy> {
        if max_hedges == 0 {
            return Err(InvalidPolicy::ZeroHedges);
        }
        Ok(Self::Hedge(Hedge {
            max: max_hedges,
            delay,
        }))
    }

    /// On a handled failure, calls `factory` once.
    ///
    /// A successful factory result becomes success. A factory error stays a
    /// failure and is not passed through this fallback again. An observer panic
    /// does not replace that result.
    #[must_use]
    pub fn fallback(factory: impl FnMut() -> Result<T, E> + Send + 'static) -> Self {
        Self::Fallback(Fallback {
            factory: std::sync::Arc::new(std::sync::Mutex::new(Box::new(factory))),
        })
    }
}

/// Policies applied from outermost to innermost.
///
/// The list is the composition. There is no fluent builder.
///
/// `[timeout, retry]` is `timeout(retry(operation))`: one deadline across every
/// retry. The async timeout drops the retry future when its sleeper finishes, so
/// a later attempt does not start.
///
/// `[retry, timeout]` is `retry(timeout(operation))`: each attempt gets its own
/// deadline. A timed-out attempt can be retried until the attempt cap is spent.
#[derive(Clone, Debug)]
pub struct Pipeline<T, E> {
    policies: Vec<Policy<T, E>>,
}

impl<T, E> Pipeline<T, E> {
    /// Builds a pipeline. An empty list runs the operation once.
    #[must_use]
    pub fn new(policies: impl IntoIterator<Item = Policy<T, E>>) -> Self {
        Self {
            policies: policies.into_iter().collect(),
        }
    }

    /// Reports whether hedging or fallback could bypass a single delivery result.
    #[must_use]
    pub fn contains_hedge_or_fallback(&self) -> bool {
        self.policies
            .iter()
            .any(|policy| matches!(policy, Policy::Hedge(_) | Policy::Fallback(_)))
    }

    /// Runs `operation` through the policies on this thread.
    ///
    /// `sleep` receives each retry or hedge delay. It is not a timeout timer: sync
    /// timeout only sets a [`CancelFlag`]. Pass [`std::thread::sleep`] for a real
    /// blocking wait. A sleeper that returns at once does not sleep, which is what
    /// tests use.
    ///
    /// `random` is consulted only by a jittered retry. `observer` is called with
    /// [`Event`] values and cannot change the outcome, including by panicking.
    pub fn execute(
        &self,
        operation: impl FnMut(&CancelFlag<'_>) -> Result<T, E>,
        exec: &Exec<'_>,
        classifier: impl FnMut(Classified<'_, E>) -> Class,
        observer: impl FnMut(&Event),
        random: impl FnMut() -> u64,
        sleep: impl FnMut(Duration),
    ) -> Outcome<T, E> {
        crate::exec::run_sync(
            &self.policies,
            operation,
            exec,
            classifier,
            observer,
            random,
            sleep,
        )
    }

    /// Runs `operation` through the policies without blocking a thread.
    ///
    /// `operation` is called once per attempt and must return a future the caller
    /// built. Hedging races those futures and drops the losers. It does not spawn
    /// threads.
    ///
    /// `sleep` is the timer for retry delays, hedge delays, and async timeouts.
    /// When a timeout's sleeper finishes, the in-flight operation future is
    /// dropped. That drop is the cancel, and the outcome is [`Outcome::TimedOut`]
    /// unless the caller already cancelled.
    pub async fn execute_async<Fut, SleepFut>(
        &self,
        operation: impl FnMut(&CancelFlag<'_>) -> Fut,
        exec: &Exec<'_>,
        classifier: impl FnMut(Classified<'_, E>) -> Class,
        observer: impl FnMut(&Event),
        random: impl FnMut() -> u64,
        sleep: impl FnMut(Duration) -> SleepFut,
    ) -> Outcome<T, E>
    where
        Fut: Future<Output = Result<T, E>>,
        SleepFut: Future<Output = ()>,
    {
        crate::exec::run_async(
            &self.policies,
            operation,
            exec,
            classifier,
            observer,
            random,
            sleep,
        )
        .await
    }
}

#[cfg(test)]
mod inspection_tests {
    use super::{Pipeline, Policy};
    use std::time::Duration;

    #[test]
    fn identifies_hedges_and_fallbacks_without_exposing_factories() {
        let empty = Pipeline::<(), ()>::new([]);
        assert!(!empty.contains_hedge_or_fallback());
        let timeout = Pipeline::<(), ()>::new([Policy::timeout(Duration::from_secs(1))]);
        assert!(!timeout.contains_hedge_or_fallback());
        let hedge = Pipeline::<(), ()>::new([Policy::hedge(2, Duration::ZERO).unwrap()]);
        assert!(hedge.contains_hedge_or_fallback());
        let fallback = Pipeline::<(), ()>::new([Policy::fallback(|| Ok(()))]);
        assert!(fallback.contains_hedge_or_fallback());
    }
}
