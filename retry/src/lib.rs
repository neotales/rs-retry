//! An immutable retry policy with fixed, exponential, and jittered delays.
#![doc = include_str!("../README.md")]

use std::{
    error::Error,
    fmt,
    future::Future,
    panic::{self, AssertUnwindSafe},
    thread,
    time::{Duration, Instant},
};

/// How a retry policy spaces its waits.
///
/// The shape is chosen when the policy is built and never changes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Delay {
    /// Wait the same duration before every retry.
    Fixed(Duration),
    /// Start at `base` and double the wait after every retry, up to `max`.
    Exponential {
        /// The wait before the first retry.
        base: Duration,
        /// The longest wait the policy will ask for.
        max: Duration,
    },
    /// Decorrelated jitter: `min(max, random_between(base, previous * 3))`.
    ///
    /// The upper bound grows with the previous wait, which spreads retries from
    /// concurrent callers over time instead of synchronising them into a herd.
    /// The first wait is drawn from `base..=base * 3`.
    JitteredExponential {
        /// The floor of the first wait and of every later wait.
        base: Duration,
        /// The longest wait the policy will ask for.
        max: Duration,
    },
}

impl Delay {
    /// Computes the next wait from the previous one and the caller's random source.
    fn wait(self, previous: Option<Duration>, random: &mut impl FnMut() -> u64) -> Duration {
        match self {
            Self::Fixed(wait) => wait,
            Self::Exponential { base, max } => {
                let wait = previous.map_or(base, |last| last.saturating_mul(2));
                wait.min(max)
            }
            Self::JitteredExponential { base, max } => {
                let ceiling =
                    previous.map_or(base.saturating_mul(3), |last| last.saturating_mul(3));
                between(base, ceiling, random).min(max)
            }
        }
    }
}

/// Picks a duration in `low..=high` using `random`.
///
/// `low` is returned when the range is empty or inverted, so a caller-supplied
/// random source cannot make a wait shorter than the floor.
fn between(low: Duration, high: Duration, random: &mut impl FnMut() -> u64) -> Duration {
    let floor = low.as_nanos();
    let ceiling = high.as_nanos();
    if ceiling <= floor {
        return low;
    }
    let offset = u128::from(random()) % (ceiling - floor + 1);
    from_nanos(floor + offset)
}

/// Builds a duration from nanoseconds, clamping a count that will not fit.
fn from_nanos(nanos: u128) -> Duration {
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// Calls an observer, discarding a panic so it cannot affect the retry.
fn observe(on_retry: &mut impl FnMut(u32, Duration), attempt: u32, wait: Duration) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| on_retry(attempt, wait)));
}

/// An immutable retry policy: an attempt cap, a delay shape, and an optional
/// elapsed budget.
///
/// The policy holds no state, no clock and no random source, so it is `Copy` and
/// cheap to clone. [`RetryPolicy::run`] and [`RetryPolicy::run_async`] take the
/// operation, a predicate, a random source and a sleeper per call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    max_attempts: u32,
    delay: Delay,
    budget: Option<Duration>,
}

// The same decision state drives synchronous and asynchronous executions.
struct Cursor {
    policy: RetryPolicy,
    started: Instant,
    attempts: u32,
    previous: Option<Duration>,
}

impl Cursor {
    fn new(policy: RetryPolicy) -> Self {
        Self {
            policy,
            started: Instant::now(),
            attempts: 0,
            previous: None,
        }
    }

    fn spent(&self) -> bool {
        self.policy.budget_spent(self.started)
    }

    fn start_attempt(&mut self) {
        self.attempts += 1;
    }

    fn wait<E>(
        &mut self,
        error: &E,
        retry_if: &mut impl FnMut(&E) -> bool,
        random: &mut impl FnMut() -> u64,
    ) -> Option<Duration> {
        if self.attempts >= self.policy.max_attempts || !retry_if(error) || self.spent() {
            return None;
        }
        let wait = self.policy.delay.wait(self.previous, random);
        self.previous = Some(wait);
        Some(wait)
    }
}

/// A policy was built with a value that cannot be used.
///
/// The only case is an attempt cap of `0`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidPolicy;

impl fmt::Display for InvalidPolicy {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("retry policy needs at least one attempt")
    }
}

impl Error for InvalidPolicy {}

impl RetryPolicy {
    /// Builds a policy that makes at most `max_attempts` attempts, counting the
    /// first try.
    ///
    /// `1` means the operation is tried once and never retried.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidPolicy`] when `max_attempts` is `0`. That is a
    /// configuration error, not a panic.
    pub fn new(max_attempts: u32, delay: Delay) -> Result<Self, InvalidPolicy> {
        if max_attempts == 0 {
            return Err(InvalidPolicy);
        }
        Ok(Self {
            max_attempts,
            delay,
            budget: None,
        })
    }

    /// Sets an elapsed-time budget, measured from the start of a run.
    ///
    /// The budget is wall-clock time read from [`std::time::Instant`], so it does
    /// not depend on the sleeper. A sleeper that returns without waiting still
    /// spends real time, and one that waits longer than asked spends that longer
    /// time too. The first attempt always starts, even when the budget is zero;
    /// the budget only refuses later attempts.
    ///
    /// Once the budget is gone the policy returns the last error instead of
    /// starting another attempt or scheduling another wait. It never cancels a
    /// wait or an attempt already in flight.
    #[must_use]
    pub const fn with_budget(mut self, budget: Duration) -> Self {
        self.budget = Some(budget);
        self
    }

    /// Runs `operation`, retrying while `retry_if` accepts the error.
    ///
    /// The operation is called at most `max_attempts` times. When `retry_if`
    /// returns `false`, or the attempt cap or elapsed budget is spent, the
    /// operation's last error is returned unchanged rather than wrapped.
    ///
    /// `random` is consulted only by [`Delay::JitteredExponential`], once per
    /// wait. The returned `u64` is treated as uniform over `0..=u64::MAX` and
    /// reduced by remainder into the chosen range, so a caller cannot shape the
    /// distribution beyond choosing the source. A fixed or exponential policy
    /// ignores the value entirely and never calls the closure, but the argument is
    /// still required so the signature does not depend on the delay shape.
    ///
    /// Waits go through [`std::thread::sleep`]. Use
    /// [`RetryPolicy::run_with_sleeper`] to record or replace them.
    ///
    /// # Errors
    ///
    /// Returns the operation's own error: the first refused error, or the last
    /// error once the attempt cap or the budget is spent.
    pub fn run<T, E>(
        &self,
        operation: impl FnMut() -> Result<T, E>,
        retry_if: impl FnMut(&E) -> bool,
        random: impl FnMut() -> u64,
    ) -> Result<T, E> {
        self.run_with_sleeper_and_observer(operation, retry_if, random, thread::sleep, |_, _| {})
    }

    /// Like [`RetryPolicy::run`], but calls `on_retry` before each wait.
    ///
    /// The observer receives the number of the attempt that just failed, where
    /// the first try is `1`, and the wait about to start. It runs after the
    /// error has been accepted for another attempt and before the wait begins,
    /// so it is not called for a successful attempt, a refused error, or the
    /// final attempt.
    ///
    /// A panic from `on_retry` is caught and discarded; it does not change the
    /// result and does not stop the retry.
    ///
    /// # Errors
    ///
    /// Returns the operation's own error, exactly as [`RetryPolicy::run`] does.
    pub fn run_with_observer<T, E>(
        &self,
        operation: impl FnMut() -> Result<T, E>,
        retry_if: impl FnMut(&E) -> bool,
        random: impl FnMut() -> u64,
        on_retry: impl FnMut(u32, Duration),
    ) -> Result<T, E> {
        self.run_with_sleeper_and_observer(operation, retry_if, random, thread::sleep, on_retry)
    }

    /// Like [`RetryPolicy::run`], but with a caller-supplied sleeper.
    ///
    /// The sleeper receives each wait the policy has chosen. It is not called
    /// for a successful attempt, a refused error, or the final attempt.
    ///
    /// # Errors
    ///
    /// Returns the operation's own error, exactly as [`RetryPolicy::run`] does.
    pub fn run_with_sleeper<T, E>(
        &self,
        operation: impl FnMut() -> Result<T, E>,
        retry_if: impl FnMut(&E) -> bool,
        random: impl FnMut() -> u64,
        sleep: impl FnMut(Duration),
    ) -> Result<T, E> {
        self.run_with_sleeper_and_observer(operation, retry_if, random, sleep, |_, _| {})
    }

    /// Like [`RetryPolicy::run_with_sleeper`], but calls `on_retry` before each
    /// wait.
    ///
    /// The observer receives the number of the attempt that just failed, where
    /// the first try is `1`, and the wait about to start. It is called before
    /// the sleeper, and only when another attempt will follow.
    ///
    /// A panic from `on_retry` is caught and discarded; it does not change the
    /// result, skip the wait, or stop the retry.
    ///
    /// # Errors
    ///
    /// Returns the operation's own error, exactly as [`RetryPolicy::run`] does.
    pub fn run_with_sleeper_and_observer<T, E>(
        &self,
        mut operation: impl FnMut() -> Result<T, E>,
        mut retry_if: impl FnMut(&E) -> bool,
        mut random: impl FnMut() -> u64,
        mut sleep: impl FnMut(Duration),
        mut on_retry: impl FnMut(u32, Duration),
    ) -> Result<T, E> {
        let mut cursor = Cursor::new(*self);
        let mut waiting_on = None;
        loop {
            // A wait that crossed the deadline must not be followed by another
            // attempt, even though the wait itself was allowed to finish.
            if let Some(error) = waiting_on.take()
                && cursor.spent()
            {
                return Err(error);
            }
            cursor.start_attempt();
            match operation() {
                Ok(value) => return Ok(value),
                Err(error) => {
                    let Some(wait) = cursor.wait(&error, &mut retry_if, &mut random) else {
                        return Err(error);
                    };
                    waiting_on = Some(error);
                    observe(&mut on_retry, cursor.attempts, wait);
                    sleep(wait);
                }
            }
        }
    }

    /// Runs an asynchronous `operation`, retrying like [`RetryPolicy::run`].
    ///
    /// The crate has no runtime: `operation` and `sleep` return whatever futures
    /// the caller's executor drives. `sleep` is called with each chosen wait and
    /// the returned future is awaited. Nothing is spawned, so dropping the future
    /// returned by this method is how a caller stops waiting.
    ///
    /// # Errors
    ///
    /// Returns the operation's own error, exactly as [`RetryPolicy::run`] does.
    pub async fn run_async<T, E, Operation, Attempt, Retry, Random, Sleep, Waiting>(
        &self,
        operation: Operation,
        retry_if: Retry,
        random: Random,
        sleep: Sleep,
    ) -> Result<T, E>
    where
        Operation: FnMut() -> Attempt,
        Attempt: Future<Output = Result<T, E>>,
        Retry: FnMut(&E) -> bool,
        Random: FnMut() -> u64,
        Sleep: FnMut(Duration) -> Waiting,
        Waiting: Future<Output = ()>,
    {
        self.run_async_with_observer(operation, retry_if, random, sleep, |_, _| {})
            .await
    }

    /// Like [`RetryPolicy::run_async`], but calls `on_retry` before each wait.
    ///
    /// The observer receives the number of the attempt that just failed, where
    /// the first try is `1`, and the wait about to start. It is called before
    /// the sleeper future is created, and only when another attempt will follow.
    ///
    /// A panic from `on_retry` is caught and discarded; it does not change the
    /// result, skip the wait, or stop the retry.
    ///
    /// # Errors
    ///
    /// Returns the operation's own error, exactly as [`RetryPolicy::run`] does.
    pub async fn run_async_with_observer<
        T,
        E,
        Operation,
        Attempt,
        Retry,
        Random,
        Sleep,
        Waiting,
        Observer,
    >(
        &self,
        operation: Operation,
        retry_if: Retry,
        random: Random,
        sleep: Sleep,
        on_retry: Observer,
    ) -> Result<T, E>
    where
        Operation: FnMut() -> Attempt,
        Attempt: Future<Output = Result<T, E>>,
        Retry: FnMut(&E) -> bool,
        Random: FnMut() -> u64,
        Sleep: FnMut(Duration) -> Waiting,
        Waiting: Future<Output = ()>,
        Observer: FnMut(u32, Duration),
    {
        self.run_async_with_checkpoint(operation, retry_if, random, sleep, on_retry, || {
            std::future::ready(())
        })
        .await
    }

    /// Like [`RetryPolicy::run_async_with_observer`], but awaits `checkpoint`
    /// after every retry wait and before considering the next attempt.
    ///
    /// A checkpoint can yield to an enclosing cancellation or deadline race.
    /// It is not called before the first attempt, and a pending checkpoint is
    /// dropped when the returned future is dropped.
    ///
    /// # Errors
    ///
    /// Returns the operation's error with the same semantics as
    /// [`RetryPolicy::run_async`].
    pub async fn run_async_with_checkpoint<
        T,
        E,
        Operation,
        Attempt,
        Retry,
        Random,
        Sleep,
        Waiting,
        Observer,
        Checkpoint,
        CheckpointFuture,
    >(
        &self,
        mut operation: Operation,
        mut retry_if: Retry,
        mut random: Random,
        mut sleep: Sleep,
        mut on_retry: Observer,
        mut checkpoint: Checkpoint,
    ) -> Result<T, E>
    where
        Operation: FnMut() -> Attempt,
        Attempt: Future<Output = Result<T, E>>,
        Retry: FnMut(&E) -> bool,
        Random: FnMut() -> u64,
        Sleep: FnMut(Duration) -> Waiting,
        Waiting: Future<Output = ()>,
        Observer: FnMut(u32, Duration),
        Checkpoint: FnMut() -> CheckpointFuture,
        CheckpointFuture: Future<Output = ()>,
    {
        let mut cursor = Cursor::new(*self);
        let mut waiting_on = None;
        loop {
            // A wait that crossed the deadline must not be followed by another
            // attempt, even though the wait itself was allowed to finish.
            if let Some(error) = waiting_on.take()
                && cursor.spent()
            {
                return Err(error);
            }
            cursor.start_attempt();
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error) => {
                    let Some(wait) = cursor.wait(&error, &mut retry_if, &mut random) else {
                        return Err(error);
                    };
                    waiting_on = Some(error);
                    observe(&mut on_retry, cursor.attempts, wait);
                    sleep(wait).await;
                    checkpoint().await;
                }
            }
        }
    }

    /// Reports whether the elapsed budget has run out.
    fn budget_spent(&self, started: Instant) -> bool {
        self.budget
            .is_some_and(|budget| started.elapsed() >= budget)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use super::{Delay, InvalidPolicy, RetryPolicy};

    const ONE_MS: Duration = Duration::from_millis(1);

    /// An error that proves the policy returns the exact value it was given.
    #[derive(Debug, Eq, PartialEq)]
    struct AttemptError(u32);

    fn policy(attempts: u32, delay: Delay) -> RetryPolicy {
        RetryPolicy::new(attempts, delay).expect("a policy with attempts")
    }

    #[test]
    fn zero_attempts_is_rejected_by_the_constructor() {
        assert_eq!(
            RetryPolicy::new(0, Delay::Fixed(ONE_MS)),
            Err(InvalidPolicy)
        );
        assert!(RetryPolicy::new(1, Delay::Fixed(ONE_MS)).is_ok());
    }

    #[test]
    fn a_first_try_success_does_not_sleep() {
        let attempts = Cell::new(0_u32);
        let waits = RefCell::new(Vec::new());
        let result: Result<&str, &str> = policy(3, Delay::Fixed(ONE_MS)).run_with_sleeper(
            || {
                attempts.set(attempts.get() + 1);
                Ok("done")
            },
            |_| true,
            || 0,
            |wait| waits.borrow_mut().push(wait),
        );
        assert_eq!(result, Ok("done"));
        assert_eq!(attempts.get(), 1);
        assert!(waits.borrow().is_empty());
    }

    #[test]
    fn a_retry_then_success_sleeps_the_expected_delays() {
        let attempts = Cell::new(0_u32);
        let waits = RefCell::new(Vec::new());
        let result: Result<&str, AttemptError> = policy(3, Delay::Fixed(Duration::from_millis(10)))
            .run_with_sleeper(
                || {
                    attempts.set(attempts.get() + 1);
                    let attempt = attempts.get();
                    if attempt < 3 {
                        Err(AttemptError(attempt))
                    } else {
                        Ok("done")
                    }
                },
                |_| true,
                || 0,
                |wait| waits.borrow_mut().push(wait),
            );
        assert_eq!(result, Ok("done"));
        assert_eq!(attempts.get(), 3);
        assert_eq!(*waits.borrow(), [Duration::from_millis(10); 2]);
    }

    #[test]
    fn the_observer_sees_each_retry_before_the_sleeper() {
        let attempts = Cell::new(0_u32);
        let sleeps = Cell::new(0_u32);
        let observations = RefCell::new(Vec::new());
        let result: Result<&str, AttemptError> = policy(
            4,
            Delay::Exponential {
                base: Duration::from_millis(10),
                max: Duration::from_millis(100),
            },
        )
        .run_with_sleeper_and_observer(
            || {
                attempts.set(attempts.get() + 1);
                let attempt = attempts.get();
                if attempt < 3 {
                    Err(AttemptError(attempt))
                } else {
                    Ok("done")
                }
            },
            |_| true,
            || 0,
            |_| sleeps.set(sleeps.get() + 1),
            // Record how many waits had started, so the assertions show the
            // observer ran before the sleeper rather than only at some point.
            |attempt, wait| {
                observations
                    .borrow_mut()
                    .push((attempt, wait, sleeps.get()));
            },
        );
        assert_eq!(result, Ok("done"));
        assert_eq!(attempts.get(), 3);
        assert_eq!(
            *observations.borrow(),
            [
                (1, Duration::from_millis(10), 0),
                (2, Duration::from_millis(20), 1),
            ]
        );
        assert_eq!(sleeps.get(), 2);
    }

    #[test]
    fn a_panicking_observer_does_not_change_the_result() {
        let attempts = Cell::new(0_u32);
        let waits = RefCell::new(Vec::new());
        let result: Result<&str, AttemptError> = policy(3, Delay::Fixed(ONE_MS))
            .run_with_sleeper_and_observer(
                || {
                    attempts.set(attempts.get() + 1);
                    let attempt = attempts.get();
                    if attempt < 2 {
                        Err(AttemptError(attempt))
                    } else {
                        Ok("done")
                    }
                },
                |_| true,
                || 0,
                |wait| waits.borrow_mut().push(wait),
                |_, _| panic!("an observer must not change the result"),
            );
        assert_eq!(result, Ok("done"));
        assert_eq!(attempts.get(), 2);
        assert_eq!(*waits.borrow(), [ONE_MS]);
    }

    #[test]
    fn a_refused_error_does_not_sleep() {
        let attempts = Cell::new(0_u32);
        let waits = RefCell::new(Vec::new());
        let result: Result<(), &str> = policy(5, Delay::Fixed(ONE_MS)).run_with_sleeper(
            || {
                attempts.set(attempts.get() + 1);
                Err("refused")
            },
            |error| *error != "refused",
            || 0,
            |wait| waits.borrow_mut().push(wait),
        );
        assert_eq!(result, Err("refused"));
        assert_eq!(attempts.get(), 1);
        assert!(waits.borrow().is_empty());
    }

    #[test]
    fn the_attempt_cap_returns_the_last_error_unchanged() {
        let attempts = Cell::new(0_u32);
        let waits = RefCell::new(Vec::new());
        let result = policy(3, Delay::Fixed(ONE_MS)).run_with_sleeper(
            || {
                attempts.set(attempts.get() + 1);
                Err::<(), _>(AttemptError(attempts.get()))
            },
            |_| true,
            || 0,
            |wait| waits.borrow_mut().push(wait),
        );
        assert_eq!(result, Err(AttemptError(3)));
        assert_eq!(attempts.get(), 3);
        assert_eq!(waits.borrow().len(), 2);
    }

    #[test]
    fn an_exhausted_budget_refuses_another_attempt() {
        let attempts = Cell::new(0_u32);
        let waits = RefCell::new(Vec::new());
        let result = policy(5, Delay::Fixed(ONE_MS))
            .with_budget(Duration::ZERO)
            .run_with_sleeper(
                || {
                    attempts.set(attempts.get() + 1);
                    Err::<(), _>(AttemptError(attempts.get()))
                },
                |_| true,
                || 0,
                |wait| waits.borrow_mut().push(wait),
            );
        assert_eq!(result, Err(AttemptError(1)));
        assert_eq!(attempts.get(), 1);
        assert!(waits.borrow().is_empty());
    }

    #[test]
    fn exponential_delays_double_and_cap() {
        let attempts = Cell::new(0_u32);
        let waits = RefCell::new(Vec::new());
        let result = policy(
            6,
            Delay::Exponential {
                base: Duration::from_millis(10),
                max: Duration::from_millis(50),
            },
        )
        .run_with_sleeper(
            || {
                attempts.set(attempts.get() + 1);
                Err::<(), _>(AttemptError(attempts.get()))
            },
            |_| true,
            || 0,
            |wait| waits.borrow_mut().push(wait),
        );
        assert!(result.is_err());
        assert_eq!(
            *waits.borrow(),
            [
                Duration::from_millis(10),
                Duration::from_millis(20),
                Duration::from_millis(40),
                Duration::from_millis(50),
                Duration::from_millis(50),
            ]
        );
    }

    #[test]
    fn jitter_waits_use_the_supplied_function_within_the_cap() {
        // A zero draw picks the lower bound, so each wait equals `base`.
        let zero_calls = Cell::new(0_u32);
        let zero_waits = RefCell::new(Vec::new());
        let result = policy(
            3,
            Delay::JitteredExponential {
                base: Duration::from_millis(10),
                max: Duration::from_millis(100),
            },
        )
        .run_with_sleeper(
            || Err::<(), _>(AttemptError(1)),
            |_| true,
            || {
                zero_calls.set(zero_calls.get() + 1);
                0
            },
            |wait| zero_waits.borrow_mut().push(wait),
        );
        assert!(result.is_err());
        assert_eq!(zero_calls.get(), 2, "one draw per wait");
        assert_eq!(*zero_waits.borrow(), [Duration::from_millis(10); 2]);

        // Any draw is clamped to the cap.
        let cap = Duration::from_secs(1);
        let capped_waits = RefCell::new(Vec::new());
        let _ = policy(
            3,
            Delay::JitteredExponential {
                base: cap,
                max: cap,
            },
        )
        .run_with_sleeper(
            || Err::<(), _>(AttemptError(1)),
            |_| true,
            || u64::MAX,
            |wait| capped_waits.borrow_mut().push(wait),
        );
        assert_eq!(*capped_waits.borrow(), [cap; 2]);
    }

    #[test]
    fn async_retries_once_with_an_immediate_sleeper() {
        let attempts = Cell::new(0_u32);
        let waits = RefCell::new(Vec::new());
        let result = block_on(policy(3, Delay::Fixed(ONE_MS)).run_async(
            || {
                attempts.set(attempts.get() + 1);
                let attempt = attempts.get();
                let outcome: Result<&str, AttemptError> = if attempt == 1 {
                    Err(AttemptError(attempt))
                } else {
                    Ok("done")
                };
                std::future::ready(outcome)
            },
            |_| true,
            || 0,
            |wait| {
                waits.borrow_mut().push(wait);
                std::future::ready(())
            },
        ));
        assert_eq!(result, Ok("done"));
        assert_eq!(attempts.get(), 2);
        assert_eq!(*waits.borrow(), [ONE_MS]);
    }

    /// Drives a future that is ready on the first poll.
    ///
    /// The test futures never pend, so this needs no runtime and no
    /// dev-dependency. A pending future would spin here rather than sleep, which
    /// is why the helper is not used for waiting futures.
    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        let mut context = Context::from_waker(Waker::noop());
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::hint::spin_loop(),
            }
        }
    }
}
