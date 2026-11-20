//! Conformance rows from the language-neutral resilience vectors.
//!
//! Nothing here sleeps or needs Docker. Time moves only when a [`ManualClock`]
//! is advanced, and async timers are futures the test completes itself.

use std::{
    future::Future,
    pin::{Pin, pin},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use neotales_retry::Delay;

use crate::{
    BreakerState, Cancellation, CircuitBreaker, Class, Classified, Event, Exec, ManualClock,
    Outcome, Pipeline, Policy, RateLimiter, Reason, RetryPolicy,
};

// The classifier contract takes the value. These helpers only inspect it.
#[allow(clippy::needless_pass_by_value)]
fn handled<E>(item: Classified<'_, E>) -> Class {
    match item {
        Classified::Rejected(_) => Class::Unhandled,
        Classified::Failure(_) | Classified::TimedOut => Class::Handled,
    }
}

fn unhandled<E>(_: Classified<'_, E>) -> Class {
    Class::Unhandled
}

fn drive<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    for _ in 0..32 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
    }
    panic!("future still pending after a bounded number of polls");
}

fn sync_run<T, E>(
    pipeline: &Pipeline<T, E>,
    operation: impl FnMut(&crate::CancelFlag<'_>) -> Result<T, E>,
    exec: &Exec<'_>,
    classifier: impl FnMut(Classified<'_, E>) -> Class,
) -> Outcome<T, E> {
    pipeline.execute(operation, exec, classifier, |_| {}, || 0, |_| {})
}

fn calls() -> Arc<AtomicU32> {
    Arc::new(AtomicU32::new(0))
}

#[test]
fn local_async_retry_accepts_non_send_callbacks_and_futures() {
    let count = std::rc::Rc::new(std::cell::Cell::new(0));
    let operation_count = count.clone();
    let sleeper_count = count.clone();
    let policy = RetryPolicy::new(2, Delay::Fixed(Duration::ZERO)).unwrap();
    let pipeline = Pipeline::<u32, ()>::new([Policy::retry(policy)]);
    let outcome = drive(pipeline.execute_async(
        move |_| {
            let count = operation_count.clone();
            async move {
                let attempt = count.get() + 1;
                count.set(attempt);
                if attempt == 1 { Err(()) } else { Ok(attempt) }
            }
        },
        &Exec::new().idempotent(),
        handled,
        |_| {},
        || 0,
        move |_| {
            let count = sleeper_count.clone();
            async move { assert_eq!(count.get(), 1) }
        },
    ));
    assert_eq!(
        outcome,
        Outcome::Success {
            value: 2,
            attempt: 2
        }
    );
}

#[test]
fn plain_success() {
    let pipeline = Pipeline::<&str, &str>::new([]);
    let outcome = sync_run(&pipeline, |_| Ok("done"), &Exec::new(), handled);
    assert_eq!(
        outcome,
        Outcome::Success {
            value: "done",
            attempt: 1
        }
    );
}

#[test]
fn result_failure_is_handled_without_panicking() {
    let pipeline = Pipeline::<&str, &str>::new([]);
    let outcome = sync_run(&pipeline, |_| Err("nope"), &Exec::new(), handled);
    assert_eq!(
        outcome,
        Outcome::Failure {
            error: "nope",
            class: Class::Handled,
            attempt: 1
        }
    );
}

#[test]
fn unhandled_error_passes_through() {
    let pipeline = Pipeline::<&str, &str>::new([]);
    let outcome = sync_run(&pipeline, |_| Err("nope"), &Exec::new(), unhandled);
    assert_eq!(
        outcome,
        Outcome::Failure {
            error: "nope",
            class: Class::Unhandled,
            attempt: 1
        }
    );
}

#[test]
fn caller_cancelled_before_execution() {
    let cancellation = Cancellation::new();
    cancellation.cancel();
    let started = calls();
    let started_op = Arc::clone(&started);
    let pipeline = Pipeline::<&str, &str>::new([]);
    let outcome = sync_run(
        &pipeline,
        move |_| {
            started_op.fetch_add(1, Ordering::Relaxed);
            Ok("done")
        },
        &Exec::new().cancellation(&cancellation),
        handled,
    );
    assert_eq!(outcome, Outcome::Cancelled { attempt: 0 });
    assert_eq!(started.load(Ordering::Relaxed), 0);
}

#[test]
fn retry_result() {
    let policy = RetryPolicy::new(3, Delay::Fixed(Duration::from_millis(1))).expect("attempts");
    let pipeline = Pipeline::new([Policy::retry(policy)]);
    let mut attempts = 0_u32;
    let outcome = sync_run(
        &pipeline,
        |_| {
            attempts += 1;
            if attempts == 1 {
                Err("not yet")
            } else {
                Ok("done")
            }
        },
        &Exec::new().idempotent(),
        handled,
    );
    assert_eq!(
        outcome,
        Outcome::Success {
            value: "done",
            attempt: 2
        }
    );
}

#[test]
fn retry_unhandled_stops() {
    let policy = RetryPolicy::new(4, Delay::Fixed(Duration::from_millis(1))).expect("attempts");
    let pipeline = Pipeline::new([Policy::retry(policy)]);
    let mut attempts = 0_u32;
    let outcome = sync_run(
        &pipeline,
        |_| {
            attempts += 1;
            Err::<&str, _>("nope")
        },
        &Exec::new().idempotent(),
        unhandled,
    );
    assert_eq!(attempts, 1);
    assert_eq!(
        outcome,
        Outcome::Failure {
            error: "nope",
            class: Class::Unhandled,
            attempt: 1
        }
    );
}

#[test]
fn retry_exhausted() {
    let policy = RetryPolicy::new(3, Delay::Fixed(Duration::from_millis(1))).expect("attempts");
    let pipeline = Pipeline::new([Policy::retry(policy)]);
    let mut attempts = 0_u32;
    let outcome = sync_run(
        &pipeline,
        |_| {
            attempts += 1;
            Err::<&str, _>("nope")
        },
        &Exec::new().idempotent(),
        handled,
    );
    assert_eq!(attempts, 3);
    assert_eq!(
        outcome,
        Outcome::Failure {
            error: "nope",
            class: Class::Handled,
            attempt: 3
        }
    );
}

#[test]
fn timeout_drops_the_operation() {
    let pipeline = Pipeline::<&str, &str>::new([Policy::timeout(Duration::from_millis(5))]);
    let started = calls();
    let started_op = Arc::clone(&started);
    let outcome = drive(pipeline.execute_async(
        move |_flag| {
            started_op.fetch_add(1, Ordering::Relaxed);
            pending::<Result<&str, &str>>()
        },
        &Exec::new(),
        handled,
        |_| {},
        || 0,
        |_delay| std::future::ready(()),
    ));
    assert_eq!(outcome, Outcome::TimedOut { attempt: 1 });
    assert_eq!(started.load(Ordering::Relaxed), 1);
}

#[test]
fn breaker_open_does_not_call() {
    let breaker = CircuitBreaker::new(1, Duration::from_secs(30)).expect("threshold");
    let pipeline = Pipeline::new([Policy::breaker(breaker.clone())]);
    let opened = sync_run(&pipeline, |_| Err::<&str, _>("down"), &Exec::new(), handled);
    assert!(matches!(opened, Outcome::Failure { attempt: 1, .. }));
    assert_eq!(breaker.state(), BreakerState::Open);
    let mut calls = 0_u32;
    let rejected = sync_run(
        &pipeline,
        |_| {
            calls += 1;
            Ok("no")
        },
        &Exec::new(),
        handled,
    );
    assert_eq!(calls, 0);
    assert_eq!(
        rejected,
        Outcome::Rejected {
            reason: Reason::BreakerOpen,
            attempt: 0
        }
    );
}

#[test]
fn breaker_probe_closes() {
    let clock = Arc::new(ManualClock::new());
    let breaker =
        CircuitBreaker::with_clock(1, Duration::from_secs(30), ManualClock::handle(&clock))
            .expect("threshold");
    let pipeline = Pipeline::new([Policy::breaker(breaker.clone())]);
    let _ = sync_run(&pipeline, |_| Err::<&str, _>("down"), &Exec::new(), handled);
    assert_eq!(breaker.state(), BreakerState::Open);
    clock.advance(Duration::from_secs(30));
    let mut calls = 0_u32;
    let outcome = sync_run(
        &pipeline,
        |_| {
            calls += 1;
            Ok("back")
        },
        &Exec::new(),
        handled,
    );
    assert_eq!(calls, 1);
    assert_eq!(
        outcome,
        Outcome::Success {
            value: "back",
            attempt: 1
        }
    );
    assert_eq!(breaker.state(), BreakerState::Closed);
}

#[test]
fn bulkhead_full_rejects() {
    let bulkhead = crate::Bulkhead::new(1, 0);
    let _held = bulkhead.try_acquire().expect("one slot");
    let pipeline = Pipeline::<&str, &str>::new([Policy::bulkhead(bulkhead)]);
    let mut calls = 0_u32;
    let outcome = sync_run(
        &pipeline,
        |_| {
            calls += 1;
            Ok("no")
        },
        &Exec::new(),
        handled,
    );
    assert_eq!(calls, 0);
    assert_eq!(
        outcome,
        Outcome::Rejected {
            reason: Reason::BulkheadFull,
            attempt: 0
        }
    );
}

#[test]
fn rate_limit_full_rejects() {
    let limiter = RateLimiter::fixed_window(1, Duration::from_secs(1)).expect("window");
    assert!(limiter.try_acquire());
    let pipeline = Pipeline::<&str, &str>::new([Policy::rate_limit(limiter)]);
    let mut calls = 0_u32;
    let outcome = sync_run(
        &pipeline,
        |_| {
            calls += 1;
            Ok("no")
        },
        &Exec::new(),
        handled,
    );
    assert_eq!(calls, 0);
    assert_eq!(
        outcome,
        Outcome::Rejected {
            reason: Reason::RateLimited,
            attempt: 0
        }
    );
}

#[test]
fn fallback_replaces_a_handled_failure() {
    let factory_calls = calls();
    let seen = Arc::clone(&factory_calls);
    let pipeline = Pipeline::new([Policy::fallback(move || {
        seen.fetch_add(1, Ordering::Relaxed);
        Ok::<&str, &str>("stand-in")
    })]);
    let mut calls = 0_u32;
    let outcome = sync_run(
        &pipeline,
        |_| {
            calls += 1;
            Err("down")
        },
        &Exec::new(),
        handled,
    );
    assert_eq!(calls, 1);
    assert_eq!(factory_calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        outcome,
        Outcome::Success {
            value: "stand-in",
            attempt: 1
        }
    );
}

#[test]
fn fallback_failure_stays_a_failure() {
    let factory_calls = calls();
    let seen = Arc::clone(&factory_calls);
    let pipeline = Pipeline::<&str, &str>::new([Policy::fallback(move || {
        seen.fetch_add(1, Ordering::Relaxed);
        Err("still down")
    })]);
    let outcome = sync_run(&pipeline, |_| Err("down"), &Exec::new(), handled);
    assert_eq!(factory_calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        outcome,
        Outcome::Failure {
            error: "still down",
            class: Class::Handled,
            attempt: 1
        }
    );
}

#[test]
fn hedge_unsafe_rejects() {
    let pipeline = Pipeline::<&str, &str>::new([Policy::hedge(2, Duration::ZERO).expect("hedges")]);
    let started = calls();
    let started_op = Arc::clone(&started);
    let outcome = drive(pipeline.execute_async(
        move |_flag| {
            started_op.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok("no"))
        },
        &Exec::new(),
        handled,
        |_| {},
        || 0,
        |_delay| std::future::ready(()),
    ));
    assert_eq!(started.load(Ordering::Relaxed), 0);
    assert_eq!(
        outcome,
        Outcome::Rejected {
            reason: Reason::NotIdempotent,
            attempt: 0
        }
    );
}

#[test]
fn timeout_around_retry_is_one_deadline() {
    let retry = RetryPolicy::new(5, Delay::Fixed(Duration::from_millis(10))).expect("attempts");
    let pipeline = Pipeline::new([
        Policy::timeout(Duration::from_millis(50)),
        Policy::retry(retry),
    ]);
    let started = calls();
    let started_op = Arc::clone(&started);
    let outcome = drive(pipeline.execute_async(
        move |_flag| {
            started_op.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Err::<&str, _>("down"))
        },
        &Exec::new().idempotent(),
        handled,
        |_| {},
        || 0,
        |delay| gate(delay == Duration::from_millis(50)),
    ));
    assert_eq!(outcome, Outcome::TimedOut { attempt: 1 });
    assert_eq!(started.load(Ordering::Relaxed), 1);
}

#[test]
fn retry_around_timeout_is_a_deadline_per_attempt() {
    let retry = RetryPolicy::new(3, Delay::Fixed(Duration::from_millis(10))).expect("attempts");
    let pipeline = Pipeline::new([
        Policy::retry(retry),
        Policy::timeout(Duration::from_millis(5)),
    ]);
    let started = calls();
    let started_op = Arc::clone(&started);
    let outcome = drive(pipeline.execute_async(
        move |_flag| {
            let attempt = started_op.fetch_add(1, Ordering::Relaxed) + 1;
            async move {
                if attempt < 3 {
                    std::future::pending::<Result<&str, &str>>().await
                } else {
                    Ok("done")
                }
            }
        },
        &Exec::new().idempotent(),
        handled,
        |_| {},
        || 0,
        |delay| gate(delay == Duration::from_millis(5) || delay == Duration::from_millis(10)),
    ));
    assert_eq!(
        outcome,
        Outcome::Success {
            value: "done",
            attempt: 3
        }
    );
    assert_eq!(started.load(Ordering::Relaxed), 3);
}

#[test]
fn sync_timeout_flag_is_cooperative() {
    let clock = Arc::new(ManualClock::new());
    let pipeline = Pipeline::<&str, &str>::new([Policy::timeout(Duration::from_millis(5))]);
    let outcome = pipeline.execute(
        |flag| {
            clock.advance(Duration::from_millis(5));
            assert!(flag.is_timed_out());
            Err("stopped")
        },
        &Exec::new().clock(ManualClock::handle(&clock)),
        handled,
        |_| {},
        || 0,
        |_| {},
    );
    assert_eq!(outcome, Outcome::TimedOut { attempt: 1 });
}

#[test]
fn sync_timeout_ignoring_the_flag_runs_to_completion() {
    let clock = Arc::new(ManualClock::new());
    let pipeline = Pipeline::<&str, &str>::new([Policy::timeout(Duration::from_millis(5))]);
    let outcome = pipeline.execute(
        |_flag| {
            clock.advance(Duration::from_secs(10));
            Ok("finished")
        },
        &Exec::new().clock(ManualClock::handle(&clock)),
        handled,
        |_| {},
        || 0,
        |_| {},
    );
    assert_eq!(
        outcome,
        Outcome::Success {
            value: "finished",
            attempt: 1
        }
    );
}

#[test]
fn panicking_observer_does_not_change_the_outcome() {
    let pipeline = Pipeline::<&str, &str>::new([]);
    let outcome = pipeline.execute(
        |_| Ok("kept"),
        &Exec::new(),
        handled,
        |_: &Event| panic!("observer"),
        || 0,
        |_| {},
    );
    assert_eq!(
        outcome,
        Outcome::Success {
            value: "kept",
            attempt: 1
        }
    );
}

#[test]
fn cancellation_is_not_a_breaker_failure() {
    let breaker = CircuitBreaker::new(1, Duration::from_secs(30)).expect("threshold");
    let pipeline = Pipeline::<&str, &str>::new([Policy::breaker(breaker.clone())]);
    let cancellation = Cancellation::new();
    cancellation.cancel();
    let outcome = sync_run(
        &pipeline,
        |_| Err("down"),
        &Exec::new().cancellation(&cancellation),
        handled,
    );
    assert_eq!(outcome, Outcome::Cancelled { attempt: 0 });
    assert_eq!(breaker.state(), BreakerState::Closed);
}

#[test]
fn dropping_a_permit_frees_the_slot() {
    let bulkhead = crate::Bulkhead::new(1, 0);
    let permit = bulkhead.try_acquire().expect("slot");
    assert!(bulkhead.try_acquire().is_none());
    drop(permit);
    assert!(bulkhead.try_acquire().is_some());
}

#[test]
fn token_bucket_uses_the_injected_clock() {
    let clock = Arc::new(ManualClock::new());
    let limiter = RateLimiter::token_bucket_with_clock(
        1,
        1,
        Duration::from_secs(5),
        ManualClock::handle(&clock),
    )
    .expect("interval");
    assert!(limiter.try_acquire());
    assert!(!limiter.try_acquire());
    clock.advance(Duration::from_secs(5));
    assert!(limiter.try_acquire());
}

#[test]
fn fixed_window_resets_on_the_injected_clock() {
    let clock = Arc::new(ManualClock::new());
    let limiter = RateLimiter::fixed_window_with_clock(
        1,
        Duration::from_secs(5),
        ManualClock::handle(&clock),
    )
    .expect("window");
    assert!(limiter.try_acquire());
    assert!(!limiter.try_acquire());
    clock.advance(Duration::from_secs(5));
    assert!(limiter.try_acquire());
}

#[test]
fn a_failed_probe_opens_again() {
    let clock = Arc::new(ManualClock::new());
    let breaker =
        CircuitBreaker::with_clock(2, Duration::from_secs(10), ManualClock::handle(&clock))
            .expect("threshold");
    let pipeline = Pipeline::new([Policy::breaker(breaker.clone())]);
    let _ = sync_run(&pipeline, |_| Err::<&str, _>("a"), &Exec::new(), handled);
    let _ = sync_run(&pipeline, |_| Err::<&str, _>("b"), &Exec::new(), handled);
    assert_eq!(breaker.state(), BreakerState::Open);
    clock.advance(Duration::from_secs(10));
    let _ = sync_run(
        &pipeline,
        |_| Err::<&str, _>("probe"),
        &Exec::new(),
        handled,
    );
    assert_eq!(breaker.state(), BreakerState::Open);
    let mut calls = 0_u32;
    let rejected = sync_run(
        &pipeline,
        |_| {
            calls += 1;
            Ok("no")
        },
        &Exec::new(),
        handled,
    );
    assert_eq!(calls, 0);
    assert!(matches!(
        rejected,
        Outcome::Rejected {
            reason: Reason::BreakerOpen,
            ..
        }
    ));
}

#[test]
fn hedge_returns_the_first_success() {
    let pipeline = Pipeline::<&str, &str>::new([Policy::hedge(2, Duration::ZERO).expect("hedges")]);
    let started = calls();
    let started_op = Arc::clone(&started);
    let outcome = drive(pipeline.execute_async(
        move |_flag| {
            let n = started_op.fetch_add(1, Ordering::Relaxed) + 1;
            async move {
                if n == 1 {
                    std::future::pending::<Result<&str, &str>>().await
                } else {
                    Ok("won")
                }
            }
        },
        &Exec::new().idempotent(),
        handled,
        |_| {},
        || 0,
        |_delay| std::future::ready(()),
    ));
    assert_eq!(
        outcome,
        Outcome::Success {
            value: "won",
            attempt: 2
        }
    );
    assert_eq!(started.load(Ordering::Relaxed), 2);
}

#[test]
fn one_half_open_probe_rejects_a_second_call() {
    let clock = Arc::new(ManualClock::new());
    let breaker =
        CircuitBreaker::with_clock(1, Duration::from_secs(5), ManualClock::handle(&clock))
            .expect("threshold");
    let pipeline = Pipeline::<&str, &str>::new([Policy::breaker(breaker.clone())]);
    let _ = sync_run(&pipeline, |_| Err("down"), &Exec::new(), handled);
    clock.advance(Duration::from_secs(5));
    let exec = Exec::new();
    let mut probe = pin!(pipeline.execute_async(
        |_flag| pending::<Result<&str, &str>>(),
        &exec,
        handled,
        |_| {},
        || 0,
        |_delay| std::future::pending(),
    ));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    assert!(matches!(probe.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(breaker.state(), BreakerState::HalfOpen);
    let mut calls = 0_u32;
    let rejected = sync_run(
        &pipeline,
        |_| {
            calls += 1;
            Ok("no")
        },
        &Exec::new(),
        handled,
    );
    assert_eq!(calls, 0);
    assert_eq!(
        rejected,
        Outcome::Rejected {
            reason: Reason::BreakerOpen,
            attempt: 0
        }
    );
}

fn pending<T: 'static>() -> Pin<Box<dyn Future<Output = T> + Send>> {
    Box::pin(std::future::pending())
}

fn gate(ready: bool) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    if ready {
        Box::pin(std::future::ready(()))
    } else {
        Box::pin(std::future::pending())
    }
}

#[test]
fn sync_timeout_around_retry_is_one_deadline() {
    let clock = Arc::new(ManualClock::new());
    let retry = RetryPolicy::new(3, Delay::Fixed(Duration::from_millis(10))).unwrap();
    let pipeline = Pipeline::new([
        Policy::timeout(Duration::from_millis(5)),
        Policy::retry(retry),
    ]);
    let started = calls();
    let op_calls = Arc::clone(&started);
    let outcome = pipeline.execute(
        move |_| {
            op_calls.fetch_add(1, Ordering::Relaxed);
            Err::<(), _>("down")
        },
        &Exec::new().idempotent().clock(ManualClock::handle(&clock)),
        handled,
        |_| {},
        || 0,
        |delay| clock.advance(delay),
    );
    assert_eq!(outcome, Outcome::TimedOut { attempt: 1 });
    assert_eq!(started.load(Ordering::Relaxed), 1);
}

#[test]
fn retry_reports_attempts_before_rejection_sync_and_async() {
    for asynchronous in [false, true] {
        let limiter = RateLimiter::fixed_window(1, Duration::from_secs(60)).unwrap();
        let retry = RetryPolicy::new(2, Delay::Fixed(Duration::ZERO)).unwrap();
        let pipeline = Pipeline::new([Policy::retry(retry), Policy::rate_limit(limiter)]);
        let outcome = if asynchronous {
            drive(pipeline.execute_async(
                |_| std::future::ready(Err::<(), _>("down")),
                &Exec::new().idempotent(),
                handled,
                |_| {},
                || 0,
                |_| std::future::ready(()),
            ))
        } else {
            sync_run(
                &pipeline,
                |_| Err::<(), _>("down"),
                &Exec::new().idempotent(),
                handled,
            )
        };
        assert_eq!(
            outcome,
            Outcome::Rejected {
                reason: Reason::RateLimited,
                attempt: 1
            }
        );
    }
}
