//! The public retry surface, exercised from outside the crate.

use std::{
    cell::{Cell, RefCell},
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use neotales_retry::{Delay, InvalidPolicy, RetryPolicy};

/// A wait short enough that no test spends real time in it.
const ONE_MS: Duration = Duration::from_millis(1);

#[test]
fn fixed_delays_repeat_until_success() {
    let attempts = Cell::new(0_u32);
    let waits = RefCell::new(Vec::new());
    let result: Result<&str, u32> = RetryPolicy::new(3, Delay::Fixed(Duration::from_millis(10)))
        .expect("three attempts")
        .run_with_sleeper(
            || {
                attempts.set(attempts.get() + 1);
                let attempt = attempts.get();
                if attempt < 3 {
                    Err(attempt)
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
fn a_zero_attempt_policy_is_rejected() {
    assert_eq!(
        RetryPolicy::new(0, Delay::Fixed(ONE_MS)),
        Err(InvalidPolicy)
    );
}

#[test]
fn the_attempt_cap_returns_the_last_error() {
    let attempts = Cell::new(0_u32);
    let waits = RefCell::new(Vec::new());
    let result = RetryPolicy::new(2, Delay::Fixed(ONE_MS))
        .expect("two attempts")
        .run_with_sleeper(
            || {
                attempts.set(attempts.get() + 1);
                Err::<(), _>(attempts.get())
            },
            |_| true,
            || 0,
            |wait| waits.borrow_mut().push(wait),
        );

    assert_eq!(result, Err(2));
    assert_eq!(*waits.borrow(), [ONE_MS]);
}

#[test]
fn an_exhausted_budget_still_runs_the_first_attempt() {
    let attempts = Cell::new(0_u32);
    let result = RetryPolicy::new(5, Delay::Fixed(ONE_MS))
        .expect("five attempts")
        .with_budget(Duration::ZERO)
        .run_with_sleeper(
            || {
                attempts.set(attempts.get() + 1);
                Err::<(), _>(attempts.get())
            },
            |_| true,
            || 0,
            |_| panic!("no wait fits in a zero budget"),
        );

    assert_eq!(result, Err(1));
    assert_eq!(attempts.get(), 1);
}

#[test]
fn async_retry_awaits_the_caller_sleeper() {
    let attempts = Cell::new(0_u32);
    let waits = RefCell::new(Vec::new());
    let result = block_on(
        RetryPolicy::new(3, Delay::Fixed(ONE_MS))
            .expect("three attempts")
            .run_async(
                || {
                    attempts.set(attempts.get() + 1);
                    let attempt = attempts.get();
                    let outcome = if attempt == 1 {
                        Err(attempt)
                    } else {
                        Ok(attempt)
                    };
                    std::future::ready(outcome)
                },
                |_| true,
                || 0,
                |wait| {
                    waits.borrow_mut().push(wait);
                    std::future::ready(())
                },
            ),
    );

    assert_eq!(result, Ok(2));
    assert_eq!(*waits.borrow(), [ONE_MS]);
}

/// Drives a future whose test futures are all ready on the first poll.
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
