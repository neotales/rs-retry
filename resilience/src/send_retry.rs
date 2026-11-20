//! A deliberately narrow retry-only execution API.

use std::{
    future::{Future, poll_fn},
    pin::{Pin, pin},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use crate::{Cancellation, RetryPolicy};

/// Result of the retry-only execution, with the number of attempts started.
#[derive(Debug, PartialEq, Eq)]
pub enum RetryOutcome<T, E> {
    /// An attempt succeeded.
    Success {
        /// The returned value.
        value: T,
        /// Number of attempts started.
        attempts: u32,
    },
    /// An error was refused or retries were exhausted.
    Failure {
        /// The last error.
        error: E,
        /// Number of attempts started.
        attempts: u32,
    },
    /// Caller cancellation stopped the execution.
    Cancelled {
        /// Number of attempts started.
        attempts: u32,
    },
    /// The caller's total-budget timer completed.
    TimedOut {
        /// Number of attempts started.
        attempts: u32,
    },
}

// Poll only an already-started attempt: a failed attempt reaches the yielding
// checkpoint before the retry engine can start another one.
fn interrupted<T, E, F: Future<Output = Result<T, E>> + ?Sized>(
    mut run: Pin<&mut F>,
    cx: &mut Context<'_>,
    active: &AtomicBool,
    attempts: &AtomicU32,
    cancellation: &Cancellation,
) -> RetryOutcome<T, E> {
    if active.load(Ordering::Relaxed)
        && let Poll::Ready(Ok(value)) = run.as_mut().poll(cx)
    {
        return RetryOutcome::Success {
            value,
            attempts: attempts.load(Ordering::Relaxed),
        };
    }
    let attempts = attempts.load(Ordering::Relaxed);
    if cancellation.is_cancelled() {
        RetryOutcome::Cancelled { attempts }
    } else {
        RetryOutcome::TimedOut { attempts }
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use crate::Delay;
    use std::{
        future::{pending, ready},
        sync::atomic::AtomicBool,
        task::{Context, Wake, Waker},
    };

    struct WakeFlag(AtomicBool);
    impl Wake for WakeFlag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn policy() -> RetryPolicy {
        RetryPolicy::new(2, Delay::Fixed(Duration::from_secs(1))).unwrap()
    }

    fn assert_send<T: Send>(_: &T) {}

    fn wait(flag: &AtomicBool) -> impl Future<Output = ()> + '_ {
        poll_fn(move |_| {
            if flag.load(Ordering::Relaxed) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
    }

    #[test]
    fn send_and_cancellation_drops_pending_operation() {
        let cancel = Cancellation::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = dropped.clone();
        let future = execute_idempotent_retry(
            policy(),
            &cancel,
            pending(),
            move || {
                let guard = DropFlag(flag.clone());
                async move {
                    let _guard = guard;
                    pending::<Result<(), ()>>().await
                }
            },
            |()| true,
            || 0,
            |_| ready(()),
        );
        assert_send(&future);
        let mut future = pin!(future);
        let wake = Arc::new(WakeFlag(AtomicBool::new(false)));
        let waker = Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(future.as_mut().poll(&mut cx).is_pending());
        cancel.cancel();
        assert!(wake.0.load(Ordering::SeqCst));
        assert_eq!(
            future.as_mut().poll(&mut cx),
            Poll::Ready(RetryOutcome::Cancelled { attempts: 1 })
        );
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn cancellation_and_timeout_interrupt_backoff() {
        for timeout in [false, true] {
            let cancel = Cancellation::new();
            let dropped = Arc::new(AtomicBool::new(false));
            let flag = dropped.clone();
            let future = execute_idempotent_retry(
                policy(),
                &cancel,
                async move {
                    if !timeout {
                        pending::<()>().await;
                    }
                },
                || ready(Err::<(), _>("failure")),
                |_| true,
                || 0,
                move |_| {
                    let guard = DropFlag(flag.clone());
                    async move {
                        let _guard = guard;
                        pending::<()>().await;
                    }
                },
            );
            let mut future = pin!(future);
            let mut cx = Context::from_waker(Waker::noop());
            if timeout {
                assert_eq!(
                    future.as_mut().poll(&mut cx),
                    Poll::Ready(RetryOutcome::TimedOut { attempts: 1 })
                );
            } else {
                assert!(future.as_mut().poll(&mut cx).is_pending());
                cancel.cancel();
                assert_eq!(
                    future.as_mut().poll(&mut cx),
                    Poll::Ready(RetryOutcome::Cancelled { attempts: 1 })
                );
            }
            assert!(dropped.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn attempt_limit_and_refusal_use_shared_policy() {
        for retry in [false, true] {
            let cancel = Cancellation::new();
            let future = execute_idempotent_retry(
                policy(),
                &cancel,
                pending(),
                || ready(Err::<(), _>("unknown")),
                move |_| retry,
                || 0,
                |_| ready(()),
            );
            let mut future = pin!(future);
            let mut cx = Context::from_waker(Waker::noop());
            if retry {
                assert!(future.as_mut().poll(&mut cx).is_pending());
            }
            assert_eq!(
                future.as_mut().poll(&mut cx),
                Poll::Ready(RetryOutcome::Failure {
                    error: "unknown",
                    attempts: if retry { 2 } else { 1 },
                })
            );
        }
    }

    #[test]
    fn pre_cancel_wins_without_starting_and_ready_work_beats_timer() {
        let cancel = Cancellation::new();
        cancel.cancel();
        let mut never = pin!(execute_idempotent_retry(
            policy(),
            &cancel,
            ready(()),
            || -> std::future::Ready<Result<(), ()>> { panic!("cancelled work started") },
            |()| true,
            || 0,
            |_| ready(()),
        ));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(
            never.as_mut().poll(&mut cx),
            Poll::Ready(RetryOutcome::Cancelled { attempts: 0 })
        );

        let cancel = Cancellation::new();
        let mut work = pin!(execute_idempotent_retry(
            policy(),
            &cancel,
            ready(()),
            || ready(Ok::<_, ()>(42)),
            |()| true,
            || 0,
            |_| ready(()),
        ));
        assert_eq!(
            work.as_mut().poll(&mut cx),
            Poll::Ready(RetryOutcome::Success {
                value: 42,
                attempts: 1
            })
        );
    }

    #[test]
    fn dropping_pending_execution_unregisters_waker() {
        let cancel = Cancellation::new();
        let wake = Arc::new(WakeFlag(AtomicBool::new(false)));
        let weak = Arc::downgrade(&wake);
        {
            let waker = Waker::from(wake.clone());
            let mut cx = Context::from_waker(&waker);
            let mut future = pin!(execute_idempotent_retry(
                policy(),
                &cancel,
                pending(),
                pending::<Result<(), ()>>,
                |()| true,
                || 0,
                |_| ready(()),
            ));
            assert!(future.as_mut().poll(&mut cx).is_pending());
        }
        drop(wake);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn retry_budget_zero_still_starts_once() {
        let cancel = Cancellation::new();
        let mut work = pin!(execute_idempotent_retry(
            policy().with_budget(Duration::ZERO),
            &cancel,
            pending(),
            || ready(Err::<(), _>("failure")),
            |_| true,
            || 0,
            |_| ready(()),
        ));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(
            work.as_mut().poll(&mut cx),
            Poll::Ready(RetryOutcome::Failure {
                error: "failure",
                attempts: 1
            })
        );
    }

    #[test]
    fn cancellation_inside_failed_attempt_stops_retry() {
        let cancel = Cancellation::new();
        let attempts = AtomicU32::new(0);
        let mut work = pin!(execute_idempotent_retry(
            RetryPolicy::new(10, Delay::Fixed(Duration::ZERO)).unwrap(),
            &cancel,
            pending(),
            || {
                attempts.fetch_add(1, Ordering::Relaxed);
                cancel.cancel();
                ready(Err::<(), _>("failure"))
            },
            |_| true,
            || 0,
            |_| ready(()),
        ));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(
            work.as_mut().poll(&mut cx),
            Poll::Ready(RetryOutcome::Cancelled { attempts: 1 })
        );
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn ready_backoff_and_deadline_do_not_start_second_attempt() {
        let cancel = Cancellation::new();
        let deadline = AtomicBool::new(false);
        let backoff = AtomicBool::new(false);
        let calls = AtomicU32::new(0);
        let mut work = pin!(execute_idempotent_retry(
            policy(),
            &cancel,
            wait(&deadline),
            || {
                calls.fetch_add(1, Ordering::Relaxed);
                ready(Err::<(), _>("failure"))
            },
            |_| true,
            || 0,
            |_| wait(&backoff),
        ));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(work.as_mut().poll(&mut cx).is_pending());
        backoff.store(true, Ordering::Relaxed);
        deadline.store(true, Ordering::Relaxed);
        assert_eq!(
            work.as_mut().poll(&mut cx),
            Poll::Ready(RetryOutcome::TimedOut { attempts: 1 })
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn immediate_retries_yield_and_can_be_cancelled() {
        let cancel = Cancellation::new();
        let calls = AtomicU32::new(0);
        let mut work = pin!(execute_idempotent_retry(
            RetryPolicy::new(10_000, Delay::Fixed(Duration::ZERO)).unwrap(),
            &cancel,
            pending(),
            || {
                calls.fetch_add(1, Ordering::Relaxed);
                ready(Err::<(), _>("failure"))
            },
            |_| true,
            || 0,
            |_| ready(()),
        ));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(work.as_mut().poll(&mut cx).is_pending());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        cancel.cancel();
        assert_eq!(
            work.as_mut().poll(&mut cx),
            Poll::Ready(RetryOutcome::Cancelled { attempts: 1 })
        );
    }

    #[test]
    fn pending_attempt_respects_deadline_and_ready_success_wins() {
        for success in [false, true] {
            let cancel = Cancellation::new();
            let deadline = AtomicBool::new(false);
            let result_ready = AtomicBool::new(false);
            let mut work = pin!(execute_idempotent_retry(
                policy(),
                &cancel,
                poll_fn(|_| {
                    if deadline.load(Ordering::Relaxed) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }),
                || poll_fn(|_| {
                    if result_ready.load(Ordering::Relaxed) {
                        Poll::Ready(Ok::<_, ()>(42))
                    } else {
                        Poll::Pending
                    }
                }),
                |()| true,
                || 0,
                |_| ready(()),
            ));
            let mut cx = Context::from_waker(Waker::noop());
            assert!(work.as_mut().poll(&mut cx).is_pending());
            deadline.store(true, Ordering::Relaxed);
            result_ready.store(success, Ordering::Relaxed);
            assert_eq!(
                work.as_mut().poll(&mut cx),
                Poll::Ready(if success {
                    RetryOutcome::Success {
                        value: 42,
                        attempts: 1,
                    }
                } else {
                    RetryOutcome::TimedOut { attempts: 1 }
                })
            );
        }
    }

    #[test]
    fn already_started_ready_success_wins_cancellation() {
        let cancel = Cancellation::new();
        let ready_flag = AtomicBool::new(false);
        let mut work = pin!(execute_idempotent_retry(
            policy(),
            &cancel,
            pending(),
            || poll_fn(|_| {
                if ready_flag.load(Ordering::Relaxed) {
                    Poll::Ready(Ok::<_, ()>(7))
                } else {
                    Poll::Pending
                }
            }),
            |()| true,
            || 0,
            |_| ready(()),
        ));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(work.as_mut().poll(&mut cx).is_pending());
        ready_flag.store(true, Ordering::Relaxed);
        cancel.cancel();
        assert_eq!(
            work.as_mut().poll(&mut cx),
            Poll::Ready(RetryOutcome::Success {
                value: 7,
                attempts: 1
            })
        );
    }

    #[test]
    fn cancellation_during_waker_replacement_preserves_ready_success() {
        struct CancelOnDrop {
            cancel: Arc<Cancellation>,
            ready: Arc<AtomicBool>,
        }
        #[allow(clippy::manual_noop_waker)] // Its destructor triggers the race.
        impl Wake for CancelOnDrop {
            fn wake(self: Arc<Self>) {}
        }
        impl Drop for CancelOnDrop {
            fn drop(&mut self) {
                self.ready.store(true, Ordering::SeqCst);
                self.cancel.cancel();
            }
        }

        let cancel = Arc::new(Cancellation::new());
        let ready_flag = Arc::new(AtomicBool::new(false));
        let mut work = pin!(execute_idempotent_retry(
            policy(),
            &cancel,
            pending(),
            || poll_fn(|_| {
                if ready_flag.load(Ordering::SeqCst) {
                    Poll::Ready(Ok::<_, ()>(42))
                } else {
                    Poll::Pending
                }
            }),
            |()| true,
            || 0,
            |_| ready(()),
        ));
        {
            let waker = Waker::from(Arc::new(CancelOnDrop {
                cancel: cancel.clone(),
                ready: ready_flag.clone(),
            }));
            assert!(
                work.as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }
        assert_eq!(
            work.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(RetryOutcome::Success {
                value: 42,
                attempts: 1
            })
        );
    }

    #[test]
    fn timer_makes_pending_attempt_ready_success() {
        let cancel = Cancellation::new();
        let ready_flag = AtomicBool::new(false);
        let mut work = pin!(execute_idempotent_retry(
            policy(),
            &cancel,
            poll_fn(|_| {
                ready_flag.store(true, Ordering::SeqCst);
                Poll::Ready(())
            }),
            || poll_fn(|_| {
                if ready_flag.load(Ordering::SeqCst) {
                    Poll::Ready(Ok::<_, ()>(42))
                } else {
                    Poll::Pending
                }
            }),
            |()| true,
            || 0,
            |_| ready(()),
        ));
        assert_eq!(
            work.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(RetryOutcome::Success {
                value: 42,
                attempts: 1
            })
        );
    }

    #[test]
    fn changed_waker_is_unregistered_on_completion() {
        let cancel = Cancellation::new();
        let first = Arc::new(WakeFlag(AtomicBool::new(false)));
        let second = Arc::new(WakeFlag(AtomicBool::new(false)));
        let weak_first = Arc::downgrade(&first);
        let weak_second = Arc::downgrade(&second);
        {
            let first_waker = Waker::from(first.clone());
            let second_waker = Waker::from(second.clone());
            let mut work = pin!(execute_idempotent_retry(
                policy(),
                &cancel,
                pending(),
                pending::<Result<(), ()>>,
                |()| true,
                || 0,
                |_| ready(()),
            ));
            assert!(
                work.as_mut()
                    .poll(&mut Context::from_waker(&first_waker))
                    .is_pending()
            );
            assert!(
                work.as_mut()
                    .poll(&mut Context::from_waker(&second_waker))
                    .is_pending()
            );
            cancel.cancel();
            assert!(!first.0.load(Ordering::SeqCst));
            assert!(second.0.load(Ordering::SeqCst));
            assert_eq!(
                work.as_mut().poll(&mut Context::from_waker(&second_waker)),
                Poll::Ready(RetryOutcome::Cancelled { attempts: 1 })
            );
        }
        drop(first);
        drop(second);
        assert!(weak_first.upgrade().is_none());
        assert!(weak_second.upgrade().is_none());
    }

    #[test]
    fn retry_delays_match_existing_async_engine() {
        let policy = RetryPolicy::new(
            4,
            Delay::Exponential {
                base: Duration::from_millis(2),
                max: Duration::from_millis(5),
            },
        )
        .unwrap();
        let local_delays = std::cell::RefCell::new(Vec::new());
        let mut local = pin!(policy.run_async(
            || ready(Err::<(), _>("failure")),
            |_| true,
            || 0,
            |delay| {
                local_delays.borrow_mut().push(delay);
                ready(())
            },
        ));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(local.as_mut().poll(&mut cx), Poll::Ready(Err("failure")));
        let cancel = Cancellation::new();
        let send_delays = std::cell::RefCell::new(Vec::new());
        let mut work = pin!(execute_idempotent_retry(
            policy,
            &cancel,
            pending(),
            || ready(Err::<(), _>("failure")),
            |_| true,
            || 0,
            |delay| {
                send_delays.borrow_mut().push(delay);
                ready(())
            },
        ));
        for _ in 0..3 {
            assert!(work.as_mut().poll(&mut cx).is_pending());
        }
        assert_eq!(
            work.as_mut().poll(&mut cx),
            Poll::Ready(RetryOutcome::Failure {
                error: "failure",
                attempts: 4,
            })
        );
        assert_eq!(*send_delays.borrow(), *local_delays.borrow());
    }
}

/// Runs explicitly idempotent work through the shared retry engine.
///
/// This is not a [`crate::Pipeline`] executor: it supports retry and one
/// caller-provided total-budget timer, not other pipeline policies. `timeout`
/// must be an actual timer future (or a pending future for no deadline).
/// Cancellation and timeout drop the in-flight operation or backoff future;
/// neither proves that an external side effect did not occur.
///
/// The caller must ensure every attempt is safe to repeat and must refuse
/// errors with unknown commit outcomes in `retry_if`. Never use this to retry
/// an unconfirmed dispatched commit. The returned future is `Send` when the
/// supplied callbacks, futures and values are `Send`. No tasks are spawned.
///
/// Already-started successful work takes precedence over simultaneous
/// cancellation or a ready timer. When cancellation is observed while an
/// attempt is pending, that attempt is polled once to check for success.
/// Between attempts, cancellation and the timer are checked before
/// starting more work. Each retry yields to the executor at that boundary.
/// Neither a timer nor cancellation can interrupt a blocking `poll` or callback.
/// [`RetryPolicy::with_budget`] only stops starting later attempts and cannot
/// interrupt pending work; the separate timer supplies that total deadline.
/// It must wake the executor and be constructed to measure the desired interval.
///
/// ```
/// use std::{future::{Future, pending, ready}, pin::pin, task::{Context, Poll, Waker}, time::Duration};
/// use neotales_resilience::{Cancellation, Delay, RetryPolicy, RetryOutcome, execute_idempotent_retry};
/// let cancel = Cancellation::new();
/// let policy = RetryPolicy::new(2, Delay::Fixed(Duration::ZERO)).unwrap();
/// let mut future = pin!(execute_idempotent_retry(
///     policy, &cancel, pending(), || ready(Ok::<_, ()>(42)),
///     |()| false, || 0, |_| ready(()),
/// ));
/// let mut cx = Context::from_waker(Waker::noop());
/// assert_eq!(future.as_mut().poll(&mut cx), Poll::Ready(RetryOutcome::Success {
///     value: 42, attempts: 1
/// }));
/// ```
#[allow(clippy::too_many_lines)] // Keep the race's ordering visible in one place.
pub async fn execute_idempotent_retry<
    T,
    E,
    Operation,
    Attempt,
    Retry,
    Random,
    Sleep,
    Waiting,
    Timer,
>(
    policy: RetryPolicy,
    cancellation: &Cancellation,
    timeout: Timer,
    mut operation: Operation,
    retry_if: Retry,
    random: Random,
    sleep: Sleep,
) -> RetryOutcome<T, E>
where
    Operation: FnMut() -> Attempt,
    Attempt: Future<Output = Result<T, E>>,
    Retry: FnMut(&E) -> bool,
    Random: FnMut() -> u64,
    Sleep: FnMut(Duration) -> Waiting,
    Waiting: Future<Output = ()>,
    Timer: Future<Output = ()>,
{
    let attempts = AtomicU32::new(0);
    let checkpointing = AtomicBool::new(false);
    let active = AtomicBool::new(false);
    if cancellation.is_cancelled() {
        return RetryOutcome::Cancelled { attempts: 0 };
    }
    let run = policy.run_async_with_checkpoint(
        || {
            attempts.fetch_add(1, Ordering::Relaxed);
            active.store(true, Ordering::Relaxed);
            let future = operation();
            let active = &active;
            async move {
                let result = future.await;
                active.store(false, Ordering::Relaxed);
                result
            }
        },
        retry_if,
        random,
        sleep,
        |_, _| {},
        || {
            let checkpointing = &checkpointing;
            let mut yielded = false;
            poll_fn(move |cx| {
                if yielded {
                    checkpointing.store(false, Ordering::Relaxed);
                    Poll::Ready(())
                } else {
                    yielded = true;
                    checkpointing.store(true, Ordering::Relaxed);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
        },
    );
    let mut run = pin!(run);
    let mut timeout = pin!(timeout);
    let mut registration = None;
    poll_fn(|cx| {
        if cancellation.is_cancelled() {
            return Poll::Ready(interrupted(
                run.as_mut(),
                cx,
                &active,
                &attempts,
                cancellation,
            ));
        }
        let waker = cx.waker().clone();
        registration = cancellation.listen(Arc::new(move || waker.wake_by_ref()));
        if registration.is_none() {
            return Poll::Ready(interrupted(
                run.as_mut(),
                cx,
                &active,
                &attempts,
                cancellation,
            ));
        }
        if checkpointing.load(Ordering::Relaxed) && timeout.as_mut().poll(cx).is_ready() {
            return Poll::Ready(interrupted(
                run.as_mut(),
                cx,
                &active,
                &attempts,
                cancellation,
            ));
        }
        if cancellation.is_cancelled() {
            return Poll::Ready(interrupted(
                run.as_mut(),
                cx,
                &active,
                &attempts,
                cancellation,
            ));
        }
        if let Poll::Ready(result) = run.as_mut().poll(cx) {
            let started = attempts.load(Ordering::Relaxed);
            let error = match result {
                Ok(value) => {
                    return Poll::Ready(RetryOutcome::Success {
                        value,
                        attempts: started,
                    });
                }
                Err(error) => error,
            };
            if cancellation.is_cancelled() {
                return Poll::Ready(interrupted(
                    run.as_mut(),
                    cx,
                    &active,
                    &attempts,
                    cancellation,
                ));
            }
            if timeout.as_mut().poll(cx).is_ready() {
                return Poll::Ready(interrupted(
                    run.as_mut(),
                    cx,
                    &active,
                    &attempts,
                    cancellation,
                ));
            }
            return Poll::Ready(RetryOutcome::Failure {
                error,
                attempts: started,
            });
        }
        if cancellation.is_cancelled() {
            return Poll::Ready(interrupted(
                run.as_mut(),
                cx,
                &active,
                &attempts,
                cancellation,
            ));
        }
        if timeout.as_mut().poll(cx).is_ready() {
            return Poll::Ready(interrupted(
                run.as_mut(),
                cx,
                &active,
                &attempts,
                cancellation,
            ));
        }
        Poll::Pending
    })
    .await
}
