//! Sync and async execution. Policies are applied outermost first.

use std::{
    cell::{Cell, RefCell},
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::{Pin, pin},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use neotales_retry::RetryPolicy;

type AttemptFut<'a, T, E> = Pin<Box<dyn Future<Output = Outcome<T, E>> + 'a>>;
type DelayFut<'a> = Pin<Box<dyn Future<Output = ()> + 'a>>;

use crate::{
    breaker::{Admit, CircuitBreaker, transition_reason},
    bulkhead::Bulkhead,
    cancel::{CancelFlag, Cancellation},
    clock::Clock,
    limit::RateLimiter,
    outcome::{Class, Classified, Event, Outcome, OutcomeKind, Reason},
    pipeline::{Exec, Fallback, Hedge, Policy},
    sync_util::lock,
};

pub(crate) fn run_sync<T, E, Make, C, O, R, S>(
    policies: &[Policy<T, E>],
    operation: Make,
    exec: &Exec<'_>,
    classifier: C,
    mut observer: O,
    random: R,
    sleep: S,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Result<T, E>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    S: FnMut(Duration),
{
    let cancellation = exec.cancellation_flag();
    if cancellation.is_cancelled() {
        let outcome = cancelled::<T, E>(0);
        emit_direct(&mut observer, outcome.event());
        return outcome;
    }
    let frame = SyncFrame {
        operation: RefCell::new(operation),
        classifier: RefCell::new(classifier),
        observer: RefCell::new(observer),
        random: RefCell::new(random),
        sleep: RefCell::new(sleep),
        attempts: Cell::new(0),
        last_kind: Cell::new(OutcomeKind::Failure),
        idempotent: exec.is_idempotent(),
        cancellation,
        clock: exec.clock_ref(),
        deadlines: RefCell::new(Vec::new()),
        observed: Cell::new(false),
    };
    let outcome = dispatch_sync(policies, 0, &frame);
    emit(&frame.observer, outcome.event());
    outcome
}

struct SyncFrame<'a, Make, C, O, R, S> {
    operation: RefCell<Make>,
    classifier: RefCell<C>,
    observer: RefCell<O>,
    random: RefCell<R>,
    sleep: RefCell<S>,
    attempts: Cell<u32>,
    last_kind: Cell<OutcomeKind>,
    idempotent: bool,
    cancellation: &'a Cancellation,
    clock: &'a dyn Clock,
    deadlines: RefCell<Vec<Instant>>,
    observed: Cell<bool>,
}

fn dispatch_sync<T, E, Make, C, O, R, S>(
    policies: &[Policy<T, E>],
    index: usize,
    frame: &SyncFrame<'_, Make, C, O, R, S>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Result<T, E>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    S: FnMut(Duration),
{
    let Some(policy) = policies.get(index) else {
        return leaf_sync(frame);
    };
    match policy {
        Policy::Retry(policy) => retry_sync(policies, index, *policy, frame),
        Policy::Timeout(limit) => timeout_sync(policies, index, *limit, frame),
        Policy::Breaker(breaker) => breaker_sync(policies, index, breaker, frame),
        Policy::Bulkhead(bulkhead) => bulkhead_sync(policies, index, bulkhead, frame),
        Policy::RateLimit(limiter) => limit_sync(policies, index, limiter, frame),
        Policy::Hedge(_) => rejected(Reason::AsyncOnly, frame.attempts.get()),
        Policy::Fallback(fallback) => fallback_sync(policies, index, fallback, frame),
    }
}

fn leaf_sync<T, E, Make, C, O, R, S>(frame: &SyncFrame<'_, Make, C, O, R, S>) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Result<T, E>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
{
    if frame.cancellation.is_cancelled() {
        return cancelled(frame.attempts.get());
    }
    let attempt = next_attempt(&frame.attempts);
    let flag = flag_for(frame);
    match frame.operation.borrow_mut()(&flag) {
        Ok(value) => Outcome::Success { value, attempt },
        Err(error) => failure_or_cancel(frame, error, attempt),
    }
}

fn retry_sync<T, E, Make, C, O, R, S>(
    policies: &[Policy<T, E>],
    index: usize,
    policy: RetryPolicy,
    frame: &SyncFrame<'_, Make, C, O, R, S>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Result<T, E>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    S: FnMut(Duration),
{
    if !frame.idempotent {
        return rejected(Reason::NotIdempotent, frame.attempts.get());
    }
    let result = policy.run_with_sleeper(
        || {
            if frame.cancellation.is_cancelled() {
                return Ok(cancelled(frame.attempts.get()));
            }
            if frame
                .deadlines
                .borrow()
                .iter()
                .any(|deadline| frame.clock.now() >= *deadline)
            {
                return Ok(Outcome::TimedOut {
                    attempt: frame.attempts.get(),
                });
            }
            let outcome = dispatch_sync(policies, index + 1, frame);
            split_retry(frame, outcome)
        },
        |_| true,
        || frame.random.borrow_mut()(),
        |delay| {
            emit(
                &frame.observer,
                Event {
                    outcome: frame.last_kind.get(),
                    attempt: frame.attempts.get(),
                    reason: Reason::Retry,
                },
            );
            frame.sleep.borrow_mut()(delay);
        },
    );
    fold_retry(result)
}

fn timeout_sync<T, E, Make, C, O, R, S>(
    policies: &[Policy<T, E>],
    index: usize,
    limit: Duration,
    frame: &SyncFrame<'_, Make, C, O, R, S>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Result<T, E>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    S: FnMut(Duration),
{
    let pushed = push_deadline(frame, limit);
    let observed_before = frame.observed.get();
    let outcome = dispatch_sync(policies, index + 1, frame);
    pop_deadline(frame, pushed);
    apply_sync_timeout(frame, outcome, observed_before)
}

fn breaker_sync<T, E, Make, C, O, R, S>(
    policies: &[Policy<T, E>],
    index: usize,
    breaker: &CircuitBreaker,
    frame: &SyncFrame<'_, Make, C, O, R, S>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Result<T, E>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    S: FnMut(Duration),
{
    let before = breaker.state();
    match breaker.admit() {
        Admit::Rejected => rejected(Reason::BreakerOpen, frame.attempts.get()),
        Admit::Allowed(mut guard) => {
            let outcome = dispatch_sync(policies, index + 1, frame);
            finish_breaker(breaker, &mut guard, before, frame, outcome)
        }
        Admit::Probe(mut guard) => {
            emit_half_open(frame, frame.attempts.get());
            let outcome = dispatch_sync(policies, index + 1, frame);
            finish_breaker(breaker, &mut guard, before, frame, outcome)
        }
    }
}

fn bulkhead_sync<T, E, Make, C, O, R, S>(
    policies: &[Policy<T, E>],
    index: usize,
    bulkhead: &Bulkhead,
    frame: &SyncFrame<'_, Make, C, O, R, S>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Result<T, E>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    S: FnMut(Duration),
{
    let Some(_permit) = bulkhead.acquire(frame.cancellation) else {
        return absent(frame);
    };
    dispatch_sync(policies, index + 1, frame)
}

fn limit_sync<T, E, Make, C, O, R, S>(
    policies: &[Policy<T, E>],
    index: usize,
    limiter: &RateLimiter,
    frame: &SyncFrame<'_, Make, C, O, R, S>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Result<T, E>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    S: FnMut(Duration),
{
    if !limiter.try_acquire() {
        return rejected(Reason::RateLimited, frame.attempts.get());
    }
    dispatch_sync(policies, index + 1, frame)
}

fn fallback_sync<T, E, Make, C, O, R, S>(
    policies: &[Policy<T, E>],
    index: usize,
    fallback: &Fallback<T, E>,
    frame: &SyncFrame<'_, Make, C, O, R, S>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Result<T, E>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    S: FnMut(Duration),
{
    let outcome = dispatch_sync(policies, index + 1, frame);
    if !handled(&frame.classifier, &outcome) {
        return outcome;
    }
    apply_fallback(fallback, frame, outcome.attempt())
}

pub(crate) async fn run_async<T, E, Make, Fut, C, O, R, Sleep, SleepFut>(
    policies: &[Policy<T, E>],
    operation: Make,
    exec: &Exec<'_>,
    classifier: C,
    mut observer: O,
    random: R,
    sleep: Sleep,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
{
    let cancellation = exec.cancellation_flag();
    if cancellation.is_cancelled() {
        let outcome = cancelled::<T, E>(0);
        emit_direct(&mut observer, outcome.event());
        return outcome;
    }
    let frame = AsyncFrame {
        operation: RefCell::new(operation),
        classifier: RefCell::new(classifier),
        observer: RefCell::new(observer),
        random: RefCell::new(random),
        sleep: RefCell::new(sleep),
        attempts: Cell::new(0),
        last_kind: Cell::new(OutcomeKind::Failure),
        idempotent: exec.is_idempotent(),
        cancellation,
        clock: exec.clock_ref(),
        deadlines: RefCell::new(Vec::new()),
        observed: Cell::new(false),
    };
    let outcome = dispatch_async(policies, 0, &frame).await;
    emit(&frame.observer, outcome.event());
    outcome
}

struct AsyncFrame<'a, Make, C, O, R, Sleep> {
    operation: RefCell<Make>,
    classifier: RefCell<C>,
    observer: RefCell<O>,
    random: RefCell<R>,
    sleep: RefCell<Sleep>,
    attempts: Cell<u32>,
    last_kind: Cell<OutcomeKind>,
    idempotent: bool,
    cancellation: &'a Cancellation,
    clock: &'a dyn Clock,
    deadlines: RefCell<Vec<Instant>>,
    observed: Cell<bool>,
}

fn dispatch_async<'a, T, E, Make, Fut, C, O, R, Sleep, SleepFut>(
    policies: &'a [Policy<T, E>],
    index: usize,
    frame: &'a AsyncFrame<'a, Make, C, O, R, Sleep>,
) -> Pin<Box<dyn Future<Output = Outcome<T, E>> + 'a>>
where
    Make: FnMut(&CancelFlag<'_>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    C: for<'b> FnMut(Classified<'b, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
{
    Box::pin(async move {
        let Some(policy) = policies.get(index) else {
            return leaf_async(frame).await;
        };
        match policy {
            Policy::Retry(policy) => retry_async(policies, index, *policy, frame).await,
            Policy::Timeout(limit) => timeout_async(policies, index, *limit, frame).await,
            Policy::Breaker(breaker) => breaker_async(policies, index, breaker, frame).await,
            Policy::Bulkhead(bulkhead) => bulkhead_async(policies, index, bulkhead, frame).await,
            Policy::RateLimit(limiter) => limit_async(policies, index, limiter, frame).await,
            Policy::Hedge(hedge) => hedge_async(policies, index, *hedge, frame).await,
            Policy::Fallback(fallback) => fallback_async(policies, index, fallback, frame).await,
        }
    })
}

async fn leaf_async<T, E, Make, Fut, C, O, R, Sleep>(
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
{
    if frame.cancellation.is_cancelled() {
        return cancelled(frame.attempts.get());
    }
    let attempt = next_attempt(&frame.attempts);
    let flag = flag_for_async(frame);
    let future = {
        let mut operation = frame.operation.borrow_mut();
        operation(&flag)
    };
    match future.await {
        Ok(value) => Outcome::Success { value, attempt },
        Err(error) => failure_or_cancel_async(frame, error, attempt),
    }
}

async fn retry_async<T, E, Make, Fut, C, O, R, Sleep, SleepFut>(
    policies: &[Policy<T, E>],
    index: usize,
    policy: RetryPolicy,
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
{
    if !frame.idempotent {
        return rejected(Reason::NotIdempotent, frame.attempts.get());
    }
    let result = policy
        .run_async(
            || {
                let next = dispatch_async(policies, index + 1, frame);
                async move {
                    if frame.cancellation.is_cancelled() {
                        return Ok(cancelled(frame.attempts.get()));
                    }
                    let outcome = next.await;
                    split_retry_async(frame, outcome)
                }
            },
            |_| true,
            || frame.random.borrow_mut()(),
            |delay| sleep_and_note(frame, delay),
        )
        .await;
    fold_retry(result)
}

async fn timeout_async<T, E, Make, Fut, C, O, R, Sleep, SleepFut>(
    policies: &[Policy<T, E>],
    index: usize,
    limit: Duration,
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
{
    let pushed = push_deadline_async(frame, limit);
    let observed_before = frame.observed.get();
    let sleeper = {
        let mut sleep = frame.sleep.borrow_mut();
        sleep(limit)
    };
    let inner = dispatch_async(policies, index + 1, frame);
    let raced = race(inner, sleeper).await;
    pop_deadline_async(frame, pushed);
    match raced {
        Ok(outcome) => apply_sync_timeout_async(frame, outcome, observed_before),
        Err(()) => timeout_won(frame),
    }
}

async fn breaker_async<T, E, Make, Fut, C, O, R, Sleep, SleepFut>(
    policies: &[Policy<T, E>],
    index: usize,
    breaker: &CircuitBreaker,
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
{
    let before = breaker.state();
    match breaker.admit() {
        Admit::Rejected => rejected(Reason::BreakerOpen, frame.attempts.get()),
        Admit::Allowed(mut guard) => {
            let outcome = dispatch_async(policies, index + 1, frame).await;
            finish_breaker_async(breaker, &mut guard, before, frame, outcome)
        }
        Admit::Probe(mut guard) => {
            emit_half_open_async(frame, frame.attempts.get());
            let outcome = dispatch_async(policies, index + 1, frame).await;
            finish_breaker_async(breaker, &mut guard, before, frame, outcome)
        }
    }
}

async fn bulkhead_async<T, E, Make, Fut, C, O, R, Sleep, SleepFut>(
    policies: &[Policy<T, E>],
    index: usize,
    bulkhead: &Bulkhead,
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
{
    let Some(_permit) = bulkhead.acquire_async(frame.cancellation).await else {
        return absent_async(frame);
    };
    dispatch_async(policies, index + 1, frame).await
}

async fn limit_async<T, E, Make, Fut, C, O, R, Sleep, SleepFut>(
    policies: &[Policy<T, E>],
    index: usize,
    limiter: &RateLimiter,
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
{
    if !limiter.try_acquire() {
        return rejected(Reason::RateLimited, frame.attempts.get());
    }
    dispatch_async(policies, index + 1, frame).await
}

async fn fallback_async<T, E, Make, Fut, C, O, R, Sleep, SleepFut>(
    policies: &[Policy<T, E>],
    index: usize,
    fallback: &Fallback<T, E>,
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
{
    let outcome = dispatch_async(policies, index + 1, frame).await;
    if !handled(&frame.classifier, &outcome) {
        return outcome;
    }
    apply_fallback_async(fallback, frame, outcome.attempt())
}

async fn hedge_async<T, E, Make, Fut, C, O, R, Sleep, SleepFut>(
    policies: &[Policy<T, E>],
    index: usize,
    hedge: Hedge,
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
{
    if !frame.idempotent {
        return rejected(Reason::NotIdempotent, frame.attempts.get());
    }
    race_hedges(policies, index, hedge, frame).await
}

async fn race_hedges<T, E, Make, Fut, C, O, R, Sleep, SleepFut>(
    policies: &[Policy<T, E>],
    index: usize,
    hedge: Hedge,
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
) -> Outcome<T, E>
where
    Make: FnMut(&CancelFlag<'_>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
{
    let mut started = 1_u32;
    let mut inflight: Vec<AttemptFut<'_, T, E>> = vec![dispatch_async(policies, index + 1, frame)];
    let mut waiting: Option<DelayFut<'_>> = None;
    let mut last = None;
    std::future::poll_fn(|cx| {
        poll_hedges(
            cx,
            policies,
            index,
            hedge,
            frame,
            &mut started,
            &mut inflight,
            &mut waiting,
            &mut last,
        )
    })
    .await
}

#[allow(clippy::too_many_arguments)]
fn poll_hedges<'a, T, E, Make, Fut, C, O, R, Sleep, SleepFut>(
    cx: &mut Context<'_>,
    policies: &'a [Policy<T, E>],
    index: usize,
    hedge: Hedge,
    frame: &'a AsyncFrame<'a, Make, C, O, R, Sleep>,
    started: &mut u32,
    inflight: &mut Vec<AttemptFut<'a, T, E>>,
    waiting: &mut Option<DelayFut<'a>>,
    last: &mut Option<Outcome<T, E>>,
) -> Poll<Outcome<T, E>>
where
    Make: FnMut(&CancelFlag<'_>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    C: for<'b> FnMut(Classified<'b, E>) -> Class,
    O: FnMut(&Event),
    R: FnMut() -> u64,
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()> + 'a,
{
    loop {
        if let Some(outcome) = take_ready(cx, frame, inflight, last) {
            return Poll::Ready(outcome);
        }
        if *started >= hedge.max {
            break;
        }
        if waiting.is_none() {
            *waiting = Some(Box::pin(start_hedge_delay(frame, hedge.delay, *started)));
        }
        let ready = waiting
            .as_mut()
            .is_some_and(|sleeper| sleeper.as_mut().poll(cx).is_ready());
        if !ready {
            break;
        }
        *waiting = None;
        *started += 1;
        inflight.push(dispatch_async(policies, index + 1, frame));
    }
    if inflight.is_empty() {
        Poll::Ready(
            last.take()
                .unwrap_or_else(|| rejected(Reason::NotIdempotent, frame.attempts.get())),
        )
    } else {
        Poll::Pending
    }
}

fn take_ready<T, E, C>(
    cx: &mut Context<'_>,
    frame: &AsyncFrame<'_, impl Sized, C, impl Sized, impl Sized, impl Sized>,
    inflight: &mut Vec<AttemptFut<'_, T, E>>,
    last: &mut Option<Outcome<T, E>>,
) -> Option<Outcome<T, E>>
where
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
{
    let mut index = 0;
    while index < inflight.len() {
        match inflight[index].as_mut().poll(cx) {
            Poll::Ready(outcome) => {
                if stops_hedge(&frame.classifier, &outcome) {
                    return Some(outcome);
                }
                *last = Some(outcome);
                drop(inflight.swap_remove(index));
            }
            Poll::Pending => index += 1,
        }
    }
    None
}

fn start_hedge_delay<Make, C, O, R, Sleep, SleepFut>(
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
    delay: Duration,
    started: u32,
) -> SleepFut
where
    O: FnMut(&Event),
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
{
    emit(
        &frame.observer,
        Event {
            outcome: OutcomeKind::Failure,
            attempt: started,
            reason: Reason::Hedge,
        },
    );
    let mut sleep = frame.sleep.borrow_mut();
    sleep(delay)
}

fn sleep_and_note<Make, C, O, R, Sleep, SleepFut>(
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
    delay: Duration,
) -> SleepFut
where
    O: FnMut(&Event),
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
{
    emit(
        &frame.observer,
        Event {
            outcome: frame.last_kind.get(),
            attempt: frame.attempts.get(),
            reason: Reason::Retry,
        },
    );
    let mut sleep = frame.sleep.borrow_mut();
    sleep(delay)
}

async fn race<F, S>(inner: F, sleeper: S) -> Result<F::Output, ()>
where
    F: Future,
    S: Future<Output = ()>,
{
    let mut inner = pin!(inner);
    let mut sleeper = pin!(sleeper);
    std::future::poll_fn(|cx| {
        if let Poll::Ready(output) = inner.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }
        if sleeper.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(()));
        }
        Poll::Pending
    })
    .await
}

fn finish_breaker<T, E, Make, C, O, R, S>(
    breaker: &CircuitBreaker,
    guard: &mut crate::breaker::Guard,
    before: crate::breaker::BreakerState,
    frame: &SyncFrame<'_, Make, C, O, R, S>,
    outcome: Outcome<T, E>,
) -> Outcome<T, E>
where
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
{
    let counts = handled(&frame.classifier, &outcome);
    breaker.settle(guard, &outcome, counts);
    note_transition(&frame.observer, before, breaker, outcome.attempt());
    outcome
}

fn finish_breaker_async<T, E, Make, C, O, R, Sleep>(
    breaker: &CircuitBreaker,
    guard: &mut crate::breaker::Guard,
    before: crate::breaker::BreakerState,
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
    outcome: Outcome<T, E>,
) -> Outcome<T, E>
where
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
{
    let counts = handled(&frame.classifier, &outcome);
    breaker.settle(guard, &outcome, counts);
    note_transition(&frame.observer, before, breaker, outcome.attempt());
    outcome
}

fn note_transition<O: FnMut(&Event)>(
    observer: &RefCell<O>,
    before: crate::breaker::BreakerState,
    breaker: &CircuitBreaker,
    attempt: u32,
) {
    let Some(reason) = transition_reason(before, breaker.state()) else {
        return;
    };
    let outcome = match reason {
        Reason::BreakerClosed => OutcomeKind::Success,
        Reason::BreakerOpen | Reason::BreakerHalfOpen => OutcomeKind::Rejected,
        _ => OutcomeKind::Failure,
    };
    emit(
        observer,
        Event {
            outcome,
            attempt,
            reason,
        },
    );
}

fn apply_fallback<T, E, Make, C, O, R, S>(
    fallback: &Fallback<T, E>,
    frame: &SyncFrame<'_, Make, C, O, R, S>,
    attempt: u32,
) -> Outcome<T, E>
where
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
{
    let produced = {
        let mut factory = lock(&fallback.factory);
        factory()
    };
    let outcome = map_factory(&frame.classifier, produced, attempt);
    emit(
        &frame.observer,
        Event {
            outcome: outcome.kind(),
            attempt,
            reason: Reason::Fallback,
        },
    );
    outcome
}

fn apply_fallback_async<T, E, Make, C, O, R, Sleep>(
    fallback: &Fallback<T, E>,
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
    attempt: u32,
) -> Outcome<T, E>
where
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
    O: FnMut(&Event),
{
    let produced = {
        let mut factory = lock(&fallback.factory);
        factory()
    };
    let outcome = map_factory(&frame.classifier, produced, attempt);
    emit(
        &frame.observer,
        Event {
            outcome: outcome.kind(),
            attempt,
            reason: Reason::Fallback,
        },
    );
    outcome
}

fn map_factory<T, E, C>(
    classifier: &RefCell<C>,
    produced: Result<T, E>,
    attempt: u32,
) -> Outcome<T, E>
where
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
{
    match produced {
        Ok(value) => Outcome::Success { value, attempt },
        Err(error) => {
            let class = classify(classifier, Classified::Failure(&error));
            Outcome::Failure {
                error,
                class,
                attempt,
            }
        }
    }
}

fn split_retry<T, E, Make, C, O, R, S>(
    frame: &SyncFrame<'_, Make, C, O, R, S>,
    outcome: Outcome<T, E>,
) -> Result<Outcome<T, E>, Outcome<T, E>>
where
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
{
    if handled(&frame.classifier, &outcome) {
        frame.last_kind.set(outcome.kind());
        Err(outcome)
    } else {
        Ok(outcome)
    }
}

fn split_retry_async<T, E, Make, C, O, R, Sleep>(
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
    outcome: Outcome<T, E>,
) -> Result<Outcome<T, E>, Outcome<T, E>>
where
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
{
    if handled(&frame.classifier, &outcome) {
        frame.last_kind.set(outcome.kind());
        Err(outcome)
    } else {
        Ok(outcome)
    }
}

fn fold_retry<T, E>(result: Result<Outcome<T, E>, Outcome<T, E>>) -> Outcome<T, E> {
    match result {
        Ok(outcome) | Err(outcome) => outcome,
    }
}

fn apply_sync_timeout<T, E, Make, C, O, R, S>(
    frame: &SyncFrame<'_, Make, C, O, R, S>,
    outcome: Outcome<T, E>,
    observed_before: bool,
) -> Outcome<T, E> {
    let noticed = frame.observed.get() && !observed_before;
    let outcome = prefer_caller(frame.cancellation, outcome);
    if noticed && convertible(&outcome) {
        Outcome::TimedOut {
            attempt: outcome.attempt(),
        }
    } else {
        outcome
    }
}

fn apply_sync_timeout_async<T, E>(
    frame: &AsyncFrame<'_, impl Sized, impl Sized, impl Sized, impl Sized, impl Sized>,
    outcome: Outcome<T, E>,
    observed_before: bool,
) -> Outcome<T, E> {
    let noticed = frame.observed.get() && !observed_before;
    let outcome = prefer_caller(frame.cancellation, outcome);
    if noticed && convertible(&outcome) {
        Outcome::TimedOut {
            attempt: outcome.attempt(),
        }
    } else {
        outcome
    }
}

fn timeout_won<T, E>(
    frame: &AsyncFrame<'_, impl Sized, impl Sized, impl Sized, impl Sized, impl Sized>,
) -> Outcome<T, E> {
    let attempt = frame.attempts.get();
    if frame.cancellation.is_cancelled() {
        cancelled(attempt)
    } else {
        Outcome::TimedOut { attempt }
    }
}

fn failure_or_cancel<T, E, Make, C, O, R, S>(
    frame: &SyncFrame<'_, Make, C, O, R, S>,
    error: E,
    attempt: u32,
) -> Outcome<T, E>
where
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
{
    if frame.cancellation.is_cancelled() {
        return cancelled(attempt);
    }
    let class = classify(&frame.classifier, Classified::Failure(&error));
    Outcome::Failure {
        error,
        class,
        attempt,
    }
}

fn failure_or_cancel_async<T, E, Make, C, O, R, Sleep>(
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
    error: E,
    attempt: u32,
) -> Outcome<T, E>
where
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
{
    if frame.cancellation.is_cancelled() {
        return cancelled(attempt);
    }
    let class = classify(&frame.classifier, Classified::Failure(&error));
    Outcome::Failure {
        error,
        class,
        attempt,
    }
}

fn flag_for<'a, Make, C, O, R, S>(frame: &'a SyncFrame<'a, Make, C, O, R, S>) -> CancelFlag<'a> {
    let deadline = frame.deadlines.borrow().iter().copied().min();
    CancelFlag::new(deadline, frame.clock, frame.cancellation, &frame.observed)
}

fn flag_for_async<'a, Make, C, O, R, Sleep>(
    frame: &'a AsyncFrame<'a, Make, C, O, R, Sleep>,
) -> CancelFlag<'a> {
    let deadline = frame.deadlines.borrow().iter().copied().min();
    CancelFlag::new(deadline, frame.clock, frame.cancellation, &frame.observed)
}

fn push_deadline<Make, C, O, R, S>(
    frame: &SyncFrame<'_, Make, C, O, R, S>,
    limit: Duration,
) -> bool {
    let Some(deadline) = frame.clock.now().checked_add(limit) else {
        return false;
    };
    frame.deadlines.borrow_mut().push(deadline);
    true
}

fn pop_deadline<Make, C, O, R, S>(frame: &SyncFrame<'_, Make, C, O, R, S>, pushed: bool) {
    if pushed {
        let _ = frame.deadlines.borrow_mut().pop();
    }
}

fn push_deadline_async<Make, C, O, R, Sleep>(
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
    limit: Duration,
) -> bool {
    let Some(deadline) = frame.clock.now().checked_add(limit) else {
        return false;
    };
    frame.deadlines.borrow_mut().push(deadline);
    true
}

fn pop_deadline_async<Make, C, O, R, Sleep>(
    frame: &AsyncFrame<'_, Make, C, O, R, Sleep>,
    pushed: bool,
) {
    if pushed {
        let _ = frame.deadlines.borrow_mut().pop();
    }
}

fn absent<T, E, Make, C, O, R, S>(frame: &SyncFrame<'_, Make, C, O, R, S>) -> Outcome<T, E> {
    if frame.cancellation.is_cancelled() {
        cancelled(frame.attempts.get())
    } else {
        rejected(Reason::BulkheadFull, frame.attempts.get())
    }
}

fn absent_async<T, E>(
    frame: &AsyncFrame<'_, impl Sized, impl Sized, impl Sized, impl Sized, impl Sized>,
) -> Outcome<T, E> {
    if frame.cancellation.is_cancelled() {
        cancelled(frame.attempts.get())
    } else {
        rejected(Reason::BulkheadFull, frame.attempts.get())
    }
}

fn emit_half_open<O: FnMut(&Event)>(
    frame: &SyncFrame<'_, impl Sized, impl Sized, O, impl Sized, impl Sized>,
    attempt: u32,
) {
    emit(
        &frame.observer,
        Event {
            outcome: OutcomeKind::Rejected,
            attempt,
            reason: Reason::BreakerHalfOpen,
        },
    );
}

fn emit_half_open_async<O: FnMut(&Event)>(
    frame: &AsyncFrame<'_, impl Sized, impl Sized, O, impl Sized, impl Sized>,
    attempt: u32,
) {
    emit(
        &frame.observer,
        Event {
            outcome: OutcomeKind::Rejected,
            attempt,
            reason: Reason::BreakerHalfOpen,
        },
    );
}

fn handled<T, E, C>(classifier: &RefCell<C>, outcome: &Outcome<T, E>) -> bool
where
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
{
    match outcome {
        Outcome::Success { .. } | Outcome::Cancelled { .. } => false,
        Outcome::Failure { class, .. } => *class == Class::Handled,
        Outcome::TimedOut { .. } => classify(classifier, Classified::TimedOut) == Class::Handled,
        Outcome::Rejected { reason, .. } => {
            classify(classifier, Classified::Rejected(*reason)) == Class::Handled
        }
    }
}

fn stops_hedge<T, E, C>(classifier: &RefCell<C>, outcome: &Outcome<T, E>) -> bool
where
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
{
    match outcome {
        Outcome::Failure {
            class: Class::Handled,
            ..
        } => false,
        Outcome::TimedOut { .. } => classify(classifier, Classified::TimedOut) != Class::Handled,
        Outcome::Success { .. }
        | Outcome::Cancelled { .. }
        | Outcome::Rejected { .. }
        | Outcome::Failure { .. } => true,
    }
}

fn classify<E, C>(classifier: &RefCell<C>, item: Classified<'_, E>) -> Class
where
    C: for<'a> FnMut(Classified<'a, E>) -> Class,
{
    let mut guard = classifier.borrow_mut();
    guard(item)
}

fn emit<O: FnMut(&Event)>(observer: &RefCell<O>, event: Event) {
    let mut guard = observer.borrow_mut();
    let _ = catch_unwind(AssertUnwindSafe(|| guard(&event)));
}

fn emit_direct<O: FnMut(&Event)>(observer: &mut O, event: Event) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer(&event)));
}

fn prefer_caller<T, E>(cancellation: &Cancellation, outcome: Outcome<T, E>) -> Outcome<T, E> {
    if cancellation.is_cancelled() && convertible(&outcome) {
        cancelled(outcome.attempt())
    } else {
        outcome
    }
}

fn convertible<T, E>(outcome: &Outcome<T, E>) -> bool {
    !matches!(outcome, Outcome::Success { .. } | Outcome::Cancelled { .. })
}

fn next_attempt(attempts: &Cell<u32>) -> u32 {
    let attempt = attempts.get().saturating_add(1);
    attempts.set(attempt);
    attempt
}

const fn cancelled<T, E>(attempt: u32) -> Outcome<T, E> {
    Outcome::Cancelled { attempt }
}

const fn rejected<T, E>(reason: Reason, attempt: u32) -> Outcome<T, E> {
    Outcome::Rejected { reason, attempt }
}
