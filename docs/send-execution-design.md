# Send-capable execution: design and unresolved implementation

The full pipeline design below is not implemented. A separate
`execute_idempotent_retry` API implements only retry, cancellation and a total
timer; it reuses the retry crate's async engine and makes no claim to support
other pipeline policies. `execute_async` remains local-future capable. In
particular, callers may use `Rc`, `RefCell`, and non-`Send` operation and
sleeper futures. Do not add `Send` bounds to that method.

## Reproduction and diagnosis

At commit `5a652fe`, even an empty pipeline with an immediately ready, entirely
`Send` operation fails `fn require_send(_: impl Send) {}` when passed the
future returned by `execute_async`. `dispatch_async` explicitly erases its
recursive future as `dyn Future` without `Send`. Adding `+ Send` alone is not
the fix: the future borrows `AsyncFrame`, whose `Cell` and `RefCell` fields are
not `Sync`; hedge also stores local-only erased attempt and delay futures.
`CancelFlag` itself references `Cell<bool>`, so a flag retained across a
suspension would also make a future non-`Send`. Strengthening the existing
erased future and all callback bounds would break existing local callers.

Cancellation currently wakes bulkhead acquisition, but an operation or retry
sleeper that stays pending is not independently woken by `Cancellation`.
`RetryPolicy::with_budget` is a limit on starting later attempts, not an
interrupting deadline. A total wall-clock budget requires a surrounding
`Policy::timeout` and a real caller-supplied timer.

## Proposed shared engine

Introduce a single policy state machine that computes the next action and
receives completed outcomes. It must contain the actual policy transitions,
retry accounting and classification, rather than delegating to two copied
policy match trees. Factor the retry crate's existing attempt/budget/delay
decisions into a reusable cursor first; keep its synchronous and asynchronous
entry points using that same cursor. Preserve the retry crate's elapsed-budget
semantics, including first attempt at zero budget and returning the last error
on exhaustion. Do not silently substitute an interrupting timeout.

Have local and Send-capable drivers interpret those actions using their
respective future representations. The Send driver accepts only `Send`
callbacks and futures and must itself be checked with a compile-time `Send`
assertion. Shared state held across polls must be `Send`: use owned state or
short-lived synchronization that is never held while polling a future or
invoking user callbacks. The local driver must retain its current permissive
types. A single generic driver is preferable if Rust's recursive future and
callback lifetime constraints can express it; otherwise use thin drivers over
the shared state machine, not a second retry engine.

Make deadline state branch-local, not a global push/pop stack. Hedged branches
can overlap and complete in a different order; dropping a timed-out branch must
not remove another branch's deadline. Carry inherited deadlines explicitly and
use a cleanup guard for any registered state. Preserve the existing observed
deadline and successful-result precedence contract. Any change to `CancelFlag`
must preserve synchronous callers and local async callers; an atomic observation
cell is one possible implementation, but the API and existing tests must be
checked before adopting it.

## Cancellation and cleanup

For the new driver, race the entire execution against a cancellation future,
including admission, operation, hedge delays, and retry backoff. Register a
waker with `Cancellation` under its existing listener lock, recheck the flag
without a lost-wakeup window, replace stale registrations when a task's waker
changes, and unregister on drop. Do not call arbitrary wakers while holding a
lock. Drop the losing execution before returning cancellation so bulkhead
permits, queued acquisition, breaker guards, timers and hedge losers are
released. Test concurrent cancel/register and cancel/drop as well as a changed
waker and an already-cancelled token.

Specify and test ties: existing timeout races poll their inner future first;
successful results are not converted to cancellation by `prefer_caller`.
Already-cancelled execution starts no operation. A cancellation race must not
accidentally discard a simultaneously ready success or convert a completed
success into failure. Verify the precise precedence for cancellation versus
timeout and for completed non-success against existing behavior before wiring
the driver. No executor-owned task, thread, or unsafe code is necessary.

`[timeout, retry]` provides an interrupting total budget when its sleeper
measures elapsed time; `[retry, timeout]` provides per-attempt timeouts.
Test both orders with controlled pending futures and timers, including
acquisition and backoff, not merely an immediately ready mock timer.

## DB boundary and validation gate

Do not connect dbapi retry until this engine and its conformance suite exist.
Only explicitly idempotent work may retry; dispatched commit with unknown
outcome must never be automatically retried. Cancellation or dropping a database
future does not prove the server rolled back or that a commit did not happen.
Preserve dbapi's transaction cleanup and unknown-commit contracts.

Before claiming completion, compare every policy and event sequence against the
existing local engine: retry, both timeout orders, breaker and probe cleanup,
bulkhead queue and permit cleanup, rate limit, hedge, fallback, combinations,
and cancellation. Include `!Send` local callbacks/futures, the positive `Send`
assertion, pending cancellation, timer ties and budget tests. Run formatting,
clippy with warnings denied, tests and docs for resilience/retry and existing
consumers. The narrow retry API does not complete this full-pipeline design.
