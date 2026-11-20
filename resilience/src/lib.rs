//! A policy pipeline for timeouts, retries, circuit breakers, and load shedding.
#![doc = include_str!("../README.md")]

mod breaker;
mod bulkhead;
mod cancel;
mod clock;
mod exec;
mod invalid;
mod limit;
mod outcome;
mod pipeline;
mod send_retry;
mod sync_util;

pub use breaker::{BreakerState, CircuitBreaker};
pub use bulkhead::{Bulkhead, Permit};
pub use cancel::{CancelFlag, Cancellation};
pub use clock::{Clock, ManualClock, SystemClock};
pub use invalid::InvalidPolicy;
pub use limit::RateLimiter;
pub use neotales_retry::{Delay, RetryPolicy};
pub use outcome::{Class, Classified, Event, Outcome, OutcomeKind, Reason};
pub use pipeline::{Exec, Pipeline, Policy};
pub use send_retry::{RetryOutcome, execute_idempotent_retry};

#[cfg(test)]
mod conformance;
