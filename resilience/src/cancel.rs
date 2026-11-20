//! Caller cancellation and the flag a synchronous timeout hands to an operation.

use std::{
    cell::Cell,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use crate::clock::Clock;

/// A flag the caller sets to cancel an execution.
///
/// Cancellation is not a handled failure. Policies do not retry it, and a
/// breaker does not count it.
#[derive(Default)]
pub struct Cancellation {
    flag: AtomicBool,
    listeners: Mutex<Vec<Arc<dyn Fn() + Send + Sync>>>,
}

impl Cancellation {
    /// Creates a flag that is not cancelled.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            flag: AtomicBool::new(false),
            listeners: Mutex::new(Vec::new()),
        }
    }

    /// Requests cancellation. In-flight work observes this on its next check.
    pub fn cancel(&self) {
        let listeners = {
            let mut listeners = crate::sync_util::lock(&self.listeners);
            self.flag.store(true, Ordering::Release);
            std::mem::take(&mut *listeners)
        };
        for listener in listeners {
            listener();
        }
    }

    /// Reports whether [`Cancellation::cancel`] has been called.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }
}

impl fmt::Debug for Cancellation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cancellation")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl Cancellation {
    pub(crate) fn listen(&self, listener: Arc<dyn Fn() + Send + Sync>) -> Option<Registration<'_>> {
        let mut listeners = crate::sync_util::lock(&self.listeners);
        if self.is_cancelled() {
            return None;
        }
        listeners.push(Arc::clone(&listener));
        Some(Registration {
            cancellation: self,
            listener,
        })
    }
}

pub(crate) struct Registration<'a> {
    cancellation: &'a Cancellation,
    listener: Arc<dyn Fn() + Send + Sync>,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        crate::sync_util::lock(&self.cancellation.listeners)
            .retain(|listener| !Arc::ptr_eq(listener, &self.listener));
    }
}

/// The flag a synchronous operation can inspect.
///
/// Sync timeout does not kill a thread. It passes this flag. A blocking closure
/// that never calls [`CancelFlag::is_cancelled`] or [`CancelFlag::is_timed_out`]
/// runs to completion, and the policy does not replace the result that closure
/// returned.
///
/// Async timeout is different: the policy drops the operation future when the
/// caller-supplied sleeper finishes. That drop is the cancel.
pub struct CancelFlag<'a> {
    deadline: Option<Instant>,
    clock: &'a dyn Clock,
    caller: &'a Cancellation,
    observed: &'a Cell<bool>,
}

impl fmt::Debug for CancelFlag<'_> {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("CancelFlag")
            .field("deadline", &self.deadline)
            .field("caller_cancelled", &self.caller.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl<'a> CancelFlag<'a> {
    pub(crate) fn new(
        deadline: Option<Instant>,
        clock: &'a dyn Clock,
        caller: &'a Cancellation,
        observed: &'a Cell<bool>,
    ) -> Self {
        Self {
            deadline,
            clock,
            caller,
            observed,
        }
    }

    /// Reports whether the caller cancelled or a sync timeout deadline has passed.
    ///
    /// Seeing a passed deadline records that the operation noticed it. The sync
    /// timeout turns a non-success result into [`crate::Outcome::TimedOut`] only
    /// when this, or [`CancelFlag::is_timed_out`], was called after the deadline.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.caller.is_cancelled() || self.observe_deadline()
    }

    /// Reports whether the caller cancelled, ignoring any timeout deadline.
    #[must_use]
    pub fn is_caller_cancelled(&self) -> bool {
        self.caller.is_cancelled()
    }

    /// Reports whether a sync timeout deadline has passed, and records the look.
    #[must_use]
    pub fn is_timed_out(&self) -> bool {
        self.observe_deadline()
    }

    fn observe_deadline(&self) -> bool {
        let expired = self
            .deadline
            .is_some_and(|deadline| self.clock.now() >= deadline);
        if expired {
            self.observed.set(true);
        }
        expired
    }
}
