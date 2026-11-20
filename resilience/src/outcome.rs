//! Terminal outcomes, classification, and telemetry events.

/// Whether a failure participates in a policy.
///
/// Caller cancellation is never a handled failure. Policies do not ask the
/// classifier about it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Class {
    /// Retry, count the failure against a breaker, or fall back.
    Handled,
    /// Pass the failure through unchanged.
    Unhandled,
}

/// What a classifier is asked to judge.
///
/// Success is not classified. Caller cancellation is not presented here, so a
/// classifier cannot turn it into a handled failure.
#[derive(Clone, Copy, Debug)]
pub enum Classified<'a, E> {
    /// The operation returned this error.
    Failure(&'a E),
    /// An inner timeout expired.
    TimedOut,
    /// An inner policy refused to start the operation.
    Rejected(Reason),
}

/// Why an event or a rejection happened.
///
/// A reason is a closed label. It never carries a payload, a credential, or an
/// error's display text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reason {
    /// The operation finished and was accepted.
    Completed,
    /// A handled failure.
    HandledFailure,
    /// A failure the classifier declined.
    UnhandledFailure,
    /// The caller cancelled. This is not a handled failure.
    CallerCancelled,
    /// A timeout policy's deadline expired.
    Deadline,
    /// The breaker is open, so the operation was not called.
    BreakerOpen,
    /// A probe succeeded and the breaker closed.
    BreakerClosed,
    /// The break duration elapsed and a probe was admitted.
    BreakerHalfOpen,
    /// In-flight slots and the queue are both full.
    BulkheadFull,
    /// The rate limiter had no token.
    RateLimited,
    /// Retry or hedging was asked to run an operation that was not marked idempotent.
    NotIdempotent,
    /// Hedging was reached from the synchronous API, which cannot race futures.
    AsyncOnly,
    /// Another attempt was scheduled.
    Retry,
    /// An additional hedge was started.
    Hedge,
    /// A fallback factory ran.
    Fallback,
}

/// The terminal of an [`Event`], without a value or an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutcomeKind {
    /// Accepted value.
    Success,
    /// Handled or unhandled failure.
    Failure,
    /// Caller cancellation.
    Cancelled,
    /// A timeout policy's deadline.
    TimedOut,
    /// A policy refused to start the operation.
    Rejected,
}

/// A telemetry observation.
///
/// Observers receive this by shared reference and cannot replace the policy
/// result. A panic inside an observer is caught and does not change the outcome.
/// The event names the outcome, the attempt, and the reason. It does not contain
/// a payload, a credential, or an error's display text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Event {
    /// Which terminal this observation names.
    pub outcome: OutcomeKind,
    /// How many times the operation has been started. Zero when it was not called.
    pub attempt: u32,
    /// Why this observation was emitted.
    pub reason: Reason,
}

/// The single terminal result of an execution.
///
/// Exactly one of success, failure, cancelled, timed out, or rejected.
#[derive(Debug, Eq, PartialEq)]
#[must_use = "an outcome is the execution result"]
pub enum Outcome<T, E> {
    /// The operation completed with an accepted value.
    Success {
        /// The accepted value.
        value: T,
        /// How many times the operation was started.
        attempt: u32,
    },
    /// The operation failed. `class` says whether policies may act on it.
    Failure {
        /// The error the operation returned.
        error: E,
        /// Handled failures participate in retry, breakers, and fallback.
        class: Class,
        /// How many times the operation was started.
        attempt: u32,
    },
    /// The caller cancelled. This is not a handled failure.
    Cancelled {
        /// How many times the operation was started before cancellation stopped it.
        attempt: u32,
    },
    /// A timeout deadline expired. Outer policies may handle this.
    TimedOut {
        /// How many times the operation was started.
        attempt: u32,
    },
    /// A policy refused to start the operation.
    Rejected {
        /// Why the policy refused.
        reason: Reason,
        /// How many times the operation was started before this rejection.
        attempt: u32,
    },
}

impl<T, E> Outcome<T, E> {
    /// Returns how many times the operation was started.
    #[must_use]
    pub const fn attempt(&self) -> u32 {
        match self {
            Self::Success { attempt, .. }
            | Self::Failure { attempt, .. }
            | Self::Cancelled { attempt }
            | Self::TimedOut { attempt }
            | Self::Rejected { attempt, .. } => *attempt,
        }
    }

    /// Returns the terminal without the value or the error.
    #[must_use]
    pub const fn kind(&self) -> OutcomeKind {
        match self {
            Self::Success { .. } => OutcomeKind::Success,
            Self::Failure { .. } => OutcomeKind::Failure,
            Self::Cancelled { .. } => OutcomeKind::Cancelled,
            Self::TimedOut { .. } => OutcomeKind::TimedOut,
            Self::Rejected { .. } => OutcomeKind::Rejected,
        }
    }

    /// Builds the completion event for this outcome.
    pub(crate) const fn event(&self) -> Event {
        let attempt = self.attempt();
        let (outcome, reason) = match self {
            Self::Success { .. } => (OutcomeKind::Success, Reason::Completed),
            Self::Failure { class, .. } => (
                OutcomeKind::Failure,
                match class {
                    Class::Handled => Reason::HandledFailure,
                    Class::Unhandled => Reason::UnhandledFailure,
                },
            ),
            Self::Cancelled { .. } => (OutcomeKind::Cancelled, Reason::CallerCancelled),
            Self::TimedOut { .. } => (OutcomeKind::TimedOut, Reason::Deadline),
            Self::Rejected { reason, .. } => (OutcomeKind::Rejected, *reason),
        };
        Event {
            outcome,
            attempt,
            reason,
        }
    }
}
