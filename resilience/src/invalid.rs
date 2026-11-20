//! Configuration values a constructor refuses.

use std::{error::Error, fmt};

/// A policy was built with a value that cannot be used.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidPolicy {
    /// A circuit breaker threshold of zero would open before any call.
    ZeroThreshold,
    /// A hedge count of zero cannot start an attempt.
    ZeroHedges,
    /// A zero rate-limit interval cannot refill or roll a window.
    ZeroInterval,
}

impl fmt::Display for InvalidPolicy {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::ZeroThreshold => "circuit breaker threshold must be at least one",
            Self::ZeroHedges => "hedge count must be at least one",
            Self::ZeroInterval => "rate limit interval must be non-zero",
        };
        output.write_str(message)
    }
}

impl Error for InvalidPolicy {}
