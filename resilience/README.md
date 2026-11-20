# neotales-resilience

A policy pipeline for timeouts, retries, circuit breakers, and load shedding.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-retry)

<!-- badges: crates.io, docs.rs, CI. Add once the first version exists. -->

## Install 📦

```sh
cargo add neotales-resilience
```

## What you can do 🛡️

**Retry handled failures.** Policies run from an ordered list, outermost first. Retry and hedging refuse a call that was not marked idempotent, so nothing starts until you opt in.

```rust
use std::time::Duration;

use neotales_resilience::{
    Class, Classified, Delay, Exec, Outcome, Pipeline, Policy, RetryPolicy,
};

let retry = RetryPolicy::new(2, Delay::Fixed(Duration::from_millis(1)))
    .expect("two attempts");
let pipeline = Pipeline::new([Policy::retry(retry)]);
let mut attempts = 0;
let outcome = pipeline.execute(
    |_flag| {
        attempts += 1;
        if attempts == 1 { Err("not yet") } else { Ok("done") }
    },
    &Exec::new().idempotent(),
    |_: Classified<'_, &str>| Class::Handled,
    |_| {},
    || 0,
    |_| {},
);
assert!(matches!(outcome, Outcome::Success { value: "done", attempt: 2 }));
```

**Replace a handled failure with a fallback.** The factory runs once, and its own error stays a failure rather than looping back through the fallback.

```rust
use neotales_resilience::{Class, Classified, Exec, Outcome, Pipeline, Policy};

let pipeline = Pipeline::new([Policy::fallback(|| Ok::<&str, &str>("cached"))]);
let outcome = pipeline.execute(
    |_flag| Err("down"),
    &Exec::new(),
    |_: Classified<'_, &str>| Class::Handled,
    |_| {},
    || 0,
    |_| {},
);
assert!(matches!(outcome, Outcome::Success { value: "cached", attempt: 1 }));
```

**Turn time in tests into a value.** A breaker and a rate limiter read the clock they were built with. `ManualClock` moves only when the test advances it, so no test sleeps.

```rust
use std::sync::Arc;
use std::time::Duration;

use neotales_resilience::{ManualClock, RateLimiter};

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
```

`[timeout, retry]` is one deadline across every retry; `[retry, timeout]` gives each attempt its own. An observer sees `Event` values, never payloads or error text, and a panicking observer cannot change the outcome.

## Licence

[MIT](LICENSE.md)
