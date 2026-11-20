# neotales-retry

An immutable retry policy with fixed, exponential, and jittered backoff.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-retry)

<!-- badges: crates.io, docs.rs, CI. Add once the first version exists. -->

## Install 📦

```sh
cargo add neotales-retry
```

## What you can do 🔁

**Retry with a fixed delay.** `max_attempts` counts the first try, so `3` means one try and at most two retries. The sleeper receives each wait; pass `std::thread::sleep` to actually block.

```rust
use std::time::Duration;

use neotales_retry::{Delay, RetryPolicy};

let policy = RetryPolicy::new(3, Delay::Fixed(Duration::from_millis(50)))
    .expect("three attempts");
let mut attempts = 0;
let result: Result<&str, &str> = policy.run_with_sleeper(
    || {
        attempts += 1;
        if attempts < 3 { Err("not yet") } else { Ok("done") }
    },
    |_| true,
    || 0,
    // A stand-in sleeper: it returns at once, so the example never waits.
    |_| {},
);
assert_eq!(result, Ok("done"));
```

**Back off exponentially within a budget.** The delay doubles from `base` and stops growing at `max`. `with_budget` refuses another attempt once the elapsed time is gone; it never cancels an attempt already in flight.

```rust
use std::time::Duration;

use neotales_retry::{Delay, RetryPolicy};

let policy = RetryPolicy::new(
    5,
    Delay::Exponential {
        base: Duration::from_millis(10),
        max: Duration::from_millis(200),
    },
)
.expect("five attempts")
.with_budget(Duration::from_secs(10));
let mut waits = Vec::new();
let mut observed = Vec::new();
let result: Result<(), &str> = policy.run_with_sleeper_and_observer(
    || Err("down"),
    |_| true,
    || 0,
    |wait| waits.push(wait),
    // The attempt number that failed and the wait that follows it.
    |attempt, wait| observed.push((attempt, wait)),
);
assert!(result.is_err());
assert_eq!(waits.len(), 4);
assert_eq!(observed.len(), 4);
assert_eq!(observed[0], (1, Duration::from_millis(10)));
```

**Spread retries with jitter.** `JitteredExponential` draws the next wait between `base` and three times the previous wait, so concurrent callers stop retrying in lockstep. The caller supplies the random source.

```rust
use std::time::Duration;

use neotales_retry::{Delay, RetryPolicy};

let policy = RetryPolicy::new(
    3,
    Delay::JitteredExponential {
        base: Duration::from_millis(10),
        max: Duration::from_millis(100),
    },
)
.expect("three attempts");
let mut waits = Vec::new();
let _ = policy.run_with_sleeper(
    || Err::<(), _>("down"),
    |_| true,
    || 42,
    |wait| waits.push(wait),
);
assert_eq!(waits.len(), 2);
```

The asynchronous path is `run_async`, which takes a sleeper that returns a future. The crate has no runtime and spawns nothing, so dropping the returned future is how a caller stops waiting.

```rust
use std::time::Duration;

use neotales_retry::{Delay, RetryPolicy};

async fn fetch() -> Result<&'static str, &'static str> {
    RetryPolicy::new(3, Delay::Fixed(Duration::from_millis(20)))
        .expect("three attempts")
        .run_async(|| async { Ok("done") }, |_| true, || 0, |_| async {})
        .await
}

// The future is built here; the reader's executor drives it.
let _ = fetch();
```

## Licence

[MIT](LICENSE.md)
