# neotales-resilience-http

HTTP classification and replay checks for a retry decision.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-retry)

<!-- badges: crates.io, docs.rs, CI. Add once the first version exists. -->

## Install 📦

```sh
cargo add neotales-resilience-http
```

## What you can do 🌐

**Ask whether a status is worth retrying.** `408`, `429`, and every `5xx` are retryable. `400`, `404`, and the rest are not.

```rust
use http::StatusCode;
use neotales_resilience_http::retry_status;

assert!(retry_status(StatusCode::TOO_MANY_REQUESTS));
assert!(retry_status(StatusCode::SERVICE_UNAVAILABLE));
assert!(!retry_status(StatusCode::BAD_REQUEST));
assert!(!retry_status(StatusCode::NOT_FOUND));
```

**Read `Retry-After` against your own clock.** The header is delta-seconds or an HTTP-date. The caller supplies the current time and the longest wait it will accept, so the crate reads no clock and never logs the header text.

```rust
use std::time::{Duration, SystemTime};

use http::HeaderValue;
use neotales_resilience_http::retry_after;

let wait = retry_after(
    Some(&HeaderValue::from_static("120")),
    SystemTime::now(),
    Duration::from_secs(30),
);
assert_eq!(wait, Some(Duration::from_secs(30)));
```

**Decide whether the body can be sent again.** `GET`, `HEAD`, `OPTIONS`, `TRACE`, `PUT`, and `DELETE` pass when the caller reports the body as rewindable. `POST`, `PATCH`, `CONNECT`, and unknown methods do not.

```rust
use http::Method;
use neotales_resilience_http::may_retry_method;

assert!(may_retry_method(&Method::GET, true));
assert!(!may_retry_method(&Method::GET, false));
assert!(!may_retry_method(&Method::POST, true));
```

The crate sends no request and owns no retry loop. Pass its answers into the predicate of a retry policy such as `neotales-retry`.

## Licence

[MIT](LICENSE.md)
