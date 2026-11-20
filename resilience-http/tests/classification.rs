//! Behaviour of the public classification functions.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use http::{HeaderValue, Method, StatusCode};
use neotales_resilience_http::{may_retry_method, retry_after, retry_status};

/// The epoch second of `Sun, 06 Nov 1994 08:49:37 GMT`.
const DATE_EPOCH: u64 = 784_111_777;

/// A maximum long enough that only the header decides the wait.
const MAX: Duration = Duration::from_secs(3_600);

/// An instant at `epoch_seconds` after the Unix epoch.
fn at(epoch_seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(epoch_seconds)
}

/// A header value from a literal.
fn header(value: &'static str) -> HeaderValue {
    HeaderValue::from_static(value)
}

#[test]
fn status_table_covers_every_code() {
    for code in 100..=999 {
        let status = StatusCode::from_u16(code).expect("100..=999 is a valid status");
        let expected = matches!(code, 408 | 429 | 500..=599);
        assert_eq!(retry_status(status), expected, "status {code}");
    }

    assert!(retry_status(StatusCode::REQUEST_TIMEOUT));
    assert!(retry_status(StatusCode::TOO_MANY_REQUESTS));
    assert!(retry_status(StatusCode::INTERNAL_SERVER_ERROR));
    assert!(!retry_status(StatusCode::BAD_REQUEST));
    assert!(!retry_status(StatusCode::NOT_FOUND));
    assert!(!retry_status(StatusCode::OK));
}

#[test]
fn delta_seconds_is_read_as_a_wait() {
    let now = at(DATE_EPOCH);
    assert_eq!(
        retry_after(Some(&header("120")), now, MAX),
        Some(Duration::from_secs(120))
    );
    assert_eq!(
        retry_after(Some(&header("0")), now, MAX),
        Some(Duration::ZERO)
    );

    // Surrounding whitespace is not part of the number.
    let padded = HeaderValue::from_str(" 120 ").expect("a valid header value");
    assert_eq!(
        retry_after(Some(&padded), now, MAX),
        Some(Duration::from_secs(120))
    );
}

#[test]
fn http_date_is_read_against_the_caller_clock() {
    let date = header("Sun, 06 Nov 1994 08:49:37 GMT");

    // The caller's clock is one minute before the date.
    assert_eq!(
        retry_after(Some(&date), at(DATE_EPOCH - 60), MAX),
        Some(Duration::from_secs(60))
    );

    // A date already in the past means wait nothing, not a negative delay.
    assert_eq!(
        retry_after(Some(&date), at(DATE_EPOCH + 60), MAX),
        Some(Duration::ZERO)
    );
}

#[test]
fn obsolete_date_forms_are_accepted() {
    let now = at(DATE_EPOCH - 60);
    let expected = Some(Duration::from_secs(60));

    assert_eq!(
        retry_after(Some(&header("Sunday, 06-Nov-94 08:49:37 GMT")), now, MAX),
        expected
    );
    assert_eq!(
        retry_after(Some(&header("Sun Nov  6 08:49:37 1994")), now, MAX),
        expected
    );
}

#[test]
fn the_caller_maximum_caps_both_forms() {
    let max = Duration::from_secs(30);
    let date = header("Sun, 06 Nov 1994 08:49:37 GMT");

    assert_eq!(
        retry_after(Some(&header("3600")), at(DATE_EPOCH), max),
        Some(max)
    );
    assert_eq!(
        retry_after(Some(&date), at(DATE_EPOCH - 3_600), max),
        Some(max)
    );

    // Zero is a valid cap: the header is understood, but no wait is allowed.
    assert_eq!(
        retry_after(Some(&header("120")), at(DATE_EPOCH), Duration::ZERO),
        Some(Duration::ZERO)
    );
}

#[test]
fn absent_or_unparseable_values_are_none() {
    let now = at(DATE_EPOCH);
    assert_eq!(retry_after(None, now, MAX), None);

    for value in [
        "",
        " ",
        "soon",
        "12x",
        "-1",
        "12 34",
        "Sun, 06 Nov 1994 08:49:37",
        "1994-11-06T08:49:37Z",
        // The 31st of February is not a date.
        "Sun, 31 Feb 1994 08:49:37 GMT",
    ] {
        let value = HeaderValue::from_str(value).expect("a valid header value");
        assert_eq!(
            retry_after(Some(&value), now, MAX),
            None,
            "unparseable value"
        );
    }

    // A value with bytes outside UTF-8 is unparseable, not an error to report.
    let binary = HeaderValue::from_bytes(&[0xff, 0xfe]).expect("opaque header bytes are valid");
    assert_eq!(retry_after(Some(&binary), now, MAX), None);
}

#[test]
fn method_rules_match_the_contract() {
    for method in [
        Method::GET,
        Method::HEAD,
        Method::OPTIONS,
        Method::TRACE,
        Method::PUT,
        Method::DELETE,
    ] {
        assert!(may_retry_method(&method, true), "{method}");
        assert!(
            !may_retry_method(&method, false),
            "{method} with a body that cannot be replayed"
        );
    }

    for method in [Method::POST, Method::PATCH, Method::CONNECT] {
        assert!(!may_retry_method(&method, true), "{method}");
        assert!(!may_retry_method(&method, false), "{method}");
    }

    // An unknown method is left to the caller to classify.
    let custom = Method::from_bytes(b"BREW").expect("a valid method");
    assert!(!may_retry_method(&custom, true));
}
