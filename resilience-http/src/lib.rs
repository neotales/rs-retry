#![doc = include_str!("../README.md")]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use http::{HeaderValue, Method, StatusCode};

/// Seconds in one day, used to turn a calendar date into an instant.
const SECONDS_PER_DAY: i64 = 86_400;

/// Whether a response status is worth retrying.
///
/// `408 Request Timeout`, `429 Too Many Requests`, and every `5xx` status are
/// retryable. Nothing else is, including `400` and `404`: repeating the same
/// request cannot change those outcomes.
#[must_use]
pub const fn retry_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500..=599)
}

/// Reads a `Retry-After` value as a wait no longer than `max`.
///
/// The value is either delta-seconds (`120`) or an HTTP-date
/// (`Sun, 06 Nov 1994 08:49:37 GMT`). A date that has already passed gives
/// [`Duration::ZERO`]. A missing header, a value that is not UTF-8, or a value
/// in neither form gives `None`.
///
/// `now` is supplied by the caller, so this crate reads no clock. `max` caps a
/// server that asks for a longer wait than the caller will accept.
///
/// The value is never logged or echoed: an unparseable header is reported as
/// `None` and its text goes no further.
#[must_use]
pub fn retry_after(
    value: Option<&HeaderValue>,
    now: SystemTime,
    max: Duration,
) -> Option<Duration> {
    let text = value?.to_str().ok()?;
    let text = text.trim_ascii();
    if text.is_empty() {
        return None;
    }
    let wait = if text.bytes().all(|byte| byte.is_ascii_digit()) {
        Duration::from_secs(parse_delta_seconds(text))
    } else {
        let at = parse_http_date(text)?;
        let elapsed = at.saturating_sub(unix_seconds(now));
        // A date in the past means "now", never a negative wait.
        Duration::from_secs(u64::try_from(elapsed).unwrap_or(0))
    };
    Some(wait.min(max))
}

/// Whether `method` may be retried when the request body can be reused.
///
/// `GET`, `HEAD`, `OPTIONS`, `TRACE`, `PUT`, and `DELETE` may be retried.
/// `POST`, `PATCH`, `CONNECT`, and any method this crate does not know are not:
/// the caller is better placed to know whether an unknown method is idempotent.
///
/// `body_rewindable` is the caller's answer to "can this body be sent again?".
/// When it is `false` no method is retryable, because a body that cannot be
/// replayed makes the second attempt send something different from the first.
/// The function never reads the body itself.
#[must_use]
pub fn may_retry_method(method: &Method, body_rewindable: bool) -> bool {
    if !body_rewindable {
        return false;
    }
    matches!(
        method,
        &Method::GET
            | &Method::HEAD
            | &Method::OPTIONS
            | &Method::TRACE
            | &Method::PUT
            | &Method::DELETE
    )
}

/// Parses delta-seconds, saturating a count too large for `u64`.
///
/// The caller has already checked that every byte is a digit.
fn parse_delta_seconds(value: &str) -> u64 {
    value.bytes().fold(0_u64, |total, byte| {
        total
            .saturating_mul(10)
            .saturating_add(u64::from(byte - b'0'))
    })
}

/// Converts an instant to signed seconds since the Unix epoch.
///
/// A time before 1970 is negative rather than an error, so a clock set to
/// anything sane still produces a comparable number.
fn unix_seconds(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(elapsed) => i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX),
        Err(before) => {
            i64::try_from(before.duration().as_secs()).map_or(i64::MIN, i64::saturating_neg)
        }
    }
}

/// Parses any of the three HTTP-date forms into seconds since the epoch.
///
/// The preferred form is IMF-fixdate. The two obsolete forms, RFC 850 and
/// asctime, are still emitted by deployed servers, so they are accepted.
fn parse_http_date(value: &str) -> Option<i64> {
    parse_imf_fixdate(value)
        .or_else(|| parse_rfc850(value))
        .or_else(|| parse_asctime(value))
}

/// Parses `Sun, 06 Nov 1994 08:49:37 GMT`.
fn parse_imf_fixdate(value: &str) -> Option<i64> {
    let (weekday, rest) = value.split_once(", ")?;
    if !is_short_weekday(weekday) {
        return None;
    }
    let mut parts = rest.split(' ');
    let day = parse_digits(parts.next()?)?;
    let month = month_number(parts.next()?)?;
    let year = parse_digits(parts.next()?)?;
    let clock = parse_clock(parts.next()?)?;
    if !parts.next()?.eq_ignore_ascii_case("GMT") || parts.next().is_some() {
        return None;
    }
    civil_seconds(year, month, day, clock)
}

/// Parses `Sunday, 06-Nov-94 08:49:37 GMT`.
fn parse_rfc850(value: &str) -> Option<i64> {
    let (weekday, rest) = value.split_once(", ")?;
    if weekday.is_empty() || !weekday.bytes().all(|byte| byte.is_ascii_alphabetic()) {
        return None;
    }
    let mut parts = rest.split_whitespace();
    let date = parts.next()?;
    let clock = parse_clock(parts.next()?)?;
    if !parts.next()?.eq_ignore_ascii_case("GMT") || parts.next().is_some() {
        return None;
    }
    let mut date_parts = date.split('-');
    let day = parse_digits(date_parts.next()?)?;
    let month = month_number(date_parts.next()?)?;
    let year = two_digit_year(parse_digits(date_parts.next()?)?)?;
    if date_parts.next().is_some() {
        return None;
    }
    civil_seconds(year, month, day, clock)
}

/// Parses `Sun Nov  6 08:49:37 1994`.
fn parse_asctime(value: &str) -> Option<i64> {
    let mut parts = value.split_whitespace();
    if !is_short_weekday(parts.next()?) {
        return None;
    }
    let month = month_number(parts.next()?)?;
    let day = parse_digits(parts.next()?)?;
    let clock = parse_clock(parts.next()?)?;
    let year = parse_digits(parts.next()?)?;
    if parts.next().is_some() {
        return None;
    }
    civil_seconds(year, month, day, clock)
}

/// Interprets a two-digit year as the nearest past year, per RFC 9110.
///
/// `00` through `69` are the 2000s and `70` through `99` are the 1900s.
fn two_digit_year(year: i64) -> Option<i64> {
    match year {
        0..=69 => Some(2_000 + year),
        70..=99 => Some(1_900 + year),
        _ => None,
    }
}

/// Turns a valid calendar date and clock time into seconds since the epoch.
///
/// Every field is range-checked, so an impossible date such as `31 Feb` is
/// rejected instead of silently rolled over into March.
fn civil_seconds(year: i64, month: i64, day: i64, clock: (i64, i64, i64)) -> Option<i64> {
    let (hour, minute, second) = clock;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    if day > days_in_month(year, month) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(days_from_civil(year, month, day) * SECONDS_PER_DAY + hour * 3_600 + minute * 60 + second)
}

/// Days from the Unix epoch to a civil date.
///
/// This is Howard Hinnant's `days_from_civil`: it counts whole days for a
/// proleptic Gregorian calendar, so it needs no table of leap years.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_of_year = (month + 9) % 12;
    let day_of_year = (153 * month_of_year + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// The number of days in a month, honouring the Gregorian leap year rule.
fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

/// Whether `year` is a leap year in the proleptic Gregorian calendar.
fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Parses `HH:MM:SS` into its three fields without range-checking them.
fn parse_clock(value: &str) -> Option<(i64, i64, i64)> {
    let mut parts = value.split(':');
    let hour = parse_digits(parts.next()?)?;
    let minute = parse_digits(parts.next()?)?;
    let second = parse_digits(parts.next()?)?;
    if parts.next().is_some() {
        return None;
    }
    Some((hour, minute, second))
}

/// Parses a non-empty run of ASCII digits as a number.
fn parse_digits(value: &str) -> Option<i64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

/// The month number for a three-letter English month name.
fn month_number(name: &str) -> Option<i64> {
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let index = MONTHS
        .iter()
        .position(|month| month.eq_ignore_ascii_case(name))?;
    i64::try_from(index).ok().map(|index| index + 1)
}

/// The day-of-week names HTTP servers send in a date.
fn is_short_weekday(name: &str) -> bool {
    const WEEKDAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    WEEKDAYS.iter().any(|day| day.eq_ignore_ascii_case(name))
}
