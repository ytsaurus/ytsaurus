#![doc = include_str ! ("../README.md")]
extern crate core;
extern crate strum;

use strum::{Display, EnumMessage};

mod config;
mod date;
mod datetime;
mod duration;
mod numbers;
mod time;
mod util;

pub use config::{
    DateConfig, DateConfigBuilder, DateTimeConfig, DateTimeConfigBuilder, TimeConfig, TimeConfigBuilder, TimestampUnit,
};
pub use date::Date;
pub use datetime::DateTime;
pub use duration::Duration;
pub use time::{MicrosecondsPrecisionOverflowBehavior, Time};

pub use numbers::{float_parse_bytes, float_parse_str, int_parse_bytes, int_parse_str, IntFloat};

// Parsing datetime, date, time & duration values

// get a character from the bytes as as a decimal
macro_rules! get_digit {
    ($bytes:ident, $index:expr, $error:ident) => {
        match $bytes.get($index) {
            Some(c) if c.is_ascii_digit() => c - b'0',
            _ => return Err(ParseError::$error),
        }
    };
}
pub(crate) use get_digit;

// as above without bounds check, requires length to checked first!
macro_rules! get_digit_unchecked {
    ($bytes:ident, $index:expr, $error:ident) => {
        match $bytes.get_unchecked($index) {
            c if c.is_ascii_digit() => c - b'0',
            _ => return Err(ParseError::$error),
        }
    };
}
pub(crate) use get_digit_unchecked;

/// Details about errors when parsing datetime, date, time & duration values
///
/// As well as comparing enum values, machine and human readable representations of
/// errors are provided.
///
/// # Examples
/// (Note: the `strum::EnumMessage` trait must be used to support `.get_documentation()`)
/// ```
/// use strum::EnumMessage;
/// use speedate::{Date, ParseError};
///
/// match Date::parse_str("invalid") {
///      Ok(_) => println!("Parsed successfully"),
///      Err(error) => {
///          assert_eq!(error, ParseError::TooShort);
///          assert_eq!(error.to_string(), "too_short");
///          assert_eq!(error.get_documentation(), Some("input is too short"));
///      }
/// };
/// ```
#[derive(Debug, Display, EnumMessage, PartialEq, Eq, Clone)]
#[strum(serialize_all = "snake_case")]
pub enum ParseError {
    /// input is too short
    TooShort,
    /// unexpected extra characters at the end of the input
    ExtraCharacters,
    /// invalid datetime separator, expected `T`, `t`, `_` or space
    InvalidCharDateTimeSep,
    /// invalid date separator, expected `-`
    InvalidCharDateSep,
    /// Timestamp is not an exact date
    DateNotExact,
    /// invalid character in year
    InvalidCharYear,
    /// invalid character in month
    InvalidCharMonth,
    /// invalid character in day
    InvalidCharDay,
    /// invalid time separator, expected `:`
    InvalidCharTimeSep,
    /// invalid character in hour
    InvalidCharHour,
    /// invalid character in minute
    InvalidCharMinute,
    /// invalid character in second
    InvalidCharSecond,
    /// invalid character in second fraction
    InvalidCharSecondFraction,
    /// invalid timezone sign
    InvalidCharTzSign,
    /// invalid timezone hour
    InvalidCharTzHour,
    /// invalid timezone minute
    InvalidCharTzMinute,
    /// timezone minute value is outside expected range of 0-59
    OutOfRangeTzMinute,
    /// timezone offset must be less than 24 hours
    OutOfRangeTz,
    /// timezone is required to adjust to a new timezone
    TzRequired,
    /// Error getting system time
    SystemTimeError,
    /// month value is outside expected range of 1-12
    OutOfRangeMonth,
    /// day value is outside expected range
    OutOfRangeDay,
    /// hour value is outside expected range of 0-23
    OutOfRangeHour,
    /// minute value is outside expected range of 0-59
    OutOfRangeMinute,
    /// second value is outside expected range of 0-59
    OutOfRangeSecond,
    /// second fraction value is more than 6 digits long
    SecondFractionTooLong,
    /// second fraction digits missing after `.`
    SecondFractionMissing,
    /// millisecond fraction value is more than 3 digits long
    MillisecondFractionTooLong,
    /// invalid digit in duration
    DurationInvalidNumber,
    /// `t` character repeated in duration
    DurationTRepeated,
    /// quantity fraction invalid in duration
    DurationInvalidFraction,
    /// quantity invalid in time part of duration
    DurationInvalidTimeUnit,
    /// quantity invalid in date part of duration
    DurationInvalidDateUnit,
    /// "day" identifier in duration not correctly formatted
    DurationInvalidDays,
    /// a numeric value in the duration is too large
    DurationValueTooLarge,
    /// durations may not exceed 999,999,999 hours
    DurationHourValueTooLarge,
    /// durations may not exceed 999,999,999 days
    DurationDaysTooLarge,
    /// dates before 0000 are not supported as unix timestamps
    DateTooSmall,
    /// dates after 9999 are not supported as unix timestamps
    DateTooLarge,
    /// numeric times may not exceed 86,399 seconds
    TimeTooLarge,
}

#[derive(Debug, Display, EnumMessage, PartialEq, Eq, Clone)]
#[strum(serialize_all = "snake_case")]
pub enum ConfigError {
    // SecondsPrecisionOverflowBehavior string representation, must be one of "error" or "truncate"
    UnknownMicrosecondsPrecisionOverflowBehaviorString,
    // TimestampUnit string representation, must be one of "s", "ms" or "infer"
    UnknownTimestampUnitString,
}

/// Used internally to write numbers to a buffer for `Display` of speedate types
fn display_num_buf(num: usize, start: usize, value: u32, buf: &mut [u8]) {
    for i in 0..num {
        if (i + 1) == num {
            buf[i + start] = b'0' + (value % 10) as u8;
        } else if num <= 2 {
            buf[i + start] = b'0' + (value / (10i32.pow((num - 1 - i) as u32)) as u32) as u8;
        } else {
            buf[i + start] = b'0' + (value / (10i32.pow((num - 1 - i) as u32)) as u32 % 10) as u8;
        }
    }
}
