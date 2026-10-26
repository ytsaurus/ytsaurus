use std::fmt;
use std::str::FromStr;

use crate::config::DateConfig;
use crate::numbers::int_parse_bytes;
use crate::util::timestamp_to_seconds_micros;
use crate::{get_digit_unchecked, DateTime, ParseError};

/// A Date
///
/// Allowed formats:
/// * `YYYY-MM-DD`
///
/// Leap years are correct calculated according to the Gregorian calendar.
/// Thus `2000-02-29` is a valid date, but `2001-02-29` is not.
///
/// # Comparison
///
/// `Date` supports equality (`==`) and inequality (`>`, `<`, `>=`, `<=`) comparisons.
///
/// ```
/// use speedate::Date;
///
/// let d1 = Date::parse_str("2022-01-01").unwrap();
/// let d2 = Date::parse_str("2022-01-02").unwrap();
/// assert!(d2 > d1);
/// ```
#[derive(Debug, PartialEq, Eq, PartialOrd, Clone, Copy)]
pub struct Date {
    /// Year: four digits
    pub year: u16,
    /// Month: 1 to 12
    pub month: u8,
    /// Day: 1 to {28, 29, 30, 31} (based on month & year)
    pub day: u8,
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut buf: [u8; 10] = *b"0000-00-00";
        crate::display_num_buf(4, 0, self.year as u32, &mut buf);
        crate::display_num_buf(2, 5, self.month as u32, &mut buf);
        crate::display_num_buf(2, 8, self.day as u32, &mut buf);
        f.write_str(std::str::from_utf8(&buf[..]).unwrap())
    }
}

impl FromStr for Date {
    type Err = ParseError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Delegate to parse_str, which is more permissive - users can call parse_str_rfc3339 directly instead if they
        // want to be stricter
        Self::parse_str(s)
    }
}

// 2e10 if greater than this, the number is in ms, if less than or equal, it's in seconds
// (in seconds this is 11th October 2603, in ms it's 20th August 1970)
pub(crate) const MS_WATERSHED: i64 = 20_000_000_000;
// 9999-12-31T23:59:59 as a unix timestamp, used as max allowed value below
const UNIX_9999: i64 = 253_402_300_799;
// 0000-01-01T00:00:00+00:00 as a unix timestamp, used as min allowed value below
const UNIX_0000: i64 = -62_167_219_200;

impl Date {
    /// Parse a date from a string using RFC 3339 format
    ///
    /// # Arguments
    ///
    /// * `str` - The string to parse
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::Date;
    ///
    /// let d = Date::parse_str_rfc3339("2020-01-01").unwrap();
    /// assert_eq!(
    ///     d,
    ///     Date {
    ///         year: 2020,
    ///         month: 1,
    ///         day: 1
    ///     }
    /// );
    /// assert_eq!(d.to_string(), "2020-01-01");
    /// ```
    #[inline]
    pub fn parse_str_rfc3339(str: &str) -> Result<Self, ParseError> {
        Self::parse_bytes_rfc3339(str.as_bytes())
    }

    /// Parse a date from a string using RFC 3339 format, or a unix timestamp.
    ///
    /// In the input is purely numeric, then the number is interpreted as a unix timestamp,
    /// using [`Date::from_timestamp`].
    ///
    /// # Arguments
    ///
    /// * `str` - The string to parse
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::Date;
    ///
    /// let d = Date::parse_str("2020-01-01").unwrap();
    /// assert_eq!(d.to_string(), "2020-01-01");
    /// let d = Date::parse_str("1577836800").unwrap();
    /// assert_eq!(d.to_string(), "2020-01-01");
    /// ```
    #[inline]
    pub fn parse_str(str: &str) -> Result<Self, ParseError> {
        Self::parse_bytes(str.as_bytes())
    }

    /// As with [`Date::parse_str`] but with a [`DateConfig`].
    #[inline]
    pub fn parse_str_with_config(str: &str, config: &DateConfig) -> Result<Self, ParseError> {
        Self::parse_bytes_with_config(str.as_bytes(), config)
    }

    /// Parse a date from bytes using RFC 3339 format
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to parse
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::Date;
    ///
    /// let d = Date::parse_bytes_rfc3339(b"2020-01-01").unwrap();
    /// assert_eq!(
    ///     d,
    ///     Date {
    ///         year: 2020,
    ///         month: 1,
    ///         day: 1
    ///     }
    /// );
    /// assert_eq!(d.to_string(), "2020-01-01");
    /// ```
    #[inline]
    pub fn parse_bytes_rfc3339(bytes: &[u8]) -> Result<Self, ParseError> {
        let d = Self::parse_bytes_partial(bytes)?;

        if bytes.len() > 10 {
            return Err(ParseError::ExtraCharacters);
        }

        Ok(d)
    }

    /// Parse a date from bytes using RFC 3339 format, or a unix timestamp.
    ///
    /// In the input is purely numeric, then the number is interpreted as a unix timestamp,
    /// using [`Date::from_timestamp`].
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to parse
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::Date;
    ///
    /// let d = Date::parse_bytes(b"2020-01-01").unwrap();
    /// assert_eq!(d.to_string(), "2020-01-01");
    ///
    /// let d = Date::parse_bytes(b"1577836800").unwrap();
    /// assert_eq!(d.to_string(), "2020-01-01");
    /// ```
    #[inline]
    pub fn parse_bytes(bytes: &[u8]) -> Result<Self, ParseError> {
        Self::parse_bytes_with_config(bytes, &DateConfig::default())
    }

    /// Same as [`Date::parse_bytes`] but with a [`DateConfig`].
    #[inline]
    pub fn parse_bytes_with_config(bytes: &[u8], config: &DateConfig) -> Result<Self, ParseError> {
        match Self::parse_bytes_rfc3339(bytes) {
            Ok(d) => Ok(d),
            Err(e) => match int_parse_bytes(bytes) {
                Some(int) => Self::from_timestamp(int, true, config),
                None => Err(e),
            },
        }
    }

    /// Create a date from a Unix Timestamp in seconds or milliseconds
    ///
    /// ("Unix Timestamp" means number of seconds or milliseconds since 1970-01-01)
    ///
    /// Input must be between `-62,167,219,200,000` (`0000-01-01`) and `253,402,300,799,000` (`9999-12-31`) inclusive.
    ///
    /// If the absolute value is > 2e10 (`20,000,000,000`) it is interpreted as being in milliseconds.
    ///
    /// That means:
    /// * `20,000,000,000` is `2603-10-11`
    /// * `20,000,000,001` is `1970-08-20`
    /// * `-62,167,219,200,001` gives an error - `DateTooSmall` as it would be before 0000-01-01
    /// * `-20,000,000,001` is `1969-05-14`
    /// * `-20,000,000,000` is `1336-03-23`
    ///
    /// # Arguments
    ///
    /// * `timestamp` - timestamp in either seconds or milliseconds
    /// * `require_exact` - if true, then the timestamp must be exactly at midnight, otherwise it will be rounded down
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::{Date, DateConfig};
    ///
    /// let d = Date::from_timestamp(1_654_560_000, true, &DateConfig::default()).unwrap();
    /// assert_eq!(d.to_string(), "2022-06-07");
    /// ```
    pub fn from_timestamp(timestamp: i64, require_exact: bool, config: &DateConfig) -> Result<Self, ParseError> {
        let (seconds, microseconds) = timestamp_to_seconds_micros(timestamp, config.timestamp_unit)?;
        let (d, remaining_seconds) = Self::from_timestamp_calc(seconds)?;
        if require_exact && (remaining_seconds != 0 || microseconds != 0) {
            return Err(ParseError::DateNotExact);
        }
        Ok(d)
    }

    /// Unix timestamp in seconds (number of seconds between self and 1970-01-01)
    ///
    /// # Example
    ///
    /// ```
    /// use speedate::Date;
    ///
    /// let d = Date::parse_str("2022-06-07").unwrap();
    /// assert_eq!(d.timestamp(), 1_654_560_000);
    /// ```
    pub fn timestamp(&self) -> i64 {
        let days =
            (self.year as i64) * 365 + (self.ordinal_day() - 1) as i64 + intervening_leap_years(self.year as i64);
        days * 86400 + UNIX_0000
    }

    /// Unix timestamp in milliseconds (number of milliseconds between self and 1970-01-01)
    ///
    /// # Example
    ///
    /// ```
    /// use speedate::Date;
    ///
    /// let d = Date::parse_str("2022-06-07").unwrap();
    /// assert_eq!(d.timestamp_ms(), 1_654_560_000_000);
    /// ```
    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp() * 1000
    }

    /// Current date. Internally, this uses [DateTime::now].
    ///
    /// # Arguments
    ///
    /// * `tz_offset` - timezone offset in seconds, meaning as per [DateTime::now], must be less than `86_400`
    ///
    /// # Example
    ///
    /// ```
    /// use speedate::Date;
    ///
    /// let d = Date::today(0).unwrap();
    /// println!("The date today is: {}", d)
    /// ```
    pub fn today(tz_offset: i32) -> Result<Self, ParseError> {
        Ok(DateTime::now(tz_offset)?.date)
    }

    /// Day of the year, starting from 1.
    #[allow(clippy::bool_to_int_with_if)]
    pub fn ordinal_day(&self) -> u16 {
        let leap_extra = if is_leap_year(self.year) { 1 } else { 0 };
        let day = self.day as u16;
        match self.month {
            1 => day,
            2 => day + 31,
            3 => day + 59 + leap_extra,
            4 => day + 90 + leap_extra,
            5 => day + 120 + leap_extra,
            6 => day + 151 + leap_extra,
            7 => day + 181 + leap_extra,
            8 => day + 212 + leap_extra,
            9 => day + 243 + leap_extra,
            10 => day + 273 + leap_extra,
            11 => day + 304 + leap_extra,
            _ => day + 334 + leap_extra,
        }
    }

    pub(crate) fn from_timestamp_calc(timestamp_second: i64) -> Result<(Self, u32), ParseError> {
        if timestamp_second < UNIX_0000 {
            return Err(ParseError::DateTooSmall);
        }
        if timestamp_second > UNIX_9999 {
            return Err(ParseError::DateTooLarge);
        }
        let seconds_diff = timestamp_second - UNIX_0000;
        let delta_days = seconds_diff / 86_400;
        let delta_years = delta_days / 365;
        let leap_years = intervening_leap_years(delta_years);

        // year day is the day of the year, starting from 1
        let mut ordinal_day: i16 = (delta_days % 365 - leap_years + 1) as i16;
        let mut year: u16 = delta_years as u16;
        let mut leap_year: bool = is_leap_year(year);
        while ordinal_day < 1 {
            year -= 1;
            leap_year = is_leap_year(year);
            ordinal_day += if leap_year { 366 } else { 365 };
        }
        let (month, day) = match leap_year {
            true => leap_year_month_day(ordinal_day),
            false => common_year_month_day(ordinal_day),
        };
        Ok((Self { year, month, day }, (timestamp_second.rem_euclid(86_400)) as u32))
    }

    /// Parse a date from bytes, no check is performed for extract characters at the end of the string
    pub(crate) fn parse_bytes_partial(bytes: &[u8]) -> Result<Self, ParseError> {
        if bytes.len() < 10 {
            return Err(ParseError::TooShort);
        }
        let year: u16;
        let month: u8;
        let day: u8;
        unsafe {
            let y1 = get_digit_unchecked!(bytes, 0, InvalidCharYear) as u16;
            let y2 = get_digit_unchecked!(bytes, 1, InvalidCharYear) as u16;
            let y3 = get_digit_unchecked!(bytes, 2, InvalidCharYear) as u16;
            let y4 = get_digit_unchecked!(bytes, 3, InvalidCharYear) as u16;
            year = y1 * 1000 + y2 * 100 + y3 * 10 + y4;

            match bytes.get_unchecked(4) {
                b'-' => (),
                _ => return Err(ParseError::InvalidCharDateSep),
            }

            let m1 = get_digit_unchecked!(bytes, 5, InvalidCharMonth);
            let m2 = get_digit_unchecked!(bytes, 6, InvalidCharMonth);
            month = m1 * 10 + m2;

            match bytes.get_unchecked(7) {
                b'-' => (),
                _ => return Err(ParseError::InvalidCharDateSep),
            }

            let d1 = get_digit_unchecked!(bytes, 8, InvalidCharDay);
            let d2 = get_digit_unchecked!(bytes, 9, InvalidCharDay);
            day = d1 * 10 + d2;
        }

        // calculate the maximum number of days in the month, accounting for leap years in the
        // gregorian calendar
        let max_days = match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => {
                if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                    29
                } else {
                    28
                }
            }
            _ => return Err(ParseError::OutOfRangeMonth),
        };

        if day < 1 || day > max_days {
            return Err(ParseError::OutOfRangeDay);
        }

        Ok(Self { year, month, day })
    }
}

fn is_leap_year(year: u16) -> bool {
    if year % 100 == 0 {
        year % 400 == 0
    } else {
        year % 4 == 0
    }
}

/// internal function to calculate the number of leap years since 0000, `delta_years` is the number of
/// years since 0000
fn intervening_leap_years(delta_years: i64) -> i64 {
    if delta_years == 0 {
        0
    } else {
        (delta_years - 1) / 4 - (delta_years - 1) / 100 + (delta_years - 1) / 400 + 1
    }
}

fn leap_year_month_day(day: i16) -> (u8, u8) {
    match day {
        1..=31 => (1, day as u8),
        32..=60 => (2, day as u8 - 31),
        61..=91 => (3, day as u8 - 60),
        92..=121 => (4, day as u8 - 91),
        122..=152 => (5, day as u8 - 121),
        153..=182 => (6, day as u8 - 152),
        183..=213 => (7, day as u8 - 182),
        214..=244 => (8, day as u8 - 213),
        245..=274 => (9, (day - 244) as u8),
        275..=305 => (10, (day - 274) as u8),
        306..=335 => (11, (day - 305) as u8),
        _ => (12, (day - 335) as u8),
    }
}

fn common_year_month_day(day: i16) -> (u8, u8) {
    match day {
        1..=31 => (1, day as u8),
        32..=59 => (2, day as u8 - 31),
        60..=90 => (3, day as u8 - 59),
        91..=120 => (4, day as u8 - 90),
        121..=151 => (5, day as u8 - 120),
        152..=181 => (6, day as u8 - 151),
        182..=212 => (7, day as u8 - 181),
        213..=243 => (8, day as u8 - 212),
        244..=273 => (9, (day - 243) as u8),
        274..=304 => (10, (day - 273) as u8),
        305..=334 => (11, (day - 304) as u8),
        _ => (12, (day - 334) as u8),
    }
}
