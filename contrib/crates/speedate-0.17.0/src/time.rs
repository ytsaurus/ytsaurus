use std::cmp::Ordering;
use std::default::Default;
use std::fmt;
use std::str::FromStr;

use crate::config::TimeConfigBuilder;
use crate::{get_digit, get_digit_unchecked, ConfigError, ParseError, TimeConfig};

/// A Time
///
/// Allowed formats:
/// * `HH:MM:SS`
/// * `HH:MM:SS.FFFFFF` 1 to 6 digits are allowed
/// * `HH:MM`
/// * `HH:MM:SSZ`
/// * `HH:MM:SS.FFFFFFZ`
///
/// Fractions of a second are to microsecond precision, if the value contains greater
/// precision, an error is raised.
///
/// # Comparison
///
/// `Time` supports equality (`==`) and inequality (`>`, `<`, `>=`, `<=`) comparisons.
///
/// See [Time::partial_cmp] for how this works.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Time {
    /// Hour: 0 to 23
    pub hour: u8,
    /// Minute: 0 to 59
    pub minute: u8,
    /// Second: 0 to 59
    pub second: u8,
    /// microseconds: 0 to 999999
    pub microsecond: u32,
    /// timezone offset in seconds if provided, must be >-24h and <24h
    // This range is to match python,
    // Note: [Stack Overflow suggests](https://stackoverflow.com/a/8131056/949890) larger offsets can happen
    pub tz_offset: Option<i32>,
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.microsecond != 0 {
            let mut buf: [u8; 15] = *b"00:00:00.000000";
            crate::display_num_buf(2, 0, self.hour as u32, &mut buf);
            crate::display_num_buf(2, 3, self.minute as u32, &mut buf);
            crate::display_num_buf(2, 6, self.second as u32, &mut buf);
            crate::display_num_buf(6, 9, self.microsecond, &mut buf);
            f.write_str(std::str::from_utf8(&buf[..]).unwrap())?
        } else {
            let mut buf: [u8; 8] = *b"00:00:00";
            crate::display_num_buf(2, 0, self.hour as u32, &mut buf);
            crate::display_num_buf(2, 3, self.minute as u32, &mut buf);
            crate::display_num_buf(2, 6, self.second as u32, &mut buf);
            f.write_str(std::str::from_utf8(&buf[..]).unwrap())?
        }
        if let Some(tz_offset) = self.tz_offset {
            if tz_offset == 0 {
                write!(f, "Z")?;
            } else {
                // tz offset is given in seconds, so we do convertions from seconds -> mins -> hours
                let total_minutes = tz_offset / 60;
                let hours = total_minutes / 60;
                let minutes = total_minutes % 60;
                let mut buf: [u8; 6] = *b"+00:00";
                if tz_offset < 0 {
                    buf[0] = b'-';
                }
                crate::display_num_buf(2, 1, hours.unsigned_abs(), &mut buf);
                crate::display_num_buf(2, 4, minutes.unsigned_abs(), &mut buf);
                f.write_str(std::str::from_utf8(&buf[..]).unwrap())?;
            }
        }
        Ok(())
    }
}

impl FromStr for Time {
    type Err = ParseError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_str(s)
    }
}

impl PartialOrd for Time {
    /// Compare two times by inequality.
    ///
    /// `Time` supports equality (`==`, `!=`) and inequality comparisons (`>`, `<`, `>=` & `<=`).
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::Time;
    ///
    /// let t1 = Time::parse_str("04:05:06.07").unwrap();
    /// let t2 = Time::parse_str("04:05:06.08").unwrap();
    ///
    /// assert!(t1 < t2);
    /// ```
    ///
    ///  # Comparison with Timezones
    ///
    /// When comparing two times, we want "less than" or "greater than" refer to "earlier" or "later"
    /// in the absolute course of time. We therefore need to be careful when comparing times with different
    /// timezones. (If it wasn't for timezones, we could omit all this extra logic and thinking and just compare
    /// struct members directly as we do with [crate::Date] and [crate::Duration]).
    ///
    /// See [crate::DateTime::partial_cmp] for more information about comparisons with timezones.
    ///
    /// ## Timezone Examples
    ///
    /// ```
    /// use speedate::Time;
    ///
    /// let t1 = Time::parse_str("15:00:00Z").unwrap();
    /// let t2 = Time::parse_str("15:00:00+01:00").unwrap();
    ///
    /// assert!(t1 > t2);
    ///
    /// let t3 = Time::parse_str("15:00:00-01:00").unwrap();
    /// let t4 = Time::parse_str("15:00:00+01:00").unwrap();
    ///
    /// assert!(t3 > t4);
    /// ```
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self.tz_offset, other.tz_offset) {
            (Some(tz_offset), Some(other_tz_offset)) => match (self.total_seconds() as i64 - tz_offset as i64)
                .partial_cmp(&(other.total_seconds() as i64 - other_tz_offset as i64))
            {
                Some(Ordering::Equal) => self.microsecond.partial_cmp(&other.microsecond),
                otherwise => otherwise,
            },
            _ => match self.total_seconds().partial_cmp(&other.total_seconds()) {
                Some(Ordering::Equal) => self.microsecond.partial_cmp(&other.microsecond),
                otherwise => otherwise,
            },
        }
    }
}

impl Time {
    /// Parse a time from a string
    ///
    /// # Arguments
    ///
    /// * `str` - The string to parse
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::Time;
    ///
    /// let d = Time::parse_str("12:13:14.123456").unwrap();
    /// assert_eq!(
    ///     d,
    ///     Time {
    ///         hour: 12,
    ///         minute: 13,
    ///         second: 14,
    ///         microsecond: 123456,
    ///         tz_offset: None,
    ///     }
    /// );
    /// assert_eq!(d.to_string(), "12:13:14.123456");
    /// ```
    #[inline]
    pub fn parse_str(str: &str) -> Result<Self, ParseError> {
        Self::parse_bytes(str.as_bytes())
    }

    /// Parse a time from bytes
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to parse
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::Time;
    ///
    /// let d = Time::parse_bytes(b"12:13:14.123456").unwrap();
    /// assert_eq!(
    ///     d,
    ///     Time {
    ///         hour: 12,
    ///         minute: 13,
    ///         second: 14,
    ///         microsecond: 123456,
    ///         tz_offset: None,
    ///     }
    /// );
    /// assert_eq!(d.to_string(), "12:13:14.123456");
    /// ```
    #[inline]
    pub fn parse_bytes(bytes: &[u8]) -> Result<Self, ParseError> {
        Self::parse_bytes_offset(bytes, 0, &TimeConfigBuilder::new().build())
    }

    /// Same as `Time::parse_bytes` but with a `TimeConfig`.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to parse
    /// * `config` - The `TimeConfig` to use
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::{Time, TimeConfigBuilder};
    ///
    /// let d = Time::parse_bytes_with_config(b"12:13:14.123456", &TimeConfigBuilder::new().build()).unwrap();
    /// assert_eq!(
    ///     d,
    ///     Time {
    ///         hour: 12,
    ///         minute: 13,
    ///         second: 14,
    ///         microsecond: 123456,
    ///         tz_offset: None,
    ///     }
    /// );
    /// assert_eq!(d.to_string(), "12:13:14.123456");
    /// ```
    #[inline]
    pub fn parse_bytes_with_config(bytes: &[u8], config: &TimeConfig) -> Result<Self, ParseError> {
        Self::parse_bytes_offset(bytes, 0, config)
    }

    /// Create a time from seconds and microseconds.
    ///
    /// # Arguments
    ///
    /// * `timestamp_second` - timestamp in seconds
    /// * `timestamp_microsecond` - microseconds fraction of a second timestamp
    ///
    /// If `seconds + timestamp_microsecond` exceeds 86400, an error is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::Time;
    ///
    /// let d = Time::from_timestamp(3740, 123).unwrap();
    /// assert_eq!(d.to_string(), "01:02:20.000123");
    /// ```
    pub fn from_timestamp(timestamp_second: u32, timestamp_microsecond: u32) -> Result<Self, ParseError> {
        Time::from_timestamp_with_config(
            timestamp_second,
            timestamp_microsecond,
            &TimeConfigBuilder::new().build(),
        )
    }

    /// Like `from_timestamp` but with a `TimeConfig`
    ///
    /// # Arguments
    ///
    /// * `timestamp_second` - timestamp in seconds
    /// * `timestamp_microsecond` - microseconds fraction of a second timestamp
    /// * `config` - the `TimeConfig` to use
    ///
    /// If `seconds + timestamp_microsecond` exceeds 86400, an error is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::{Time, TimeConfigBuilder};
    ///
    /// let d = Time::from_timestamp_with_config(3740, 123, &TimeConfigBuilder::new().build()).unwrap();
    /// assert_eq!(d.to_string(), "01:02:20.000123");
    /// ```
    pub fn from_timestamp_with_config(
        timestamp_second: u32,
        timestamp_microsecond: u32,
        config: &TimeConfig,
    ) -> Result<Self, ParseError> {
        let mut second = timestamp_second;
        let mut microsecond = timestamp_microsecond;
        if microsecond >= 1_000_000 {
            second = second
                .checked_add(microsecond / 1_000_000)
                .ok_or(ParseError::TimeTooLarge)?;
            microsecond %= 1_000_000;
        }
        if second >= 86_400 {
            return Err(ParseError::TimeTooLarge);
        }
        Ok(Self {
            hour: (second / 3600) as u8,
            minute: ((second % 3600) / 60) as u8,
            second: (second % 60) as u8,
            microsecond,
            tz_offset: config.unix_timestamp_offset,
        })
    }

    /// Parse a time from bytes with a starting index, extra characters at the end of the string result in an error
    pub(crate) fn parse_bytes_offset(bytes: &[u8], offset: usize, config: &TimeConfig) -> Result<Self, ParseError> {
        let pure_time = PureTime::parse(bytes, offset, config)?;

        // Parse the timezone offset
        let mut tz_offset: Option<i32> = None;
        let mut position = pure_time.position;

        if let Some(next_char) = bytes.get(position).copied() {
            position += 1;
            if next_char == b'Z' || next_char == b'z' {
                tz_offset = Some(0);
            } else {
                let sign = match next_char {
                    b'+' => 1,
                    b'-' => -1,
                    226 => {
                        // U+2212 MINUS "−" is allowed under ISO 8601 for negative timezones
                        // > python -c 'print([c for c in "−".encode()])'
                        // its raw byte values are [226, 136, 146]
                        if bytes.get(position).copied() != Some(136) {
                            return Err(ParseError::InvalidCharTzSign);
                        }
                        if bytes.get(position + 1).copied() != Some(146) {
                            return Err(ParseError::InvalidCharTzSign);
                        }
                        position += 2;
                        -1
                    }
                    _ => return Err(ParseError::InvalidCharTzSign),
                };

                let h1 = get_digit!(bytes, position, InvalidCharTzHour) as i32;
                let h2 = get_digit!(bytes, position + 1, InvalidCharTzHour) as i32;

                let m1 = match bytes.get(position + 2) {
                    Some(b':') => {
                        position += 3;
                        get_digit!(bytes, position, InvalidCharTzMinute) as i32
                    }
                    Some(c) if c.is_ascii_digit() => {
                        position += 2;
                        (c - b'0') as i32
                    }
                    _ => return Err(ParseError::InvalidCharTzMinute),
                };
                let m2 = get_digit!(bytes, position + 1, InvalidCharTzMinute) as i32;

                let minute_seconds = m1 * 600 + m2 * 60;
                if minute_seconds >= 3600 {
                    return Err(ParseError::OutOfRangeTzMinute);
                }

                let offset_val = sign * (h1 * 36000 + h2 * 3600 + minute_seconds);
                // TZ must be less than 24 hours to match python
                if offset_val.abs() >= 24 * 3600 {
                    return Err(ParseError::OutOfRangeTz);
                }
                tz_offset = Some(offset_val);
                position += 2;
            }
        }

        if bytes.len() > position {
            return Err(ParseError::ExtraCharacters);
        }

        Ok(Self {
            hour: pure_time.hour,
            minute: pure_time.minute,
            second: pure_time.second,
            microsecond: pure_time.microsecond,
            tz_offset,
        })
    }

    /// Get the total seconds of the time
    ///
    /// E.g. hours + minutes + seconds
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::Time;
    ///
    /// let d = Time::parse_str("12:13:14.123456").unwrap();
    /// assert_eq!(d.total_seconds(), 12 * 3600 + 13 * 60 + 14);
    /// ```
    pub fn total_seconds(&self) -> u32 {
        let mut total_seconds = self.hour as u32 * 3600;
        total_seconds += self.minute as u32 * 60;
        total_seconds += self.second as u32;
        total_seconds
    }

    /// Get the total milliseconds of the time.
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::Time;
    ///
    /// let d = Time::parse_str("12:13:14.123456").unwrap();
    /// assert_eq!(d.total_ms(), (12 * 3600 + 13 * 60 + 14) * 1000 + 123);
    /// ```
    pub fn total_ms(&self) -> u32 {
        self.total_seconds() * 1000 + self.microsecond / 1000
    }

    /// Clone the time and set a new timezone offset.
    ///
    /// The returned time will represent a different point in time since the timezone offset is changed without
    /// modifying the time. See [Time::in_timezone] for alternative behaviour.
    ///
    /// # Arguments
    ///
    /// * `tz_offset` - optional timezone offset in seconds.
    ///
    /// This method will return `Err(ParseError::OutOfRangeTz)` if `abs(tz_offset)` is not less than
    /// 24 hours - `86_400` seconds.
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::Time;
    ///
    /// let t1 = Time::parse_str("12:13:14Z").unwrap();
    ///
    /// let t2 = t1.with_timezone_offset(Some(-8 * 3600)).unwrap();
    /// assert_eq!(t2.to_string(), "12:13:14-08:00");
    /// ```
    pub fn with_timezone_offset(&self, tz_offset: Option<i32>) -> Result<Self, ParseError> {
        if let Some(offset_val) = tz_offset {
            if offset_val.abs() >= 24 * 3600 {
                return Err(ParseError::OutOfRangeTz);
            }
        }
        let mut time = *self;
        time.tz_offset = tz_offset;
        Ok(time)
    }

    /// Create a new time in a different timezone.
    /// See [Time::with_timezone_offset] for alternative behaviour.
    ///
    /// The time must have an offset, otherwise a `ParseError::TzRequired` error is returned.
    ///
    /// # Arguments
    ///
    /// * `tz_offset` - new timezone offset in seconds.
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::Time;
    ///
    /// let t1 = Time::parse_str("15:00:00Z").unwrap();
    ///
    /// let t2 = t1.in_timezone(7200).unwrap();
    // / assert_eq!(t2.to_string(), "17:00:00+02:00");
    /// ```
    pub fn in_timezone(&self, tz_offset: i32) -> Result<Self, ParseError> {
        if tz_offset.abs() >= 24 * 3600 {
            Err(ParseError::OutOfRangeTz)
        } else if let Some(current_offset) = self.tz_offset {
            let offset = tz_offset - current_offset;
            let seconds = self.total_seconds().saturating_add_signed(offset);
            let mut time = Self::from_timestamp(seconds, self.microsecond)?;
            time.tz_offset = Some(offset);
            Ok(time)
        } else {
            Err(ParseError::TzRequired)
        }
    }
}

/// Used internally for parsing both times and durations from time format
pub(crate) struct PureTime {
    /// Hour: 0 to 23
    hour: u8,
    /// Minute: 0 to 59
    minute: u8,
    /// Second: 0 to 59
    second: u8,
    /// microseconds: 0 to 999999
    pub microsecond: u32,
    /// position of the cursor after parsing
    pub position: usize,
}

impl PureTime {
    pub fn parse(bytes: &[u8], offset: usize, config: &TimeConfig) -> Result<Self, ParseError> {
        if bytes.len() - offset < 5 {
            return Err(ParseError::TooShort);
        }
        let hour: u8;
        let minute: u8;
        unsafe {
            let h1 = get_digit_unchecked!(bytes, offset, InvalidCharHour);
            let h2 = get_digit_unchecked!(bytes, offset + 1, InvalidCharHour);
            hour = h1 * 10 + h2;

            match bytes.get_unchecked(offset + 2) {
                b':' => (),
                _ => return Err(ParseError::InvalidCharTimeSep),
            }
            let m1 = get_digit_unchecked!(bytes, offset + 3, InvalidCharMinute);
            let m2 = get_digit_unchecked!(bytes, offset + 4, InvalidCharMinute);
            minute = m1 * 10 + m2;
        }

        if hour > 23 {
            return Err(ParseError::OutOfRangeHour);
        }

        if minute > 59 {
            return Err(ParseError::OutOfRangeMinute);
        }

        let mut length: usize = 5;
        let (second, microsecond) = match bytes.get(offset + 5) {
            Some(b':') => {
                let s1 = get_digit!(bytes, offset + 6, InvalidCharSecond);
                let s2 = get_digit!(bytes, offset + 7, InvalidCharSecond);
                let second = s1 * 10 + s2;
                if second > 59 {
                    return Err(ParseError::OutOfRangeSecond);
                }
                length = 8;

                let mut microsecond = 0;
                let frac_sep = bytes.get(offset + 8).copied();
                if frac_sep == Some(b'.') || frac_sep == Some(b',') {
                    length = 9;
                    let mut i: usize = 0;
                    loop {
                        match bytes.get(offset + length + i) {
                            Some(c) if c.is_ascii_digit() => {
                                // If we've passed `i=6` then we are "truncating" the extra precision
                                // The easiest way to do this is to simply no-op and continue the loop
                                if i < 6 {
                                    microsecond *= 10;
                                    microsecond += (c - b'0') as u32;
                                }
                            }
                            _ => {
                                break;
                            }
                        }
                        i += 1;
                        if i > 6 {
                            match config.microseconds_precision_overflow_behavior {
                                MicrosecondsPrecisionOverflowBehavior::Truncate => continue,
                                MicrosecondsPrecisionOverflowBehavior::Error => {
                                    return Err(ParseError::SecondFractionTooLong)
                                }
                            }
                        }
                    }
                    if i == 0 {
                        return Err(ParseError::SecondFractionMissing);
                    }
                    if i < 6 {
                        microsecond *= 10_u32.pow(6 - i as u32);
                    }
                    length += i;
                }
                (second, microsecond)
            }
            _ => (0, 0),
        };

        Ok(Self {
            hour,
            minute,
            second,
            microsecond,
            position: offset + length,
        })
    }

    pub fn total_seconds(&self) -> u32 {
        self.hour as u32 * 3_600 + self.minute as u32 * 60 + self.second as u32
    }
}

#[derive(Debug, Clone, Default, Copy, PartialEq)]
pub enum MicrosecondsPrecisionOverflowBehavior {
    Truncate,
    #[default]
    Error,
}

impl FromStr for MicrosecondsPrecisionOverflowBehavior {
    type Err = ConfigError;
    fn from_str(value: &str) -> Result<Self, ConfigError> {
        match value.to_lowercase().as_str() {
            "truncate" => Ok(Self::Truncate),
            "error" => Ok(Self::Error),
            _ => Err(ConfigError::UnknownMicrosecondsPrecisionOverflowBehaviorString),
        }
    }
}
