use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use crate::{ParseError, TimeConfig, TimeConfigBuilder};

/// A Duration
///
/// Allowed values:
/// * `PnYnMnDTnHnMnS` - ISO 8601 duration format,
///   see [wikipedia](https://en.wikipedia.org/wiki/ISO_8601#Durations) for more details,
///   `W` for weeks is also allowed before the `T` separator - **Note**: `W` is allowed combined
///   with other quantities which is a slight deviation from the ISO 8601 standard.
/// * `HH:MM:SS` - any of the above time formats are allowed to represent a duration
/// * `D days, HH:MM:SS` - time prefixed by `X days`, case-insensitive,
///   spaces `s` and `,` are all optional
/// * `D d, HH:MM:SS` - time prefixed by `X d`, case-insensitive, spaces and `,` are optional
///
/// All duration formats can be prefixed with `+` or `-` to indicate
/// positive and negative durations respectively.
///
/// `Duration` stores durations in days, seconds and microseconds (all ints), therefore
/// durations like years need be scaled when creating a `Duration`. The following scaling
/// factors are used:
/// * `Y` - 365 days
/// * `M` - 30 days
/// * `W` - 7 days
/// * `D` - 1 day
/// * `H` - 3600 seconds
/// * `M` - 60 seconds
/// * `S` - 1 second
///
/// Fractions of quantities are permitted by ISO 8601 in the final quantity included, e.g.
/// `P1.5Y` or `P1Y1.5M`. Wen fractions of quantities are found `day`, `second` and `microsecond`
/// are calculated to most accurately represent the fraction. For example `P1.123W` is represented
/// as
/// ```text
/// Duration {
///    positive: true,
///    day: 7,
///    second: 74390,
///    microsecond: 400_000
/// }
/// ```
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Duration {
    /// The positive or negative sign of the duration
    pub positive: bool,
    /// The number of days
    pub day: u32,
    /// The number of seconds, range 0 to 86399
    pub second: u32,
    /// The number of microseconds, range 0 to 999999
    pub microsecond: u32,
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.positive {
            write!(f, "-")?;
        }
        write!(f, "P")?;
        if self.day != 0 {
            let year = self.day / 365;
            if year != 0 {
                write!(f, "{year}Y")?;
            }
            let day = self.day % 365;
            if day != 0 {
                write!(f, "{day}D")?;
            }
        }
        if self.second != 0 || self.microsecond != 0 {
            let (hour, minute, sec) = self.to_hms();
            write!(f, "T")?;
            if hour != 0 {
                write!(f, "{hour}H")?;
            }
            if minute != 0 {
                write!(f, "{minute}M")?;
            }
            if sec != 0 || self.microsecond != 0 {
                write!(f, "{sec}")?;
                if self.microsecond != 0 {
                    let s = format!("{:06}", self.microsecond);
                    write!(f, ".{}", s.trim_end_matches('0'))?;
                }
                write!(f, "S")?;
            }
        }
        if self.second == 0 && self.microsecond == 0 && self.day == 0 {
            write!(f, "T0S")?;
        }
        Ok(())
    }
}

impl FromStr for Duration {
    type Err = ParseError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_str(s)
    }
}

impl Duration {
    fn to_hms(&self) -> (u32, u32, u32) {
        let hours = self.second / 3600;
        let minutes = (self.second % 3600) / 60;
        let remaining_seconds = self.second % 60;

        (hours, minutes, remaining_seconds)
    }
}

impl PartialOrd for Duration {
    /// Compare two durations by inequality.
    ///
    /// `Duration` supports equality (`==`, `!=`) and inequality (`>`, `<`, `>=` & `<=`) comparisons.
    ///
    /// # Example
    ///
    /// ```
    /// use speedate::Duration;
    ///
    /// let duration = |s| Duration::parse_str(s).unwrap();
    ///
    /// let d1 = duration("P3DT4H5M6.7S");
    /// let d2 = duration("P4DT1H");
    ///
    /// assert!(d2 > d1);
    /// ```
    ///
    /// `positive` is included in in comparisons, thus `+P1D` is greater than `-P2D`,
    /// similarly `-P2D` is less than `-P1D`.
    /// ```
    /// # use speedate::Duration;
    /// # let duration = |s| Duration::parse_str(s).unwrap();
    /// assert!(duration("+P1D") > duration("-P2D"));
    /// assert!(duration("-P2D") < duration("-P1D"));
    /// ```
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self.positive, other.positive) {
            (true, false) => Some(Ordering::Greater),
            (false, true) => Some(Ordering::Less),
            (self_positive, _) => {
                let self_t = (self.day, self.second, self.microsecond);
                let other_t = (other.day, other.second, other.microsecond);
                if self_positive {
                    self_t.partial_cmp(&other_t)
                } else {
                    other_t.partial_cmp(&self_t)
                }
            }
        }
    }
}

macro_rules! checked {
    ($a:ident + $b:expr) => {
        $a.checked_add($b).ok_or(ParseError::DurationValueTooLarge)?
    };
    ($a:ident * $b:expr) => {
        $a.checked_mul($b).ok_or(ParseError::DurationValueTooLarge)?
    };
}

impl Duration {
    /// Create a duration from raw values.
    ///
    /// # Arguments
    /// * `positive` - the positive or negative sign of the duration
    /// * `day` - the number of days in the `Duration`, max allowed value is `999_999_999` to match python's `timedelta`
    /// * `second` - the number of seconds in the `Duration`
    /// * `microsecond` - the number of microseconds in the `Duration`
    ///
    /// `second` and `microsecond` are normalised to be in the ranges 0 to `86_400` and 0 to `999_999`
    /// respectively.
    ///
    /// Due to the limit on days, the maximum duration which can be represented is `P2739726Y9DT86399.999999S`,
    /// that is 1 microsecond short of 2,739,726 years and 10 days, positive or negative.
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::Duration;
    ///
    /// let d = Duration::new(false, 1, 86500, 1_000_123).unwrap();
    /// assert_eq!(
    ///     d,
    ///     Duration {
    ///         positive: false,
    ///         day: 2,
    ///         second: 101,
    ///         microsecond: 123,
    ///     }
    /// );
    /// ```
    pub fn new(positive: bool, day: u32, second: u32, microsecond: u32) -> Result<Self, ParseError> {
        let mut d = Self {
            positive,
            day,
            second,
            microsecond,
        };
        d.normalize()?;
        Ok(d)
    }

    /// Parse a duration from a string
    ///
    /// # Arguments
    ///
    /// * `str` - The string to parse
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::Duration;
    ///
    /// let d = Duration::parse_str("P1YT2.1S").unwrap();
    /// assert_eq!(
    ///     d,
    ///     Duration {
    ///         positive: true,
    ///         day: 365,
    ///         second: 2,
    ///         microsecond: 100_000
    ///     }
    /// );
    /// assert_eq!(d.to_string(), "P1YT2.1S");
    /// ```
    #[inline]
    pub fn parse_str(str: &str) -> Result<Self, ParseError> {
        Self::parse_bytes(str.as_bytes())
    }

    /// Parse a duration from bytes
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to parse
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::Duration;
    ///
    /// let d = Duration::parse_bytes(b"P1Y").unwrap();
    /// assert_eq!(
    ///     d,
    ///     Duration {
    ///         positive: true,
    ///         day: 365,
    ///         second: 0,
    ///         microsecond: 0
    ///     }
    /// );
    /// assert_eq!(d.to_string(), "P1Y");
    /// ```
    #[inline]
    pub fn parse_bytes(bytes: &[u8]) -> Result<Self, ParseError> {
        Duration::parse_bytes_with_config(bytes, &TimeConfigBuilder::new().build())
    }

    /// Same as `Duration::parse_bytes` but with a TimeConfig component.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to parse
    /// * `config` - The `TimeConfig` to use
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::{Duration, TimeConfigBuilder};
    ///
    /// let d = Duration::parse_bytes_with_config(b"P1Y", &TimeConfigBuilder::new().build()).unwrap();
    /// assert_eq!(
    ///     d,
    ///     Duration {
    ///         positive: true,
    ///         day: 365,
    ///         second: 0,
    ///         microsecond: 0
    ///     }
    /// );
    /// assert_eq!(d.to_string(), "P1Y");
    /// ```
    #[inline]
    pub fn parse_bytes_with_config(bytes: &[u8], config: &TimeConfig) -> Result<Self, ParseError> {
        let (positive, bytes) = match bytes {
            [b'-', bytes @ ..] => (false, bytes),
            [b'+', bytes @ ..] | bytes => (true, bytes),
        };
        let mut d = match bytes {
            [] => return Err(ParseError::TooShort),
            [b'P', iso_duration @ ..] => Self::parse_iso_duration(iso_duration)?,
            bytes => {
                if Self::is_duration_date_format(bytes) || bytes.len() < 5 {
                    Self::parse_days_time(bytes, config)?
                } else {
                    Self::parse_time(bytes, config)?
                }
            }
        };
        d.positive = positive;

        d.normalize()?;
        Ok(d)
    }

    /// Total number of seconds in the duration (days + seconds) with sign based on `self.positive`
    #[inline]
    pub fn signed_total_seconds(&self) -> i64 {
        let sign = if self.positive { 1 } else { -1 };
        sign * (self.day as i64 * 86400 + self.second as i64)
    }

    /// Total number of milliseconds in the duration (days + seconds) with sign.
    #[inline]
    pub fn signed_total_ms(&self) -> i64 {
        self.signed_total_seconds() * 1000 + (self.signed_microseconds() as i64) / 1000
    }

    /// Microseconds in the duration with sign based on `self.positive`
    #[inline]
    pub fn signed_microseconds(&self) -> i32 {
        let sign = if self.positive { 1 } else { -1 };
        sign * self.microsecond as i32
    }

    fn normalize(&mut self) -> Result<(), ParseError> {
        if self.microsecond >= 1_000_000 {
            self.second = self
                .second
                .checked_add(self.microsecond / 1_000_000)
                .ok_or(ParseError::DurationValueTooLarge)?;
            self.microsecond %= 1_000_000;
        }
        if self.second >= 86_400 {
            self.day = self
                .day
                .checked_add(self.second / 86_400)
                .ok_or(ParseError::DurationValueTooLarge)?;
            self.second %= 86_400;
        }
        if self.day > 999_999_999 {
            Err(ParseError::DurationDaysTooLarge)
        } else {
            Ok(())
        }
    }

    /// Parse ISO duration (excluding the 'P' prefix)
    fn parse_iso_duration(bytes: &[u8]) -> Result<Self, ParseError> {
        let mut got_t = false;
        let mut last_had_fraction = false;
        let mut position: usize = 0;
        let mut day: u32 = 0;
        let mut second: u32 = 0;
        let mut microsecond: u32 = 0;
        loop {
            match bytes.get(position).copied() {
                Some(b'T') => {
                    if got_t {
                        return Err(ParseError::DurationTRepeated);
                    }
                    got_t = true;
                }
                Some(c) => {
                    let (value, op_fraction, offset) = Self::parse_number_frac(&bytes[position..], c)?;
                    if last_had_fraction {
                        return Err(ParseError::DurationInvalidFraction);
                    }
                    if op_fraction.is_some() {
                        last_had_fraction = true;
                    }
                    position += offset;
                    if got_t {
                        let mult: u32 = match bytes.get(position).copied() {
                            Some(b'H') => 3600,
                            Some(b'M') => 60,
                            Some(b'S') => 1,
                            _ => return Err(ParseError::DurationInvalidTimeUnit),
                        };
                        second = checked!(second + checked!(mult * value));
                        if let Some(fraction) = op_fraction {
                            let extra_seconds = fraction * mult as f64;
                            let extra_full_seconds = extra_seconds.trunc();
                            second = checked!(second + extra_full_seconds as u32);
                            let micro_extra = ((extra_seconds - extra_full_seconds) * 1_000_000.0).round() as u32;
                            microsecond = checked!(microsecond + micro_extra);
                        }
                    } else {
                        let mult: u32 = match bytes.get(position).copied() {
                            Some(b'Y') => 365,
                            Some(b'M') => 30,
                            Some(b'W') => 7,
                            Some(b'D') => 1,
                            _ => return Err(ParseError::DurationInvalidDateUnit),
                        };
                        day = checked!(day + checked!(value * mult));
                        if let Some(fraction) = op_fraction {
                            let extra_days = fraction * mult as f64;
                            let extra_full_days = extra_days.trunc();
                            day = checked!(day + extra_full_days as u32);
                            let extra_seconds = (extra_days - extra_full_days) * 86_400.0;
                            let extra_full_seconds = extra_seconds.trunc();
                            second = checked!(second + extra_full_seconds as u32);
                            microsecond += ((extra_seconds - extra_full_seconds) * 1_000_000.0).round() as u32;
                        }
                    }
                }
                None => break,
            }
            position += 1;
        }
        // require at least one field
        if position < 2 {
            return Err(ParseError::TooShort);
        }

        Ok(Self {
            positive: false, // is set above
            day,
            second,
            microsecond,
        })
    }

    fn is_duration_date_format(bytes: &[u8]) -> bool {
        bytes.iter().any(|&byte| byte == b'd' || byte == b'D')
    }

    fn parse_days_time(bytes: &[u8], config: &TimeConfig) -> Result<Self, ParseError> {
        let (day, position) = match bytes.first().copied() {
            Some(c) => Self::parse_number(bytes, c),
            _ => Err(ParseError::TooShort),
        }?;

        let Some(
            // expect d or D next, optionally prefixed by a space
            [b' ', b'd' | b'D', remaining @ ..] | [b'd' | b'D', remaining @ ..],
        ) = bytes.get(position..)
        else {
            return Err(ParseError::DurationInvalidDays);
        };

        // optionally consume the rest of the word "day/days"
        let remaining = match remaining {
                // ays
                [b'a', b'y', b's', remaining @ ..]
                // ay
                | [b'a', b'y', remaining @ ..]
                // AYS
                | [b'A', b'Y', b'S', remaining @ ..]
                // AY
                | [b'A', b'Y', remaining @ ..]
            => {
                remaining
            }
            // word continued but not finished correctly
            [b'a', ..] | [b'A', ..] => return Err(ParseError::DurationInvalidDays),
            remaining => remaining,
        };

        // days are finished, next prepare to parse the time
        // optionally consume a comma "," and maybe a space
        let remaining = match remaining {
            // b", "
            [b',', b' ', remaining @ ..]
            // b","
            | [b',', remaining @ ..]
            // b" "
            | [b' ', remaining @ ..]
            // no comma / space
            | remaining => remaining,
        };

        if remaining.is_empty() {
            return Ok(Self {
                positive: false, // is set above
                day,
                second: 0,
                microsecond: 0,
            });
        }

        let t = Self::parse_time(remaining, config)?;
        if t.day > 0 {
            // 1d 24:00:00 is not allowed
            return Err(ParseError::DurationHourValueTooLarge);
        }

        Ok(Self {
            positive: false, // is set above
            day,
            second: t.second,
            microsecond: t.microsecond,
        })
    }

    fn parse_time(bytes: &[u8], config: &TimeConfig) -> Result<Self, ParseError> {
        let byte_len = bytes.len();
        if byte_len < 5 {
            return Err(ParseError::TooShort);
        }
        const HOUR_NUMERIC_LIMIT: i64 = 24 * 10i64.pow(8);
        let mut hour: i64 = 0;

        let mut chunks = bytes.splitn(2, |&byte| byte == b':');

        // can just use `.split_once()` in future maybe, if that stabilises
        let (hour_part, mut remaining) = match (chunks.next(), chunks.next(), chunks.next()) {
            (_, _, Some(_)) | (None, _, _) => unreachable!("should always be 1 or 2 chunks"),
            (Some(_hour_part), None, _) => return Err(ParseError::InvalidCharHour),
            (Some(hour_part), Some(remaining), None) => (hour_part, remaining),
        };

        // > 9.999.999.999
        if hour_part.len() > 10 {
            return Err(ParseError::DurationHourValueTooLarge);
        }

        for byte in hour_part {
            let h = byte.wrapping_sub(b'0');
            if h > 9 {
                return Err(ParseError::InvalidCharHour);
            }
            hour = (hour * 10) + (h as i64);
        }
        if hour > HOUR_NUMERIC_LIMIT {
            return Err(ParseError::DurationHourValueTooLarge);
        }

        let mut new_bytes = *b"00:00:00.000000";
        if 3 + remaining.len() > new_bytes.len() {
            match config.microseconds_precision_overflow_behavior {
                crate::MicrosecondsPrecisionOverflowBehavior::Truncate => remaining = &remaining[..new_bytes.len() - 3],
                crate::MicrosecondsPrecisionOverflowBehavior::Error => return Err(ParseError::SecondFractionTooLong),
            }
        }
        let new_bytes = &mut new_bytes[..3 + remaining.len()];
        new_bytes[3..].copy_from_slice(remaining);

        let t = crate::time::PureTime::parse(new_bytes, 0, config)?;

        if new_bytes.len() > t.position {
            return Err(ParseError::ExtraCharacters);
        }
        let day = hour as u32 / 24;
        hour %= 24;

        Ok(Self {
            positive: false, // is set above
            day,
            second: t.total_seconds() + (hour as u32) * 3_600,
            microsecond: t.microsecond,
        })
    }

    fn parse_number(bytes: &[u8], d1: u8) -> Result<(u32, usize), ParseError> {
        let mut value = match d1 {
            c if d1.is_ascii_digit() => (c - b'0') as u32,
            _ => return Err(ParseError::DurationInvalidNumber),
        };
        let mut position = 1;
        loop {
            match bytes.get(position) {
                Some(c) if c.is_ascii_digit() => {
                    value = checked!(value * 10);
                    value = checked!(value + (c - b'0') as u32);
                    position += 1;
                }
                _ => return Ok((value, position)),
            }
        }
    }

    fn parse_number_frac(bytes: &[u8], d1: u8) -> Result<(u32, Option<f64>, usize), ParseError> {
        let (value, mut position) = Self::parse_number(bytes, d1)?;
        let next_char = bytes.get(position).copied();
        if next_char == Some(b'.') || next_char == Some(b',') {
            let mut decimal = 0_f64;
            let mut denominator = 1_f64;
            loop {
                position += 1;
                match bytes.get(position) {
                    Some(c) if c.is_ascii_digit() => {
                        decimal *= 10.0;
                        decimal += (c - b'0') as f64;
                        denominator *= 10.0;
                    }
                    _ => return Ok((value, Some(decimal / denominator), position)),
                }
            }
        } else {
            Ok((value, None, position))
        }
    }
}
