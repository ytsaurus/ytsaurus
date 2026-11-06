use crate::config::DateTimeConfig;
use crate::date::MS_WATERSHED;
use crate::util::timestamp_to_seconds_micros;
use crate::{
    float_parse_bytes, numbers::decimal_digits, IntFloat, MicrosecondsPrecisionOverflowBehavior, TimeConfigBuilder,
    TimestampUnit,
};
use crate::{Date, ParseError, Time, TimeConfig};
use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;
use std::time::SystemTime;
/// A DateTime
///
/// Combines a [Date], [Time].
/// Allowed values:
/// * `YYYY-MM-DDTHH:MM:SS` - all the above time formats are allowed for the time part
/// * `YYYY-MM-DD HH:MM:SS` - `T`, `t`, ` ` and `_` are allowed as separators
/// * `YYYY-MM-DDTHH:MM:SSZ` - `Z` or `z` is allowed as timezone
/// * `YYYY-MM-DDTHH:MM:SS+08:00`- positive and negative timezone are allowed,
///   as per ISO 8601, U+2212 minus `−` is allowed as well as ascii minus `-` (U+002D)
/// * `YYYY-MM-DDTHH:MM:SS+0800` - the colon (`:`) in the timezone is optional
///
/// # Comparison
///
/// `DateTime` supports equality (`==`) and inequality (`>`, `<`, `>=`, `<=`) comparisons.
///
/// See [DateTime::partial_cmp] for how this works.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct DateTime {
    /// date part of the datetime
    pub date: Date,
    /// time part of the datetime
    pub time: Time,
}

impl fmt::Display for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.date)?;
        write!(f, "T")?;
        write!(f, "{}", self.time)?;
        Ok(())
    }
}

impl FromStr for DateTime {
    type Err = ParseError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Delegate to parse_str, which is more permissive - users can call parse_str_rfc3339 directly instead if they
        // want to be stricter
        Self::parse_str(s)
    }
}

impl PartialOrd for DateTime {
    /// Compare two datetimes by inequality.
    ///
    /// `DateTime` supports equality (`==`, `!=`) and inequality comparisons (`>`, `<`, `>=` & `<=`).
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::DateTime;
    ///
    /// let dt1 = DateTime::parse_str("2020-02-03T04:05:06.07").unwrap();
    /// let dt2 = DateTime::parse_str("2020-02-03T04:05:06.08").unwrap();
    ///
    /// assert!(dt2 > dt1);
    /// ```
    ///
    ///  # Comparison with Timezones
    ///
    /// When comparing two datetimes, we want "less than" or "greater than" refer to "earlier" or "later"
    /// in the absolute course of time. We therefore need to be careful when comparing datetimes with different
    /// timezones. (If it wasn't for timezones, we could omit all this extra logic and thinking and just compare
    /// struct members directly as we do with [Date] and [crate::Duration]).
    ///
    /// From [wikipedia](https://en.wikipedia.org/wiki/UTC_offset#Time_zones_and_time_offsets)
    ///
    /// > The UTC offset is an amount of time subtracted from or added to UTC time to specify the local solar time...
    ///
    /// So, we can imagine that at 3pm in the UK (UTC+0) (in winter, to avoid DST confusion) it's 4pm in France (UTC+1).
    ///
    /// Thus to compare two datetimes in absolute terms we need to **SUBTRACT** the timezone offset.
    ///
    /// As if timezones weren't complicated enough, there are three extra considerations here:
    /// 1. **naïve vs. non-naïve:** We also have to consider the case where one datetime has a timezone and the other
    ///    does not (e.g. is "timezone "naïve"). When comparing naïve datetimes to non-naïve, this library
    ///    assumes the naïve datetime has the same timezone as the non-naïve, this is different to other
    ///    implementations (e.g. python) where such comparisons fail.
    /// 2. **Direction:** As described in PostgreSQL's docs, in the POSIX Time Zone Specification
    ///    "The positive sign is used for zones west of Greenwich", which is opposite to the ISO-8601 sign convention.
    ///    In other words, the offset is reversed, see the end of
    ///    [this blog](http://blog.untrod.com/2016/08/actually-understanding-timezones-in-postgresql.html)
    ///    and the [PostgreSQL docs](https://www.postgresql.org/docs/14/datetime-posix-timezone-specs.html) for more
    ///    info.
    /// 3. **Equality comparison:** None of this logic is used for equality (`==`) comparison where we can just compare
    ///    struct members directly, e.g. require the timezone offset to be the same for two datetimes to be equal.
    ///
    /// ## Timezone Examples
    ///
    /// ```
    /// use speedate::DateTime;
    ///
    /// let dt_uk_3pm = DateTime::parse_str("2000-01-01T15:00:00Z").unwrap();
    /// let dt_france_4pm = DateTime::parse_str("2000-01-01T16:00:00+01:00").unwrap();
    ///
    /// assert!(dt_uk_3pm >= dt_france_4pm);  // the two dts are actually the same instant
    /// assert!(dt_uk_3pm <= dt_france_4pm);  // the two dts are actually the same instant
    /// assert_ne!(dt_uk_3pm, dt_france_4pm);  // no equal because timezones much match for equality
    ///
    /// let dt_uk_330pm = DateTime::parse_str("2000-01-01T15:30:00Z").unwrap();
    ///
    /// assert!(dt_uk_330pm > dt_uk_3pm);
    /// assert!(dt_uk_330pm > dt_france_4pm);
    ///
    /// // as described in point 1 above, naïve datetimes are assumed to
    /// // have the same timezone as the non-naïve
    /// let dt_naive_330pm = DateTime::parse_str("2000-01-01T15:30:00").unwrap();
    /// assert!(dt_uk_3pm < dt_naive_330pm);
    /// assert!(dt_france_4pm > dt_naive_330pm);
    /// ```
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self.time.tz_offset, other.time.tz_offset) {
            (Some(_), Some(_)) => match self.timestamp_tz().partial_cmp(&other.timestamp_tz()) {
                Some(Ordering::Equal) => self.time.microsecond.partial_cmp(&other.time.microsecond),
                otherwise => otherwise,
            },
            _ => match self.date.partial_cmp(&other.date) {
                Some(Ordering::Equal) => self.time.partial_cmp(&other.time),
                otherwise => otherwise,
            },
        }
    }
}

impl DateTime {
    /// Parse a datetime from a string
    ///
    /// # Arguments
    ///
    /// * `str` - The string to parse
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::{DateTime, Date, Time};
    ///
    /// let dt = DateTime::parse_str("2022-01-01T12:13:14Z").unwrap();
    /// assert_eq!(
    ///     dt,
    ///     DateTime {
    ///         date: Date {
    ///             year: 2022,
    ///             month: 1,
    ///             day: 1,
    ///         },
    ///         time: Time {
    ///             hour: 12,
    ///             minute: 13,
    ///             second: 14,
    ///             microsecond: 0,
    ///             tz_offset: Some(0),
    ///         },
    ///     }
    /// );
    /// assert_eq!(dt.to_string(), "2022-01-01T12:13:14Z");
    /// ```
    ///
    /// With a non-zero timezone
    /// (we also use a different separator and omit the colon in timezone here):
    ///
    /// ```
    /// use speedate::{DateTime, Date, Time};
    ///
    /// let dt = DateTime::parse_str_rfc3339("2000-02-29 12:13:14-0830").unwrap();
    /// assert_eq!(
    ///     dt,
    ///     DateTime {
    ///         date: Date {
    ///             year: 2000,
    ///             month: 2,
    ///             day: 29,
    ///         },
    ///         time: Time {
    ///             hour: 12,
    ///             minute: 13,
    ///             second: 14,
    ///             microsecond: 0,
    ///             tz_offset: Some(-30600),
    ///         },
    ///     }
    /// );
    /// assert_eq!(dt.to_string(), "2000-02-29T12:13:14-08:30");
    /// ```
    /// (note: the string representation is still canonical ISO8601)
    #[inline]
    pub fn parse_str_rfc3339(str: &str) -> Result<Self, ParseError> {
        Self::parse_bytes_rfc3339(str.as_bytes())
    }

    /// As with [DateTime::parse_str] but also supports unix timestamps.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to parse
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::DateTime;
    ///
    /// let dt = DateTime::parse_str("2022-01-01T12:13:14Z").unwrap();
    /// assert_eq!(dt.to_string(), "2022-01-01T12:13:14Z");
    ///
    /// let dt = DateTime::parse_str("1641039194").unwrap();
    /// assert_eq!(dt.to_string(), "2022-01-01T12:13:14");
    /// ```
    pub fn parse_str(str: &str) -> Result<Self, ParseError> {
        Self::parse_bytes(str.as_bytes())
    }

    /// As with [`DateTime::parse_str`] but with a [`DateTimeConfig`].
    pub fn parse_str_with_config(str: &str, config: &DateTimeConfig) -> Result<Self, ParseError> {
        Self::parse_bytes_with_config(str.as_bytes(), config)
    }
    /// Parse a datetime from bytes using RFC 3339 format
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to parse
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::{DateTime, Date, Time};
    ///
    /// let dt = DateTime::parse_bytes_rfc3339(b"2022-01-01T12:13:14Z").unwrap();
    /// assert_eq!(
    ///     dt,
    ///     DateTime {
    ///         date: Date {
    ///             year: 2022,
    ///             month: 1,
    ///             day: 1,
    ///         },
    ///         time: Time {
    ///             hour: 12,
    ///             minute: 13,
    ///             second: 14,
    ///             microsecond: 0,
    ///             tz_offset: Some(0),
    ///         },
    ///     }
    /// );
    /// assert_eq!(dt.to_string(), "2022-01-01T12:13:14Z");
    /// ```
    pub fn parse_bytes_rfc3339(bytes: &[u8]) -> Result<Self, ParseError> {
        DateTime::parse_bytes_rfc3339_with_config(bytes, &TimeConfigBuilder::new().build())
    }

    /// Same as `parse_bytes_rfc3339` with with a `TimeConfig` parameter.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to parse
    /// * `config` - The `TimeConfig` to use
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::{DateTime, Date, Time, TimeConfigBuilder};
    ///
    /// let dt = DateTime::parse_bytes_rfc3339_with_config(b"2022-01-01T12:13:14Z", &TimeConfigBuilder::new().build()).unwrap();
    /// assert_eq!(
    ///     dt,
    ///     DateTime {
    ///         date: Date {
    ///             year: 2022,
    ///             month: 1,
    ///             day: 1,
    ///         },
    ///         time: Time {
    ///             hour: 12,
    ///             minute: 13,
    ///             second: 14,
    ///             microsecond: 0,
    ///             tz_offset: Some(0),
    ///         },
    ///     }
    /// );
    /// assert_eq!(dt.to_string(), "2022-01-01T12:13:14Z");
    /// ```
    pub fn parse_bytes_rfc3339_with_config(bytes: &[u8], config: &TimeConfig) -> Result<Self, ParseError> {
        // First up, parse the full date if we can
        let date = Date::parse_bytes_partial(bytes)?;

        // Next parse the separator between date and time
        let sep = bytes.get(10).copied();
        if sep != Some(b'T') && sep != Some(b't') && sep != Some(b' ') && sep != Some(b'_') {
            return Err(ParseError::InvalidCharDateTimeSep);
        }

        // Next try to parse the time
        let time = Time::parse_bytes_offset(bytes, 11, config)?;

        Ok(Self { date, time })
    }

    /// As with [DateTime::parse_bytes_rfc3339] but also supports unix timestamps.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to parse
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::{DateTime, Date, Time};
    ///
    /// let dt = DateTime::parse_bytes(b"2022-01-01T12:13:14Z").unwrap();
    /// assert_eq!(dt.to_string(), "2022-01-01T12:13:14Z");
    ///
    /// let dt = DateTime::parse_bytes(b"1641039194").unwrap();
    /// assert_eq!(dt.to_string(), "2022-01-01T12:13:14");
    /// ```
    pub fn parse_bytes(bytes: &[u8]) -> Result<Self, ParseError> {
        DateTime::parse_bytes_with_config(bytes, &DateTimeConfig::default())
    }

    /// Same as `DateTime::parse_bytes` but supporting `DateTimeConfig`
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to parse
    /// * `config` - The `DateTimeConfig` to use when parsing the time portion
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::{DateTime, Date, Time, DateTimeConfig};
    ///
    /// let dt = DateTime::parse_bytes_with_config(
    ///     b"2022-01-01T12:13:14Z",
    ///     &DateTimeConfig::default(),
    /// ).unwrap();
    /// assert_eq!(dt.to_string(), "2022-01-01T12:13:14Z");
    /// ```
    pub fn parse_bytes_with_config(bytes: &[u8], config: &DateTimeConfig) -> Result<Self, ParseError> {
        match Self::parse_bytes_rfc3339_with_config(bytes, &config.time_config) {
            Ok(d) => Ok(d),
            Err(e) => match float_parse_bytes(bytes) {
                IntFloat::Int(int) => Self::from_timestamp_with_config(int, 0, config),
                IntFloat::Float(float) => {
                    let timestamp_in_milliseconds = float.abs() > MS_WATERSHED as f64;

                    if config.time_config.microseconds_precision_overflow_behavior
                        == MicrosecondsPrecisionOverflowBehavior::Error
                    {
                        let decimal_digits_count = decimal_digits(bytes);

                        // If the number of decimal digits exceeds the maximum allowed for the timestamp precision,
                        // return an error. For timestamps in milliseconds, the maximum is 3, for timestamps in seconds,
                        // the maximum is 6. These end up being the same in terms of allowing microsecond precision.
                        if timestamp_in_milliseconds && decimal_digits_count > 3 {
                            return Err(ParseError::MillisecondFractionTooLong);
                        } else if !timestamp_in_milliseconds && decimal_digits_count > 6 {
                            return Err(ParseError::SecondFractionTooLong);
                        }
                    }

                    let timestamp_normalized: f64 = if timestamp_in_milliseconds {
                        float / 1_000f64
                    } else {
                        float
                    };

                    // if seconds is negative, we round down (left on the number line), so -6.25 -> -7
                    // which allows for a positive number of microseconds to compensate back up to -6.25
                    // which is the equivalent of doing (seconds - 1) and (microseconds + 1_000_000)
                    // like we do in timestamp_watershed
                    let seconds = timestamp_normalized.floor() as i64;
                    let microseconds = ((timestamp_normalized - seconds as f64) * 1_000_000f64).round() as u32;

                    Self::from_timestamp_with_config(seconds, microseconds, config)
                }
                IntFloat::Err => Err(e),
            },
        }
    }

    /// Like `from_timestamp` but with a `TimeConfig`.
    ///
    /// ("Unix Timestamp" means number of seconds or milliseconds since 1970-01-01)
    ///
    /// Input must be between `-11_676_096_000` (`1600-01-01T00:00:00`) and
    /// `253_402_300_799_000` (`9999-12-31T23:59:59.999999`) inclusive.
    ///
    /// If the absolute value is > 2e10 (`20_000_000_000`) it is interpreted as being in milliseconds.
    ///
    /// That means:
    /// * `20_000_000_000` is `2603-10-11T11:33:20`
    /// * `20_000_000_001` is `1970-08-20T11:33:20.001`
    /// * `-20_000_000_000` gives an error - `DateTooSmall` as it would be before 1600
    /// * `-20_000_000_001` is `1969-05-14T12:26:39.999`
    ///
    /// # Arguments
    ///
    /// * `timestamp` - timestamp in either seconds or milliseconds
    /// * `timestamp_microsecond` - microseconds fraction of a second timestamp
    /// * `config` - the `TimeConfig` to use
    ///
    /// Where `timestamp` is interrupted as milliseconds and is not a whole second, the remainder is added to
    /// `timestamp_microsecond`.
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::{DateTime, DateTimeConfig};
    ///
    /// let d = DateTime::from_timestamp_with_config(
    ///     1_654_619_320,
    ///     123,
    ///     &DateTimeConfig::default(),
    /// ).unwrap();
    /// assert_eq!(d.to_string(), "2022-06-07T16:28:40.000123");
    ///
    /// let d = DateTime::from_timestamp_with_config(
    ///     1_654_619_320_123,
    ///     123_000,
    ///     &DateTimeConfig::default(),
    /// ).unwrap();
    /// assert_eq!(d.to_string(), "2022-06-07T16:28:40.246000");
    /// ```
    pub fn from_timestamp_with_config(
        timestamp: i64,
        timestamp_microsecond: u32,
        config: &DateTimeConfig,
    ) -> Result<Self, ParseError> {
        let (mut second, extra_microsecond) = timestamp_to_seconds_micros(timestamp, config.timestamp_unit)?;
        let mut total_microsecond = timestamp_microsecond
            .checked_add(extra_microsecond)
            .ok_or(ParseError::TimeTooLarge)?;
        if total_microsecond >= 1_000_000 {
            second = second
                .checked_add(total_microsecond as i64 / 1_000_000)
                .ok_or(ParseError::TimeTooLarge)?;
            total_microsecond %= 1_000_000;
        }
        let (date, time_second) = Date::from_timestamp_calc(second)?;
        Ok(Self {
            date,
            time: Time::from_timestamp_with_config(time_second, total_microsecond, &config.time_config)?,
        })
    }

    /// Same as `from_timestamp_with_config` where the timestamp and milliseconds
    /// are encoded as a single float.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - timestamp in either seconds or milliseconds
    /// * `config` - the `DateTimeConfig` to use
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::{DateTime, DateTimeConfig};
    ///
    /// let d = DateTime::from_float_with_config(
    ///     1_654_619_320.000123,
    ///     &DateTimeConfig::default(),
    /// ).unwrap();
    /// assert_eq!(d.to_string(), "2022-06-07T16:28:40.000123");
    ///
    /// let d = DateTime::from_float_with_config(
    ///     1_654_619_320_123.123,
    ///     &DateTimeConfig::default(),
    /// ).unwrap();
    /// assert_eq!(d.to_string(), "2022-06-07T16:28:40.123123");
    /// ```
    pub fn from_float_with_config(timestamp: f64, mode: &DateTimeConfig) -> Result<Self, ParseError> {
        let microseconds = match mode.timestamp_unit {
            TimestampUnit::Second => timestamp.fract().abs() * 1_000_000.0,
            TimestampUnit::Millisecond => timestamp.fract().abs() * 1_000.0,
            TimestampUnit::Infer => {
                if timestamp.abs() <= MS_WATERSHED as f64 {
                    // treat as seconds
                    timestamp.fract().abs() * 1_000_000.0
                } else {
                    // treat as milliseconds
                    timestamp.fract().abs() * 1_000.0
                }
            }
        };
        // checking for extra digits in microseconds is unreliable with large floats,
        // so we just round to the nearest microsecond
        Self::from_timestamp_with_config(timestamp.floor() as i64, microseconds.round() as u32, mode)
    }

    /// Create a datetime from a Unix Timestamp in seconds or milliseconds
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
    /// * `timestamp_microsecond` - microseconds fraction of a second timestamp
    ///
    /// Where `timestamp` is interrupted as milliseconds and is not a whole second, the remainder is added to
    /// `timestamp_microsecond`.
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::DateTime;
    ///
    /// let d = DateTime::from_timestamp(1_654_619_320, 123).unwrap();
    /// assert_eq!(d.to_string(), "2022-06-07T16:28:40.000123");
    ///
    /// let d = DateTime::from_timestamp(1_654_619_320_123, 123_000).unwrap();
    /// assert_eq!(d.to_string(), "2022-06-07T16:28:40.246000");
    /// ```
    pub fn from_timestamp(timestamp: i64, timestamp_microsecond: u32) -> Result<Self, ParseError> {
        Self::from_timestamp_with_config(timestamp, timestamp_microsecond, &DateTimeConfig::default())
    }

    /// Create a datetime from the system time. This method uses [std::time::SystemTime] to get
    /// the system time and uses it to create a [DateTime] adjusted to the specified timezone offset.
    ///
    /// # Arguments
    ///
    /// * `tz_offset` - timezone offset in seconds, must be less than `86_400`
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::DateTime;
    ///
    /// let now = DateTime::now(0).unwrap();
    /// println!("Current date and time: {}", now);
    /// ```
    pub fn now(tz_offset: i32) -> Result<Self, ParseError> {
        let t = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| ParseError::SystemTimeError)?;
        let mut now = Self::from_timestamp(t.as_secs() as i64, t.subsec_micros())?;
        now.time.tz_offset = Some(0);
        if tz_offset == 0 {
            Ok(now)
        } else {
            now.in_timezone(tz_offset)
        }
    }

    /// Clone the datetime and set a new timezone offset.
    ///
    /// The returned datetime will represent a different point in time since the timezone offset is changed without
    /// modifying the date and time. See [DateTime::in_timezone] for alternative behaviour.
    ///
    /// # Arguments
    ///
    /// * `tz_offset` - optional timezone offset in seconds, set to `None` to create a naïve datetime.
    ///
    /// This method will return `Err(ParseError::OutOfRangeTz)` if `abs(tz_offset)` is not less than 24 hours `86_400`.
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::DateTime;
    ///
    /// let dt = DateTime::parse_str("2022-01-01T12:13:14Z").unwrap();
    ///
    /// let dt2 = dt.with_timezone_offset(Some(-8 * 3600)).unwrap();
    /// assert_eq!(dt2.to_string(), "2022-01-01T12:13:14-08:00");
    /// ```
    pub fn with_timezone_offset(&self, tz_offset: Option<i32>) -> Result<Self, ParseError> {
        Ok(Self {
            date: self.date,
            time: self.time.with_timezone_offset(tz_offset)?,
        })
    }

    /// Create a new datetime in a different timezone with date & time adjusted to represent the same moment in time.
    /// See [DateTime::with_timezone_offset] for alternative behaviour.
    ///
    /// The datetime must have a offset, otherwise a `ParseError::TzRequired` error is returned.
    ///
    /// # Arguments
    ///
    /// * `tz_offset` - new timezone offset in seconds.
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::DateTime;
    ///
    /// let dt_z = DateTime::parse_str("2000-01-01T15:00:00Z").unwrap();
    ///
    /// let dt_utc_plus2 = dt_z.in_timezone(7200).unwrap();
    /// assert_eq!(dt_utc_plus2.to_string(), "2000-01-01T17:00:00+02:00");
    /// ```
    pub fn in_timezone(&self, tz_offset: i32) -> Result<Self, ParseError> {
        if tz_offset.abs() >= 24 * 3600 {
            Err(ParseError::OutOfRangeTz)
        } else if let Some(current_offset) = self.time.tz_offset {
            let new_ts = self.timestamp() + (tz_offset - current_offset) as i64;
            let mut new_dt = Self::from_timestamp(new_ts, self.time.microsecond)?;
            new_dt.time.tz_offset = Some(tz_offset);
            Ok(new_dt)
        } else {
            Err(ParseError::TzRequired)
        }
    }

    /// Unix timestamp (seconds since epoch, 1970-01-01T00:00:00) omitting timezone offset
    /// (or equivalently comparing to 1970-01-01T00:00:00 in the same timezone as self)
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::DateTime;
    ///
    /// let dt = DateTime::from_timestamp(1_654_619_320, 123).unwrap();
    /// assert_eq!(dt.to_string(), "2022-06-07T16:28:40.000123");
    /// assert_eq!(dt.timestamp(), 1_654_619_320);
    ///
    /// let dt = DateTime::parse_str("1970-01-02T00:00").unwrap();
    /// assert_eq!(dt.timestamp(), 24 * 3600);
    /// ```
    pub fn timestamp(&self) -> i64 {
        self.date.timestamp() + self.time.total_seconds() as i64
    }

    /// Unix timestamp in milliseconds, ignoring timezone offset.
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::DateTime;
    ///
    /// let dt = DateTime::parse_str("1970-01-02T00:00:00.123").unwrap();
    /// assert_eq!(dt.timestamp_ms(), 86_400_123);
    /// ```
    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp() * 1000 + (self.time.microsecond / 1000) as i64
    }

    /// Unix timestamp assuming epoch is in zulu timezone (1970-01-01T00:00:00Z) and accounting for
    /// timezone offset.
    ///
    /// This is effectively [Self::timestamp] minus [Self.time::tz_offset], see [Self::partial_cmp] for details on
    /// why timezone offset is subtracted. If [Self.time::tz_offset] if `None`, this is the same as [Self::timestamp].
    ///
    /// # Examples
    ///
    /// ```
    /// use speedate::DateTime;
    ///
    /// let dt_naive = DateTime::parse_str("1970-01-02T00:00").unwrap();
    /// assert_eq!(dt_naive.timestamp_tz(), 24 * 3600);
    ///
    /// let dt_zulu = DateTime::parse_str("1970-01-02T00:00Z").unwrap();
    /// assert_eq!(dt_zulu.timestamp_tz(), 24 * 3600);
    ///
    /// let dt_plus_1 = DateTime::parse_str("1970-01-02T00:00+01:00").unwrap();
    /// assert_eq!(dt_plus_1.timestamp_tz(), 23 * 3600);
    /// ```
    pub fn timestamp_tz(&self) -> i64 {
        match self.time.tz_offset {
            Some(tz_offset) => self.timestamp() - (tz_offset as i64),
            None => self.timestamp(),
        }
    }

    /// Unix timestamp in milliseconds accounting for timezone offset.
    pub fn timestamp_tz_ms(&self) -> i64 {
        self.timestamp_tz() * 1000 + (self.time.microsecond / 1000) as i64
    }
}
