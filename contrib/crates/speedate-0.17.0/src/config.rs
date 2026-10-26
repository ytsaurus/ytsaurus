use crate::{ConfigError, MicrosecondsPrecisionOverflowBehavior};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum TimestampUnit {
    /// Interpret as seconds since the UNIX epoch.
    Second,
    /// Interpret as milliseconds since the UNIX epoch.
    Millisecond,
    /// Let the parser infer units based on value length.
    #[default]
    Infer,
}

impl FromStr for TimestampUnit {
    type Err = ConfigError;
    fn from_str(value: &str) -> Result<Self, ConfigError> {
        match value.to_lowercase().as_str() {
            "s" => Ok(Self::Second),
            "ms" => Ok(Self::Millisecond),
            "infer" => Ok(Self::Infer),
            _ => Err(ConfigError::UnknownTimestampUnitString),
        }
    }
}
/// Configuration for parsing `Date`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DateConfig {
    /// How to interpret numeric timestamps (seconds, milliseconds, etc.).
    pub timestamp_unit: TimestampUnit,
}

#[derive(Debug, Clone, Default)]
pub struct DateConfigBuilder {
    timestamp_unit: Option<TimestampUnit>,
}

impl DateConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn timestamp_unit(mut self, timestamp_unit: TimestampUnit) -> Self {
        self.timestamp_unit = Some(timestamp_unit);
        self
    }
    pub fn build(self) -> DateConfig {
        DateConfig {
            timestamp_unit: self.timestamp_unit.unwrap_or_default(),
        }
    }
}

impl DateConfig {
    pub fn builder() -> DateConfigBuilder {
        DateConfigBuilder::new()
    }
}

/// Configuration for parsing `DateTime`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DateTimeConfig {
    /// How to interpret numeric timestamps (seconds, milliseconds, etc.).
    pub timestamp_unit: TimestampUnit,
    /// Configuration used when parsing the time component.
    pub time_config: TimeConfig,
}

/// Builder for [`DateTimeConfig`].
#[derive(Debug, Clone, Default)]
pub struct DateTimeConfigBuilder {
    timestamp_unit: Option<TimestampUnit>,
    time_config: Option<TimeConfig>,
}

impl DateTimeConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn timestamp_unit(mut self, timestamp_unit: TimestampUnit) -> Self {
        self.timestamp_unit = Some(timestamp_unit);
        self
    }

    pub fn time_config(mut self, time_config: TimeConfig) -> Self {
        self.time_config = Some(time_config);
        self
    }

    pub fn build(self) -> DateTimeConfig {
        DateTimeConfig {
            timestamp_unit: self.timestamp_unit.unwrap_or_default(),
            time_config: self.time_config.unwrap_or_default(),
        }
    }
}

impl DateTimeConfig {
    pub fn builder() -> DateTimeConfigBuilder {
        DateTimeConfigBuilder::new()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TimeConfig {
    pub microseconds_precision_overflow_behavior: MicrosecondsPrecisionOverflowBehavior,
    pub unix_timestamp_offset: Option<i32>,
}

impl TimeConfig {
    pub fn builder() -> TimeConfigBuilder {
        TimeConfigBuilder::new()
    }
}

#[derive(Debug, Clone, Default)]
pub struct TimeConfigBuilder {
    microseconds_precision_overflow_behavior: Option<MicrosecondsPrecisionOverflowBehavior>,
    unix_timestamp_offset: Option<i32>,
}

impl TimeConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn microseconds_precision_overflow_behavior(
        mut self,
        microseconds_precision_overflow_behavior: MicrosecondsPrecisionOverflowBehavior,
    ) -> Self {
        self.microseconds_precision_overflow_behavior = Some(microseconds_precision_overflow_behavior);
        self
    }
    pub fn unix_timestamp_offset(mut self, unix_timestamp_offset: Option<i32>) -> Self {
        self.unix_timestamp_offset = unix_timestamp_offset;
        self
    }
    pub fn build(self) -> TimeConfig {
        TimeConfig {
            microseconds_precision_overflow_behavior: self.microseconds_precision_overflow_behavior.unwrap_or_default(),
            unix_timestamp_offset: self.unix_timestamp_offset,
        }
    }
}
