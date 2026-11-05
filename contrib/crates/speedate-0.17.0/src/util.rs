use crate::date::MS_WATERSHED;
use crate::{ParseError, TimestampUnit};

pub(crate) fn timestamp_watershed(timestamp: i64) -> Result<(i64, u32), ParseError> {
    let ts_abs = timestamp.checked_abs().ok_or(ParseError::DateTooSmall)?;
    if ts_abs <= MS_WATERSHED {
        return Ok((timestamp, 0));
    }
    let mut seconds = timestamp / 1_000;
    let mut microseconds = ((timestamp % 1_000) * 1000) as i32;
    if microseconds < 0 {
        seconds -= 1;
        microseconds += 1_000_000;
    }
    Ok((seconds, microseconds as u32))
}

pub fn timestamp_to_seconds_micros(timestamp: i64, unit: TimestampUnit) -> Result<(i64, u32), ParseError> {
    match unit {
        TimestampUnit::Second => Ok((timestamp, 0)),
        TimestampUnit::Millisecond => {
            let mut seconds = timestamp / 1_000;
            let mut microseconds = ((timestamp % 1_000) * 1000) as i32;
            if microseconds < 0 {
                seconds -= 1;
                microseconds += 1_000_000;
            }
            Ok((seconds, microseconds as u32))
        }
        TimestampUnit::Infer => timestamp_watershed(timestamp),
    }
}
