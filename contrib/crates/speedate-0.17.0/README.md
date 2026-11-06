# speedate

[![CI](https://github.com/pydantic/speedate/actions/workflows/ci.yml/badge.svg?event=push)](https://github.com/pydantic/speedate/actions/workflows/ci.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/pydantic/speedate/branch/main/graph/badge.svg)](https://codecov.io/gh/pydantic/speedate)
[![Crates.io](https://img.shields.io/crates/v/speedate?color=green)](https://crates.io/crates/speedate)

Fast and simple datetime, date, time and duration parsing for rust.

**speedate** is a lax† **RFC 3339** date and time parser, in other words, it parses common **ISO 8601**
formats.

**†** - all relaxations of from [RFC 3339](https://tools.ietf.org/html/rfc3339)
are compliant with [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601).

The following formats are supported:
* Date: `YYYY-MM-DD`
* Time: `HH:MM:SS`
* Time: `HH:MM:SS.FFFFFF` 1 to 6 digits are reflected in the `time.microsecond`, extra digits are ignored
* Time: `HH:MM`
* Date time: `YYYY-MM-DDTHH:MM:SS` - all the above time formats are allowed for the time part
* Date time: `YYYY-MM-DD HH:MM:SS` - `T`, `t`, ` ` and `_` are allowed as separators
* Date time: `YYYY-MM-DDTHH:MM:SSZ` - `Z` or `z` is allowed as timezone
* Date time: `YYYY-MM-DDTHH:MM:SS+08:00`- positive and negative timezone are allowed, as per ISO 8601, U+2212 minus `−`
  is allowed as well as ascii minus `-` (U+002D)
* Date time: `YYYY-MM-DDTHH:MM:SS+0800` - the colon (`:`) in the timezone is optional
* Duration: `PnYnMnDTnHnMnS` - ISO 8601 duration format,
  see [wikipedia](https://en.wikipedia.org/wiki/ISO_8601#Durations) for more details, `W` for weeks is also allowed
* Duration: `HH:MM:SS` - any of the above time formats are allowed to represent a duration
* Duration: `D days, HH:MM:SS` - time prefixed by `X days`, case-insensitive, spaces `s` and `,` are all optional
* Duration: `D d, HH:MM:SS` - time prefixed by `X d`, case-insensitive, spaces and `,` are optional
* Duration: `±...` - all duration formats shown here can be prefixed with `+` or `-` to indicate
  positive and negative durations respectively

In addition, unix timestamps (both seconds and milliseconds) can be used to create dates and datetimes.
The interpretation of numeric timestamps can be controlled via the `TimestampUnit` configuration.
By default the unit is inferred from the length of the number, but you can set it explicitly using a
`DateConfig` or `DateTimeConfig`.

See [the documentation](https://docs.rs/speedate/latest/speedate/index.html#structs) for each struct for more details.

This will be the datetime parsing logic for [pydantic-core](https://github.com/pydantic/pydantic-core).

## Usage

```rust
use speedate::{DateTime, Date, Time};

let dt = DateTime::parse_str("2022-01-01T12:13:14Z").unwrap();
assert_eq!(
    dt,
    DateTime {
        date: Date {
            year: 2022,
            month: 1,
            day: 1,
        },
        time: Time {
            hour: 12,
            minute: 13,
            second: 14,
            microsecond: 0,
            tz_offset: Some(0),
        },
    }
);
assert_eq!(dt.to_string(), "2022-01-01T12:13:14Z");
```

To control the specifics of time parsing you can use provide a `TimeConfig`:

```rust
use speedate::{
    DateTime, Date, Time, DateTimeConfig, TimeConfig,
    MicrosecondsPrecisionOverflowBehavior, TimestampUnit,
};
let dt = DateTime::parse_bytes_with_config(
    "1689102037.5586429".as_bytes(),
    &DateTimeConfig::builder()
        .time_config(
            TimeConfig::builder()
                .unix_timestamp_offset(Some(0))
                .microseconds_precision_overflow_behavior(MicrosecondsPrecisionOverflowBehavior::Truncate)
                .build(),
        )
        .build(),
).unwrap();
assert_eq!(
    dt,
    DateTime {
        date: Date {
            year: 2023,
            month: 7,
            day: 11,
        },
        time: Time {
            hour: 19,
            minute: 0,
            second: 37,
            microsecond: 558643,
            tz_offset: Some(0),
        },
    }
);
assert_eq!(dt.to_string(), "2023-07-11T19:00:37.558643Z");
```

The `timestamp_unit` field on `DateConfig` and `DateTimeConfig` controls how
numeric timestamps are interpreted. By default, the unit is inferred based on
the value's length. You can force seconds or milliseconds parsing:

```rust
use speedate::{DateTime, DateTimeConfig, TimestampUnit, TimeConfig};

let cfg = DateTimeConfig::builder()
    .timestamp_unit(TimestampUnit::Millisecond)
    .time_config(TimeConfig::builder().unix_timestamp_offset(Some(0)).build())
    .build();

let dt = DateTime::parse_bytes_with_config(b"1641039194000", &cfg).unwrap();
assert_eq!(dt.to_string(), "2022-01-01T12:13:14Z");
```

Likewise, you can configure `Date` parsing:

```rust
use speedate::{Date, DateConfig, TimestampUnit};

let cfg = DateConfig::builder()
    .timestamp_unit(TimestampUnit::Second)
    .build();
let d = Date::parse_bytes_with_config(b"1640995200", &cfg).unwrap();
assert_eq!(d.to_string(), "2022-01-01");
```

## Performance

**speedate** is significantly faster than
[chrono's `parse_from_rfc3339`](https://docs.rs/chrono/latest/chrono/struct.DateTime.html#method.parse_from_rfc3339)
and [iso8601](https://crates.io/crates/iso8601).

Micro-benchmarking from [`benches/main.rs`](https://github.com/pydantic/speedate/blob/main/benches/main.rs):

```text
test datetime_error_speedate ... bench:           6 ns/iter (+/- 0)
test datetime_error_chrono   ... bench:          50 ns/iter (+/- 1)
test datetime_error_iso8601  ... bench:         118 ns/iter (+/- 2)
test datetime_ok_speedate    ... bench:           9 ns/iter (+/- 0)
test datetime_ok_chrono      ... bench:         182 ns/iter (+/- 0)
test datetime_ok_iso8601     ... bench:          77 ns/iter (+/- 1)
test duration_ok_speedate    ... bench:          23 ns/iter (+/- 0)
test duration_ok_iso8601     ... bench:          48 ns/iter (+/- 0)
test timestamp_ok_speedate   ... bench:           9 ns/iter (+/- 0)
test timestamp_ok_chrono     ... bench:          10 ns/iter (+/- 0)
```

## Why not full iso8601?

ISO8601 allows many formats, see
[ijmacd.github.io/rfc3339-iso8601](https://ijmacd.github.io/rfc3339-iso8601/).

Most of these are unknown to most users, and not desired. This library aims to support the most common formats
without introducing ambiguity.
