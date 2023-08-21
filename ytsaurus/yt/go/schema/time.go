package schema

import (
	"fmt"
	"time"
)

var (
	minTime, _ = time.Parse(time.RFC3339Nano, "1970-01-01T00:00:00.000000Z")
	maxTime, _ = time.Parse(time.RFC3339Nano, "2105-12-31T23:59:59.999999Z")

	minInterval = time.Duration((-49673*86400*1000000 + 1) * 1000)
	maxInterval = time.Duration((49673*86400*1000000 - 1) * 1000)
)

type RangeError struct {
	MinValue any
	MaxValue any
}

func (r RangeError) Error() string {
	return fmt.Sprintf("value is out of [%s, %s] range", r.MinValue, r.MaxValue)
}

type (
	// Date is YT type representing number of days since beginning of the unix epoch.
	Date uint64
	// Datetime is YT type representing number of seconds since beginning of the unix epoch.
	Datetime uint64
	// Timestamp is YT type representing number of microseconds since beginning of the unix epoch.
	Timestamp uint64

	// Interval is YT type representing distance between two Timestamps-s in microseconds.
	Interval int64
)

func NewDate(t time.Time) (Date, error) {
	if t.Before(minTime) || t.After(maxTime) {
		return 0, &RangeError{MinValue: minTime, MaxValue: maxTime}
	}
	return Date(t.Unix() / (24 * 60 * 60)), nil
}

func (t Date) Time() time.Time {
	return time.Unix(int64(t)*24*60*60, 0).UTC()
}

func NewDatetime(t time.Time) (Datetime, error) {
	if t.Before(minTime) || t.After(maxTime) {
		return 0, &RangeError{MinValue: minTime, MaxValue: maxTime}
	}
	return Datetime(t.Unix()), nil
}

func (t Datetime) Time() time.Time {
	return time.Unix(int64(t), 0).UTC()
}

func NewTimestamp(t time.Time) (Timestamp, error) {
	if t.Before(minTime) || t.After(maxTime) {
		return 0, &RangeError{MinValue: minTime, MaxValue: maxTime}
	}
	return Timestamp(t.UnixNano() / 1000), nil
}

func (t Timestamp) Time() time.Time {
	return time.Unix(0, time.Microsecond.Nanoseconds()*int64(t)).UTC()
}

func NewInterval(d time.Duration) (Interval, error) {
	if d < minInterval || d > maxInterval {
		return 0, &RangeError{MinValue: minInterval, MaxValue: maxInterval}
	}
	return Interval(d / 1000), nil
}

func (i Interval) Duration() time.Duration {
	return time.Duration(i) * time.Microsecond
}
