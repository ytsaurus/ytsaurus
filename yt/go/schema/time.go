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

	minDate32      = int64(-53375809)
	maxDate32      = int64(53375808 - 1)
	minDatetime64  = minDate32 * 86400
	maxDatetime64  = maxDate32*86400 + 86400 - 1
	minTimestamp64 = minDatetime64 * 1000000
	maxTimestamp64 = maxDatetime64*1000000 + 999999
	minInterval64  = int64(-9223339708800000000)
	maxInterval64  = int64(9223339708800000000)

	minTimestamp64Time = time.Unix(minDatetime64, 0).UTC()
	maxTimestamp64Time = time.Unix(maxDatetime64, 999999000).UTC()
)

type RangeError struct {
	MinValue any
	MaxValue any
}

func (r RangeError) Error() string {
	return fmt.Sprintf("value is out of [%v, %v] range", r.MinValue, r.MaxValue)
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

	// Date32 is YT type representing number of days since beginning of the unix epoch.
	Date32 int32
	// Datetime64 is YT type representing number of seconds since beginning of the unix epoch.
	Datetime64 int64
	// Timestamp64 is YT type representing number of microseconds since beginning of the unix epoch.
	Timestamp64 int64
	// Interval64 is YT type representing distance between two Timestamp64-s in microseconds.
	Interval64 int64
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

func NewDate32(t time.Time) (Date32, error) {
	seconds := t.Unix()
	days := seconds / (24 * 60 * 60)
	if seconds < 0 && seconds%(24*60*60) != 0 {
		days--
	}
	if days < minDate32 || days > maxDate32 {
		return 0, &RangeError{MinValue: minDate32, MaxValue: maxDate32}
	}
	return Date32(days), nil
}

func (t Date32) Time() time.Time {
	return time.Unix(int64(t)*24*60*60, 0).UTC()
}

func NewDatetime64(t time.Time) (Datetime64, error) {
	seconds := t.Unix()
	if seconds < minDatetime64 || seconds > maxDatetime64 {
		return 0, &RangeError{MinValue: minDatetime64, MaxValue: maxDatetime64}
	}
	return Datetime64(seconds), nil
}

func (t Datetime64) Time() time.Time {
	return time.Unix(int64(t), 0).UTC()
}

func NewTimestamp64(t time.Time) (Timestamp64, error) {
	if t.Before(minTimestamp64Time) || t.After(maxTimestamp64Time) {
		return 0, &RangeError{MinValue: minTimestamp64, MaxValue: maxTimestamp64}
	}
	seconds := t.Unix()
	microseconds := seconds*1000000 + int64(t.Nanosecond())/1000
	return Timestamp64(microseconds), nil
}

func (t Timestamp64) Time() time.Time {
	microseconds := int64(t)
	return time.Unix(microseconds/1000000, (microseconds%1000000)*1000).UTC()
}

func NewInterval64(d time.Duration) (Interval64, error) {
	microseconds := d / time.Microsecond
	return NewInterval64FromMicroseconds(int64(microseconds))
}

// NewInterval64FromMicroseconds creates an Interval64 from its YT representation.
func NewInterval64FromMicroseconds(microseconds int64) (Interval64, error) {
	if microseconds < minInterval64 || microseconds > maxInterval64 {
		return 0, &RangeError{MinValue: minInterval64, MaxValue: maxInterval64}
	}
	return Interval64(microseconds), nil
}

// Microseconds returns the Interval64 value in its YT representation.
func (i Interval64) Microseconds() int64 {
	return int64(i)
}

func (i Interval64) Duration() (time.Duration, error) {
	const maxDurationMicroseconds = int64(1<<63-1) / int64(time.Microsecond)
	const minDurationMicroseconds = -maxDurationMicroseconds

	if i < Interval64(minDurationMicroseconds) || i > Interval64(maxDurationMicroseconds) {
		return 0, fmt.Errorf("interval64 value %d does not fit into time.Duration", i)
	}
	return time.Duration(i) * time.Microsecond, nil
}
