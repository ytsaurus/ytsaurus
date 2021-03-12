package schema

import "time"

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

func NewDate(t time.Time) Date {
	return Date(t.Unix() / (24 * 60 * 60))
}

func (t Date) Time() time.Time {
	return time.Unix(int64(t)*24*60*60, 0).UTC()
}

func NewDatetime(t time.Time) Datetime {
	return Datetime(t.Unix())
}

func (t Datetime) Time() time.Time {
	return time.Unix(int64(t), 0).UTC()
}

func NewTimestamp(t time.Time) Timestamp {
	return Timestamp(t.UnixNano() / 1000)
}

func (t Timestamp) Time() time.Time {
	return time.Unix(0, time.Microsecond.Nanoseconds()*int64(t)).UTC()
}

func NewInterval(d time.Duration) Interval {
	return Interval(d / 1000)
}

func (i Interval) Duration() time.Duration {
	return time.Duration(i) * time.Microsecond
}
