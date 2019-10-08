package ytypes

import (
	"time"

	"a.yandex-team.ru/library/go/core/xerrors"
)

const (
	// Seconds field of the earliest valid Timestamp.
	// This is time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).Unix().
	minValidSeconds = -62135596800
	// Seconds field just after the latest valid Timestamp.
	// This is time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).Unix().
	maxValidSeconds = 253402300800
)

// validateTimestamp determines whether a Timestamp is valid.
// A valid timestamp represents a time in the range
// [0001-01-01, 10000-01-01) and has a Nanos field
// in the range [0, 1e9).
//
// If the Timestamp is valid, validateTimestamp returns nil.
// Otherwise, it returns an error that describes
// the problem.
//
// Every valid Timestamp can be represented by a time.Time, but the converse is not true.
func validateTimestamp(ts *Timestamp) error {
	switch {
	case ts == nil:
		return xerrors.New("timestamp: nil Timestamp")
	case ts.Seconds < minValidSeconds:
		return xerrors.Errorf("timestamp: %v before 0001-01-01", ts)
	case ts.Seconds >= maxValidSeconds:
		return xerrors.Errorf("timestamp: %v after 10000-01-01", ts)
	case ts.Nanos < 0 || ts.Nanos >= 1e9:
		return xerrors.Errorf("timestamp: %v: nanos not in range [0, 1e9)", ts)
	default:
		return nil
	}
}

type Timestamp struct {
	// Represents seconds of UTC time since Unix epoch
	// 1970-01-01T00:00:00Z. Must be from 0001-01-01T00:00:00Z to
	// 9999-12-31T23:59:59Z inclusive.
	Seconds int64 `yson:"seconds" json:"seconds"`

	// Non-negative fractions of a second at nanosecond resolution. Negative
	// second values with fractions must still have non-negative nanos values
	// that count forward in time. Must be from 0 to 999,999,999
	// inclusive.
	Nanos int32 `yson:"nanos" json:"nanos"`
}

// Time converts a Timestamp proto to a time.Time.
func (ts *Timestamp) Time() (time.Time, error) {
	return time.Unix(ts.Seconds, int64(ts.Nanos)).UTC(), validateTimestamp(ts)
}

// TimestampNow returns a ytypes.Timestamp for the current time.
func TimestampNow() *Timestamp {
	return TimestampYson(time.Now())
}

// TimestampYson converts the time.Time to a ytypes.Timestamp.
func TimestampYson(t time.Time) *Timestamp {
	return &Timestamp{
		Seconds: t.Unix(),
		Nanos:   int32(t.Nanosecond()),
	}
}
