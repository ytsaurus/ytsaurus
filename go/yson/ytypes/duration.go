package ytypes

import (
	"time"

	"a.yandex-team.ru/library/go/core/xerrors"
)

type Duration struct {
	// Signed seconds of the span of time. Must be from -315,576,000,000
	// to +315,576,000,000 inclusive. Note: these bounds are computed from:
	// 60 sec/min * 60 min/hr * 24 hr/day * 365.25 days/year * 10000 years
	Seconds int64 `yson:"seconds" json:"seconds"`

	// Signed fractions of a second at nanosecond resolution of the span
	// of time. Durations less than one second are represented with a 0
	// `seconds` field and a positive or negative `nanos` field. For durations
	// of one second or more, a non-zero value for the `nanos` field must be
	// of the same sign as the `seconds` field. Must be from -999,999,999
	// to +999,999,999 inclusive.
	Nanos int32 `yson:"nanos" json:"nanos"`
}

// Duration converts a Duration proto to a time.Duration.
func (d *Duration) Duration() (time.Duration, error) {
	duration := time.Duration(d.Seconds) * time.Second
	if int64(duration/time.Second) != d.Seconds {
		return duration, xerrors.Errorf("duration: %v is out of range for time.Duration", d)
	}

	if d.Nanos != 0 {
		duration += time.Duration(d.Nanos) * time.Nanosecond
		if (duration < 0) != (d.Nanos < 0) {
			return duration, xerrors.Errorf("duration: %v is out of range for time.Duration", d)
		}
	}

	return duration, nil
}

// DurationYSON converts the time.Duration to a ytypes.Duration.
func DurationYSON(d time.Duration) *Duration {
	nanos := d.Nanoseconds()
	secs := nanos / 1e9
	nanos -= secs * 1e9

	return &Duration{
		Seconds: secs,
		Nanos:   int32(nanos),
	}
}
