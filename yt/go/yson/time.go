package yson

import "time"

const ytTimeLayout = "2006-01-02T15:04:05.000000Z"

// Time is an alias for time.Time with YT specific time representation format.
type Time time.Time

func (t Time) IsZero() bool {
	return time.Time(t).IsZero()
}

func (t *Time) UnmarshalText(text []byte) error {
	ts, err := UnmarshalTime(string(text))
	if err != nil {
		return err
	}

	*t = ts
	return nil
}

func (t Time) MarshalText() (text []byte, err error) {
	s, err := MarshalTime(t)
	return []byte(s), err
}

// UnmarshalTime decodes time from YT-specific time format.
//
// Entity is decoded into zero time.
func UnmarshalTime(in string) (t Time, err error) {
	if in == "#" {
		return Time{}, nil
	}
	var tt time.Time
	tt, err = time.Parse(ytTimeLayout, in)
	t = Time(tt)
	return
}

// MarshalTime encodes time to YT-specific time format.
//
// Zero time is encoded into entity.
func MarshalTime(t Time) (s string, err error) {
	if time.Time(t).IsZero() {
		return "#", nil
	}
	return time.Time(t).UTC().Format(ytTimeLayout), nil
}

// Duration is an alias for time.Duration with YT specific time representation format.
type Duration time.Duration
