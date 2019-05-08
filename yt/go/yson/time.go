package yson

import "time"

const ytTimeLayout = "2006-01-02T15:04:05.999999Z"

// Time is alias for time.Time with YT specific time representation format.
type Time time.Time

// Duration is alias for time.Duration with YT specific time representation format.
type Duration time.Duration

// UnmarshalTime decodes time from YT-specific time format.
func UnmarshalTime(in string) (t Time, err error) {
	var tt time.Time
	tt, err = time.Parse(ytTimeLayout, in)
	t = Time(tt)
	return
}

// UnmarshalTime encodes time to YT-specific time format.
func MarshalTime(t Time) (s string, err error) {
	return time.Time(t).Format(ytTimeLayout), nil
}
