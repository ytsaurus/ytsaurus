package instanttime

import "time"

const ytTimeLayout = "2006-01-02 15:04:05,000"

type InstantTime time.Time

func (t *InstantTime) UnmarshalText(text []byte) error {
	ts, err := UnmarshalTime(string(text))
	if err != nil {
		return err
	}

	*t = ts
	return nil
}

func (t InstantTime) MarshalText() (text []byte, err error) {
	s, err := MarshalTime(t)
	return []byte(s), err
}

func UnmarshalTime(in string) (t InstantTime, err error) {
	if in == "#" {
		return InstantTime{}, nil
	}
	var tt time.Time
	tt, err = time.Parse(ytTimeLayout, in)
	if err != nil {
		return InstantTime{}, err
	}
	return InstantTime(tt), nil
}

func MarshalTime(t InstantTime) (s string, err error) {
	if time.Time(t).IsZero() {
		return "#", nil
	}
	return time.Time(t).UTC().Format(ytTimeLayout), nil
}

func (t InstantTime) GobEncode() ([]byte, error) {
	return time.Time(t).MarshalBinary()
}

func (t *InstantTime) GobDecode(data []byte) error {
	var tt time.Time
	if err := tt.UnmarshalBinary(data); err != nil {
		return err
	}
	*t = InstantTime(tt)
	return nil
}

func (t *InstantTime) GetTableIndex(interval time.Duration) int64 {
	return time.Time(*t).Unix() / int64(interval.Seconds())
}

func (t *InstantTime) ToTableName(interval time.Duration, format string) string {
	index := t.GetTableIndex(interval)
	tt := time.Unix(index*int64(interval.Seconds()), 0).UTC()
	return tt.Format(format)
}
