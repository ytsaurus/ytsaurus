package schema

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimeConversion(t *testing.T) {
	maxTime, err := time.Parse(time.RFC3339Nano, "2105-12-31T23:59:59.999999Z")
	require.NoError(t, err)

	t.Run("Date", func(t *testing.T) {
		var d Date = 49673 - 1
		require.Equal(t, "2105-12-31T00:00:00Z", d.Time().Format(time.RFC3339Nano))

		d, err := NewDate(maxTime)
		require.NoError(t, err)
		require.Equal(t, "2105-12-31T00:00:00Z", d.Time().Format(time.RFC3339Nano))
	})

	t.Run("Datetime", func(t *testing.T) {
		var d Datetime = 49673*86400 - 1
		require.Equal(t, "2105-12-31T23:59:59Z", d.Time().Format(time.RFC3339Nano))

		d, err = NewDatetime(maxTime)
		require.NoError(t, err)
		require.Equal(t, "2105-12-31T23:59:59Z", d.Time().Format(time.RFC3339Nano))
	})

	t.Run("Timestamp", func(t *testing.T) {
		var d Timestamp = 49673*86400*1000000 - 1
		require.Equal(t, "2105-12-31T23:59:59.999999Z", d.Time().Format(time.RFC3339Nano))

		d, err = NewTimestamp(maxTime)
		require.NoError(t, err)
		require.Equal(t, "2105-12-31T23:59:59.999999Z", d.Time().Format(time.RFC3339Nano))
	})

	t.Run("Date32 before unix epoch", func(t *testing.T) {
		beforeEpoch := time.Unix(-1, 0).UTC()
		d, err := NewDate32(beforeEpoch)
		require.NoError(t, err)
		require.Equal(t, Date32(-1), d)
		require.Equal(t, "1969-12-31T00:00:00Z", d.Time().Format(time.RFC3339Nano))

		date := time.Date(1960, time.January, 2, 0, 0, 0, 0, time.UTC)
		d, err = NewDate32(date)
		require.NoError(t, err)
		require.Equal(t, date, d.Time())
	})

	t.Run("Datetime64 before unix epoch", func(t *testing.T) {
		date := time.Date(1960, time.January, 2, 3, 4, 5, 0, time.UTC)
		d, err := NewDatetime64(date)
		require.NoError(t, err)
		require.Equal(t, date, d.Time())
	})
}

func TestTimeConversion_range(t *testing.T) {
	t.Run("Date", func(t *testing.T) {
		_, err := NewDate(minTime.Add(-time.Nanosecond))
		require.Error(t, err)
		require.IsType(t, &RangeError{}, err)

		_, err = NewDate(maxTime.Add(time.Nanosecond))
		require.Error(t, err)
		require.IsType(t, &RangeError{}, err)
	})

	t.Run("Datetime", func(t *testing.T) {
		_, err := NewDatetime(minTime.Add(-time.Nanosecond))
		require.Error(t, err)
		require.IsType(t, &RangeError{}, err)

		_, err = NewDatetime(maxTime.Add(time.Nanosecond))
		require.Error(t, err)
		require.IsType(t, &RangeError{}, err)
	})

	t.Run("Timestamp", func(t *testing.T) {
		_, err := NewTimestamp(minTime.Add(-time.Nanosecond))
		require.Error(t, err)
		require.IsType(t, &RangeError{}, err)

		_, err = NewTimestamp(maxTime.Add(time.Nanosecond))
		require.Error(t, err)
		require.IsType(t, &RangeError{}, err)
	})

	t.Run("Interval", func(t *testing.T) {
		_, err := NewInterval(minInterval - time.Nanosecond)
		require.Error(t, err)
		require.IsType(t, &RangeError{}, err)

		_, err = NewInterval(maxInterval + time.Nanosecond)
		require.Error(t, err)
		require.IsType(t, &RangeError{}, err)
	})

	t.Run("Timestamp64", func(t *testing.T) {
		_, err := NewTimestamp64(minTimestamp64Time.Add(-time.Nanosecond))
		require.Error(t, err)
		require.IsType(t, &RangeError{}, err)

		_, err = NewTimestamp64(maxTimestamp64Time.Add(time.Nanosecond))
		require.Error(t, err)
		require.IsType(t, &RangeError{}, err)

		for _, seconds := range []int64{-18_446_744_073_709, 18_446_744_073_709} {
			_, err = NewTimestamp64(time.Unix(seconds, 0))
			require.Error(t, err)
			require.IsType(t, &RangeError{}, err)
		}
	})
}

func TestRangeError(t *testing.T) {
	err := RangeError{MinValue: int64(-1), MaxValue: int64(1)}
	require.Equal(t, "value is out of [-1, 1] range", err.Error())
}

func TestInterval64Conversion(t *testing.T) {
	const maxInterval64 int64 = 9223339708799000000

	i, err := NewInterval64FromMicroseconds(maxInterval64)
	require.NoError(t, err)
	require.Equal(t, maxInterval64, i.Microseconds())

	_, err = i.Duration()
	require.Error(t, err)

	i, err = NewInterval64FromMicroseconds(1_000_000)
	require.NoError(t, err)
	d, err := i.Duration()
	require.NoError(t, err)
	require.Equal(t, time.Second, d)
}
