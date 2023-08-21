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
}
