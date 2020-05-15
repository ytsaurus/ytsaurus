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

		d = NewDate(maxTime)
		require.Equal(t, "2105-12-31T00:00:00Z", d.Time().Format(time.RFC3339Nano))
	})

	t.Run("Datetime", func(t *testing.T) {
		var d Datetime = 49673*86400 - 1
		require.Equal(t, "2105-12-31T23:59:59Z", d.Time().Format(time.RFC3339Nano))

		d = NewDatetime(maxTime)
		require.Equal(t, "2105-12-31T23:59:59Z", d.Time().Format(time.RFC3339Nano))
	})

	t.Run("Timestamp", func(t *testing.T) {
		var d Timestamp = 49673*86400*1000000 - 1
		require.Equal(t, "2105-12-31T23:59:59.999999Z", d.Time().Format(time.RFC3339Nano))

		d = NewTimestamp(maxTime)
		require.Equal(t, "2105-12-31T23:59:59.999999Z", d.Time().Format(time.RFC3339Nano))
	})
}
