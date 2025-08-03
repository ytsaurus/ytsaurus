package skiff

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
)

type TestRow struct {
	First  *int64 `yson:"first"`
	Second int64  `yson:"second,omitempty"`
	Third  string `yson:"third"`
}

var (
	testSchema = Schema{
		Type: TypeTuple,
		Children: []Schema{
			systemColumns["$key_switch"],
			systemColumns["$row_index"],
			systemColumns["$range_index"],
			optionalColumn("first", TypeInt64),
			optionalColumn("second", TypeInt64),
			{Type: TypeString32, Name: "third"},
		},
	}

	testFormat = Format{
		Name:           "skiff",
		TableSchemas:   []any{&testSchema},
		SchemaRegistry: nil,
	}
)

func TestDecoder(t *testing.T) {
	input := bytes.NewBuffer([]byte{
		0x00, 0x00, // first row
		0x01,                                                 // key switch
		0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // row index == 2
		0x01, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // range index == 3
		0x00,                                                 // first field is missing
		0x01, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // second field is present
		0x08, 0x00, 0x00, 0x00, 'a', 'b', 'b', 'a', 'c', 'a', 'b', 'a', // third field is string

		0x00, 0x00, // second row
		0x00,                                                 // no key switch
		0x00,                                                 // no row index
		0x00,                                                 // no range index
		0x01, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // first field is present
		0x00,                   // second field is missing
		0x00, 0x00, 0x00, 0x00, // empty string

		0x00, 0x00, // third row
		0x00,                   // no key switch
		0x00,                   // no row index
		0x00,                   // no range index
		0x00,                   // first field is missing
		0x00,                   // second field is missing
		0x00, 0x00, 0x00, 0x00, // empty string

		0x00, 0x00, // forth row
		0x00,                                                 // no key switch
		0x00,                                                 // no row index
		0x00,                                                 // no range index
		0x01, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // first field is present
		0x00,                                                 // second field is missing
		0x06, 0x00, 0x00, 0x00, 'f', 'o', 'o', 'b', 'a', 'r', // third field is string

	})

	decoder, err := NewDecoder(input, testFormat)
	require.NoError(t, err)

	var row TestRow

	require.True(t, decoder.Next())
	tableIndex := decoder.TableIndex()
	require.Equal(t, 0, tableIndex)
	keySwitch := decoder.KeySwitch()
	require.True(t, keySwitch)
	rowIndex := decoder.RowIndex()
	require.Equal(t, int64(2), rowIndex)
	rangeIndex := decoder.RangeIndex()
	require.Equal(t, 3, rangeIndex)
	require.NoError(t, decoder.Scan(&row))
	require.Equal(t, row, TestRow{Second: 3, Third: "abbacaba"})

	require.True(t, decoder.Next())
	tableIndex = decoder.TableIndex()
	require.Equal(t, 0, tableIndex)
	keySwitch = decoder.KeySwitch()
	require.False(t, keySwitch)
	rowIndex = decoder.RowIndex()
	require.Equal(t, int64(3), rowIndex)
	rangeIndex = decoder.RangeIndex()
	require.Equal(t, 3, rangeIndex)

	require.NoError(t, decoder.Scan(&row))
	require.Equal(t, row, TestRow{First: ptr.Int64(5)})

	// Test backup() && checkpoint().
	require.NoError(t, decoder.Scan(&row))
	require.Equal(t, row, TestRow{First: ptr.Int64(5)})

	// Row is skipped.
	require.True(t, decoder.Next())
	tableIndex = decoder.TableIndex()
	require.Equal(t, 0, tableIndex)
	keySwitch = decoder.KeySwitch()
	require.False(t, keySwitch)
	rowIndex = decoder.RowIndex()
	require.Equal(t, int64(4), rowIndex)
	rangeIndex = decoder.RangeIndex()
	require.Equal(t, 3, rangeIndex)

	require.True(t, decoder.Next())

	expected := map[string]any{
		"first":  int64(6),
		"second": nil,
		"third":  "foobar",
	}

	var i any
	require.NoError(t, decoder.Scan(&i))
	require.Equal(t, expected, i)

	var m map[string]any
	require.NoError(t, decoder.Scan(&m))
	require.Equal(t, expected, m)

	var intMap map[string]int
	require.Error(t, decoder.Scan(&intMap))

	require.False(t, decoder.Next())
	require.NoError(t, decoder.Err())
}
