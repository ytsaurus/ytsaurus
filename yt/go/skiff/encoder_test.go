package skiff

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/ptr"
)

func TestEncoder(t *testing.T) {
	var buf bytes.Buffer

	e, err := NewEncoder(&buf, testSchema)
	require.NoError(t, err)

	require.NoError(t, e.Write(TestRow{First: ptr.Int64(5)}))
	require.NoError(t, e.Write(TestRow{Second: 6}))
	require.NoError(t, e.Write(TestRow{Third: "abbacaba"}))
	require.NoError(t, e.Flush())

	expectedOutput := []byte{
		0x00, 0x00, // first row
		0x00,                                                 // no key switch
		0x00,                                                 // no row index
		0x00,                                                 // no range index
		0x01, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // first field is present
		0x00,                   // second field is missing
		0x00, 0x00, 0x00, 0x00, // empty string

		0x00, 0x00, // second row
		0x00,                                                 // no key switch
		0x00,                                                 // no row index
		0x00,                                                 // no range index
		0x00,                                                 // first field is missing
		0x01, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // second field is present
		0x00, 0x00, 0x00, 0x00, // empty string

		0x00, 0x00, // third row
		0x00,                                                           // no key switch
		0x00,                                                           // no row index
		0x00,                                                           // no range index
		0x00,                                                           // first field is missing
		0x00,                                                           // second field is missing
		0x08, 0x00, 0x00, 0x00, 'a', 'b', 'b', 'a', 'c', 'a', 'b', 'a', // third field is string
	}

	require.Equal(t, expectedOutput, buf.Bytes())
}

func TestEncoderDecoder(t *testing.T) {
	var buf bytes.Buffer

	var testRow = TestRow{
		First:  ptr.Int64(42),
		Second: 42,
		Third:  "foobar",
	}

	e, err := NewEncoder(&buf, testSchema)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, e.Write(testRow))
	}
	require.NoError(t, e.Flush())

	d, err := NewDecoder(&buf, testFormat)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.True(t, d.Next())

		var out TestRow
		require.NoError(t, d.Scan(&out))
		require.Equal(t, testRow, out)
	}

	require.False(t, d.Next())
	require.NoError(t, d.Err())
}
