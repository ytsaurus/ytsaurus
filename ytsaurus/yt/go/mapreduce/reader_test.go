package mapreduce

import (
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testJobInput = `
<"table_index"=4;>#;
<"range_index"=5;>#;
<"row_index"=2;>#;
{"a"=2;};
<"key_switch"=%true;>#;
{"a"=3;};
<"key_switch"=%true;>#;
<"row_index"=0;>#;
{"a"=1;}
`

func TestReadControlAttributes(t *testing.T) {
	buf := bytes.NewBufferString(testJobInput)
	r := newReader(buf, nil)

	var row struct {
		A int `yson:"a"`
	}

	require.True(t, r.Next())

	require.Equal(t, 4, r.TableIndex())
	require.Equal(t, 5, r.RangeIndex())
	require.Equal(t, int64(2), r.RowIndex())
	require.False(t, r.KeySwitch())
	require.NoError(t, r.Scan(&row))
	require.Equal(t, 2, row.A)

	require.True(t, r.Next())

	require.Equal(t, 4, r.TableIndex())
	require.Equal(t, 5, r.RangeIndex())
	require.Equal(t, int64(3), r.RowIndex())
	require.True(t, r.KeySwitch())
	require.NoError(t, r.Scan(&row))
	require.Equal(t, 3, row.A)

	require.True(t, r.Next())

	require.Equal(t, 4, r.TableIndex())
	require.Equal(t, 5, r.RangeIndex())
	require.Equal(t, int64(0), r.RowIndex())
	require.True(t, r.KeySwitch())
	require.NoError(t, r.Scan(&row))
	require.Equal(t, 1, row.A)

	require.False(t, r.Next())
}

var testInput = "ewECQT0CAjsBAkI9AQZmb287fTt7AQJBPQIEOwECQj0BBmJhcjt9Ow=="

func TestReader_SimpleInput(t *testing.T) {
	ys, err := base64.StdEncoding.DecodeString(testInput)
	require.NoError(t, err)

	input := bytes.NewBuffer(ys)
	r := newReader(input, nil)

	var row struct {
		A int
		B string
	}

	require.True(t, r.Next())

	require.Equal(t, 0, r.TableIndex())

	require.NoError(t, r.Scan(&row))
	assert.Equal(t, 1, row.A)
	assert.Equal(t, "foo", row.B)

	require.True(t, r.Next())

	require.Equal(t, 0, r.TableIndex())
	require.NoError(t, r.Scan(&row))
	assert.Equal(t, 2, row.A)
	assert.Equal(t, "bar", row.B)

	require.False(t, r.Next())
	require.NoError(t, r.Err())
}
