package mapreduce

import (
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testInput = "ewECQT0CAjsBAkI9AQZmb287fTt7AQJBPQIEOwECQj0BBmJhcjt9Ow=="

func TestReader_SimpleInput(t *testing.T) {
	ys, err := base64.StdEncoding.DecodeString(testInput)
	require.NoError(t, err)

	input := bytes.NewBuffer(ys)
	r := newReader(input)

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
