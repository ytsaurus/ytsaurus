package yson

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvalidInput(t *testing.T) {
	assert.IsType(t, Unmarshal([]byte{}, 0), &UnsupportedTypeError{})
	assert.IsType(t, Unmarshal([]byte{}, nil), &UnsupportedTypeError{})
}

func TestInvalidSyntax(t *testing.T) {
	var v interface{}
	require.Error(t, Unmarshal([]byte("0>0"), &v))
}

func TestUnmarshalZeroInitializes(t *testing.T) {

}

func TestUnmarshalRecursionLimit(t *testing.T) {
	in := strings.Repeat("[", 512) + strings.Repeat("]", 512)

	var value interface{}
	require.Error(t, Unmarshal([]byte(in), &value))
}

func TestUnmarhalListFragment(t *testing.T) {
	in := "1;2;"

	r := NewReaderKindFromBytes([]byte(in), StreamListFragment)
	d := Decoder{r}

	var i int
	require.Nil(t, d.Decode(&i))
	require.Equal(t, 1, i)

	require.Nil(t, d.Decode(&i))
	require.Equal(t, 2, i)
}
