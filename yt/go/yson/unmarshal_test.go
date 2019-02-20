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

type structWithRawFields struct {
	A RawValue
	B RawValue
}

func TestRawField(t *testing.T) {
	var s structWithRawFields
	require.NoError(t, Unmarshal([]byte("{A=abc;B=[1;2;3]}"), &s))
	require.Equal(t, []byte("abc"), []byte(s.A))
	require.Equal(t, []byte("[1;2;3]"), []byte(s.B))

	buf, err := MarshalFormat(map[string]interface{}{"A": "abc", "B": []int{1, 2, 3}}, FormatBinary)
	require.NoError(t, err)
	require.NoError(t, Unmarshal(buf, &s))
}

type simplestStruct struct {
	A int
}

func TestUnmarshalZeroInitializes(t *testing.T) {
	var v simplestStruct
	v.A = 1

	require.NoError(t, Unmarshal([]byte("{}"), &v))
	require.Equal(t, 0, v.A)
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

type nestedStruct struct {
	A int
}

type outerStruct struct {
	B *nestedStruct
}

func TestUnmarshalPointerField(t *testing.T) {
	var s outerStruct

	require.NoError(t, Unmarshal([]byte("{B={A=1}}"), &s))

	assert.NotNil(t, s.B)
	assert.Equal(t, 1, s.B.A)
}
