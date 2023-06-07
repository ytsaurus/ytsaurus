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
	var v any
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

	buf, err := MarshalFormat(map[string]any{"A": "abc", "B": []int{1, 2, 3}}, FormatBinary)
	require.NoError(t, err)
	require.NoError(t, Unmarshal(buf, &s))
}

type simplestStruct struct {
	A int
}

func TestUnmarshalZeroInitializes(t *testing.T) {
	var v simplestStruct
	v.A = 1

	require.NoError(t, Unmarshal([]byte(`{}`), &v))
	require.Equal(t, 1, v.A)
}

func TestUnmarshalRecursionLimit(t *testing.T) {
	in := strings.Repeat("[", 512) + strings.Repeat("]", 512)

	var value any
	require.Error(t, Unmarshal([]byte(in), &value))
}

func TestUnmarshalListFragment(t *testing.T) {
	in := "{A=1;};{B=2;};"

	r := NewReaderKindFromBytes([]byte(in), StreamListFragment)
	d := Decoder{R: r}

	ok, err := r.NextListItem()
	require.NoError(t, err)
	require.True(t, ok)

	var v RawValue
	require.NoError(t, d.Decode(&v))
	require.Equal(t, "{A=1;}", string(v))

	ok, err = r.NextListItem()
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, d.Decode(&v))
	require.Equal(t, "{B=2;}", string(v))

	ok, err = r.NextListItem()
	require.NoError(t, err)
	require.False(t, ok)
}

func TestUnmarshalPointerField(t *testing.T) {
	type nestedStruct struct {
		A int
	}

	type outerStruct struct {
		B *nestedStruct
	}

	var s outerStruct

	require.NoError(t, Unmarshal([]byte("{B={A=1}}"), &s))

	assert.NotNil(t, s.B)
	assert.Equal(t, 1, s.B.A)
}

func TestUnmarshalEntity(t *testing.T) {
	type nestedStruct struct {
		A int `yson:"A,attr"`
	}

	type outerStruct struct {
		B *nestedStruct
	}

	var s outerStruct

	require.NoError(t, Unmarshal([]byte("{B=#}"), &s))
	assert.Nil(t, s.B)

	require.NoError(t, Unmarshal([]byte("{B=<A=12>#}"), &s))
	require.NotNil(t, s.B)
	require.Equal(t, 12, s.B.A)
}
