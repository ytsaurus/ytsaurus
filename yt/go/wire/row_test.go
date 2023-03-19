package wire

import (
	"fmt"
	"testing"

	"github.com/mitchellh/copystructure"
	"github.com/stretchr/testify/require"
)

func TestValueType_String(t *testing.T) {
	for _, tc := range []struct {
		typ      ValueType
		expected string
	}{
		{typ: TypeNull, expected: "null"},
		{typ: TypeInt64, expected: "int64"},
		{typ: TypeUint64, expected: "uint64"},
		{typ: TypeFloat64, expected: "float64"},
		{typ: TypeBool, expected: "bool"},
		{typ: TypeBytes, expected: "bytes"},
		{typ: TypeAny, expected: "any"},
	} {
		t.Run(fmt.Sprintf("%d", tc.typ), func(t *testing.T) {
			actual := tc.typ.String()
			require.Equal(t, tc.expected, actual)
		})
	}
}

var testRow = Row{
	NewNull(1),
	NewBool(2, true),
	NewBool(3, false),
	NewInt64(4, -42),
	NewUint64(5, 42),
	NewFloat64(6, 1.25),
	NewBytes(7, []byte("foobar")),
	NewAny(8, []byte("[1;2;3]")),
	NewBytes(9, []byte("")),
}

func TestRowsetMarshal(t *testing.T) {
	rowset := []Row{
		nil,
		{},
		testRow,
	}

	bytes, err := MarshalRowset(rowset)
	require.NoError(t, err)
	require.NotEmpty(t, bytes)

	result, err := UnmarshalRowset(bytes)
	require.NoError(t, err)
	require.Equal(t, result, rowset)
}

var testRowset []Row

func init() {
	for i := 0; i < 10000; i++ {
		testRowset = append(testRowset, copystructure.Must(copystructure.Copy(testRow)).(Row))
	}
}

func BenchmarkMarshalRowset(b *testing.B) {
	var size int
	for i := 0; i < b.N; i++ {
		blob, err := MarshalRowset(testRowset)
		require.NoError(b, err)
		size = len(blob)
	}

	b.SetBytes(int64(size))
}
