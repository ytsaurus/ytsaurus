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
		{typ: TypeComposite, expected: "composite"},
	} {
		t.Run(fmt.Sprintf("%d", tc.typ), func(t *testing.T) {
			actual := tc.typ.String()
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestValueType_Code(t *testing.T) {
	for _, tc := range []struct {
		typ  ValueType
		code uint8
	}{
		{typ: TypeNull, code: 0x02},
		{typ: TypeInt64, code: 0x03},
		{typ: TypeUint64, code: 0x04},
		{typ: TypeFloat64, code: 0x05},
		{typ: TypeBool, code: 0x06},
		{typ: TypeBytes, code: 0x10},
		{typ: TypeAny, code: 0x11},
		{typ: TypeComposite, code: 0x12},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			require.Equal(t, tc.code, tc.typ.Code())

			typ, err := ValueTypeFromCode(tc.code)
			require.NoError(t, err)
			require.Equal(t, tc.typ, typ)
		})
	}
}

func TestValueTypeFromCode_Invalid(t *testing.T) {
	// 0x00, 0x01 and 0xef are the query sentinels Min, TheBottom and Max.
	for _, code := range []uint8{0x00, 0x01, 0x07, 0x0f, 0x13, 0xef, 0xff} {
		t.Run(fmt.Sprintf("0x%02x", code), func(t *testing.T) {
			_, err := ValueTypeFromCode(code)
			require.Error(t, err)
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
