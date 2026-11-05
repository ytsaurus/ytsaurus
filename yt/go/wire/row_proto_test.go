package wire

import (
	"encoding/hex"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// C++ NTableClient::SerializeToString is the authority for these vectors.
var rowProtoGoldenVectors = []struct {
	name string
	row  Row
	hex  string
}{
	{"empty", Row{}, "0000"},
	{"int64_zero", Row{NewInt64(1, 0)}, "0001010300"},
	{"int64_neg1", Row{NewInt64(1, -1)}, "0001010301"},
	{"int64_min", Row{NewInt64(1, math.MinInt64)}, "00010103ffffffffffffffffff01"},
	{"int64_max", Row{NewInt64(1, math.MaxInt64)}, "00010103feffffffffffffffff01"},
	{"uint64_max", Row{NewUint64(2, math.MaxUint64)}, "00010204ffffffffffffffffff01"},
	{"double_neg", Row{NewFloat64(3, -2.5e10)}, "00010305000000e8764817c2"},
	{"bool_true", Row{NewBool(4, true)}, "0001040601"},
	{"bool_false", Row{NewBool(4, false)}, "0001040600"},
	{"string_empty", Row{NewBytes(5, []byte{})}, "0001051000"},
	{"string_hello", Row{NewBytes(5, []byte("hello"))}, "000105100568656c6c6f"},
	{"null", Row{NewNull(6)}, "00010602"},
	{"any", Row{NewAny(7, []byte("{a=1}"))}, "00010711057b613d317d"},
	{"composite", Row{NewComposite(8, []byte("[1;2]"))}, "00010812055b313b325d"},
	{"mixed_multibyte_ids", Row{
		NewBytes(1, []byte("a")),
		NewNull(300),
		NewBool(math.MaxUint16, true),
	}, "000301100161ac0202ffff030601"},
}

func requireRowProtoErrorCategory(t *testing.T, err error, category error) {
	t.Helper()
	require.Error(t, err)

	for _, candidate := range []error{ErrMalformedRowProto, ErrUnsupportedRowProto, ErrIntegerOverflow} {
		if candidate == category {
			require.True(t, errors.Is(err, candidate), "expected %v to match %v", err, candidate)
		} else {
			require.False(t, errors.Is(err, candidate), "expected %v not to match %v", err, candidate)
		}
	}
}

type testRowProtoCause struct{}

func (*testRowProtoCause) Error() string {
	return "wire: detailed cause"
}

func TestRowProtoErrorSentinelStrings(t *testing.T) {
	require.EqualError(t, ErrMalformedRowProto, "wire: malformed row proto")
	require.EqualError(t, ErrUnsupportedRowProto, "wire: unsupported row proto")
}

func TestRowProtoErrorPreservesCause(t *testing.T) {
	cause := &testRowProtoCause{}
	err := classifyRowProtoError(ErrMalformedRowProto, cause)

	require.EqualError(t, err, cause.Error())
	requireRowProtoErrorCategory(t, err, ErrMalformedRowProto)

	var got *testRowProtoCause
	require.True(t, errors.As(err, &got))
	require.Same(t, cause, got)
}

func TestMarshalRowProtoMatchesCppGolden(t *testing.T) {
	for _, tc := range rowProtoGoldenVectors {
		t.Run(tc.name, func(t *testing.T) {
			got, err := MarshalRowProto(tc.row)
			require.NoError(t, err)
			require.Equal(t, tc.hex, hex.EncodeToString(got))
		})
	}
}

func TestUnmarshalRowProtoMatchesCppGolden(t *testing.T) {
	for _, tc := range rowProtoGoldenVectors {
		t.Run(tc.name, func(t *testing.T) {
			data, err := hex.DecodeString(tc.hex)
			require.NoError(t, err)

			row, err := UnmarshalRowProto(data)
			require.NoError(t, err)
			require.Equal(t, tc.row, row)
		})
	}
}

func TestRowProtoDoubleEncoding(t *testing.T) {
	for _, value := range []float64{0, 1.5, -2.5e10, math.MaxFloat64, math.SmallestNonzeroFloat64} {
		data, err := MarshalRowProto(Row{NewFloat64(3, value)})
		require.NoError(t, err)
		require.Len(t, data, 4+8, "double %v must occupy exactly 8 payload bytes", value)

		row, err := UnmarshalRowProto(data)
		require.NoError(t, err)
		require.Equal(t, value, row[0].Float64())
	}
}

func TestRowProtoNullAndEmptyRows(t *testing.T) {
	data, err := MarshalRowProto(nil)
	require.NoError(t, err)
	require.Nil(t, data)

	for _, empty := range [][]byte{nil, {}} {
		row, err := UnmarshalRowProto(empty)
		require.NoError(t, err)
		require.Nil(t, row)
	}

	data, err = MarshalRowProto(Row{})
	require.NoError(t, err)
	require.Equal(t, "0000", hex.EncodeToString(data))

	row, err := UnmarshalRowProto(data)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Empty(t, row)
}

func TestAppendRowProtoReusesBuffer(t *testing.T) {
	dst := make([]byte, 1, 32)
	dst[0] = 0xAA
	start := &dst[0]

	got, err := AppendRowProto(dst, Row{NewInt64(300, -1)})
	require.NoError(t, err)
	require.True(t, start == &got[0])
	require.Equal(t, "aa0001ac020301", hex.EncodeToString(got))

	got, err = AppendRowProto(got, nil)
	require.NoError(t, err)
	require.True(t, start == &got[0])
	require.Equal(t, "aa0001ac020301", hex.EncodeToString(got))
}

func TestMarshalRowProtoRejectsAggregate(t *testing.T) {
	value := NewInt64(1, 42)
	value.Aggregate = true
	dst := []byte{0xAA}

	got, err := AppendRowProto(dst, Row{value})
	requireRowProtoErrorCategory(t, err, ErrUnsupportedRowProto)
	require.ErrorContains(t, err, "aggregate value")
	require.EqualError(t, err, "wire: aggregate value of column 1 is not supported")
	require.Equal(t, dst, got)
}

func TestRowProtoZeroLengthBlob(t *testing.T) {
	for _, blob := range [][]byte{nil, {}} {
		data, err := MarshalRowProto(Row{NewBytes(1, blob)})
		require.NoError(t, err)

		row, err := UnmarshalRowProto(data)
		require.NoError(t, err)
		require.NotNil(t, row[0].Bytes())
		require.Empty(t, row[0].Bytes())
	}
}

func TestUnmarshalRowProtoCopiesBlobs(t *testing.T) {
	expected := [][]byte{
		[]byte("bytes"),
		[]byte("{any=value}"),
		[]byte("[composite]"),
	}
	data, err := MarshalRowProto(Row{
		NewBytes(1, expected[0]),
		NewAny(2, expected[1]),
		NewComposite(3, expected[2]),
	})
	require.NoError(t, err)

	row, err := UnmarshalRowProto(data)
	require.NoError(t, err)
	require.Len(t, row, len(expected))

	for i := range data {
		data[i] ^= 0xFF
	}
	for i := range expected {
		require.Equal(t, expected[i], row[i].Bytes())
	}
}

func TestRowProtoVarUint32Size(t *testing.T) {
	size, err := rowProtoVarUint32Size(math.MaxUint32)
	require.NoError(t, err)
	require.Equal(t, uint64(5), size)

	_, err = rowProtoVarUint32Size(uint64(math.MaxUint32) + 1)
	requireRowProtoErrorCategory(t, err, ErrIntegerOverflow)
	require.ErrorContains(t, err, "exceeds uint32")
	require.EqualError(t, err, "4294967296 exceeds uint32")
}

func TestRowProtoHasNoPolicyValueCountLimit(t *testing.T) {
	row := make(Row, 1025)
	for i := range row {
		row[i] = NewNull(uint16(i))
	}

	data, err := MarshalRowProto(row)
	require.NoError(t, err)
	got, err := UnmarshalRowProto(data)
	require.NoError(t, err)
	require.Equal(t, row, got)
}

func TestUnmarshalRowProtoRejectsVarUint32Overflow(t *testing.T) {
	tooLarge := []byte{0x80, 0x80, 0x80, 0x80, 0x10}
	tests := []struct {
		name  string
		field string
		data  []byte
	}{
		{"version", "version", tooLarge},
		{"value_count", "value count", append([]byte{0x00}, tooLarge...)},
		{"column_id", "column id", append(append([]byte{0x00, 0x01}, tooLarge...), 0x02)},
		{"value_type", "value type", append([]byte{0x00, 0x01, 0x00}, tooLarge...)},
		{"value_length", "value length", append([]byte{0x00, 0x01, 0x00, 0x10}, tooLarge...)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnmarshalRowProto(tc.data)
			requireRowProtoErrorCategory(t, err, ErrMalformedRowProto)
			require.ErrorContains(t, err, tc.field)
			require.ErrorContains(t, err, "exceeds uint32")
		})
	}
}

func TestUnmarshalRowProtoRejectsStructuralCount(t *testing.T) {
	for _, data := range [][]byte{
		{0x00, 0x02, 0x00, 0x00},
		{0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F},
	} {
		_, err := UnmarshalRowProto(data)
		requireRowProtoErrorCategory(t, err, ErrMalformedRowProto)
		require.ErrorContains(t, err, "exceeds structural maximum")
	}
}

func TestUnmarshalRowProtoMatchesCppColumnIDNarrowing(t *testing.T) {
	// C++ reads 65537 as ui32 and narrows it to ui16 without validation.
	data, err := hex.DecodeString("00018180040300")
	require.NoError(t, err)

	row, err := UnmarshalRowProto(data)
	require.NoError(t, err)
	require.Len(t, row, 1)
	require.Equal(t, uint16(1), row[0].ID)
}

func TestUnmarshalRowProtoRejectsNonZeroVersion(t *testing.T) {
	_, err := UnmarshalRowProto([]byte{0x01, 0x00})
	requireRowProtoErrorCategory(t, err, ErrUnsupportedRowProto)
	require.ErrorContains(t, err, "does not support version")
	require.EqualError(t, err, "wire: unversioned row does not support version 1")
}

func TestRowProtoRejectsUnsupportedTypes(t *testing.T) {
	for name, encodedType := range map[string][]byte{
		"min":        {0x00},
		"the_bottom": {0x01},
		"max":        {0xEF, 0x01},
	} {
		t.Run("unmarshal_"+name, func(t *testing.T) {
			data := append([]byte{0x00, 0x01, 0x01}, encodedType...)
			_, err := UnmarshalRowProto(data)
			requireRowProtoErrorCategory(t, err, ErrUnsupportedRowProto)
			require.ErrorContains(t, err, "sentinel value type")
		})
	}

	_, err := UnmarshalRowProto([]byte{0x00, 0x01, 0x01, 0xEF})
	requireRowProtoErrorCategory(t, err, ErrMalformedRowProto)
	require.ErrorContains(t, err, "malformed value type")

	_, err = UnmarshalRowProto([]byte{0x00, 0x01, 0x01, 0x80, 0x02})
	requireRowProtoErrorCategory(t, err, ErrUnsupportedRowProto)
	require.ErrorContains(t, err, "unknown value type")

	_, err = UnmarshalRowProto([]byte{0x00, 0x01, 0x01, 0x07})
	requireRowProtoErrorCategory(t, err, ErrUnsupportedRowProto)
	require.ErrorContains(t, err, "unknown value type")

	_, err = MarshalRowProto(Row{{ID: 1, Type: ValueType(rowProtoTypeMax - typeOffset)}})
	requireRowProtoErrorCategory(t, err, ErrUnsupportedRowProto)
	require.ErrorContains(t, err, "unsupported value type")
}

func TestUnmarshalRowProtoReadsBoolAsRawByte(t *testing.T) {
	row, err := UnmarshalRowProto([]byte{0x00, 0x01, 0x01, 0x06, 0x81})
	require.NoError(t, err)
	require.False(t, row[0].Bool())

	_, err = UnmarshalRowProto([]byte{0x00, 0x01, 0x01, 0x06, 0x81, 0x01})
	requireRowProtoErrorCategory(t, err, ErrMalformedRowProto)
	require.ErrorContains(t, err, "trailing data")

	_, err = UnmarshalRowProto([]byte{0x00, 0x01, 0x01, 0x06})
	requireRowProtoErrorCategory(t, err, ErrMalformedRowProto)
	require.ErrorContains(t, err, "truncated boolean")
}

func TestUnmarshalRowProtoRejectsTrailingData(t *testing.T) {
	_, err := UnmarshalRowProto([]byte{0x00, 0x00, 0xFF})
	requireRowProtoErrorCategory(t, err, ErrMalformedRowProto)
	require.ErrorContains(t, err, "trailing data")
}

func TestUnmarshalRowProtoClassifiesMalformedScalarPayloads(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"version_varint", []byte{0x80}},
		{"int64_varint", []byte{0x00, 0x01, 0x01, 0x03, 0x80}},
		{"uint64_varint", []byte{0x00, 0x01, 0x01, 0x04, 0x80}},
		{"double", []byte{0x00, 0x01, 0x01, 0x05, 0, 0, 0, 0, 0, 0, 0}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnmarshalRowProto(tc.data)
			requireRowProtoErrorCategory(t, err, ErrMalformedRowProto)
		})
	}
}

func TestUnmarshalRowProtoRejectsTruncatedInput(t *testing.T) {
	full, err := MarshalRowProto(Row{NewBytes(1, []byte("hello"))})
	require.NoError(t, err)

	for i := 1; i < len(full); i++ {
		_, err := UnmarshalRowProto(full[:i])
		require.Error(t, err, "truncation at %d bytes must not decode", i)
		requireRowProtoErrorCategory(t, err, ErrMalformedRowProto)
	}
}
