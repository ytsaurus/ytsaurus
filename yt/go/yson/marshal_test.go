package yson

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testRoundtrip(t *testing.T, value any) {
	t.Helper()
	t.Logf("checking value: %v", value)

	data, err := Marshal(value)
	assert.NoError(t, err)

	valueCopy := reflect.New(reflect.TypeOf(value))
	err = Unmarshal(data, valueCopy.Interface())
	assert.NoError(t, err)

	t.Logf("after unmarshal: %v", valueCopy.Elem())
	switch vv := value.(type) {
	case float32:
		if math.IsNaN(float64(vv)) {
			assert.True(t, math.IsNaN(float64(valueCopy.Elem().Interface().(float32))))
			return
		}
	case float64:
		if math.IsNaN(vv) {
			assert.True(t, math.IsNaN(valueCopy.Elem().Interface().(float64)))
			return
		}
	}

	assert.True(t, reflect.DeepEqual(value, valueCopy.Elem().Interface()))
}

func TestRoundtripBasicTypes(t *testing.T) {
	testRoundtrip(t, 0)
	testRoundtrip(t, 1)

	testRoundtrip(t, int(-10))
	testRoundtrip(t, uint(10))

	testRoundtrip(t, int8(-10))
	testRoundtrip(t, int16(-10))
	testRoundtrip(t, int32(-10))
	testRoundtrip(t, int64(-10))

	testRoundtrip(t, uint8(10))
	testRoundtrip(t, uint16(10))
	testRoundtrip(t, uint32(10))
	testRoundtrip(t, uint64(10))

	testRoundtrip(t, "")
	testRoundtrip(t, []byte{})

	var nilByteSlice []byte
	testRoundtrip(t, nilByteSlice)
	var nilByteSlicePtr *[]byte
	testRoundtrip(t, nilByteSlicePtr)

	var nilSlice []int
	testRoundtrip(t, nilSlice)
	var nilSlicePtr *[]int
	testRoundtrip(t, nilSlicePtr)

	testRoundtrip(t, "foo0")
	testRoundtrip(t, []byte{0x01, 0x02, 0x03})

	testRoundtrip(t, true)
	testRoundtrip(t, false)

	testRoundtrip(t, 3.14)
	testRoundtrip(t, math.NaN())
	testRoundtrip(t, math.Inf(1))
	testRoundtrip(t, math.Inf(-1))
}

func TestRoundtripSlices(t *testing.T) {
	testRoundtrip(t, []int{1, 2, 3})
	testRoundtrip(t, []string{"a", "b", "c"})
	testRoundtrip(t, []byte{1, 2})
	testRoundtrip(t, []int8{1, 2})
	testRoundtrip(t, []uint8{1, 2})
	testRoundtrip(t, []bool{true, false})
}

func TestRoundtripArrays(t *testing.T) {
	testRoundtrip(t, [4]int{1, 2, 3, 4})
	testRoundtrip(t, [3]any{uint64(1), 2.3, "4"})
	testRoundtrip(t, [4]byte{1, 2, 3, 4})
	testRoundtrip(t, [4]uint8{1, 2, 3, 4})
	testRoundtrip(t, [4]int8{1, 2, 3, 4})
	testRoundtrip(t, [2]bool{true, false})
}

func TestMarshalStruct(t *testing.T) {
	var simple simpleStruct
	simple.String = "bar0"
	simple.Int = 10

	testRoundtrip(t, simple)
}

type structWithMaps struct {
	M1 map[string]any
	M2 map[string]int
	M3 map[string]int8
	M4 map[string]uint8
}

func TestMarshalMaps(t *testing.T) {
	s := structWithMaps{
		M1: map[string]any{
			"a": "c",
		},
		M2: map[string]int{
			"b": 2,
		},
		M3: map[string]int8{
			"int8": int8(2),
		},
		M4: map[string]uint8{
			"uint8": uint8(2),
		},
	}

	testRoundtrip(t, s)
}

type textMarshaler struct {
	A, B string
}

func (m textMarshaler) MarshalText() ([]byte, error) {
	return []byte(m.A + ":" + m.B), nil
}

func (m *textMarshaler) UnmarshalText(b []byte) error {
	pos := bytes.IndexByte(b, ':')
	if pos == -1 {
		return errors.New("missing separator")
	}
	m.A, m.B = string(b[:pos]), string(b[pos+1:])
	return nil
}

type binaryMarshaler struct {
	A, B string
}

func (m binaryMarshaler) MarshalBinary() ([]byte, error) {
	return []byte(m.A + ":" + m.B), nil
}

func (m *binaryMarshaler) UnmarshalBinary(b []byte) error {
	pos := bytes.IndexByte(b, ':')
	if pos == -1 {
		return errors.New("missing separator")
	}
	m.A, m.B = string(b[:pos]), string(b[pos+1:])
	return nil
}

func TestMarshalMapKeys(t *testing.T) {
	testRoundtrip(t, map[string]string{"1": "2"})

	type MyString string
	testRoundtrip(t, map[MyString]MyString{"1": "2"})

	testRoundtrip(t, map[textMarshaler]textMarshaler{
		{"x", "y"}: {"a", "b"},
		{"p", "q"}: {"r", "s"},
	})

	testRoundtrip(t, map[binaryMarshaler]binaryMarshaler{
		{"x", "y"}: {"a", "b"},
		{"p", "q"}: {"r", "s"},
	})
}

type customMarshal struct{}

func (s *customMarshal) MarshalYSON() ([]byte, error) {
	var buf bytes.Buffer
	w := NewWriterFormat(&buf, FormatBinary)
	w.BeginMap()
	w.MapKeyString("a")
	w.String("b")
	w.MapKeyString("c")
	w.BeginList()
	w.Int64(1)
	w.Int64(2)
	w.Int64(3)
	w.EndList()
	w.EndMap()

	err := w.Finish()
	return buf.Bytes(), err
}

var _ Marshaler = (*customMarshal)(nil)

func TestYSONTranscoding(t *testing.T) {
	data, err := Marshal(&customMarshal{})
	require.NoError(t, err)
	assert.Equal(t, []byte("{a=b;c=[1;2;3;];}"), data)
}

func TestArraySizeMismatch(t *testing.T) {
	var v [3]int

	require.NoError(t, Unmarshal([]byte(`[1]`), &v))
	require.Equal(t, [3]int{1, 0, 0}, v)

	require.NoError(t, Unmarshal([]byte(`[1;2;3;4]`), &v))
	require.Equal(t, [3]int{1, 2, 3}, v)
}

func TestMarshalWideStruct(t *testing.T) {
	wide := wideStruct{
		MyInt8:    127,
		MyInt16:   22222,
		MyInt32:   72999581,
		MyInt64:   1824885299695929,
		MyUint8:   8,
		MyUint16:  22222,
		MyUint32:  38587587,
		MyUint64:  8248848929948,
		MyFloat:   0.100301,
		MyDouble:  0.499942,
		MyBytes:   []byte("hold my beer"),
		MyString:  "All work and no play makes Jack a dull boy",
		MyBoolean: true,
	}

	testRoundtrip(t, wide)
}

func TestMarshalMapKeysAreSorted(t *testing.T) {
	b, err := Marshal(map[string]int{
		"x": 1,
		"y": 2,
		"a": 3,
		"z": 4,
	})
	if err != nil {
		t.Fatalf("Failed to Marshal string map: %v", err)
	}
	const want = `{a=3;x=1;y=2;z=4;}`
	if string(b) != want {
		t.Errorf("Marshal map with string keys: got %#q, want %#q", b, want)
	}
}

func TestTextMarshalerMapKeysAreSorted(t *testing.T) {
	b, err := Marshal(map[textMarshaler]int{
		{"x", "y"}: 1,
		{"y", "x"}: 2,
		{"a", "z"}: 3,
		{"z", "a"}: 4,
	})
	if err != nil {
		t.Fatalf("Failed to Marshal text.Marshaler: %v", err)
	}
	const want = `{"a:z"=3;"x:y"=1;"y:x"=2;"z:a"=4;}`
	if string(b) != want {
		t.Errorf("Marshal map with text.Marshaler keys: got %#q, want %#q", b, want)
	}
}

func TestBinaryMarshalerMapKeysAreSorted(t *testing.T) {
	b, err := Marshal(map[binaryMarshaler]int{
		{"x", "y"}: 1,
		{"y", "x"}: 2,
		{"a", "z"}: 3,
		{"z", "a"}: 4,
	})
	if err != nil {
		t.Fatalf("Failed to Marshal binary.Marshaler: %v", err)
	}
	const want = `{"a:z"=3;"x:y"=1;"y:x"=2;"z:a"=4;}`
	if string(b) != want {
		t.Errorf("Marshal map with binary.Marshaler keys: got %#q, want %#q", b, want)
	}
}
