package yson

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
)

type simpleStruct struct {
	Int    int    `yson:"a"`
	String string `yson:"b"`
}

func TestDecodeSimpleStruct(t *testing.T) {
	var s simpleStruct
	s.Int = 1
	s.String = "c"

	runTests(t, []testCase{
		{"{a=1;b=c}", s},
		{"{a=1;b=c;d=a;e=a}", s},
		{" { a = 1 ; b = c } ", s},
		{"<foo=bar>{a=<foo=bar>1;b=<foo=bar>c}", s},
	})
}

func BenchmarkDecodeSpeed(b *testing.B) {
	for i := 0; i < b.N; i++ {
		data := []byte("{a=1;b=c}")
		decoder := NewDecoderFromBytes(data)
		var s simpleStruct

		if err := decoder.Decode(&s); err != nil {
			b.Fail()
		}
	}
}

type complexStruct struct {
	Int   *int                    `yson:"a"`
	Map   map[string]simpleStruct `yson:"b"`
	Slice []simpleStruct          `yson:"c"`
}

func TestDecodeComplexStruct(t *testing.T) {
	var s complexStruct
	s.Int = new(int)
	*s.Int = 1

	s.Map = make(map[string]simpleStruct)
	s.Map["foo"] = simpleStruct{Int: 10, String: "foofoo"}
	s.Map["bar"] = simpleStruct{Int: 20, String: "barbar"}

	s.Slice = []simpleStruct{
		{10, "foofoo"},
		{20, "barbar"},
	}

	runTests(t, []testCase{
		{"{a=1;b={foo={a=10;b=foofoo};bar={a=20;b=barbar}};c=[{a=10;b=foofoo};{a=20;b=barbar}]}", s},
	})
}

func BenchmarkComplexDecodeSpeed(b *testing.B) {
	for i := 0; i < b.N; i++ {
		data := []byte("{a=1;b={foo={a=10;b=foofoo};bar={a=20;b=barbar}};c=[{a=10;b=foofoo};{a=20;b=barbar}]}")
		decoder := NewDecoderFromBytes(data)
		var s complexStruct

		if err := decoder.Decode(&s); err != nil {
			b.Fail()
		}
	}
}

type structWithAttributes struct {
	Int    int    `yson:"a,attr"`
	String string `yson:"b,attr"`
	Value  string `yson:",value"`
}

func TestDecodeStructWithAttributes(t *testing.T) {
	var s structWithAttributes
	s.Int = 1
	s.String = "foo"
	s.Value = "bar"

	runTests(t, []testCase{
		{"<a=1;b=foo>bar", s},
	})
}

type structWithSingleValue struct {
	Value string `yson:",value"`
}

func TestDecodeStructWithSingleValue(t *testing.T) {
	var s structWithSingleValue
	s.Value = "bar1"

	runTests(t, []testCase{
		{"<a=1;b=foo>bar1", s},
		{"<>bar1", s},
		{"bar1", s},
	})
}

type structWithOmitEmpty struct {
	I1 int `yson:",omitempty,attr"`
	I2 int `yson:",attr"`

	S1 string `yson:",omitempty"`
	S2 string
}

func TestOmitEmpty(t *testing.T) {
	var s structWithOmitEmpty
	s.I2 = 1
	s.S2 = "a"

	data, err := Marshal(s)
	require.NoError(t, err)
	assert.Equal(t, "<I2=1;>{S2=a;}", string(data))
}

type rawValueWithAttrs struct {
	TableIndex int      `yson:"table_index,attr"`
	Value      RawValue `yson:",value"`
}

func TestDecodeRawValueWithAttrs(t *testing.T) {
	var v rawValueWithAttrs

	require.NoError(t, Unmarshal([]byte("<table_index=1>{A=1}"), &v))
	require.Equal(t, 1, v.TableIndex)
	require.Equal(t, "{A=1}", string(v.Value))

	require.NoError(t, Unmarshal([]byte("{A=2}"), &v))
	require.Equal(t, 1, v.TableIndex)
	require.Equal(t, "{A=2}", string(v.Value))
}

type intWithAttrs struct {
	TableIndex int `yson:"table_index,attr"`
	Value      int `yson:",value"`
}

func TestDecodeIntWithAttrs(t *testing.T) {
	var v intWithAttrs

	require.NoError(t, Unmarshal([]byte("<table_index=1>11"), &v))
	require.Equal(t, 1, v.TableIndex)
	require.Equal(t, 11, v.Value)

	require.NoError(t, Unmarshal([]byte("12"), &v))
	require.Equal(t, 1, v.TableIndex)
	require.Equal(t, 12, v.Value)
}

type valueWithGenericAttrs struct {
	Attrs map[string]any `yson:",attrs"`
	Value any            `yson:",value"`
}

func TestDecodeValueWithGenericAttrs(t *testing.T) {
	var v valueWithGenericAttrs

	require.NoError(t, Unmarshal([]byte("<table_index=1>11"), &v))
	require.Equal(t, map[string]any{"table_index": int64(1)}, v.Attrs)
	require.Equal(t, int64(11), v.Value)
}

func TestMarshalGenericAttrsAreSorted(t *testing.T) {
	b, err := Marshal(valueWithGenericAttrs{
		Attrs: map[string]any{
			"x": 1,
			"y": 2,
			"a": 3,
			"z": 4,
		},
		Value: 29,
	})

	if err != nil {
		t.Fatalf("Failed to Marshal generic attrs: %v", err)
	}
	const want = `<a=3;x=1;y=2;z=4;>29`
	if string(b) != want {
		t.Errorf("Marshal map with generic attrs: got %#q, want %#q", b, want)
	}
}

func TestYPAPIMap(t *testing.T) {
	type Progress struct {
		PodsTotal int              `yson:"pods_total"`
		PodsReady int              `yson:"pods_ready"`
		Nested    map[int32]string `yson:"nested,omitempty"`
	}

	type Details struct {
		Revisions      map[uint64]*Progress `yson:"revisions"`
		EmptyRevisions map[uint64]*Progress `yson:"empty_revisions"`
	}

	details := Details{
		Revisions: map[uint64]*Progress{
			400: {
				PodsTotal: 1,
				PodsReady: 1,
				Nested: map[int32]string{
					42: "hello",
				},
			},
			200: {
				PodsTotal: 2,
				PodsReady: 1,
			},
		},
	}

	encoded, err := MarshalOptions(details, &EncoderOptions{SupportYPAPIMaps: true})
	require.NoError(t, err)

	var decoded Details
	err = UnmarshalOptions(encoded, &decoded, &DecoderOptions{SupportYPAPIMaps: true})
	require.NoError(t, err)

	require.Len(t, decoded.Revisions, 2)
	require.Empty(t, decoded.EmptyRevisions)
	require.Contains(t, decoded.Revisions, uint64(400))
	require.Contains(t, decoded.Revisions, uint64(200))
	require.Equal(t, &Progress{1, 1, map[int32]string{42: "hello"}}, decoded.Revisions[uint64(400)])
	require.Equal(t, &Progress{2, 1, nil}, decoded.Revisions[uint64(200)])
}

type intPtr struct {
	X *int
}

func TestUnmarshalNonNilPtr(t *testing.T) {
	v := intPtr{X: ptr.Int(42)}

	require.NoError(t, json.Unmarshal([]byte(`{"X": null}`), &v))
	require.True(t, v.X == nil)

	v = intPtr{X: ptr.Int(42)}

	require.NoError(t, Unmarshal([]byte(`{X=#}`), &v))
	require.True(t, v.X == nil)
}

type wideStruct struct {
	MyInt8    int8    `yson:"MyInt8"`
	MyInt16   int16   `yson:"MyInt16"`
	MyInt32   int32   `yson:"MyInt32"`
	MyInt64   int64   `yson:"MyInt64"`
	MyUint8   uint8   `yson:"MyUint8"`
	MyUint16  uint16  `yson:"MyUint16"`
	MyUint32  uint32  `yson:"MyUint32"`
	MyUint64  uint64  `yson:"MyUint64"`
	MyFloat   float32 `yson:"MyFloat"`
	MyDouble  float64 `yson:"MyDouble"`
	MyBytes   []byte  `yson:"MyBytes"`
	MyString  string  `yson:"MyString"`
	MyBoolean bool    `yson:"MyBoolean"`
}
