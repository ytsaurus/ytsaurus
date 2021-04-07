package yson

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	Attrs map[string]interface{} `yson:",attrs"`
	Value interface{}            `yson:",value"`
}

func TestDecodeValueWithGenericAttrs(t *testing.T) {
	var v valueWithGenericAttrs

	require.NoError(t, Unmarshal([]byte("<table_index=1>11"), &v))
	require.Equal(t, map[string]interface{}{"table_index": int64(1)}, v.Attrs)
	require.Equal(t, int64(11), v.Value)
}
