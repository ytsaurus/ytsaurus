package yson2json

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yson"
)

func TestRawMessage_MarshalYSON(t *testing.T) {
	for i, testCase := range []struct {
		Input  string
		Output string

		UseInt64  bool
		UseUint64 bool
	}{
		{
			Input:  "3",
			Output: "3.000000",
		},
		{
			Input:     "3",
			Output:    "3u",
			UseUint64: true,
		},
		{
			Input:  "true",
			Output: "%true",
		},
		{
			Input:    "3",
			Output:   "3",
			UseInt64: true,
		},
		{
			Input:     "3",
			Output:    "3",
			UseInt64:  true,
			UseUint64: true,
		},
		{
			Input:     "18446744073709551615",
			Output:    "18446744073709551615u",
			UseInt64:  true,
			UseUint64: true,
		},
		{
			Input:  `[{"a":[{"b":[]}]}]`,
			Output: `[{a=[{b=[];};];};]`,
		},
		{
			Input:    `[{"a":[{"b":[],"c":3,"d":null}]}]`,
			Output:   `[{a=[{b=[];c=3;d=#;};];};]`,
			UseInt64: true,
		},
		{
			Input:    `{"$value": {"x": "y"}, "$attributes": {"attr": 10}}`,
			Output:   `<attr=10;>{x=y;}`,
			UseInt64: true,
		},
		{
			Input:    `{"$value": null, "$attributes": {"attr1": 10, "attr2": true}}`,
			Output:   `<attr1=10;attr2=%true;>#`,
			UseInt64: true,
		},
		{
			Input: `{"schema":{
"$value": [{"name": "id","type_v3": "int64"},{"name": "val","type_v3": "utf8"}],
"$attributes": {"strict": true}
}}`,
			Output: `{schema=<strict=%true;>[{name=id;"type_v3"=int64;};{name=val;"type_v3"=utf8;};];}`,
		},
		{
			Input:  `{}`,
			Output: `{}`,
		},
		{
			Input:  `{"one":{"$value": {"x": "y"}, "$attributes": {"attr": 10}}, "two": {"$value": {"x": "y"}, "$attributes": {"attr2": 10}}}`,
			Output: `{one=<attr=10.000000;>{x=y;};two=<attr2=10.000000;>{x=y;};}`,
		},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			m := RawMessage{
				JSON:      []byte(testCase.Input),
				UseInt64:  testCase.UseInt64,
				UseUint64: testCase.UseUint64,
			}

			out, err := yson.MarshalFormat(m, yson.FormatText)
			require.NoError(t, err)
			require.Equal(t, []byte(testCase.Output), out)
		})
	}
}

func TestRawMessage_UnmarshalYSON(t *testing.T) {
	for i, testCase := range []struct {
		Input  string // YSON
		Output string // JSON
	}{
		// Basic literals.
		{
			Input:  "%true",
			Output: "true",
		},
		{
			Input:  "%false",
			Output: "false",
		},
		{
			Input:  "#",
			Output: "null",
		},
		{
			Input:  "hello",
			Output: `"hello"`,
		},
		{
			Input:  "3.14",
			Output: "3.14",
		},
		{
			Input:  "42",
			Output: "42",
		},
		{
			Input:  "42u",
			Output: "42",
		},
		// Lists.
		{
			Input:  "[]",
			Output: "[]",
		},
		{
			Input:  "[1;2;3]",
			Output: "[1,2,3]",
		},
		{
			Input:  "[%true;hello;#]",
			Output: `[true,"hello",null]`,
		},
		// Maps.
		{
			Input:  "{}",
			Output: "{}",
		},
		{
			Input:  "{a=1;b=hello}",
			Output: `{"a":1,"b":"hello"}`,
		},
		{
			Input:  "{nested={x=42;};list=[1;2]}",
			Output: `{"nested":{"x":42},"list":[1,2]}`,
		},
		// Values with attributes.
		{
			Input:  "<attr=10;>42",
			Output: `{"$attributes":{"attr":10},"$value":42}`,
		},
		{
			Input:  "<a=1;b=%true;>hello",
			Output: `{"$attributes":{"a":1,"b":true},"$value":"hello"}`,
		},
		{
			Input:  "<strict=%true;>[{name=id;};{name=val;}]",
			Output: `{"$attributes":{"strict":true},"$value":[{"name":"id"},{"name":"val"}]}`,
		},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			var m RawMessage
			err := yson.Unmarshal([]byte(testCase.Input), &m)
			require.NoError(t, err)

			var expected, actual any
			err = json.Unmarshal([]byte(testCase.Output), &expected)
			require.NoError(t, err)
			err = json.Unmarshal(m.JSON, &actual)
			require.NoError(t, err)

			require.Equal(t, expected, actual, "YSON: %s, Expected JSON: %s, Got JSON: %s", testCase.Input, testCase.Output, string(m.JSON))
		})
	}
}

func TestRawMessage_RoundTrip(t *testing.T) {
	testCases := []string{
		`true`,
		`false`,
		`null`,
		`"hello world"`,
		`42`,
		`3.14159`,
		`[]`,
		`[1,2,3]`,
		`["hello","world"]`,
		`{}`,
		`{"key":"value"}`,
		`{"number":42,"bool":true,"null":null}`,
		`{"nested":{"array":[1,2,3],"object":{"key":"value"}}}`,
		`{"$value":"test","$attributes":{"attr":10}}`,
		`{"$value":{"nested":"object"},"$attributes":{"a":1,"b":true}}`,
		`{"$value":[1,2,3],"$attributes":{"type":"array"}}`,
	}

	for i, jsonStr := range testCases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			for _, useInt64 := range []bool{false, true} {
				for _, useUint64 := range []bool{false, true} {
					t.Run(fmt.Sprintf("Int64=%v,Uint64=%v", useInt64, useUint64), func(t *testing.T) {
						original := &RawMessage{
							JSON:      json.RawMessage(jsonStr),
							UseInt64:  useInt64,
							UseUint64: useUint64,
						}

						ysonBytes, err := yson.MarshalFormat(original, yson.FormatBinary)
						require.NoError(t, err)

						var result RawMessage
						err = yson.Unmarshal(ysonBytes, &result)
						require.NoError(t, err)

						var originalValue, resultValue any
						err = json.Unmarshal(original.JSON, &originalValue)
						require.NoError(t, err)

						err = json.Unmarshal(result.JSON, &resultValue)
						require.NoError(t, err)

						require.Equal(t, originalValue, resultValue,
							"Round-trip failed. Original JSON: %s, Result JSON: %s",
							string(original.JSON), string(result.JSON))
					})
				}
			}
		})
	}
}

func Benchmark(b *testing.B) {
	testInputJSON := []byte(`[{"a":[{"b":[],"c":3,"d":null}]}]`)
	testInputYSON := []byte(`[{a=[{b=[];c=3;d=#;};];};]`)

	b.Run("MarshalYSONRawMessage", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			m := RawMessage{JSON: json.RawMessage(testInputJSON)}
			_, err := yson.Marshal(m)
			require.NoError(b, err)
		}
	})

	b.Run("UnmarshalJSONMarshalYSON", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var v any
			err := json.Unmarshal(testInputJSON, &v)
			require.NoError(b, err)

			_, err = yson.Marshal(v)
			require.NoError(b, err)
		}
	})

	b.Run("UnmarshalYSONRawMessage", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var result RawMessage
			err := yson.Unmarshal(testInputYSON, &result)
			require.NoError(b, err)
		}
	})

	b.Run("UnmarshalYSONMarshalJSON", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var v any
			err := yson.Unmarshal(testInputYSON, &v)
			require.NoError(b, err)

			_, err = json.Marshal(v)
			require.NoError(b, err)
		}
	})
}
