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

func Benchmark(b *testing.B) {
	testInput := []byte(`[{"a":[{"b":[],"c":3,"d":null}]}]`)

	b.Run("RawMessage", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			m := RawMessage{JSON: json.RawMessage(testInput)}

			_, err := json.Marshal(m)
			require.NoError(b, err)
		}
	})

	b.Run("UnmarshalMarshal", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var v any

			err := json.Unmarshal(testInput, &v)
			require.NoError(b, err)

			_, err = json.Marshal(v)
			require.NoError(b, err)
		}
	})
}
