package yson

import (
	"fmt"
	"testing"
)

func runGenericTests(t *testing.T, tests []testCase) {
	t.Helper()

	for i, testCase := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Helper()

			testDecoder(t, []byte(testCase.input), testCase.expected, true)
		})
	}
}

func TestDecodeInterface(t *testing.T) {
	var v1 any = map[string]any{
		"a": int64(1),
		"b": "c",
	}

	runGenericTests(t, []testCase{
		{"{a=1;b=c}", v1},
	})
}

func TestDecodeGenericList(t *testing.T) {
	var v1 any = []any{
		int64(1),
		"foo",
		nil,
	}

	runGenericTests(t, []testCase{
		{"[1;foo;#]", v1},
		{"[1;foo;#;]", v1},
	})
}

func TestDecodeGenericAttrs(t *testing.T) {
	var v1 any = &ValueWithAttrs{
		Attrs: map[string]any{
			"foo": int64(1),
			"bar": "zog",
		},
		Value: int64(1),
	}

	runGenericTests(t, []testCase{
		{"<foo=1;bar=zog;>1", v1},
	})
}
