package yson

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInferAttrs(t *testing.T) {
	type IntAlias int

	type Inner struct {
		G bool `yson:"g,attr"`
	}

	type Anonymous struct {
		F *int `yson:"f,attr"`
		*Inner
	}

	type anonymous struct {
		F *int `yson:"f,attr"`
		*Inner
	}

	type inferResults struct {
		attrs []string
		err   bool
	}

	for _, tc := range []struct {
		name     string
		input    any
		expected *inferResults
	}{
		{
			name:     "nil",
			input:    nil,
			expected: &inferResults{err: true},
		},
		{
			name:     "not-a-struct",
			input:    512,
			expected: &inferResults{err: true},
		},
		{
			name: "simple",
			input: struct {
				Value  string  `yson:",value"`
				Attr   int64   `yson:"attr,attr"`
				NoName float64 `yson:",attr"`
				NoAttr string  `yson:"no_attr"`
				NoTag  string
				Inner  *Inner `yson:"inner,attr"`
			}{},
			expected: &inferResults{attrs: []string{"attr", "NoName", "inner"}},
		},
		{
			name: "struct-pointer",
			input: &struct {
				S string `yson:"s,attr"`
			}{},
			expected: &inferResults{attrs: []string{"s"}},
		},
		{
			name: "skip-field",
			input: struct {
				S string `yson:"-"`
			}{},
			expected: &inferResults{attrs: nil},
		},
		{
			name: "tagged-anonymous-int",
			input: struct {
				S        string `yson:"s,attr"`
				IntAlias `yson:"a,attr"`
			}{},
			expected: &inferResults{attrs: []string{"s", "a"}},
		},
		{
			name: "untagged-anonymous-int",
			input: struct {
				S string `yson:"s,attr"`
				IntAlias
			}{},
			expected: &inferResults{attrs: []string{"s"}},
		},
		{
			name: "tagged-anonymous-struct",
			input: struct {
				S         string `yson:"s,attr"`
				Anonymous `yson:"a,attr"`
			}{},
			expected: &inferResults{attrs: []string{"s", "a"}},
		},
		{
			name: "untagged-anonymous-struct",
			input: struct {
				S string `yson:"s,attr"`
				Anonymous
			}{},
			expected: &inferResults{attrs: []string{"s", "f", "g"}},
		},
		{
			name: "skipped-anonymous-struct",
			input: struct {
				S         string `yson:"s,attr"`
				Anonymous `yson:"-"`
			}{},
			expected: &inferResults{attrs: []string{"s"}},
		},
		{
			name: "tagged-anonymous-non-attr",
			input: struct {
				S         string `yson:"s,attr"`
				Anonymous `yson:"a,value"`
			}{},
			expected: &inferResults{attrs: []string{"s"}},
		},
		{
			name: "unexported-tagged-anonymous-struct",
			input: struct {
				S         string `yson:"s,attr"`
				anonymous `yson:"a,attr"`
			}{},
			expected: &inferResults{attrs: []string{"s", "a"}},
		},
		{
			name: "unexported-untagged-anonymous-struct",
			input: struct {
				S string `yson:"s,attr"`
				anonymous
			}{},
			expected: &inferResults{attrs: []string{"s", "f", "g"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attrs, err := InferAttrs(tc.input)
			if tc.expected.err {
				require.Error(t, err)

				require.Panics(t, func() {
					MustInferAttrs(tc.input)
				})
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected.attrs, attrs)

				require.NotPanics(t, func() {
					MustInferAttrs(tc.input)
				})
			}
		})
	}
}
