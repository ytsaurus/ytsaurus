package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompare(t *testing.T) {
	for i, testCase := range []struct {
		a, b []any
	}{
		{[]any{}, []any{1}},
		{[]any{}, []any{uint(1)}},
		{[]any{}, []any{"foo"}},
		{[]any{}, []any{true}},
		{[]any{}, []any{[]byte("bar")}},
		{[]any{}, []any{1}},

		{[]any{nil}, []any{uint(1)}},
		{[]any{nil}, []any{"foo"}},
		{[]any{nil}, []any{true}},
		{[]any{nil}, []any{[]byte("bar")}},
		{[]any{nil}, []any{0.1}},

		{[]any{1}, []any{2}},
		{[]any{false}, []any{true}},
		{[]any{0.1}, []any{0.2}},
		{[]any{uint64(1)}, []any{uint64(2)}},
		{[]any{"a"}, []any{"b"}},
		{[]any{[]byte("a")}, []any{[]byte("b")}},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Logf("a = %#v", testCase.a)
			t.Logf("b = %#v", testCase.b)

			require.Equal(t, -1, CompareRows(testCase.a, testCase.b, nil))
			require.Equal(t, 1, CompareRows(testCase.b, testCase.a, nil))
		})

		t.Run(fmt.Sprintf("%d-a-equal", i), func(t *testing.T) {
			t.Logf("a = %#v", testCase.a)

			require.Equal(t, 0, CompareRows(testCase.a, testCase.a, nil))
		})

		t.Run(fmt.Sprintf("%d-b-equal", i), func(t *testing.T) {
			t.Logf("b = %#v", testCase.b)

			require.Equal(t, 0, CompareRows(testCase.b, testCase.b, nil))
		})
	}
}
