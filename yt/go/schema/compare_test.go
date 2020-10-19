package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompare(t *testing.T) {
	for i, testCase := range []struct {
		a, b []interface{}
	}{
		{[]interface{}{}, []interface{}{1}},
		{[]interface{}{}, []interface{}{uint(1)}},
		{[]interface{}{}, []interface{}{"foo"}},
		{[]interface{}{}, []interface{}{true}},
		{[]interface{}{}, []interface{}{[]byte("bar")}},
		{[]interface{}{}, []interface{}{1}},

		{[]interface{}{nil}, []interface{}{uint(1)}},
		{[]interface{}{nil}, []interface{}{"foo"}},
		{[]interface{}{nil}, []interface{}{true}},
		{[]interface{}{nil}, []interface{}{[]byte("bar")}},
		{[]interface{}{nil}, []interface{}{0.1}},

		{[]interface{}{1}, []interface{}{2}},
		{[]interface{}{false}, []interface{}{true}},
		{[]interface{}{0.1}, []interface{}{0.2}},
		{[]interface{}{uint64(1)}, []interface{}{uint64(2)}},
		{[]interface{}{"a"}, []interface{}{"b"}},
		{[]interface{}{[]byte("a")}, []interface{}{[]byte("b")}},
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
