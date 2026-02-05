package slices_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.ytsaurus.tech/library/go/slices"
)

func TestJoin(t *testing.T) {
	testCases := []struct {
		input    any
		glue     string
		expected string
	}{
		{[]string{"hello", "world"}, "|", "hello|world"},
		{[]int{1, 2, 3, 4, 5}, ";", "1;2;3;4;5"},
		{[]float32{3.14, 42}, "/", "3.14/42"},
		{[]string{"", "", "", "", "", "", "Batman"}, "NaN", "NaNNaNNaNNaNNaNNaNBatman"},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			assert.Equal(t, tc.expected, slices.Join(tc.input, tc.glue))
		})
	}
}
