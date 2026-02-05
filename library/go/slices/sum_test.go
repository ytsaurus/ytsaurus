package slices_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/slices"
)

func TestSumTransformStruct(t *testing.T) {
	type item struct {
		value int
	}

	tests := []struct {
		name     string
		input    []item
		fn       func(item) int
		expected int
	}{
		{
			name:  "empty slice",
			input: []item{},
			fn: func(i item) int {
				return i.value
			},
			expected: 0,
		},
		{
			name:     "nil function",
			input:    []item{{value: 5}},
			fn:       nil,
			expected: 0,
		},
		{
			name:  "single element",
			input: []item{{value: 5}},
			fn: func(i item) int {
				return i.value
			},
			expected: 5,
		},
		{
			name:  "multiple elements",
			input: []item{{value: 1}, {value: 2}, {value: 3}, {value: 4}, {value: 5}},
			fn: func(i item) int {
				return i.value
			},
			expected: 15,
		},
		{
			name:  "with negative values",
			input: []item{{value: -1}, {value: -2}, {value: 3}, {value: 4}},
			fn: func(i item) int {
				return i.value
			},
			expected: 4,
		},
		{
			name:  "with zero values",
			input: []item{{value: 0}, {value: 0}, {value: 0}},
			fn: func(i item) int {
				return i.value
			},
			expected: 0,
		},
		{
			name:  "complex transformation",
			input: []item{{value: 1}, {value: 2}, {value: 3}, {value: 4}, {value: 5}},
			fn: func(i item) int {
				return i.value * 2
			},
			expected: 30, // 2+4+6+8+10 = 30
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := slices.SumTransform(tt.input, tt.fn)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSumTransformInt(t *testing.T) {
	tests := []struct {
		name     string
		input    []int
		fn       func(int) int
		expected int
	}{
		{
			name:  "empty slice",
			input: []int{},
			fn: func(i int) int {
				return i
			},
			expected: 0,
		},
		{
			name:     "nil function",
			input:    []int{5},
			fn:       nil,
			expected: 0,
		},
		{
			name:  "single element",
			input: []int{5},
			fn: func(i int) int {
				return i
			},
			expected: 5,
		},
		{
			name:  "multiple elements",
			input: []int{1, 2, 3, 4, 5},
			fn: func(i int) int {
				return i
			},
			expected: 15,
		},
		{
			name:  "with negative values",
			input: []int{-1, -2, 3, 4},
			fn: func(i int) int {
				return i
			},
			expected: 4,
		},
		{
			name:  "complex transformation",
			input: []int{1, 2, 3, 4, 5},
			fn: func(i int) int {
				return i * 2
			},
			expected: 30, // 2+4+6+8+10 = 30
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := slices.SumTransform(tt.input, tt.fn)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSumTransformUint(t *testing.T) {
	tests := []struct {
		name     string
		input    []uint
		fn       func(uint) uint
		expected uint
	}{
		{
			name:  "empty slice",
			input: []uint{},
			fn: func(i uint) uint {
				return i
			},
			expected: 0,
		},
		{
			name:     "nil function",
			input:    []uint{5},
			fn:       nil,
			expected: 0,
		},
		{
			name:  "single element",
			input: []uint{5},
			fn: func(i uint) uint {
				return i
			},
			expected: 5,
		},
		{
			name:  "multiple elements",
			input: []uint{1, 2, 3, 4, 5},
			fn: func(i uint) uint {
				return i
			},
			expected: 15,
		},
		{
			name:  "complex transformation",
			input: []uint{1, 2, 3, 4, 5},
			fn: func(i uint) uint {
				return i * 2
			},
			expected: 30, // 2+4+6+8+10 = 30
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := slices.SumTransform(tt.input, tt.fn)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSumTransformFloat(t *testing.T) {
	tests := []struct {
		name     string
		input    []float64
		fn       func(float64) float64
		expected float64
	}{
		{
			name:  "empty slice",
			input: []float64{},
			fn: func(i float64) float64 {
				return i
			},
			expected: 0,
		},
		{
			name:     "nil function",
			input:    []float64{5.5},
			fn:       nil,
			expected: 0,
		},
		{
			name:  "single element",
			input: []float64{5.5},
			fn: func(i float64) float64 {
				return i
			},
			expected: 5.5,
		},
		{
			name:  "multiple elements",
			input: []float64{1.1, 2.2, 3.3},
			fn: func(i float64) float64 {
				return i
			},
			expected: 6.6,
		},
		{
			name:  "with negative values",
			input: []float64{-1.5, -2.5, 3.5, 4.5},
			fn: func(i float64) float64 {
				return i
			},
			expected: 4.0,
		},
		{
			name:  "complex transformation",
			input: []float64{1.5, 2.5, 3.5},
			fn: func(i float64) float64 {
				return i * 2.0
			},
			expected: 15.0, // 3.0+5.0+7.0 = 15.0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := slices.SumTransform(tt.input, tt.fn)
			require.Equal(t, tt.expected, result)
		})
	}
}
