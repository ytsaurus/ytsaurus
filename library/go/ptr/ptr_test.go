package ptr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTOrNilInt(t *testing.T) {
	value5 := 5

	tests := []struct {
		name  string
		value int
		want  *int
	}{
		{
			name:  "zero",
			value: 0,
			want:  nil,
		},
		{
			name:  "not zero",
			value: value5,
			want:  &value5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			got := TOrNil(tt.value)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTOrNilStr(t *testing.T) {
	value5 := "5"

	tests := []struct {
		name  string
		value string
		want  *string
	}{
		{
			name:  "zero",
			value: "",
			want:  nil,
		},
		{
			name:  "not zero",
			value: value5,
			want:  &value5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			got := TOrNil(tt.value)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTOrNilStruct(t *testing.T) {
	type customStruct struct {
		A int64
		B string
	}

	value5 := customStruct{
		A: 5,
	}

	tests := []struct {
		name  string
		value customStruct
		want  *customStruct
	}{
		{
			name:  "zero",
			value: customStruct{},
			want:  nil,
		},
		{
			name:  "not zero",
			value: value5,
			want:  &value5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TOrNil(tt.value)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEqualVal(t *testing.T) {
	type S struct {
		i int
		s string
	}

	tests := []struct {
		name string
		v    *S
		w    *S
		want bool
	}{
		{
			name: "equal nil",
			v:    nil,
			w:    nil,
			want: true,
		},
		{
			name: "v nil",
			v:    nil,
			w:    T(S{i: 0, s: "s"}),
			want: false,
		},
		{
			name: "w nil",
			v:    T(S{i: 9, s: "c"}),
			w:    nil,
			want: false,
		},
		{
			name: "equal non-nil",
			v:    T(S{i: 1, s: "a"}),
			w:    T(S{i: 1, s: "a"}),
			want: true,
		},
		{
			name: "not equal non-nil",
			v:    T(S{i: 1, s: "a"}),
			w:    T(S{i: 5, s: "a"}),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EqualVal(tt.v, tt.w)

			assert.Equal(t, tt.want, got)
		})
	}

}
