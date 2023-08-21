package yson

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSliceYPathAttrs(t *testing.T) {
	for _, test := range []struct {
		input string
		n     int
		bad   bool
	}{
		{"//home/prime", 0, false},
		{"<>   #abc", 5, false},
		{"<append=%true; keys=[1;2;3;];>#abc", 30, false},

		{"   #abc", 0, true},
		{"", 0, true},
		{"  ", 0, true},
		{"<><>#", 0, true},
		{"1#", 0, true},
		{"<>", 0, true},
		{"<>;#", 0, true},
		{"{}//", 0, true},
	} {
		t.Logf("checking %q", test.input)
		n, err := SliceYPathAttrs([]byte(test.input))

		if !test.bad {
			require.NoError(t, err)
			require.Equal(t, test.n, n)
		} else {
			require.Error(t, err)
		}
	}
}

func TestSliceString(t *testing.T) {
	for _, test := range []struct {
		input string
		str   string
		n     int
		bad   bool
	}{
		{
			"a]",
			"a",
			1,
			false,
		},
		{
			" a]",
			"a",
			2,
			false,
		},
		{
			" a ]",
			"a",
			2,
			false,
		},
		{
			"a}",
			"a",
			1,
			false,
		},
		{
			"a,",
			"a",
			1,
			false,
		},
		{
			"a)",
			"a",
			1,
			false,
		},
		{
			`"a")`,
			"a",
			3,
			false,
		},
		{
			`"a"`,
			"",
			0,
			true,
		},
		{
			`"a`,
			"",
			0,
			true,
		},
	} {
		t.Logf("checking %q", test.input)
		var str []byte
		n, err := SliceYPathString([]byte(test.input), &str)

		if !test.bad {
			require.NoError(t, err)
			require.Equal(t, test.n, n)
			require.Equal(t, test.str, string(str))
		} else {
			require.Error(t, err)
		}
	}
}
