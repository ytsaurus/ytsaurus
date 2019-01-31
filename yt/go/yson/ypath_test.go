package yson

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSliceYPathAttrs(t *testing.T) {
	for _, test := range [][]string{
		{
			"//home/prime",
			"",
			"//home/prime",
		},
		{
			"<>   #abc",
			"<>   ",
			"#abc",
		},
		{
			"<append=%true; keys=[1;2;3;];>#abc",
			"<append=%true; keys=[1;2;3;];>",
			"#abc",
		},
	} {
		t.Logf("checking %q", test[0])
		attrs, rest, err := SliceYPath([]byte(test[0]))

		require.Nil(t, err)
		require.Equal(t, test[1], string(attrs))
		require.Equal(t, test[2], string(rest))
	}
}

func TestSliceInvalidYPath(t *testing.T) {
	check := func(in string) {
		t.Logf("testing %q", in)

		_, _, err := SliceYPath([]byte(in))
		require.Error(t, err)
	}

	check("")
	check("  ")
	check("<><>#")
	check("1#")
	check("<>")
	check("<>;#")
	check("{}//")
}
