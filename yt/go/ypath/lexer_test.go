package ypath

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplit(t *testing.T) {
	for _, testCase := range []struct {
		input Path

		parent Path
		child  string
		error  bool
	}{
		{
			input:  "/",
			parent: "/",
			child:  "",
		},
		{
			input:  "#1-2-3-4",
			parent: "#1-2-3-4",
			child:  "",
		},
		{
			input: "",
			error: true,
		},
		{
			input:  "//home/prime/table",
			parent: "//home/prime",
			child:  "/table",
		},
		{
			input:  "//home/prime/@id",
			parent: "//home/prime",
			child:  "/@id",
		},
		{
			input:  "//home",
			parent: "/",
			child:  "/home",
		},
		{
			input:  "<compression=lz4>//home/prime/table{a,b}[#1:]",
			parent: "//home/prime",
			child:  "/table",
		},
	} {

		t.Run(fmt.Sprintf("%q", testCase.input), func(t *testing.T) {
			parent, child, err := Split(testCase.input)
			if !testCase.error {
				require.NoError(t, err)
				assert.Equal(t, testCase.parent, parent)
				assert.Equal(t, testCase.child, child)
			} else {
				require.Error(t, err)
			}
		})
	}
}
