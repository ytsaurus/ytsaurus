package cypresspath

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsCanonical(t *testing.T) {
	for _, testCase := range []struct {
		path     string
		expected bool
	}{
		{path: "/", expected: true},
		{path: "//home", expected: true},
		{path: `//home/b\/c/\*/a\@b/\&/\\`, expected: true},
		{path: `//home/\x00`, expected: true},
		{path: `//home/b\x2Fc`, expected: false},
		{path: `//home/\x61`, expected: false},
		{path: "", expected: false},
		{path: "//", expected: false},
		{path: "home", expected: false},
		{path: "#1-2-3-4", expected: false},
		{path: "//home/", expected: false},
		{path: "///home", expected: false},
		{path: "//home//table", expected: false},
		{path: "//home/@acl", expected: false},
		{path: "//home&", expected: false},
		{path: "//home/*", expected: false},
		{path: "//home[1:2]", expected: false},
		{path: "//home{a}", expected: false},
		{path: `<append=%true>//home`, expected: false},
		{path: `//home/invalid\`, expected: false},
		{path: `//home/\q`, expected: false},
		{path: `//home/\xzz`, expected: false},
	} {
		require.Equal(t, testCase.expected, IsCanonical(testCase.path), "path: %q", testCase.path)
	}
}
