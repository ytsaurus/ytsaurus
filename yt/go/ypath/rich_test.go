package ypath

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	for _, testCase := range []struct {
		input    string
		expected *Rich
	}{
		{
			input:    "/",
			expected: &Rich{Path: Root},
		},
		{
			input:    " <> /",
			expected: &Rich{Path: "/"},
		},
		{
			input:    " <> //foo bar",
			expected: &Rich{Path: "//foo bar"},
		},
		{
			input:    "//\\xaa\\xff\\xAA\\xFF",
			expected: &Rich{Path: "//\\xaa\\xff\\xAA\\xFF"},
		},
		{
			input:    "<append=%true;>#bc67d12a-e1971bb1-3066dfe2-a94c59a8/foo",
			expected: NewRich("#bc67d12a-e1971bb1-3066dfe2-a94c59a8/foo").SetAppend(),
		},
		{
			input:    "//foo{bar,zog}",
			expected: NewRich("//foo").SetColumns([]string{"bar", "zog"}),
		},
		{
			input:    `//foo\\bar`,
			expected: NewRich(`//foo\\bar`),
		},
		{
			input:    "//foo{bar,zog}   ",
			expected: NewRich("//foo").SetColumns([]string{"bar", "zog"}),
		},
		{
			input:    "//foo{bar,}   ",
			expected: NewRich("//foo").SetColumns([]string{"bar"}),
		},
		{
			input:    "//foo{}",
			expected: NewRich("//foo").SetColumns([]string{}),
		},
		{
			input:    "//foo{  }",
			expected: NewRich("//foo").SetColumns([]string{}),
		},
		{
			input:    "//foo[:]",
			expected: NewRich("//foo").AddRange(Full()),
		},
		{
			input:    "//foo[,]",
			expected: NewRich("//foo").AddRange(Full()).AddRange(Full()),
		},
		{
			input:    "//foo[,:]",
			expected: NewRich("//foo").AddRange(Full()).AddRange(Full()),
		},
		{
			input:    "//foo[#1:#2]",
			expected: NewRich("//foo").AddRange(Interval(RowIndex(1), RowIndex(2))),
		},
		{
			input:    "//foo[#1:]",
			expected: NewRich("//foo").AddRange(StartingFrom(RowIndex(1))),
		},
		{
			input:    "//foo[:#1]",
			expected: NewRich("//foo").AddRange(UpTo(RowIndex(1))),
		},
		{
			input:    "//foo[#1:#2]  ",
			expected: NewRich("//foo").AddRange(Interval(RowIndex(1), RowIndex(2))),
		},
		{
			input:    "//foo[a]",
			expected: NewRich("//foo").AddRange(Exact(Key([]byte("a")))),
		},
		{
			input:    "//foo[()]",
			expected: NewRich("//foo").AddRange(Exact(Key())),
		},
		{
			input:    "//foo[(%true,)]",
			expected: NewRich("//foo").AddRange(Exact(Key(true))),
		},
		{
			input:    "//foo[(%true,):]",
			expected: NewRich("//foo").AddRange(StartingFrom(Key(true))),
		},
		{
			input:    "//foo[:(%true,)]",
			expected: NewRich("//foo").AddRange(UpTo(Key(true))),
		},
		{
			input:    "//foo[(%true,):(%true,)]",
			expected: NewRich("//foo").AddRange(Interval(Key(true), Key(true))),
		},
		{
			input: "//foo[( a, 1, 2u , %true,#)]",
			expected: NewRich("//foo").
				AddRange(Exact(Key(
					[]byte("a"),
					int64(1),
					uint64(2),
					true,
					nil,
				))),
		},
		{
			input:    "//foo\\[a]",
			expected: NewRich("//foo\\[a]"),
		},
	} {
		t.Logf("testing %q", testCase.input)
		path, err := Parse(testCase.input)
		if assert.NoError(t, err) {
			assert.Equal(t, testCase.expected, path)
		}
	}
}

func TestParseInvalid(t *testing.T) {
	for _, testCase := range []string{
		"",
		"<",
		"a",
		"//foo{a,b}{a,b}",
		"//foo[:][:]",
		"//foo[:]{a,b}",
		"//foo{a,b",
		"//foo{a,",
		"//foo{a",
		"//foo{",
		"//foo{1}",
		"//foo{a,b}{a,b}",
		"//foo[:]/@foo",
		"//foo{a,b}/@foo",
		"//t[(a)",
		"//t[(a",
		"//t[(a,",
		"//t[(",
		"//t[",
		" //tmp/parse",
	} {
		_, err := Parse(testCase)
		t.Logf("got error: %v", err)
		assert.Error(t, err, "%q", testCase)
	}
}

func TestPathSplit(t *testing.T) {
	for _, testCase := range []struct {
		path  string
		parts []string
	}{
		{"/", []string{"/"}},
		{"//@", []string{"/", "/@"}},
		{"#bc67d12a-e1971bb1-3066dfe2-a94c59a8/foo", []string{"#bc67d12a-e1971bb1-3066dfe2-a94c59a8", "/foo"}},
	} {
		parts, err := SplitTokens(testCase.path)
		require.NoError(t, err)
		require.Equal(t, testCase.parts, parts)
	}
}
