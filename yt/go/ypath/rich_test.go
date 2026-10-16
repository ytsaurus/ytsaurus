package ypath

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yson"
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
			input:    `<cluster="some_cluster";>//tmp/path/with/cluster`,
			expected: NewRich("//tmp/path/with/cluster").SetCluster("some_cluster"),
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

// TestRichUnmarshalYSONFromPartitionTables covers the shape of table_ranges
// entries returned by the partition_tables driver command: an attributed YSON
// value where the path lives in the primary value and the ranges are attached
// as the "ranges" attribute.
func TestRichUnmarshalYSONFromPartitionTables(t *testing.T) {
	// Single Rich value.
	{
		raw := []byte(`<ranges=[{lower_limit={row_index=100};upper_limit={row_index=200}}]>"//home/foo/bar"`)
		var r Rich
		require.NoError(t, yson.Unmarshal(raw, &r))
		require.Equal(t, Path("//home/foo/bar"), r.Path)
		require.Len(t, r.Ranges, 1)
		require.NotNil(t, r.Ranges[0].Lower)
		require.NotNil(t, r.Ranges[0].Lower.RowIndex)
		require.Equal(t, int64(100), *r.Ranges[0].Lower.RowIndex)
		require.NotNil(t, r.Ranges[0].Upper)
		require.NotNil(t, r.Ranges[0].Upper.RowIndex)
		require.Equal(t, int64(200), *r.Ranges[0].Upper.RowIndex)
	}
	// List of Rich values, as embedded in the partition_tables response.
	{
		raw := []byte(`[` +
			`<ranges=[{lower_limit={row_index=0};upper_limit={row_index=10}}]>"//tmp/t1";` +
			`<ranges=[{lower_limit={row_index=10};upper_limit={row_index=20}}]>"//tmp/t2"` +
			`]`)
		var rr []Rich
		require.NoError(t, yson.Unmarshal(raw, &rr))
		require.Len(t, rr, 2)
		require.Equal(t, Path("//tmp/t1"), rr[0].Path)
		require.Equal(t, int64(0), *rr[0].Ranges[0].Lower.RowIndex)
		require.Equal(t, int64(10), *rr[0].Ranges[0].Upper.RowIndex)
		require.Equal(t, Path("//tmp/t2"), rr[1].Path)
		require.Equal(t, int64(10), *rr[1].Ranges[0].Lower.RowIndex)
		require.Equal(t, int64(20), *rr[1].Ranges[0].Upper.RowIndex)
	}
	// Path without ranges (partition_mode=unordered without cookies): make sure
	// the absence of the "ranges" attribute doesn't break decoding.
	{
		raw := []byte(`"//home/foo/bar"`)
		var r Rich
		require.NoError(t, yson.Unmarshal(raw, &r))
		require.Equal(t, Path("//home/foo/bar"), r.Path)
		require.Empty(t, r.Ranges)
	}
	// Multi-table partition where one table contributes no rows to this
	// partition: attribute is present but the list is empty. See YT integration
	// test controller/test_partition_tables.py.
	{
		raw := []byte(`<ranges=[]>"//tmp/empty"`)
		var r Rich
		require.NoError(t, yson.Unmarshal(raw, &r))
		require.Equal(t, Path("//tmp/empty"), r.Path)
		require.Empty(t, r.Ranges)
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
