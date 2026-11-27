package ypath

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yson"
)

func TestPath(t *testing.T) {
	assert.Equal(t, Path("//foo/bar&/@attr/zog"), Root.Child("foo").Child("bar").SuppressSymlink().Attr("attr").Child("zog"))
	assert.Equal(t, Path("//foo/end"), Root.Child("foo").ListEnd())
	assert.Equal(t, Path("//foo/begin"), Root.Child("foo").ListBegin())
}

func TestSetMapValue(t *testing.T) {
	child := map[string]any{"keep": true}
	root := map[string]any{"a": child}
	require.NoError(t, SetMapValue(root, `/a/b\/c`, 42, false))
	assert.Equal(t, 42, child["b/c"])
	require.NoError(t, SetMapValue(root, `/a/b\x2fc`, nil, false))
	assert.Equal(t, map[string]any{"keep": true, "b/c": nil}, child)

	require.NoError(t, SetMapValue(root, `/\@key/x/y`, 1, true))
	assert.Equal(t, map[string]any{"x": map[string]any{"y": 1}}, root["@key"])
	require.NoError(t, SetMapValue(root, "/a", "replacement", false))
	assert.Equal(t, "replacement", root["a"])

	var tree, expected map[string]any
	require.NoError(t, yson.Unmarshal([]byte(`{list=[a;{};<attr=1>{}]}`), &tree))
	require.NoError(t, SetMapValue(tree, "/list/0", "b", false))
	require.NoError(t, SetMapValue(tree, "/list/-2/key", int64(1), false))
	require.NoError(t, SetMapValue(tree, "/list/2/key/nested", int64(2), true))
	require.NoError(t, yson.Unmarshal([]byte(`{list=[b;{key=1};<attr=1>{key={nested=2}}]}`), &expected))
	assert.Equal(t, expected, tree)
}

func TestSetMapValueError(t *testing.T) {
	root := map[string]any{"scalar": 1, "nilMap": map[string]any(nil), "list": []any{1}}
	require.Error(t, SetMapValue(nil, "/a", 1, true))
	require.Error(t, SetMapValue(root, "/missing/child", 1, false))
	require.Error(t, SetMapValue(root, "/scalar/child", 1, true))
	require.Error(t, SetMapValue(root, "/nilMap/child", 1, true))

	for _, path := range []Path{
		"", "/", "a/b", "//a", "/new/", "/new//key", `/new/x\q`, `/new/x\`,
		"/new/@attr", "/new/*", "/new/link&", "/new{col}", "/new[#1]", "<attr=1>/new",
		"/list/1", "/list/-2", "/list/end", "/list/0/child",
	} {
		t.Run(string(path), func(t *testing.T) {
			require.Error(t, SetMapValue(root, path, 1, true))
		})
	}
	assert.Equal(t, map[string]any{"scalar": 1, "nilMap": map[string]any(nil), "list": []any{1}}, root)
}

func TestRemoveMapValue(t *testing.T) {
	var root, expected map[string]any
	require.NoError(t, yson.Unmarshal([]byte(`{a={b=1;c=2};list=[x;<attr=1>[y;z]]}`), &root))
	for _, tc := range []struct {
		path    Path
		removed bool
	}{
		{"/a/b", true}, {"/list/0", true}, {"/list/0/-1", true},
		{"/a/missing", false}, {"/missing/key", false}, {"/list/1", false}, {"/list/0/-2", false},
	} {
		removed, err := RemoveMapValue(root, tc.path)
		require.NoError(t, err, tc.path)
		assert.Equal(t, tc.removed, removed, tc.path)
	}
	require.NoError(t, yson.Unmarshal([]byte(`{a={c=2};list=[<attr=1>[y]]}`), &expected))
	assert.Equal(t, expected, root)

	for _, path := range []Path{"/list/end", "/a/c/key", "/a/@attr", "/"} {
		_, err := RemoveMapValue(root, path)
		require.Error(t, err, path)
	}
	assert.Equal(t, expected, root)
}

func TestEscapeLiteral(t *testing.T) {
	for _, tc := range []struct{ name, value, escaped string }{
		{"empty", "", ""},
		{"ascii", "table_01", "table_01"},
		{"special", `a/b@c&d[e{f*g\h`, `a\/b\@c\&d\[e\{f\*g\\h`},
		{"control", "\x00\t\n\r", `\x00\x09\x0a\x0d`},
		{"utf8", "©", `\xc2\xa9`},
		{"non_utf8", "\xee", `\xee`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.escaped, EscapeLiteral(tc.value))
		})
	}
}

func TestUnescapeLiteral(t *testing.T) {
	for _, tc := range []struct{ name, escaped, value string }{
		{"empty", "", ""},
		{"ascii", "table_01", "table_01"},
		{"special", `a\/b\@c\&d\[e\{f\*g\\h`, `a/b@c&d[e{f*g\h`},
		{"control", `\x00\x09\x0a\x0d`, "\x00\t\n\r"},
		{"utf8", `\xd1\x8f`, "я"},
		{"unescaped_utf8", "я", "я"},
		{"non_utf8", `\xee\xFF`, "\xee\xff"},
		{"literal_escape", `\\xd1\\x8f`, `\xd1\x8f`},
		{"hex_separator", `a\x2fb`, "a/b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, err := UnescapeLiteral(tc.escaped)
			require.NoError(t, err)
			assert.Equal(t, tc.value, value)
		})
	}
}

func TestUnescapeLiteralInvalid(t *testing.T) {
	for _, value := range []string{`\`, `\x`, `\x0`, `\xgg`, `\x+1`, `\n`, `\u1234`, `\q`, `abc\`} {
		t.Run(value, func(t *testing.T) {
			_, err := UnescapeLiteral(value)
			assert.Error(t, err)
		})
	}
}

func TestEscapeUnescapeLiteral(t *testing.T) {
	var value []byte
	for i := 0; i < 256; i++ {
		value = append(value, byte(i))
	}
	decoded, err := UnescapeLiteral(EscapeLiteral(string(value)))
	require.NoError(t, err)
	assert.Equal(t, string(value), decoded)
}
