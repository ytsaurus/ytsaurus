package ypath

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPath(t *testing.T) {
	assert.Equal(t, Path("//foo/bar&/@attr/zog"), Root.Child("foo").Child("bar").SuppressSymlink().Attr("attr").Child("zog"))
	assert.Equal(t, Path("//foo/end"), Root.Child("foo").ListEnd())
	assert.Equal(t, Path("//foo/begin"), Root.Child("foo").ListBegin())
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
