// Package ypath defined type for working with YPath.
//
// YPath has two forms, simple and rich.
//
// For more information about ypath see https://ytsaurus.tech/docs/en/user-guide/storage/ypath
package ypath

import (
	"fmt"
	"strconv"
	"strings"

	"go.ytsaurus.tech/yt/go/yson"
)

// Path is ypath in simple text form.
//
// Create Path by casting from string.
//
//	path := ypath.Path("//home/prime")
//
// Edit Path using helper methods.
//
//	myHome = ypath.Root.Child("home").Child("prime")
//	myHome = ypath.Root.JoinChild("home", "prime")
//
// For complex manipulations, use Rich.
type Path string

// Root is path to cypress root.
const Root Path = "/"

// YPath is implementation of YPath interface.
func (p Path) YPath() Path {
	rich, err := Parse(p.String())
	if err != nil {
		// Garbage in, garbage out.
		return p
	}
	return rich.Path
}

// UnmarshalText is implementation of encoding.TextUnmarshaler interface.
func (p *Path) UnmarshalText(text []byte) error {
	*p = Path(text)
	return nil
}

// UnmarshalText is implementation of encoding.TextMarshaler interface.
func (p Path) MarshalText() (text []byte, err error) {
	return []byte(p), nil
}

// MarshalYSON is implementation of yson.StreamMarshaler interface.
func (p Path) MarshalYSON(w *yson.Writer) error {
	w.String(p.String())
	return w.Err()
}

// JoinChild returns path referencing grandchild of p.
//
//	root := ypath.Root
//	myHome := root.JoinChild("home", "prime")
func (p Path) JoinChild(names ...string) Path {
	for _, child := range names {
		p = p.Child(child)
	}
	return p
}

func needsEscaping(ch byte) bool {
	return isSpecialCharacter(ch) || ch < 32 || ch >= 128
}

// EscapeLiteral escapes a literal component of a YPath.
// Path.Child appends its argument without escaping.
func EscapeLiteral(value string) string {
	const hexDigits = "0123456789abcdef"

	i := 0
	for i < len(value) && !needsEscaping(value[i]) {
		i++
	}
	if i == len(value) {
		return value
	}

	var result strings.Builder
	result.Grow(i + 4*(len(value)-i))
	result.WriteString(value[:i])
	for ; i < len(value); i++ {
		ch := value[i]
		switch {
		case isSpecialCharacter(ch):
			result.WriteByte('\\')
			result.WriteByte(ch)
		case needsEscaping(ch):
			result.WriteString(`\x`)
			result.WriteByte(hexDigits[ch>>4])
			result.WriteByte(hexDigits[ch&15])
		default:
			result.WriteByte(ch)
		}
	}
	return result.String()
}

// UnescapeLiteral decodes escape sequences in a literal component of a YPath.
// It returns an error for invalid or incomplete escape sequences.
func UnescapeLiteral(value string) (string, error) {
	i := strings.IndexByte(value, '\\')
	if i == -1 {
		return value, nil
	}

	var result strings.Builder
	result.Grow(len(value))
	result.WriteString(value[:i])
	for ; i < len(value); i++ {
		if value[i] != '\\' {
			result.WriteByte(value[i])
			continue
		}

		i++
		if i == len(value) {
			return "", fmt.Errorf("ypath: unterminated escape sequence")
		}

		switch {
		case isSpecialCharacter(value[i]):
			result.WriteByte(value[i])
		case value[i] == 'x':
			if i+2 >= len(value) {
				return "", fmt.Errorf("ypath: unterminated escape sequence")
			}
			ch, err := strconv.ParseUint(value[i+1:i+3], 16, 8)
			if err != nil {
				return "", fmt.Errorf("ypath: invalid escape sequence")
			}
			result.WriteByte(byte(ch))
			i += 2
		default:
			return "", fmt.Errorf("ypath: invalid escape sequence")
		}
	}
	return result.String(), nil
}

func (p Path) Child(name string) Path {
	p += Path("/" + name)
	return p
}

// ListIndex returns path referencing n-th element of p, given that p references a list.
func (p Path) ListIndex(n int) Path {
	p += Path(fmt.Sprintf("/%d", n))
	return p
}

// ListLast returns path referencing last element of p, given that p references a list.
func (p Path) ListLast() Path {
	p += Path("/-1")
	return p
}

// Children returns path referencing all children of p.
//
// The only place such path is ever useful is RemoveNode command.
func (p Path) Children() Path {
	p += Path("/*")
	return p
}

// ListBegin returns path used for inserting elements into beginning of the list.
func (p Path) ListBegin() Path {
	p += Path("/begin")
	return p
}

// ListEnd returns path used for appending elements to the list.
func (p Path) ListEnd() Path {
	p += Path("/end")
	return p
}

// ListBefore returns path used for inserting elements into the list.
func (p Path) ListBefore(n int) Path {
	p += Path(fmt.Sprintf("/before:%d", n))
	return p
}

// ListAfter returns path used for inserting elements into the list.
func (p Path) ListAfter(n int) Path {
	p += Path(fmt.Sprintf("/after:%d", n))
	return p
}

// Attr returns path referencing attribute of p.
func (p Path) Attr(name string) Path {
	p += "/@" + Path(name)
	return p
}

// Attrs returns path referencing all attributes of p.
func (p Path) Attrs() Path {
	p += "/@"
	return p
}

// Rich creates Rich from p.
//
// This function doesn't validate and doesn't normalize p.
func (p Path) Rich() *Rich {
	return &Rich{Path: p}
}

// SuppressSymlink returns path referencing symlink node.
//
// By default, name of symlink references it target.
//
// Typical usage would be:
//
//	var linkName ypath.Path
//	linkTarget := linkName.SuppressSymlink().Attr("target_path")
func (p Path) SuppressSymlink() Path {
	p += "&"
	return p
}

// String casts Path back to string.
func (p Path) String() string {
	return string(p)
}

// YPath is interface type that is able to hold both Path and Rich.
//
// It is used by api methods that accept both kind of paths.
type YPath interface {
	// YPath returns path with all attributes stripped.
	//
	// If path is malformed, returns original path unmodified. Use Parse() if you need to check path for correctness.
	YPath() Path

	yson.StreamMarshaler
}

// Attrs returns path referencing all attributes of p while preserving rich path attributes.
func Attrs(p YPath) YPath {
	switch p := p.(type) {
	case Path:
		return p.Attrs()
	case *Path:
		return p.Attrs()
	case Rich:
		p.Path = p.Path.Attrs()
		return p
	case *Rich:
		result := *p
		result.Path = result.Path.Attrs()
		return &result
	default:
		return p.YPath().Attrs()
	}
}
