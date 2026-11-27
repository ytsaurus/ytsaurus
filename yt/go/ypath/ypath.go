// Package ypath defined type for working with YPath.
//
// YPath has two forms, simple and rich.
//
// For more information about ypath see https://ytsaurus.tech/docs/en/user-guide/storage/ypath
package ypath

import (
	"errors"
	"fmt"
	"slices"
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

// SetMapValue sets a value in nested map[string]any and []any values using a path relative to root.
// The path must contain one or more literal keys or list indices, each prefixed with a slash.
// A negative list index counts from the end of the list.
// Values wrapped in *yson.ValueWithAttrs are traversed through their Value field.
// If force is true, missing intermediate maps are created.
func SetMapValue(root map[string]any, path Path, value any, force bool) error {
	keys, err := mapPathKeys(path)
	if err != nil {
		return err
	}
	_, err = editValue(root, keys, value, force, false)
	return err
}

// RemoveMapValue removes a value from nested map[string]any and []any values using a path relative to root.
// The path has the same form as in SetMapValue.
// It reports whether the value existed; a missing key or list index is not an error.
func RemoveMapValue(root map[string]any, path Path) (bool, error) {
	keys, err := mapPathKeys(path)
	if err != nil {
		return false, err
	}
	if _, err = editValue(root, keys, nil, false, true); errors.Is(err, errNoSuchChild) {
		return false, nil
	}
	return err == nil, err
}

func mapPathKeys(path Path) ([]string, error) {
	if len(path) == 0 || path[0] != '/' {
		return nil, fmt.Errorf("ypath: expected a relative path starting with '/'")
	}
	var keys []string
	start := 1
	for i := 1; i < len(path); i++ {
		switch {
		case path[i] == '\\':
			i++
		case path[i] == '/':
			keys = append(keys, string(path[start:i]))
			start = i + 1
		case isSpecialCharacter(path[i]):
			return nil, fmt.Errorf("ypath: unexpected %q in map key", path[i])
		}
	}
	keys = append(keys, string(path[start:]))

	for i, key := range keys {
		if key == "" {
			return nil, fmt.Errorf("ypath: empty map key")
		}
		var err error
		if keys[i], err = UnescapeLiteral(key); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

var errNoSuchChild = errors.New("ypath: no such child")

// editValue returns the value to store in place of node, since removing a list element shortens the slice.
func editValue(node any, keys []string, value any, force, remove bool) (any, error) {
	key, last := keys[0], len(keys) == 1
	switch node := node.(type) {
	case *yson.ValueWithAttrs:
		if node == nil {
			break
		}
		child, err := editValue(node.Value, keys, value, force, remove)
		if err != nil {
			return nil, err
		}
		node.Value = child
		return node, nil
	case map[string]any:
		if node == nil {
			break
		}
		if last && !remove {
			node[key] = value
			return node, nil
		}
		child, ok := node[key]
		if !ok && (remove || !force) {
			return nil, fmt.Errorf("%w %q", errNoSuchChild, key)
		}
		if last {
			delete(node, key)
			return node, nil
		}
		if !ok {
			child = make(map[string]any)
		}
		child, err := editValue(child, keys[1:], value, force, remove)
		if err != nil {
			return nil, err
		}
		node[key] = child
		return node, nil
	case []any:
		index, err := strconv.Atoi(key)
		if err != nil {
			return nil, fmt.Errorf("ypath: invalid list index %q", key)
		}
		if index < 0 {
			index += len(node)
		}
		if index < 0 || index >= len(node) {
			return nil, fmt.Errorf("%w %q", errNoSuchChild, key)
		}
		if last && remove {
			return slices.Delete(node, index, index+1), nil
		}
		if last {
			node[index] = value
			return node, nil
		}
		child, err := editValue(node[index], keys[1:], value, force, remove)
		if err != nil {
			return nil, err
		}
		node[index] = child
		return node, nil
	}
	return nil, fmt.Errorf("ypath: expected a non-nil map or list to look up %q, got %T", key, node)
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
