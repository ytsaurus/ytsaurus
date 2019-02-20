// package ypath defined type and helpers for working with YPath.
//
// For more information about ypath see https://wiki.yandex-team.ru/yt/userdoc/ypath/
//
package ypath

import "a.yandex-team.ru/yt/go/yson"

// Path is ypath in text form.
type Path string

func (p Path) MarshalYSON(w *yson.Writer) error {
	w.String(string(p))
	return nil
}

func (p Path) AppendName(name string) Path {
	p += Path("/" + name)
	return p
}

func (p Path) Attr(name string) Path {
	p += "/@" + Path(name)
	return p
}

func (p Path) Attrs() Path {
	p += "/@"
	return p
}

func (p Path) NoFollowSymlink() Path {
	p += "&"
	return p
}

type Key []interface{}

type Range struct {
	Lower Key
	Upper Key
}

type Rich struct {
	Path string

	Append   bool
	SortedBy []string
	Ranges   []Range
}

func Parse(path string) (p Rich, err error) {
	var attrs, rest []byte
	attrs, rest, err = yson.SliceYPath([]byte(path))
	if err != nil {
		return
	}

	_, _ = attrs, rest
	panic("not implemented")
	return
}

func (p Rich) Push(name string) Rich {
	p.Path += "/" + name
	return p
}
