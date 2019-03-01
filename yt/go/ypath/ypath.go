// package ypath defined type and helpers for working with YPath.
//
// For more information about ypath see https://wiki.yandex-team.ru/yt/userdoc/ypath/
//
package ypath

import "a.yandex-team.ru/yt/go/yson"

// Path is ypath in text form.
type Path string

func (_ Path) YPath() {}

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

// YPath is interface type that is able to hold both Path and Rich.
type YPath interface {
	YPath()

	yson.StreamMarhsaler
}
