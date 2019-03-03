// package ypath defined type and helpers for working with YPath.
//
// For more information about ypath see https://wiki.yandex-team.ru/yt/userdoc/ypath/
//
package ypath

import (
	"fmt"

	"a.yandex-team.ru/yt/go/yson"
)

// Path is ypath in simple text form.
//
// Create Path by casting from string.
//
//     path := ypath.Path("//home/prime")
//
// Edit Path using helper methods.
//
//     myHome = ypath.Root.Child("home").Child("prime")
//     myHome = ypath.Root.JoinChild("home", "prime")
//
// For complex manipulations, use Rich.
type Path string

const Root Path = "/"

func (_ Path) YPath() {}

func (p *Path) UnmarshalText(text []byte) error {
	*p = Path(text)
	return nil
}

func (p Path) MarshalText() (text []byte, err error) {
	return []byte(p), nil
}

func (p Path) MarshalYSON(w *yson.Writer) error {
	w.String(string(p))
	return nil
}

func (p Path) JoinChild(names ...string) Path {
	for _, child := range names {
		p = p.Child(child)
	}
	return p
}

func (p Path) Child(name string) Path {
	p += Path("/" + name)
	return p
}

func (p Path) ListIndex(n int) Path {
	p += Path(fmt.Sprintf("/%d", n))
	return p
}

func (p Path) ListLast() Path {
	p += Path("/-1")
	return p
}

func (p Path) AllChildren() Path {
	p += Path("/*")
	return p
}

func (p Path) ListBegin() Path {
	p += Path("/begin")
	return p
}

func (p Path) ListEnd() Path {
	p += Path("/end")
	return p
}

func (p Path) ListBefore(n int) Path {
	p += Path(fmt.Sprintf("/before:%d", n))
	return p
}

func (p Path) ListAfter(n int) Path {
	p += Path(fmt.Sprintf("/after:%d", n))
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

func (p Path) SuppressSymlink() Path {
	p += "&"
	return p
}

func (p Path) String() string {
	return string(p)
}

// YPath is interface type that is able to hold both Path and Rich.
type YPath interface {
	YPath()

	yson.StreamMarhsaler
}
