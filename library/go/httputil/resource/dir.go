package resource

import (
	"net/http"
	"os"
	"path"
	"strings"

	"go.ytsaurus.tech/library/go/core/resource"
)

var _ http.FileSystem = new(Dir)

// A Dir implements FileSystem top of a Arcadia resource files.
//
// Dir should point to the valid resources prefix (described in the ya.make)
type Dir string

func (d Dir) Open(name string) (http.File, error) {
	dir := strings.TrimSuffix(string(d), "/")
	fullName := dir + path.Clean("/"+name)
	content := resource.Get(fullName)
	if content == nil {
		return nil, os.ErrNotExist
	}

	return NewFile(fullName, content), nil
}
