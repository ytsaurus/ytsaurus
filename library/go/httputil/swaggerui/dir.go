package swaggerui

import (
	"io"
	"net/http"
	"os"
	"time"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/httputil/resource"
)

var _ http.File = (*dir)(nil)

type dir struct {
	io.ReadSeeker
	fi fileInfo
}

func (d *dir) Stat() (os.FileInfo, error) {
	return d.fi, nil
}

func (d *dir) Close() error {
	return nil
}

func (d *dir) Readdir(int) ([]os.FileInfo, error) {
	return nil, xerrors.Errorf("cannot Readdir from resource %s", d.fi.path)
}

type fileInfo struct {
	path string
}

func (f fileInfo) Name() string {
	return f.path
}

func (f fileInfo) Size() int64 {
	return 0
}

func (f fileInfo) Mode() os.FileMode {
	return 0666
}

func (f fileInfo) ModTime() time.Time {
	return resource.BuildTime()
}

func (f fileInfo) IsDir() bool {
	return true
}

func (f fileInfo) Sys() interface{} {
	return nil
}
