package resource

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"time"

	"go.ytsaurus.tech/library/go/core/xerrors"
)

var _ http.File = new(File)

type File struct {
	io.ReadSeeker
	fi fileInfo
}

func NewFile(path string, content []byte) *File {
	return &File{
		ReadSeeker: bytes.NewReader(content),
		fi: fileInfo{
			path:  path,
			size:  int64(len(content)),
			isDir: false,
		},
	}
}

func (f *File) Stat() (os.FileInfo, error) {
	return f.fi, nil
}

func (f *File) Close() error {
	return nil
}

func (f *File) Readdir(int) ([]os.FileInfo, error) {
	return nil, xerrors.Errorf("cannot Readdir from resource %s", f.fi.path)
}

var _ os.FileInfo = fileInfo{}

type fileInfo struct {
	path  string
	size  int64
	isDir bool
}

func (f fileInfo) Name() string {
	return f.path
}

func (f fileInfo) Size() int64 {
	return f.size
}

func (f fileInfo) Mode() os.FileMode {
	return 0444
}

func (f fileInfo) ModTime() time.Time {
	return BuildTime()
}

func (f fileInfo) IsDir() bool {
	return f.isDir
}

func (f fileInfo) Sys() interface{} {
	return nil
}
