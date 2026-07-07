package swaggerui

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path"
	"sync"

	binaryResource "go.ytsaurus.tech/library/go/core/resource"
	httpResource "go.ytsaurus.tech/library/go/httputil/resource"
)

const (
	indexPagePath  = "/index.html"
	jsonSchemePath = "/scheme.json"
	yamlSchemePath = "/scheme.yaml"

	resourcePrefix = "resfs/file/library/go/httputil/swaggerui/swagger-ui-dist"
)

var _ http.FileSystem = (*FileSystem)(nil)

type FileSystem struct {
	opts         options
	indexOnce    sync.Once
	indexContent []byte
}

// NewFileSystem creates new http.FileSystem that contains SwaggerUI resources.
func NewFileSystem(opts ...Option) *FileSystem {
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}

	return &FileSystem{
		opts: o,
	}
}

func (fs *FileSystem) Open(name string) (http.File, error) {
	name = path.Clean("/" + name)
	switch name {
	case fs.opts.schemeHandler:
		return httpResource.NewFile(name, fs.opts.scheme), nil
	case indexPagePath:
		return httpResource.NewFile(name, fs.indexPage()), nil
	case "/":
		return &dir{
			fi: fileInfo{
				path: name,
			},
		}, nil
	}

	content := loadBinaryResource(name)
	if content == nil {
		return nil, os.ErrNotExist
	}

	return httpResource.NewFile(name, content), nil
}

func (fs *FileSystem) indexPage() []byte {
	fs.indexOnce.Do(func() {
		content := loadBinaryResource(indexPagePath)
		if content == nil {
			return
		}

		url, _ := json.Marshal(fs.opts.schemeURL)
		fs.indexContent = bytes.ReplaceAll(content, []byte(`#SchemeUrl#`), url)
	})
	return fs.indexContent
}

func loadBinaryResource(name string) []byte {
	return binaryResource.Get(resourcePrefix + name)
}
