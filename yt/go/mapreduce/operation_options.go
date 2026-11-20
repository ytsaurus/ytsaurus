package mapreduce

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cenkalti/backoff/v4"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type OperationOption interface {
	isOperationOption()
}

type localFilesOption struct {
	paths []string
}

func (l *localFilesOption) isOperationOption() {}

type localFileReader struct {
	file     io.Reader
	consumed bool
}

func (r *localFileReader) Read(p []byte) (int, error) {
	n, err := r.file.Read(p)
	if n > 0 {
		r.consumed = true
	}
	if err != nil && err != io.EOF {
		return n, backoff.Permanent(err)
	}
	return n, err
}

func prepareLocalFileWrite(f *os.File) func() (func(io.Writer) error, error) {
	r := &localFileReader{file: f}
	write := func(w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	}
	return func() (func(io.Writer) error, error) {
		if r.consumed {
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return nil, backoff.Permanent(err)
			}
			r.consumed = false
		}
		return write, nil
	}
}

func (l *localFilesOption) uploadLocalFiles(ctx context.Context, p *prepare) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("failed to upload local file: %w", err)
		}
	}()

	for _, filename := range l.paths {
		st, err := os.Stat(filename)
		if err != nil {
			return err
		}

		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer f.Close()

		file, err := p.uploadLockedFile(ctx, func() ypath.Path {
			return ypath.Path("//tmp").Child(guid.New().String())
		}, prepareLocalFileWrite(f), spec.File{
			Executable: st.Mode()&0100 != 0,
			FileName:   filepath.Base(filename),
		})
		if err != nil {
			return err
		}
		p.spec.VisitUserScripts(func(script *spec.UserScript) {
			script.FilePaths = append(script.FilePaths, file)
		})
	}

	return nil
}

// WithLocalFile makes local file available inside job sandbox directory.
//
// Filename and file permissions are preserved. All files are uploaded into job sandbox.
func WithLocalFiles(paths []string) OperationOption {
	return &localFilesOption{paths: paths}
}

type skipSelfUploadOption struct {
}

func (l *skipSelfUploadOption) isOperationOption() {}

func SkipSelfUpload() OperationOption {
	return &skipSelfUploadOption{}
}

type startOperationOptsOption struct {
	options *yt.StartOperationOptions
}

func (o *startOperationOptsOption) isOperationOption() {}

func WithStartOperationOptions(options *yt.StartOperationOptions) OperationOption {
	return &startOperationOptsOption{options: options}
}

type disableIndexControlAttributesOption struct{}

func (o *disableIndexControlAttributesOption) isOperationOption() {}

// disableIndexControlAttributes disables index system columns ($row_index, $range_index)
// in Skiff input format.
func disableIndexControlAttributes() OperationOption {
	return &disableIndexControlAttributesOption{}
}

type disableGetInputTablesSchemaOption struct{}

func (o *disableGetInputTablesSchemaOption) isOperationOption() {}

// disableGetInputTablesSchema disables automatic retrieval of input table schema.
func disableGetInputTablesSchema() OperationOption {
	return &disableGetInputTablesSchemaOption{}
}
