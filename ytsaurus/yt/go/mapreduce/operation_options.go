package mapreduce

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

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

		tmpPath := ypath.Path("//tmp").Child(guid.New().String())

		_, err = p.mr.yc.CreateNode(ctx, tmpPath, yt.NodeFile, nil)
		if err != nil {
			return err
		}

		w, err := p.mr.yc.WriteFile(ctx, tmpPath, &yt.WriteFileOptions{})
		if err != nil {
			return err
		}
		defer w.Close()

		if _, err = io.Copy(w, f); err != nil {
			return err
		}

		if err := w.Close(); err != nil {
			return err
		}

		p.spec.VisitUserScripts(func(script *spec.UserScript) {
			script.FilePaths = append(script.FilePaths, spec.File{
				CypressPath: tmpPath,
				Executable:  st.Mode()&0100 != 0,
				FileName:    filepath.Base(filename),
			})
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
