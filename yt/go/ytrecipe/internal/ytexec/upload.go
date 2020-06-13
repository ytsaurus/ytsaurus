package ytexec

import (
	"context"
	"io"
	"os"

	"github.com/cenkalti/backoff"
	"golang.org/x/sync/errgroup"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/jobfs"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/tarstream"
)

func (e *Exec) basicUploadFile(ctx context.Context, path ypath.Path, do func(w io.Writer) error) error {
	ok, err := e.yc.NodeExists(ctx, path, &yt.NodeExistsOptions{
		MasterReadOptions: &yt.MasterReadOptions{ReadFrom: yt.ReadFromCache},
	})
	if err != nil {
		return err
	}

	if ok {
		return nil
	}

	tmpDir := e.config.Operation.TmpDir()
	tmpPath := tmpDir.Child(guid.New().String())

	_, err = e.yc.CreateNode(ctx, tmpPath, yt.NodeFile, &yt.CreateNodeOptions{Recursive: true})
	if err != nil {
		return err
	}

	w, err := e.yc.WriteFile(ctx, tmpPath, nil)
	if err != nil {
		return err
	}
	defer w.Close()

	if err := do(w); err != nil {
		return err
	}

	if err := w.Close(); err != nil {
		return err
	}

	_, err = e.yc.MoveNode(ctx, tmpPath, path, &yt.MoveNodeOptions{Recursive: true, Force: true})
	return err
}

func (e *Exec) uploadFS(ctx context.Context, fs *jobfs.FS) error {
	var eg errgroup.Group

	uploadBlob := func(md5 jobfs.MD5, open func() (io.ReadCloser, error), commitPath func(path ypath.Path)) {
		doUploadBlob := func() error {
			if e.cache != nil {
				path, err := e.cache.Upload(ctx, md5.String(), open)
				if err != nil {
					e.l.Debug("failed to upload blob",
						log.String("md5", md5.String()),
						log.Error(err))
					return err
				}

				commitPath(path)
				return nil
			} else {
				filePath := e.config.Operation.CacheDir().Child(md5.String())
				commitPath(filePath)

				err := e.basicUploadFile(ctx, filePath, func(w io.Writer) error {
					f, err := open()
					if err != nil {
						return err
					}
					defer f.Close()

					_, err = io.Copy(w, f)
					return err
				})

				if err != nil {
					e.l.Debug("failed to upload blob",
						log.String("md5", md5.String()),
						log.Error(err))
				}

				return err
			}
		}

		eg.Go(func() error {
			backOffPolicy := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
			return backoff.Retry(doUploadBlob, backOffPolicy)
		})
	}

	for md5Hash, f := range fs.Files {
		f := f

		e.l.Debug("uploading file", log.String("path", f.LocalPath), log.String("md5", md5Hash.String()))
		uploadBlob(md5Hash,
			func() (io.ReadCloser, error) {
				return os.Open(f.LocalPath)
			},
			func(path ypath.Path) {
				f.CypressPath = path
			})
	}

	for md5Hash, dir := range fs.TarDirs {
		dir := dir

		e.l.Debug("uploading dir", log.String("path", dir.LocalPath), log.String("md5", md5Hash.String()))
		uploadBlob(md5Hash,
			func() (io.ReadCloser, error) {
				pr, pw := io.Pipe()

				go func() {
					err := tarstream.Send(dir.LocalPath, pw)
					_ = pw.CloseWithError(err)
				}()

				return pr, nil
			},
			func(path ypath.Path) {
				dir.CypressPath = path
			})
	}

	return eg.Wait()
}
