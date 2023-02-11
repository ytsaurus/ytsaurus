package ytexec

import (
	"bytes"
	"context"
	"io"
	"os"
	"time"

	"golang.org/x/sync/errgroup"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/ptr"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/jobfs"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/tarstream"
)

func (e *Exec) basicUploadFile(ctx context.Context, path, tmpPath ypath.Path, do func(w io.Writer) error) error {
	var expirationTime yson.Time
	err := e.yc.GetNode(ctx, path.Attr("expiration_time"), &expirationTime, &yt.GetNodeOptions{
		MasterReadOptions: &yt.MasterReadOptions{ReadFrom: yt.ReadFromCache},
	})

	if err != nil && !yterrors.ContainsResolveError(err) {
		return err
	}

	if err == nil && time.Until(time.Time(expirationTime)) > expirationDelay {
		return nil
	}

	expiresAt := time.Now().Add(e.config.Operation.BlobTTL)
	_, err = e.yc.CreateNode(ctx, tmpPath, yt.NodeFile, &yt.CreateNodeOptions{
		Recursive: true,
		Attributes: map[string]interface{}{
			"expiration_time": yson.Time(expiresAt),
		},
	})
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

	_, err = e.yc.MoveNode(ctx, tmpPath, path, &yt.MoveNodeOptions{
		Recursive:              true,
		Force:                  true,
		PreserveExpirationTime: ptr.Bool(true),
	})

	return err
}

func (e *Exec) uploadFS(ctx context.Context, fs *jobfs.FS) error {
	if e.cache != nil {
		err := e.cache.Migrate(ctx)
		if yterrors.ContainsErrorCode(err, yterrors.CodeAuthorizationError) && e.config.Operation.EnableResearchFallback {
			err = nil
		}

		if err != nil {
			return err
		}
	}

	var eg errgroup.Group

	uploadBlob := func(
		md5 jobfs.MD5,
		open func() (io.ReadCloser, error),
		commitPath func(path ypath.Path),
	) {
		basicUploadFile := func(to ypath.Path) error {
			filePath := to.Child(md5.String()[:2]).Child(md5.String())
			tmpPath := to.Child(guid.New().String())

			err := e.basicUploadFile(ctx, filePath, tmpPath, func(w io.Writer) error {
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
				return err
			}

			commitPath(filePath)
			return nil
		}

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
				return basicUploadFile(e.config.Operation.CacheDir())
			}
		}

		uploadWithFallback := func() error {
			err := doUploadBlob()
			if yterrors.ContainsErrorCode(err, yterrors.CodeAuthorizationError) && e.config.Operation.EnableResearchFallback {
				return basicUploadFile(tmpUploadPath)
			}
			return err
		}

		eg.Go(func() error {
			return e.retry(uploadWithFallback)
		})
	}

	for md5Hash, f := range fs.Files {
		f := f

		e.l.Debug("uploading file", log.String("path", f.LocalPath[0].Path), log.String("md5", md5Hash.String()))

		if f.Size <= jobfs.InlineBlobThreshold {
			eg.Go(func() error {
				blob, err := os.ReadFile(f.LocalPath[0].Path)
				if err != nil {
					return err
				}

				f.InlineBlob = blob
				f.Inlined = true
				return nil
			})
		} else {
			uploadBlob(md5Hash,
				func() (io.ReadCloser, error) {
					return os.Open(f.LocalPath[0].Path)
				},
				func(path ypath.Path) {
					f.CypressPath = path
				})
		}
	}

	for md5Hash, dir := range fs.TarDirs {
		dir := dir

		e.l.Debug("uploading dir", log.Strings("paths", dir.LocalPath), log.String("md5", md5Hash.String()))
		if dir.Size <= jobfs.InlineBlobThreshold {
			eg.Go(func() error {
				var blob bytes.Buffer
				if err := tarstream.Send(dir.LocalPath[0], &blob); err != nil {
					return err
				}

				dir.InlineBlob = blob.Bytes()
				dir.Inlined = true
				return nil
			})
		} else {
			uploadBlob(md5Hash,
				func() (io.ReadCloser, error) {
					pr, pw := io.Pipe()

					go func() {
						err := tarstream.Send(dir.LocalPath[0], pw)
						_ = pw.CloseWithError(err)
					}()

					return pr, nil
				},
				func(path ypath.Path) {
					dir.CypressPath = path
				})
		}
	}

	return eg.Wait()
}
