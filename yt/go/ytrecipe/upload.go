package ytrecipe

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cenkalti/backoff"
	"golang.org/x/sync/errgroup"

	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/yt"
)

func (r *Runner) basicUploadFile(ctx context.Context, relpath string, do func(w io.Writer) error) error {
	ok, err := r.YT.NodeExists(ctx, r.Config.CachePath.Child(relpath), &yt.NodeExistsOptions{
		MasterReadOptions: &yt.MasterReadOptions{ReadFrom: yt.ReadFromCache},
	})
	if err != nil {
		return err
	}

	if ok {
		return nil
	}

	tmpPath := r.Config.TmpPath.Child(guid.New().String())

	_, err = r.YT.CreateNode(ctx, tmpPath, yt.NodeFile, nil)
	if err != nil {
		return err
	}

	w, err := r.YT.WriteFile(ctx, tmpPath, nil)
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

	_, err = r.YT.MoveNode(ctx, tmpPath, r.Config.CachePath.Child(relpath), &yt.MoveNodeOptions{Recursive: true, Force: true})
	return err
}

func streamTarFile(w io.Writer, dir string) error {
	tarfile := tar.NewWriter(w)

	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relpath, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}

		h := &tar.Header{
			Name: relpath,
			Mode: int64(info.Mode()),
		}

		switch {
		case info.IsDir():
			h.Typeflag = tar.TypeDir
			return tarfile.WriteHeader(h)

		case info.Mode()&os.ModeSymlink != 0:
			h.Typeflag = tar.TypeSymlink
			h.Linkname, err = os.Readlink(path)
			if err != nil {
				return err
			}
			return tarfile.WriteHeader(h)

		default:
			h.Typeflag = tar.TypeReg
			h.Size = info.Size()
			if err := tarfile.WriteHeader(h); err != nil {
				return err
			}

			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()

			_, err = io.Copy(tarfile, f)
			if err != nil {
				return err
			}
			return nil
		}
	})
}

func (r *Runner) uploadFS(ctx context.Context, fs *FS, env *Env) error {
	var eg errgroup.Group

	for md5Hash, f := range fs.Files {
		md5Hash := md5Hash
		f := f

		uploadFile := func() error {
			if r.Cache != nil {
				openFile := func() (io.ReadCloser, error) {
					return os.Open(f.LocalPath)
				}

				path, err := r.Cache.Upload(ctx, md5Hash.String(), openFile)
				if err != nil {
					return err
				}

				f.CypressPath = path
				return nil
			} else {
				f.CypressPath = r.Config.CachePath.Child(md5Hash.String())
				err := r.basicUploadFile(ctx, md5Hash.String(), func(w io.Writer) error {
					f, err := os.Open(f.LocalPath)
					if err != nil {
						return err
					}
					defer f.Close()

					_, err = io.Copy(w, f)
					return err
				})

				if err != nil {
					return fmt.Errorf("error uploading %q to cypress: %w", f.LocalPath, err)
				}
			}

			return nil
		}

		eg.Go(func() error {
			backOffPolicy := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
			return backoff.Retry(uploadFile, backOffPolicy)
		})
	}

	return eg.Wait()
}
