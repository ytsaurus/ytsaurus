package jobfs

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/tarstream"
)

const (
	bufferSize = 4 * (1 << 20)
)

type MD5 [md5.Size]byte

func (h MD5) String() string {
	return hex.EncodeToString(h[:])
}

type TarDir struct {
	LocalPath   string
	CypressPath ypath.Path
}

type File struct {
	LocalPath   string
	Executable  bool
	CypressPath ypath.Path
}

// FS describes local file system that should be efficiently transferred to and from YT.
type FS struct {
	TarDirs  map[MD5]*TarDir
	Files    map[MD5]*File
	Symlinks map[string]string
	Dirs     map[string]struct{}

	Outputs   map[string]struct{}
	YTOutputs map[string]struct{}

	Ext4Dirs    []string
	CoredumpDir string
}

func New() *FS {
	return &FS{
		TarDirs:  make(map[MD5]*TarDir),
		Files:    make(map[MD5]*File),
		Symlinks: make(map[string]string),
		Dirs:     make(map[string]struct{}),

		Outputs:   make(map[string]struct{}),
		YTOutputs: make(map[string]struct{}),
	}
}

func (fs *FS) AddTarDir(dir string) error {
	st, err := os.Stat(dir)
	if err != nil {
		return err
	}

	if st.Mode()&os.ModeSymlink != 0 {
		dir, err = os.Readlink(dir)
		if err != nil {
			return err
		}
	}

	hw := md5.New()
	if err := tarstream.Send(dir, hw); err != nil {
		return err
	}

	var h MD5
	copy(h[:], hw.Sum(nil))
	fs.TarDirs[h] = &TarDir{
		LocalPath: dir,
	}

	return nil
}

func (fs *FS) AddFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return err
	}

	hw := md5.New()
	if _, err := io.Copy(hw, f); err != nil {
		return err
	}

	var h MD5
	copy(h[:], hw.Sum(nil))

	fs.Files[h] = &File{
		LocalPath:  path,
		Executable: st.Mode()&0100 != 0,
	}

	return nil
}

// AddBuildRoot scans directory adding all directories and symlinks to FS.
func (fs *FS) AddStructure(dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error walking dir %q: %w", dir, err)
		}

		switch {
		case info.IsDir():
			fs.Dirs[path] = struct{}{}

		case info.Mode()&os.ModeSymlink != 0:
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}

			fs.Symlinks[path] = link
		}

		return nil
	})
}

func copyFile(src, dst string) (int64, error) {
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return 0, err
	}
	defer out.Close()

	n, err := io.CopyBuffer(out, in, make([]byte, bufferSize))
	if err != nil {
		return n, err
	}
	return n, out.Close()
}

func (fs *FS) Add(c Config) error {
	for _, path := range c.UploadFile {
		if err := fs.AddFile(path); err != nil {
			return err
		}
	}

	for _, dir := range c.UploadStructure {
		if err := fs.AddStructure(dir); err != nil {
			return err
		}
	}

	for _, dir := range c.UploadTarDir {
		if err := fs.AddTarDir(dir); err != nil {
			return err
		}
	}

	for _, path := range c.Outputs {
		fs.Outputs[path] = struct{}{}
	}

	for _, path := range c.YTOutputs {
		fs.YTOutputs[path] = struct{}{}
	}

	fs.Ext4Dirs = c.Ext4Dirs
	fs.CoredumpDir = c.CoredumpDir
	return nil
}

var BlobDir = filepath.Join("tmpfs", "fs")

func (fs *FS) AttachFiles(us *spec.UserScript) {
	attachFile := func(h MD5, cypressPath ypath.Path) {
		us.FilePaths = append(us.FilePaths, spec.File{
			BypassArtifactCache: true,
			FileName:            filepath.Join(BlobDir, h.String()),
			CypressPath:         cypressPath,
		})
	}

	for h, f := range fs.Files {
		attachFile(h, f.CypressPath)
	}

	for h, d := range fs.TarDirs {
		attachFile(h, d.CypressPath)
	}
}

func (fs *FS) LocateBindPoints() ([]string, error) {
	var err error
	binds := map[string]struct{}{}

	visit := func(fsPath string, isDir bool) {
		if err != nil {
			return
		}

		path := fsPath
		if !isDir {
			path = filepath.Dir(path)
		}

		for i := 0; i < 1024; i++ {
			parent := filepath.Dir(path)

			_, err = os.Stat(parent)
			if err == nil {
				binds[path] = struct{}{}
				return
			} else if !os.IsNotExist(err) {
				return
			}

			path = parent
		}

		err = fmt.Errorf("failed to locate bind point for path %q", fsPath)
	}

	for _, d := range fs.TarDirs {
		visit(d.LocalPath, true)
	}
	for d := range fs.Dirs {
		visit(d, true)
	}
	for _, f := range fs.Files {
		visit(f.LocalPath, false)
	}
	for s := range fs.Symlinks {
		visit(s, false)
	}

	if err != nil {
		return nil, err
	}

	var bindPoints []string
	for p := range binds {
		// tmp directory is already created by the job proxy
		if p == "/var/tmp" || p == "/tmp" {
			continue
		}

		bindPoints = append(bindPoints, p)
	}
	return bindPoints, nil
}

func (fs *FS) Recreate(l log.Structured) (err error) {
	mkdirAll := func(path string) {
		if err != nil {
			return
		}

		err = os.MkdirAll(path, 0777)
		if err != nil {
			err = fmt.Errorf("mkdirall %s: %w", path, err)
		}
	}

	for d := range fs.Dirs {
		mkdirAll(d)
	}

	for _, tar := range fs.TarDirs {
		mkdirAll(tar.LocalPath)
	}

	for _, f := range fs.Files {
		mkdirAll(filepath.Dir(f.LocalPath))
	}

	mkdirAll(fs.CoredumpDir)

	if err != nil {
		return
	}

	for from, to := range fs.Symlinks {
		if err := os.Symlink(to, from); err != nil {
			return err
		}
	}

	var eg errgroup.Group
	for md5Hash, f := range fs.Files {
		md5Hash := md5Hash
		f := f

		eg.Go(func() error {
			var fileSize int64
			var err error

			l.Debug("started copying file",
				log.String("path", f.LocalPath))

			if fileSize, err = copyFile(filepath.Join(BlobDir, md5Hash.String()), f.LocalPath); err != nil {
				return err
			}

			if f.Executable {
				if err := os.Chmod(f.LocalPath, 0777); err != nil {
					return err
				}
			}

			l.Debug("finished copying file",
				log.String("path", f.LocalPath),
				log.Int64("size", fileSize))
			return nil
		})
	}

	for md5Hash, tar := range fs.TarDirs {
		md5Hash := md5Hash
		tar := tar

		eg.Go(func() error {
			tarFile, err := os.Open(filepath.Join(BlobDir, md5Hash.String()))
			if err != nil {
				return err
			}
			defer tarFile.Close()

			l.Debug("started unpacking directory",
				log.String("path", tar.LocalPath))

			if err := tarstream.Receive(tar.LocalPath, bufio.NewReaderSize(tarFile, bufferSize)); err != nil {
				return err
			}

			stat, err := tarFile.Stat()
			if err != nil {
				return err
			}

			l.Debug("finished unpacking directory",
				log.String("path", tar.LocalPath),
				log.Int64("size", stat.Size()))

			return nil
		})
	}

	return eg.Wait()
}
