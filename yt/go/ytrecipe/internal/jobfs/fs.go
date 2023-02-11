package jobfs

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sync/errgroup"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/tarstream"
)

const (
	bufferSize = 4 * (1 << 20)

	InlineBlobThreshold = 4 * (1 << 20)
)

type MD5 [md5.Size]byte

func (h MD5) MarshalYSON() ([]byte, error) {
	return yson.Marshal(h.String())
}

func (h MD5) MarshalJSON() ([]byte, error) {
	return json.Marshal(h.String())
}

func (h *MD5) UnmarshalYSON(b []byte) error {
	var s string
	if err := yson.Unmarshal(b, &s); err != nil {
		return err
	}

	hs, err := hex.DecodeString(s)
	if err != nil {
		return err
	}

	if len(hs) != len(h) {
		return fmt.Errorf("invalid MD5 format: %q", b)
	}

	copy(h[:], hs)
	return nil
}

func (h *MD5) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	hs, err := hex.DecodeString(s)
	if err != nil {
		return err
	}

	if len(hs) != len(h) {
		return fmt.Errorf("invalid MD5 format: %q", b)
	}

	copy(h[:], hs)
	return nil
}

func (h MD5) String() string {
	return hex.EncodeToString(h[:])
}

type TarDir struct {
	CypressPath ypath.Path
	LocalPath   []string

	Size       int64
	InlineBlob []byte
	Inlined    bool
}

type FilePath struct {
	Path       string
	Executable bool
}

type File struct {
	CypressPath ypath.Path
	LocalPath   []FilePath

	Size       int64
	InlineBlob []byte
	Inlined    bool
}

// FS describes local file system that should be efficiently transferred to and from YT.
type FS struct {
	TotalSize int64
	TarDirs   map[MD5]*TarDir
	Files     map[MD5]*File
	Symlinks  map[string]string
	Dirs      map[string]struct{}

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

func dirSize(dir string) (int64, error) {
	var totalSize int64

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.Mode().IsRegular() {
			totalSize += info.Size()
		}
		return nil
	})

	return totalSize, err
}

func (fs *FS) AddHashedTarDir(ref PathRef) error {
	size, err := dirSize(ref.Path)
	if err != nil {
		return err
	}

	fs.TotalSize += size
	if d, ok := fs.TarDirs[ref.MD5]; ok {
		d.LocalPath = append(d.LocalPath, ref.Path)
	} else {
		fs.TarDirs[ref.MD5] = &TarDir{
			LocalPath: []string{ref.Path},

			Size: size,
		}
	}

	return nil
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
	return fs.AddHashedTarDir(PathRef{Path: dir, MD5: h})
}

func (fs *FS) AddHashedFile(ref PathRef) error {
	st, err := os.Stat(ref.Path)
	if err != nil {
		return err
	}

	fs.TotalSize += st.Size()

	localPath := FilePath{
		Path:       ref.Path,
		Executable: st.Mode()&0100 != 0,
	}

	if f, ok := fs.Files[ref.MD5]; ok {
		f.LocalPath = append(f.LocalPath, localPath)
	} else {
		fs.Files[ref.MD5] = &File{
			LocalPath: []FilePath{localPath},

			Size: st.Size(),
		}
	}
	return nil
}

func (fs *FS) AddFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	hw := md5.New()

	if _, err = io.Copy(hw, f); err != nil {
		return err
	}

	var h MD5
	copy(h[:], hw.Sum(nil))
	return fs.AddHashedFile(PathRef{Path: path, MD5: h})
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

func copyFile(src, dst string, destroySrc bool) (int64, error) {
	in, err := os.OpenFile(src, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return 0, err
	}
	defer out.Close()

	var r io.Reader
	if destroySrc {
		r = &punchholeReader{f: in}
	} else {
		r = in
	}

	n, err := io.CopyBuffer(out, r, make([]byte, bufferSize))
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
	for _, path := range c.UploadHashedFile {
		if err := fs.AddHashedFile(path); err != nil {
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
	for _, dir := range c.UploadHashedTarDir {
		if err := fs.AddHashedTarDir(dir); err != nil {
			return err
		}
	}

	for _, path := range c.Outputs {
		fs.Outputs[path] = struct{}{}
	}

	for _, path := range c.YTOutputs {
		fs.YTOutputs[path] = struct{}{}
	}

	fs.Ext4Dirs = append([]string{c.CoredumpDir}, c.Ext4Dirs...)
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
		if !f.Inlined {
			attachFile(h, f.CypressPath)
		}
	}

	for h, d := range fs.TarDirs {
		if !d.Inlined {
			attachFile(h, d.CypressPath)
		}
	}
}

func (fs *FS) LocateBindPoints() ([]string, error) {
	var err error
	binds := map[string]struct{}{}

	visit := func(fsPath string, isDir bool) {
		fsPath = filepath.Clean(fsPath)

		// tmp directory is already created by the job proxy
		if strings.HasPrefix(fsPath, "/var/tmp/") || strings.HasPrefix(fsPath, "/tmp/") {
			return
		}

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
		for _, p := range d.LocalPath {
			visit(p, true)
		}
	}
	for d := range fs.Dirs {
		visit(d, true)
	}
	for _, f := range fs.Files {
		for _, p := range f.LocalPath {
			visit(p.Path, false)
		}
	}
	for s := range fs.Symlinks {
		visit(s, false)
	}

	if err != nil {
		return nil, err
	}

	var bindPoints []string
	for p := range binds {
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
		for _, p := range tar.LocalPath {
			mkdirAll(p)
		}
	}

	for _, f := range fs.Files {
		for _, p := range f.LocalPath {
			mkdirAll(filepath.Dir(p.Path))
		}
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
			for i, p := range f.LocalPath {
				var fileSize int64
				var err error

				l.Debug("started copying file",
					log.String("path", p.Path))

				if f.Inlined {
					if err := os.WriteFile(p.Path, f.InlineBlob, 0666); err != nil {
						return err
					}
				} else {
					last := i == len(f.LocalPath)-1
					if fileSize, err = copyFile(filepath.Join(BlobDir, md5Hash.String()), p.Path, last); err != nil {
						return err
					}
				}

				if p.Executable {
					if err := os.Chmod(p.Path, 0777); err != nil {
						return err
					}
				}

				l.Debug("finished copying file",
					log.String("path", p.Path),
					log.Int64("size", fileSize))
			}

			return nil
		})
	}

	for md5Hash, tar := range fs.TarDirs {
		md5Hash := md5Hash
		tar := tar

		eg.Go(func() error {
			for i, p := range tar.LocalPath {
				if tar.Inlined {
					l.Debug("started unpacking directory",
						log.String("path", p))

					if err := tarstream.Receive(p, bytes.NewBuffer(tar.InlineBlob)); err != nil {
						return err
					}

					l.Debug("finished unpacking directory",
						log.String("path", p),
						log.Int("size", len(tar.InlineBlob)))
				} else {
					last := i == len(tar.LocalPath)-1

					tarFile, err := os.OpenFile(filepath.Join(BlobDir, md5Hash.String()), os.O_RDWR, 0)
					if err != nil {
						return err
					}
					defer tarFile.Close()

					l.Debug("started unpacking directory",
						log.String("path", p))

					var r io.Reader
					if last {
						r = &punchholeReader{f: tarFile}
					} else {
						r = tarFile
					}

					br := bufio.NewReaderSize(r, bufferSize)
					if err := tarstream.Receive(p, br); err != nil {
						return err
					}

					stat, err := tarFile.Stat()
					if err != nil {
						return err
					}

					l.Debug("finished unpacking directory",
						log.String("path", p),
						log.Int64("size", stat.Size()))
				}
			}

			return nil
		})
	}

	return eg.Wait()
}
