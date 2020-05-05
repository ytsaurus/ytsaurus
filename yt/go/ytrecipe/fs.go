package ytrecipe

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sync/errgroup"

	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/ypath"
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

// FS describes local file system that should be efficiently transferred to YT.
type FS struct {
	TarDirs  map[MD5]*TarDir
	Files    map[MD5]*File
	Symlinks map[string]string
	Dirs     map[string]struct{}

	YTOutput string
	Outputs  map[string]struct{}
}

func NewFS() *FS {
	return &FS{
		TarDirs:  make(map[MD5]*TarDir),
		Files:    make(map[MD5]*File),
		Symlinks: make(map[string]string),
		Dirs:     make(map[string]struct{}),

		Outputs: make(map[string]struct{}),
	}
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
func (fs *FS) AddDir(dir string) error {
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

func (fs *FS) LocateBindPoints() ([]string, error) {
	binds := map[string]struct{}{}

	visit := func(fsPath string, isDir bool) error {
		path := fsPath
		if !isDir {
			path = filepath.Dir(path)
		}

		for i := 0; i < 1024; i++ {
			parent := filepath.Dir(path)
			if parent == "/" || parent == "." {
				return fmt.Errorf("can't bind %q inside job: reached root", fsPath)
			}

			_, err := os.Stat(parent)
			if err == nil {
				binds[parent] = struct{}{}
				return nil
			} else if !os.IsNotExist(err) {
				return err
			}

			path = parent
		}

		return fmt.Errorf("failed to locate bind point for path %q", fsPath)
	}

	for _, d := range fs.TarDirs {
		if err := visit(d.LocalPath, true); err != nil {
			return nil, err
		}
	}

	for d := range fs.Dirs {
		if err := visit(d, true); err != nil {
			return nil, err
		}
	}

	for _, f := range fs.Files {
		if err := visit(f.LocalPath, false); err != nil {
			return nil, err
		}
	}

	for s := range fs.Symlinks {
		if err := visit(s, false); err != nil {
			return nil, err
		}
	}

	var bindPoints []string
	for p := range binds {
		bindPoints = append(bindPoints, p)
	}
	return bindPoints, nil
}

func copyFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0777); err != nil {
		return err
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	st, err := in.Stat()
	if err != nil {
		return err
	}

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY, st.Mode())
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

func (fs *FS) Recreate(fsRoot string) error {
	for d := range fs.Dirs {
		if err := os.MkdirAll(filepath.Join(bindRoot, d), 0777); err != nil {
			return err
		}
	}

	if err := os.MkdirAll(fs.YTOutput, 0777); err != nil {
		return err
	}

	for from, to := range fs.Symlinks {
		if err := os.Symlink(to, filepath.Join(bindRoot, from)); err != nil {
			return err
		}
	}

	var eg errgroup.Group
	for md5Hash, f := range fs.Files {
		md5Hash := md5Hash
		f := f

		eg.Go(func() error {
			return copyFile(filepath.Join(fsRoot, md5Hash.String()), filepath.Join(bindRoot, f.LocalPath))
		})
	}

	return eg.Wait()
}

const CrashJobFileMarker = "ytrecipe_inject_job_crash.txt"

func stripBindRoot(path string) string {
	if !strings.HasPrefix(path, bindRoot) {
		panic(fmt.Sprintf("path %s is not located inside bind root", path))
	}

	return strings.TrimPrefix(path, bindRoot)
}

func emitDir(w mapreduce.Writer, dir string, strip bool) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.Name() == CrashJobFileMarker {
			return fmt.Errorf("found %q; job failed with system error", CrashJobFileMarker)
		}

		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}

		if info.IsDir() {
			if strip {
				path = stripBindRoot(path)
			}

			return w.Write(FileRow{FilePath: path, IsDir: true})
		}

		return emitFile(w, path, strip)
	})
}

func emitFile(w mapreduce.Writer, filename string, strip bool) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	if strip {
		filename = stripBindRoot(filename)
	}

	buf := make([]byte, maxRowSize)
	for i := 0; true; i++ {
		n, err := f.Read(buf)
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		row := FileRow{
			FilePath:  filename,
			PartIndex: i,
			Data:      buf[:n],
		}

		if err := w.Write(row); err != nil {
			return err
		}
	}

	return nil
}

func (fs *FS) EmitResults(output mapreduce.Writer) error {
	for path := range fs.Outputs {
		realPath := filepath.Join(bindRoot, path)

		st, err := os.Stat(realPath)
		if err != nil {
			return err
		}

		if st.IsDir() {
			if err := emitDir(output, realPath, true); err != nil {
				return err
			}
		} else {
			if err := emitFile(output, realPath, true); err != nil {
				return err
			}
		}
	}

	if err := mapreduce.SwitchTable(output, 1); err != nil {
		return err
	}

	if err := emitDir(output, fs.YTOutput, false); err != nil {
		return err
	}

	if err := mapreduce.SwitchTable(output, 0); err != nil {
		return err
	}

	return nil
}
