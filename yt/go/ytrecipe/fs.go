package ytrecipe

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

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

	YTOutput    string
	YTHDD       string
	YTCoreDumps string

	Outputs map[string]struct{}
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
		if err := os.MkdirAll(filepath.Join(bindRootPath, d), 0777); err != nil {
			return err
		}
	}

	if err := os.MkdirAll(filepath.Join(bindRootPath, fs.YTOutput), 0777); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Join(bindRootPath, fs.YTHDD), 0777); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Join(bindRootPath, fs.YTCoreDumps), 0777); err != nil {
		return err
	}

	for from, to := range fs.Symlinks {
		if err := os.Symlink(to, filepath.Join(bindRootPath, from)); err != nil {
			return err
		}
	}

	var eg errgroup.Group
	for md5Hash, f := range fs.Files {
		md5Hash := md5Hash
		f := f

		dstPath := filepath.Join(bindRootPath, f.LocalPath)
		if err := os.MkdirAll(filepath.Dir(dstPath), 0777); err != nil {
			return err
		}

		eg.Go(func() error {
			return copyFile(filepath.Join(fsRoot, md5Hash.String()), dstPath)
		})
	}

	return eg.Wait()
}

const CrashJobFileMarker = "ytrecipe_inject_job_crash.txt"

func stripBindRoot(path string) string {
	if !strings.HasPrefix(path, bindRootPath) {
		panic(fmt.Sprintf("path %s is not located inside bind root", path))
	}

	return strings.TrimPrefix(path, bindRootPath)
}

func emitDir(w mapreduce.Writer, dir string, strip bool) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if errors.Is(err, os.ErrPermission) {
			// TODO(prime@): log
			return nil
		}

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
	if errors.Is(err, os.ErrPermission) {
		// TODO(prime@): log
		return nil
	} else if err != nil {
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
			if i != 0 {
				break
			}
		} else if err != nil {
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
		realPath := filepath.Join(bindRootPath, path)

		st, err := os.Stat(realPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
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

	if err := emitDir(output, fs.YTHDD, false); err != nil {
		return err
	}

	if err := emitCoreDumps(output, fs.YTCoreDumps); err != nil {
		return err
	}

	if err := mapreduce.SwitchTable(output, 0); err != nil {
		return err
	}

	return nil
}

func emitCoreDumps(w mapreduce.Writer, dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if errors.Is(err, os.ErrPermission) {
			// TODO(prime@): log
			return nil
		}

		if err != nil {
			return err
		}

		if info.IsDir() {
			return w.Write(FileRow{FilePath: path, IsDir: true})
		}

		return emitSparseFile(w, path)
	})
}

const (
	SeekData = 3
	SeekHole = 4
)

func emitSparseFile(w mapreduce.Writer, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	var partIndex int
	buf := make([]byte, maxRowSize)

	emitHole := func(size int64) error {
		row := FileRow{
			FilePath:  filename,
			PartIndex: partIndex,
			DataSize:  size,
		}
		partIndex++

		return w.Write(row)
	}

	emitData := func(offset, size int64) error {
		_, err := f.Seek(offset, io.SeekStart)
		if err != nil {
			return err
		}

		lr := io.LimitReader(f, size)
		for {
			n, err := lr.Read(buf)
			if err != nil && err != io.EOF {
				return err
			}

			if n > 0 {
				row := FileRow{
					FilePath:  filename,
					PartIndex: partIndex,
					Data:      buf[:n],
				}
				partIndex++

				if err := w.Write(row); err != nil {
					return err
				}
			}

			if err == io.EOF {
				return nil
			}
		}
	}

	var dataOffset, holeOffset int64

	for partIndex := 0; true; partIndex++ {
		dataOffset, err = f.Seek(holeOffset, SeekData)
		if errors.Is(err, syscall.ENXIO) {
			// File ends with hole

			st, err := f.Stat()
			if err != nil {
				return err
			}

			return emitHole(st.Size() - holeOffset)
		} else if err != nil {
			return err
		}

		// File begins with data
		if dataOffset != holeOffset {
			if err := emitHole(dataOffset - holeOffset); err != nil {
				return err
			}
		}

		holeOffset, err = f.Seek(dataOffset, SeekHole)
		if err != nil {
			return err
		}

		if err := emitData(dataOffset, holeOffset-dataOffset); err != nil {
			return err
		}
	}

	return nil
}
