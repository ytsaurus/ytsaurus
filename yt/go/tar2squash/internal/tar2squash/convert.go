package tar2squash

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"

	"a.yandex-team.ru/yt/go/tar2squash/internal/squashfs"
)

func Convert(out *squashfs.Writer, in *tar.Reader) error {
	files := map[string]*squashfs.File{}
	directories := map[string][]*tar.Header{}

	for {
		h, err := in.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if h.Typeflag == tar.TypeReg {
			file, err := out.NewFile(h.ModTime, uint16(h.Mode), nil)
			if err != nil {
				return err
			}

			if _, err := io.Copy(file, in); err != nil {
				return err
			}

			if err := file.Close(); err != nil {
				return err
			}

			files[h.Name] = file
		}

		if path.Clean(h.Name) == "." {
			continue
		}

		dirName := path.Dir(path.Clean(h.Name))
		directories[dirName] = append(directories[dirName], h)
	}

	var flushDir func(string, *squashfs.Directory) error
	flushDir = func(p string, dir *squashfs.Directory) error {
		dirEntries := directories[path.Clean(p)]
		name := func(i int) []byte {
			return []byte(path.Base(dirEntries[i].Name))
		}

		sort.Slice(dirEntries, func(i, j int) bool {
			return bytes.Compare(name(i), name(j)) == -1
		})

		for i, e := range dirEntries {
			entryName := string(name(i))

			switch e.Typeflag {
			case tar.TypeReg:
				dir.AttachFile(files[e.Name], entryName)

			case tar.TypeSymlink:
				err := dir.Symlink(e.Linkname, entryName, e.ModTime, fs.FileMode(e.Mode))
				if err != nil {
					return err
				}

			case tar.TypeLink:
				dir.AttachFile(files[e.Linkname], entryName)

			case tar.TypeDir:
				subdir := dir.Directory(entryName, e.ModTime)
				if err := flushDir(e.Name, subdir); err != nil {
					return err
				}

			case tar.TypeBlock, tar.TypeChar:
				// We ignore block and char devices. Porto will mount /dev for us.
				continue

			case tar.TypeFifo:
				err := dir.Fifo(entryName, e.ModTime, fs.FileMode(e.Mode))
				if err != nil {
					return err
				}

			default:
				return fmt.Errorf("unsupported file type %c: %q", e.Typeflag, e.Name)
			}
		}

		return dir.Flush()
	}

	if err := flushDir(".", out.Root); err != nil {
		return err
	}

	return out.Flush()
}
