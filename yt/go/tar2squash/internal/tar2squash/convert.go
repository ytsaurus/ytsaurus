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

func buildXattrs(h *tar.Header) []squashfs.Xattr {
	var xattrs []squashfs.Xattr
	for k, v := range h.Xattrs {
		xattrs = append(xattrs, squashfs.XattrFromAttr(k, []byte(v)))
	}
	return xattrs
}

func Convert(out *squashfs.Writer, in *tar.Reader) error {
	files := map[string]*squashfs.File{}
	directories := map[string][]*tar.Header{}

	var root *tar.Header
	for {
		h, err := in.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if h.Typeflag == tar.TypeReg {
			file, err := out.NewFile(h.ModTime, uint16(h.Mode), buildXattrs(h))
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
			root = h
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
				if len(e.Xattrs) != 0 {
					subdir.SetXattrs(buildXattrs(e))
				}

				if err := flushDir(e.Name, subdir); err != nil {
					return err
				}

			case tar.TypeBlock:
				err := dir.BlockDevice(entryName, int(e.Devmajor), int(e.Devminor), e.ModTime, fs.FileMode(e.Mode))
				if err != nil {
					return err
				}

			case tar.TypeChar:
				err := dir.CharDevice(entryName, int(e.Devmajor), int(e.Devminor), e.ModTime, fs.FileMode(e.Mode))
				if err != nil {
					return err
				}

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

	if root != nil && len(root.Xattrs) != 0 {
		out.Root.SetXattrs(buildXattrs(root))
	}

	if err := flushDir(".", out.Root); err != nil {
		return err
	}

	return out.Flush()
}
