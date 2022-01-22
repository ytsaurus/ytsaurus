package squashfs

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

var fsImagePath = flag.String("fs_image_path", "", "Store the SquashFS test file system in the specified path for manual inspection")

func writeTestImage(iow io.WriteSeeker, xattr bool) error {
	w, err := NewWriter(iow, time.Now())
	if err != nil {
		return err
	}

	var xattrs []Xattr
	if xattr {
		xattrs = append(xattrs, Xattr{
			Type:     2,
			FullName: "capability",
			Value:    []byte{1, 0, 0, 2, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		})
	}
	ff, err := w.Root.File("hellö wörld", time.Now(), unix.S_IRUSR|unix.S_IRGRP|unix.S_IROTH, xattrs)
	if err != nil {
		return err
	}
	if _, err := ff.Write([]byte("hello world!")); err != nil {
		return err
	}
	if err := ff.Close(); err != nil {
		return err
	}

	ff, err = w.Root.File("leer", time.Now(), unix.S_IRUSR|unix.S_IRGRP|unix.S_IROTH, nil)
	if err != nil {
		return err
	}
	if err := ff.Close(); err != nil {
		return err
	}

	ff, err = w.Root.File("second file", time.Now(), unix.S_IRUSR|unix.S_IXUSR|
		unix.S_IRGRP|unix.S_IXGRP|
		unix.S_IROTH|unix.S_IXOTH,
		nil)
	if err != nil {
		return err
	}
	if _, err := ff.Write([]byte("NON.\n")); err != nil {
		return err
	}
	if err := ff.Close(); err != nil {
		return err
	}

	if err := w.Root.Symlink("second file", "second link", time.Now(), unix.S_IRUSR|unix.S_IRGRP|unix.S_IROTH); err != nil {
		return err
	}

	subdir := w.Root.Directory("subdir", time.Now())

	subsubdir := subdir.Directory("deep", time.Now())
	ff, err = subsubdir.File("yo", time.Now(), unix.S_IRUSR|unix.S_IRGRP|unix.S_IROTH, nil)
	if err != nil {
		return err
	}
	if _, err := ff.Write([]byte("foo\n")); err != nil {
		return err
	}
	if err := ff.Close(); err != nil {
		return err
	}
	if err := subsubdir.Flush(); err != nil {
		return err
	}

	// TODO: write another file in subdir now, will result in invalid parent inode

	ff, err = subdir.File("third file (in subdir)", time.Now(), unix.S_IRUSR|unix.S_IRGRP|unix.S_IROTH, nil)
	if err != nil {
		return err
	}
	if _, err := ff.Write([]byte("contents\n")); err != nil {
		return err
	}
	if err := ff.Close(); err != nil {
		return err
	}

	if err := subdir.Flush(); err != nil {
		return err
	}
	ff, err = w.Root.File("testbin", time.Now(), unix.S_IRUSR|unix.S_IXUSR|
		unix.S_IRGRP|unix.S_IXGRP|
		unix.S_IROTH|unix.S_IXOTH,
		nil)
	if err != nil {
		return err
	}
	zf, err := os.Open(os.Args[0])
	if err != nil {
		return err
	}
	defer zf.Close()
	if _, err := io.Copy(ff, zf); err != nil {
		return err
	}
	if err := ff.Close(); err != nil {
		return err
	}

	if err := w.Root.Flush(); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return nil
}

func TestUnsquashfs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	if _, err := exec.LookPath("unsquashfs"); err != nil {
		t.Skip("unsquashfs not found in $PATH")
	}

	for _, xattr := range []bool{false, true} {
		t.Run(fmt.Sprintf("xattr %v", xattr), func(t *testing.T) {
			var (
				f   *os.File
				err error
			)
			if *fsImagePath != "" {
				f, err = os.Create(*fsImagePath + fmt.Sprintf("-xattr-%v", xattr))
			} else {
				f, err = ioutil.TempFile("", fmt.Sprintf("squashfs-xattr-%v", xattr))
				if err == nil {
					defer os.Remove(f.Name())
				}
			}
			if err != nil {
				t.Fatal(err)
			}

			if err := writeTestImage(f, xattr); err != nil {
				t.Fatal(err)
			}

			if err := f.Close(); err != nil {
				t.Fatal(err)
			}

			// Extract our generated file system using unsquashfs(1)
			out, err := ioutil.TempDir("", fmt.Sprintf("unsquashfs-xattr-%v", xattr))
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(out)

			cmd := exec.CommandContext(ctx, "unsquashfs", "-no-xattrs", "-d", filepath.Join(out, "x"), f.Name())
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				t.Fatal(err)
			}

			fbin, err := os.Open(os.Args[0])
			if err != nil {
				t.Fatal(err)
			}

			// Verify the extracted files match our expectations.
			for _, entry := range []struct {
				path     string
				contents io.Reader
			}{
				{"leer", strings.NewReader("")},
				{"hellö wörld", strings.NewReader("hello world!")},
				{"testbin", fbin},
				{"subdir/third file (in subdir)", strings.NewReader("contents\n")},
			} {
				entry := entry // copy
				t.Run(entry.path, func(t *testing.T) {
					in, err := os.Open(filepath.Join(out, "x", entry.path))
					if err != nil {
						t.Fatal(err)
					}
					got, err := ioutil.ReadAll(in)
					if err != nil {
						t.Fatal(err)
					}
					want, err := ioutil.ReadAll(entry.contents)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(got, want) {
						t.Fatalf("path %q differs", entry.path)
					}
				})
			}
		})
	}
}

func TestReader(t *testing.T) {
	t.Parallel()

	for _, xattr := range []bool{false, true} {
		t.Run(fmt.Sprintf("xattr %v", xattr), func(t *testing.T) {
			var err error

			image, err := ioutil.TempFile("", "")
			require.NoError(t, err)

			if err := writeTestImage(image, xattr); err != nil {
				t.Fatal(err)
			}

			rd, err := NewReader(image)
			if err != nil {
				t.Fatal(err)
			}

			fbin, err := os.Open(os.Args[0])
			if err != nil {
				t.Fatal(err)
			}

			// Verify the extracted files match our expectations.
			for _, entry := range []struct {
				path     string
				contents io.Reader
			}{
				{"leer", strings.NewReader("")},
				{"hellö wörld", strings.NewReader("hello world!")},
				{"testbin", fbin},
				{"subdir/third file (in subdir)", strings.NewReader("contents\n")},
			} {
				entry := entry // copy
				t.Run(entry.path, func(t *testing.T) {
					// TODO: is this t.Parallel()-safe?
					inode, err := rd.LookupPath(entry.path)
					if err != nil {
						t.Fatal(err)
					}
					in, err := rd.FileReader(inode)
					if err != nil {
						t.Fatal(err)
					}
					got, err := ioutil.ReadAll(in)
					if err != nil {
						t.Fatal(err)
					}
					want, err := ioutil.ReadAll(entry.contents)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(got, want) {
						t.Fatalf("path %q differs", entry.path)
					}
				})
			}

			if xattr {
				t.Run("xattrs", func(t *testing.T) {
					inode, err := rd.LookupPath("hellö wörld")
					if err != nil {
						t.Fatal(err)
					}

					xattrs, err := rd.ReadXattrs(inode)
					if err != nil {
						t.Fatal(err)
					}

					if got, want := len(xattrs), 1; got != want {
						t.Fatalf("unexpected number of extended attributes: got %d, want %d", got, want)
					}
					wantXattr := Xattr{
						Type:     2,
						FullName: "security.capability",
						Value:    []byte{1, 0, 0, 2, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					}
					if diff := cmp.Diff(wantXattr, xattrs[0]); diff != "" {
						t.Errorf("unexpected extended attribute: diff (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}
