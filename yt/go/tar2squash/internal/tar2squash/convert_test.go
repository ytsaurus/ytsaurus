package tar2squash

import (
	"archive/tar"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"a.yandex-team.ru/library/go/squashfs"
	"github.com/pkg/xattr"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func runTest(t *testing.T, setup func(t *testing.T, dir string)) {
	t.Helper()

	_, err := exec.LookPath("tar")
	if err != nil {
		t.Skipf("tar binary not found: %v", err)
	}

	_, err = exec.LookPath("unsquashfs")
	if err != nil {
		t.Skipf("unsquashfs binary not found: %v", err)
	}

	tempDir := func(name string) string {
		t.Helper()

		tmpDir, err := os.MkdirTemp("", "")
		require.NoError(t, err)

		fmt.Fprintln(os.Stderr, name, tmpDir)
		return tmpDir
	}

	fsDir := tempDir("fsDir")
	setup(t, fsDir)

	tmpDir := tempDir("tmpDir")
	tarPath := filepath.Join(tmpDir, "test.tgz")
	squashPath := filepath.Join(tmpDir, "image.squashfs")

	tarCreate := exec.Command("tar", "--xattrs", "-cvf", tarPath, ".")
	tarCreate.Dir = fsDir
	tarCreate.Stderr = os.Stderr
	require.NoError(t, tarCreate.Run())

	tarList := exec.Command("tar", "-tvf", tarPath)
	tarList.Stderr = os.Stderr
	tarList.Stdout = os.Stderr
	require.NoError(t, tarList.Run())

	squashFile, err := os.Create(squashPath)
	require.NoError(t, err)
	defer squashFile.Close()

	squashWriter, err := squashfs.NewWriter(squashFile, time.Now())
	require.NoError(t, err)

	tarFile, err := os.Open(tarPath)
	require.NoError(t, err)
	defer tarFile.Close()

	tarReader := tar.NewReader(tarFile)
	require.NoError(t, Convert(squashWriter, tarReader, Options{}))
	require.NoError(t, squashFile.Close())

	squashList := exec.Command("unsquashfs", "-ll", squashPath)
	squashList.Stderr = os.Stderr
	squashList.Stdout = os.Stderr
	require.NoError(t, squashList.Run())

	unsquashDir := tempDir("unsquashDir")
	unsquash := exec.Command("unsquashfs", "-f", "-d", unsquashDir, squashPath)
	unsquash.Stderr = os.Stderr
	require.NoError(t, unsquash.Run())
}

func TestSingleFile(t *testing.T) {
	runTest(t, func(t *testing.T, dir string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "f.txt"), []byte("Hello"), 0666))
	})
}

func TestSingleFileInDir(t *testing.T) {
	runTest(t, func(t *testing.T, dir string) {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "a/b"), 0777))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "a/b/f.txt"), []byte("Hello"), 0666))
	})
}

func TestSymlink(t *testing.T) {
	runTest(t, func(t *testing.T, dir string) {
		require.NoError(t, os.Symlink("./target", filepath.Join(dir, "link")))
	})
}

func TestHardLink(t *testing.T) {
	runTest(t, func(t *testing.T, dir string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "f.txt"), []byte("Hello"), 0666))
		require.NoError(t, os.Link(filepath.Join(dir, "f.txt"), filepath.Join(dir, "copy.txt")))
	})
}

func noError(t *testing.T, err error) {
	if errors.Is(err, os.ErrPermission) {
		t.Skipf("%v", err)
	}

	require.NoError(t, err)
}

func TestCharDevice(t *testing.T) {
	runTest(t, func(t *testing.T, dir string) {
		noError(t, unix.Mknod(filepath.Join(dir, "chardev"), uint32(0666|unix.S_IFCHR), int(unix.Mkdev(0, 0))))
	})
}

func TestXattrs(t *testing.T) {
	runTest(t, func(t *testing.T, dir string) {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "a"), 0777))

		noError(t, xattr.Set(filepath.Join(dir, "a"), "trusted.overlay.opaque", []byte("y")))
	})
}

func TestMultipleXattrs(t *testing.T) {
	runTest(t, func(t *testing.T, dir string) {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "a"), 0777))

		noError(t, xattr.Set(filepath.Join(dir, "a"), "trusted.overlay.opaque", []byte("y")))
		noError(t, xattr.Set(filepath.Join(dir, "a"), "user.yt", []byte("y")))
	})
}
