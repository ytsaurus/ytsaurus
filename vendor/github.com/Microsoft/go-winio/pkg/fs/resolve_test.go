//go:build windows

package fs

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/windows"

	"github.com/Microsoft/go-winio/internal/computestorage"
	"github.com/Microsoft/go-winio/internal/fs"
	"github.com/Microsoft/go-winio/vhd"
)

func getWindowsBuildNumber() uint32 {
	// RtlGetVersion ignores manifest requirements
	vex := windows.RtlGetVersion()
	return vex.BuildNumber
}

func makeSymlink(t *testing.T, oldName string, newName string) {
	t.Helper()

	t.Logf("make symlink: %s -> %s", oldName, newName)

	if _, err := os.Lstat(oldName); err != nil {
		t.Fatalf("could not open file %q: %v", oldName, err)
	}

	if err := os.Symlink(oldName, newName); err != nil {
		t.Fatalf("creating symlink: %s", err)
	}

	if _, err := os.Lstat(newName); err != nil {
		t.Fatalf("could not open file %q: %v", newName, err)
	}
}

func getVolumeGUIDPath(t *testing.T, path string) string {
	t.Helper()

	h, err := openMetadata(path)
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(h) //nolint:errcheck
	final, err := fs.GetFinalPathNameByHandle(h, fs.FILE_NAME_OPENED|fs.VOLUME_NAME_GUID)
	if err != nil {
		t.Fatal(err)
	}
	return final
}

func openDisk(path string) (windows.Handle, error) {
	h, err := fs.CreateFile(
		path,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		fs.FILE_SHARE_READ|fs.FILE_SHARE_WRITE,
		nil, // security attributes
		fs.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL|fs.FILE_FLAG_NO_BUFFERING,
		fs.NullHandle)
	if err != nil {
		return 0, &os.PathError{
			Op:   "CreateFile",
			Path: path,
			Err:  err,
		}
	}
	return h, nil
}

func formatVHD(vhdHandle windows.Handle) error {
	h := vhdHandle
	// Pre-19H1 HcsFormatWritableLayerVhd expects a disk handle.
	// On newer builds it expects a VHD handle instead.
	// Open a handle to the VHD's disk object if needed.

	// Windows Server 1903, aka 19H1
	if getWindowsBuildNumber() < 18362 {
		diskPath, err := vhd.GetVirtualDiskPhysicalPath(syscall.Handle(h))
		if err != nil {
			return err
		}
		diskHandle, err := openDisk(diskPath)
		if err != nil {
			return err
		}
		defer windows.CloseHandle(diskHandle) //nolint:errcheck // cleanup code
		h = diskHandle
	}
	// Formatting a disk directly in Windows is a pain, so we use FormatWritableLayerVhd to do it.
	// It has a side effect of creating a sandbox directory on the formatted volume, but it's safe
	// to just ignore that for our purposes here.
	return computestorage.FormatWritableLayerVHD(h)
}

// Creates a VHD with a NTFS volume. Returns the volume path.
func setupVHDVolume(t *testing.T, vhdPath string) string {
	t.Helper()

	vhdHandle, err := vhd.CreateVirtualDisk(vhdPath,
		vhd.VirtualDiskAccessNone, vhd.CreateVirtualDiskFlagNone,
		&vhd.CreateVirtualDiskParameters{
			Version: 2,
			Version2: vhd.CreateVersion2{
				MaximumSize:      5 * 1024 * 1024 * 1024, // 5GB, thin provisioned
				BlockSizeInBytes: 1 * 1024 * 1024,        // 1MB
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = windows.CloseHandle(windows.Handle(vhdHandle))
	})
	if err := vhd.AttachVirtualDisk(vhdHandle, vhd.AttachVirtualDiskFlagNone, &vhd.AttachVirtualDiskParameters{Version: 1}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := vhd.DetachVirtualDisk(vhdHandle); err != nil {
			t.Fatal(err)
		}
	})
	if err := formatVHD(windows.Handle(vhdHandle)); err != nil {
		t.Fatalf("failed to format VHD: %s", err)
	}
	// Get the path for the volume that was just created on the disk.
	volumePath, err := computestorage.GetLayerVHDMountPath(windows.Handle(vhdHandle))
	if err != nil {
		t.Fatal(err)
	}
	return volumePath
}

func writeFile(t *testing.T, path string, content []byte) {
	t.Helper()

	if err := os.WriteFile(path, content, 0644); err != nil { //nolint:gosec // test file, can have permissive mode
		t.Fatal(err)
	}
}

func mountVolume(t *testing.T, volumePath string, mountPoint string) {
	t.Helper()

	// Create the mount point directory.
	if err := os.Mkdir(mountPoint, 0644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(mountPoint); err != nil {
			t.Fatal(err)
		}
	})
	// Volume path must end in a slash.
	if !strings.HasSuffix(volumePath, `\`) {
		volumePath += `\`
	}
	volumePathU16, err := windows.UTF16PtrFromString(volumePath)
	if err != nil {
		t.Fatal(err)
	}
	// Mount point must end in a slash.
	if !strings.HasSuffix(mountPoint, `\`) {
		mountPoint += `\`
	}
	mountPointU16, err := windows.UTF16PtrFromString(mountPoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetVolumeMountPoint(mountPointU16, volumePathU16); err != nil {
		t.Fatalf("failed to mount %s onto %s: %s", volumePath, mountPoint, err)
	}
	t.Cleanup(func() {
		if err := windows.DeleteVolumeMountPoint(mountPointU16); err != nil {
			t.Fatalf("failed to delete mount on %s: %s", mountPoint, err)
		}
	})
}

func TestResolvePath(t *testing.T) {
	if !windows.GetCurrentProcessToken().IsElevated() {
		t.Skip("requires elevated privileges")
	}

	// Set up some data to be used by the test cases.
	volumePathC := getVolumeGUIDPath(t, `C:\`)
	dir := t.TempDir()

	makeSymlink(t, `C:\windows`, filepath.Join(dir, "lnk1"))
	makeSymlink(t, `\\localhost\c$\windows`, filepath.Join(dir, "lnk2"))

	volumePathVHD1 := setupVHDVolume(t, filepath.Join(dir, "foo.vhdx"))
	writeFile(t, filepath.Join(volumePathVHD1, "data.txt"), []byte("test content 1"))
	makeSymlink(t, filepath.Join(volumePathVHD1, "data.txt"), filepath.Join(dir, "lnk3"))

	volumePathVHD2 := setupVHDVolume(t, filepath.Join(dir, "bar.vhdx"))
	writeFile(t, filepath.Join(volumePathVHD2, "data.txt"), []byte("test content 2"))
	makeSymlink(t, filepath.Join(volumePathVHD2, "data.txt"), filepath.Join(dir, "lnk4"))
	mountVolume(t, volumePathVHD2, filepath.Join(dir, "mnt"))

	for _, tc := range []struct {
		input       string
		expected    string
		description string
	}{
		{`C:\windows`, volumePathC + `Windows`, "local path"},
		{filepath.Join(dir, "lnk1"), volumePathC + `Windows`, "symlink to local path"},
		{`\\localhost\c$\windows`, `\\localhost\c$\Windows`, "UNC path"},
		{filepath.Join(dir, "lnk2"), `\\localhost\c$\Windows`, "symlink to UNC path"},
		{filepath.Join(volumePathVHD1, "data.txt"), filepath.Join(volumePathVHD1, "data.txt"), "volume with no mount point"},
		{filepath.Join(dir, "lnk3"), filepath.Join(volumePathVHD1, "data.txt"), "symlink to volume with no mount point"},
		{filepath.Join(dir, "mnt", "data.txt"), filepath.Join(volumePathVHD2, "data.txt"), "volume with mount point"},
		{filepath.Join(dir, "lnk4"), filepath.Join(volumePathVHD2, "data.txt"), "symlink to volume with mount point"},
	} {
		t.Run(tc.description, func(t *testing.T) {
			t.Logf("resolving: %s -> %s", tc.input, tc.expected)

			actual, err := ResolvePath(tc.input)
			if err != nil {
				t.Fatalf("ResolvePath should return no error, but: %v", err)
			}
			if actual != tc.expected {
				t.Fatalf("expected %v but got %v", tc.expected, actual)
			}

			// Make sure EvalSymlinks works with the resolved path, as an extra safety measure.
			t.Logf("filepath.EvalSymlinks(%s)", actual)
			p, err := filepath.EvalSymlinks(actual)
			if err != nil {
				t.Fatalf("EvalSymlinks should return no error, but %v", err)
			}
			// As an extra-extra safety, check that resolvePath(x) == EvalSymlinks(resolvePath(x)).
			// EvalSymlinks normalizes UNC path casing, but resolvePath may not, so compare with
			// case-insensitivity here.
			if !strings.EqualFold(actual, p) {
				t.Fatalf("EvalSymlinks should resolve to the same path. Expected %v but got %v", actual, p)
			}
		})
	}
}
