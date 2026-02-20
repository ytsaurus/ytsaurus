//go:build !windows

package user

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

type node struct {
	uid int
	gid int
}

func TestMkdirAllAndChown(t *testing.T) {
	requiresRoot(t)
	dirName := t.TempDir()

	testTree := map[string]node{
		"usr":              {0, 0},
		"usr/bin":          {0, 0},
		"lib":              {33, 33},
		"lib/x86_64":       {45, 45},
		"lib/x86_64/share": {1, 1},
	}

	if err := buildTree(dirName, testTree); err != nil {
		t.Fatal(err)
	}

	// test adding a directory to a pre-existing dir; only the new dir is owned by the uid/gid
	if err := MkdirAllAndChown(filepath.Join(dirName, "usr", "share"), 0o755, 99, 99); err != nil {
		t.Fatal(err)
	}
	testTree["usr/share"] = node{99, 99}
	verifyTree, err := readTree(dirName, "")
	if err != nil {
		t.Fatal(err)
	}
	compareTrees(t, testTree, verifyTree)

	// test 2-deep new directories--both should be owned by the uid/gid pair
	if err := MkdirAllAndChown(filepath.Join(dirName, "lib", "some", "other"), 0o755, 101, 101); err != nil {
		t.Fatal(err)
	}
	testTree["lib/some"] = node{101, 101}
	testTree["lib/some/other"] = node{101, 101}
	verifyTree, err = readTree(dirName, "")
	if err != nil {
		t.Fatal(err)
	}
	compareTrees(t, testTree, verifyTree)

	// test a directory that already exists; should be chowned, but nothing else
	if err := MkdirAllAndChown(filepath.Join(dirName, "usr"), 0o755, 102, 102); err != nil {
		t.Fatal(err)
	}
	testTree["usr"] = node{102, 102}
	verifyTree, err = readTree(dirName, "")
	if err != nil {
		t.Fatal(err)
	}
	compareTrees(t, testTree, verifyTree)
}

func TestMkdirAllAndChownNew(t *testing.T) {
	requiresRoot(t)
	dirName := t.TempDir()

	testTree := map[string]node{
		"usr":              {0, 0},
		"usr/bin":          {0, 0},
		"lib":              {33, 33},
		"lib/x86_64":       {45, 45},
		"lib/x86_64/share": {1, 1},
	}
	if err := buildTree(dirName, testTree); err != nil {
		t.Fatal(err)
	}

	// test adding a directory to a pre-existing dir; only the new dir is owned by the uid/gid
	if err := MkdirAllAndChown(filepath.Join(dirName, "usr", "share"), 0o755, 99, 99, WithOnlyNew); err != nil {
		t.Fatal(err)
	}

	testTree["usr/share"] = node{99, 99}
	verifyTree, err := readTree(dirName, "")
	if err != nil {
		t.Fatal(err)
	}
	compareTrees(t, testTree, verifyTree)

	// test 2-deep new directories--both should be owned by the uid/gid pair
	if err = MkdirAllAndChown(filepath.Join(dirName, "lib", "some", "other"), 0o755, 101, 101, WithOnlyNew); err != nil {
		t.Fatal(err)
	}
	testTree["lib/some"] = node{101, 101}
	testTree["lib/some/other"] = node{101, 101}
	if verifyTree, err = readTree(dirName, ""); err != nil {
		t.Fatal(err)
	}
	compareTrees(t, testTree, verifyTree)

	// test a directory that already exists; should NOT be chowned
	if err = MkdirAllAndChown(filepath.Join(dirName, "usr"), 0o755, 102, 102, WithOnlyNew); err != nil {
		t.Fatal(err)
	}
	if verifyTree, err = readTree(dirName, ""); err != nil {
		t.Fatal(err)
	}
	compareTrees(t, testTree, verifyTree)
}

func TestMkdirAllAndChownNewRelative(t *testing.T) {
	requiresRoot(t)

	tests := []struct {
		in  string
		out []string
	}{
		{
			in:  "dir1",
			out: []string{"dir1"},
		},
		{
			in:  "dir2/subdir2",
			out: []string{"dir2", "dir2/subdir2"},
		},
		{
			in:  "dir3/subdir3/",
			out: []string{"dir3", "dir3/subdir3"},
		},
		{
			in:  "dir4/subdir4/.",
			out: []string{"dir4", "dir4/subdir4"},
		},
		{
			in:  "dir5/././subdir5/",
			out: []string{"dir5", "dir5/subdir5"},
		},
		{
			in:  "./dir6",
			out: []string{"dir6"},
		},
		{
			in:  "./dir7/subdir7",
			out: []string{"dir7", "dir7/subdir7"},
		},
		{
			in:  "./dir8/subdir8/",
			out: []string{"dir8", "dir8/subdir8"},
		},
		{
			in:  "./dir9/subdir9/.",
			out: []string{"dir9", "dir9/subdir9"},
		},
		{
			in:  "./dir10/././subdir10/",
			out: []string{"dir10", "dir10/subdir10"},
		},
	}

	// Set the current working directory to the temp-dir, as we're
	// testing relative paths.
	tmpDir := t.TempDir()
	setWorkingDirectory(t, tmpDir)

	const expectedUIDGID = 101

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			for _, p := range tc.out {
				_, err := os.Stat(p)
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("expected file not exists for %v, got %v", p, err)
				}
			}

			if err := MkdirAllAndChown(tc.in, 0o755, expectedUIDGID, expectedUIDGID, WithOnlyNew); err != nil {
				t.Fatal(err)
			}

			for _, p := range tc.out {
				s := &unix.Stat_t{}
				if err := unix.Stat(p, s); err != nil {
					t.Errorf("stat %v: %v", p, err)
					continue
				}
				if s.Uid != expectedUIDGID {
					t.Errorf("expected UID: %d, got: %d", expectedUIDGID, s.Uid)
				}
				if s.Gid != expectedUIDGID {
					t.Errorf("expected GID: %d, got: %d", expectedUIDGID, s.Gid)
				}
			}
		})
	}
}

// Change the current working directory for the duration of the test. This may
// break if tests are run in parallel.
func setWorkingDirectory(t *testing.T, dir string) {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(cwd); err != nil {
			t.Error(err)
		}
	})
	if err = os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
}

func TestMkdirAndChown(t *testing.T) {
	requiresRoot(t)
	dirName := t.TempDir()

	testTree := map[string]node{
		"usr": {0, 0},
	}
	if err := buildTree(dirName, testTree); err != nil {
		t.Fatal(err)
	}

	// test a directory that already exists; should just chown to the requested uid/gid
	if err := MkdirAndChown(filepath.Join(dirName, "usr"), 0o755, 99, 99); err != nil {
		t.Fatal(err)
	}
	testTree["usr"] = node{99, 99}
	verifyTree, err := readTree(dirName, "")
	if err != nil {
		t.Fatal(err)
	}
	compareTrees(t, testTree, verifyTree)

	// create a subdir under a dir which doesn't exist--should fail
	if err := MkdirAndChown(filepath.Join(dirName, "usr", "bin", "subdir"), 0o755, 102, 102); err == nil {
		t.Fatal("Trying to create a directory with Mkdir where the parent doesn't exist should have failed")
	}

	// create a subdir under an existing dir; should only change the ownership of the new subdir
	if err := MkdirAndChown(filepath.Join(dirName, "usr", "bin"), 0o755, 102, 102); err != nil {
		t.Fatal(err)
	}
	testTree["usr/bin"] = node{102, 102}
	verifyTree, err = readTree(dirName, "")
	if err != nil {
		t.Fatal(err)
	}
	compareTrees(t, testTree, verifyTree)
}

func buildTree(base string, tree map[string]node) error {
	for path, node := range tree {
		fullPath := filepath.Join(base, path)
		if err := os.MkdirAll(fullPath, 0o755); err != nil {
			return err
		}
		if err := os.Chown(fullPath, node.uid, node.gid); err != nil {
			return err
		}
	}
	return nil
}

func readTree(base, root string) (map[string]node, error) {
	tree := make(map[string]node)

	dirInfos, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}

	for _, info := range dirInfos {
		s := &unix.Stat_t{}
		if err := unix.Stat(filepath.Join(base, info.Name()), s); err != nil {
			return nil, fmt.Errorf("can't stat file %q: %w", filepath.Join(base, info.Name()), err)
		}
		tree[filepath.Join(root, info.Name())] = node{int(s.Uid), int(s.Gid)}
		if info.IsDir() {
			// read the subdirectory
			subtree, err := readTree(filepath.Join(base, info.Name()), filepath.Join(root, info.Name()))
			if err != nil {
				return nil, err
			}
			for path, nodeinfo := range subtree {
				tree[path] = nodeinfo
			}
		}
	}
	return tree, nil
}

func compareTrees(t testing.TB, left, right map[string]node) {
	t.Helper()
	if len(left) != len(right) {
		t.Fatal("trees aren't the same size")
	}
	for path, nodeLeft := range left {
		if nodeRight, ok := right[path]; ok {
			if nodeRight.uid != nodeLeft.uid || nodeRight.gid != nodeLeft.gid {
				// mismatch
				t.Fatalf("mismatched ownership for %q: expected: %d:%d, got: %d:%d", path,
					nodeLeft.uid, nodeLeft.gid, nodeRight.uid, nodeRight.gid)
			}
			continue
		}
		t.Fatalf("right tree didn't contain path %q", path)
	}
}

func TestGetRootUIDGID(t *testing.T) {
	uidMap := []IDMap{
		{
			ID:       0,
			ParentID: int64(os.Getuid()),
			Count:    1,
		},
	}
	gidMap := []IDMap{
		{
			ID:       0,
			ParentID: int64(os.Getgid()),
			Count:    1,
		},
	}

	uid, gid, err := getRootUIDGID(uidMap, gidMap)
	if err != nil {
		t.Fatal(err)
	}
	if uid != os.Getuid() {
		t.Fatalf("expected %d, got %d", os.Getuid(), uid)
	}
	if gid != os.Getgid() {
		t.Fatalf("expected %d, got %d", os.Getgid(), gid)
	}

	uidMapError := []IDMap{
		{
			ID:       1,
			ParentID: int64(os.Getuid()),
			Count:    1,
		},
	}
	_, _, err = getRootUIDGID(uidMapError, gidMap)
	if expected := "container ID 0 cannot be mapped to a host ID"; err.Error() != expected {
		t.Fatalf("expected error: %v, got: %v", expected, err)
	}
}

func TestToContainer(t *testing.T) {
	uidMap := []IDMap{
		{
			ID:       2,
			ParentID: 2,
			Count:    1,
		},
	}

	containerID, err := toContainer(2, uidMap)
	if err != nil {
		t.Fatal(err)
	}
	if uidMap[0].ID != int64(containerID) {
		t.Fatalf("expected %d, got %d", uidMap[0].ID, containerID)
	}
}

// TestMkdirIsNotDir checks that mkdirAs() function (used by MkdirAll...)
// returns a correct error in case a directory which it is about to create
// already exists but is a file (rather than a directory).
func TestMkdirIsNotDir(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), t.Name())
	if err != nil {
		t.Fatalf("Couldn't create temp dir: %v", err)
	}

	err = mkdirAs(file.Name(), 0o755, 0, 0, false, false)
	if expected := "mkdir " + file.Name() + ": not a directory"; err.Error() != expected {
		t.Fatalf("expected error: %v, got: %v", expected, err)
	}
}

func requiresRoot(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("skipping test that requires root")
	}
}
