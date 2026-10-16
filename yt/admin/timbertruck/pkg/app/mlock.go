package app

import (
	"bufio"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

const procSelfMaps = "/proc/self/maps"

// Port of MlockFileMappings from library/cpp/yt/mlock/mlock_linux.cpp, which ytserver calls at startup.
func lockFileMappings() (lockedBytes, failedBytes int64, err error) {
	maps, err := os.Open(procSelfMaps)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = maps.Close() }()

	scanner := bufio.NewScanner(maps)
	for scanner.Scan() {
		var start, end, inode uint64
		var perms, offset, device string
		if _, err := fmt.Sscanf(scanner.Text(), "%x-%x %s %s %s %d", &start, &end, &perms, &offset, &device, &inode); err != nil {
			return lockedBytes, failedBytes, fmt.Errorf("cannot parse %q from %v: %w", scanner.Text(), procSelfMaps, err)
		}
		if perms[0] != 'r' || inode == 0 {
			continue
		}
		size := end - start
		if _, _, errno := unix.Syscall(unix.SYS_MLOCK, uintptr(start), uintptr(size), 0); errno != 0 {
			failedBytes += int64(size)
			continue
		}
		lockedBytes += int64(size)
	}
	return lockedBytes, failedBytes, scanner.Err()
}
