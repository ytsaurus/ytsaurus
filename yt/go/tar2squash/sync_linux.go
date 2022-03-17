//go:build linux

package main

import (
	"os"

	"golang.org/x/sys/unix"
)

const (
	filePrefixSize = 4096
)

func weakSync(f *os.File) error {
	syncFlags := unix.SYNC_FILE_RANGE_WAIT_BEFORE | unix.SYNC_FILE_RANGE_WRITE | unix.SYNC_FILE_RANGE_WAIT_AFTER
	return unix.SyncFileRange(int(f.Fd()), 0, filePrefixSize, syncFlags)
}
