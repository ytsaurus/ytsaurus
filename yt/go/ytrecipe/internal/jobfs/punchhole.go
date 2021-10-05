//go:build linux
// +build linux

package jobfs

import (
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

type punchholeReader struct {
	f   *os.File
	pos int
}

func (r *punchholeReader) Read(p []byte) (n int, err error) {
	n, err = r.f.Read(p)
	if err != nil {
		return
	}

	err = unix.Fallocate(int(r.f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, int64(r.pos), int64(n))
	if err != nil {
		return
	}

	runtime.KeepAlive(r.f)
	r.pos += n

	return
}
