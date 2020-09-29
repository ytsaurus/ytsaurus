//+build !linux

package jobfs

import "os"

type punchholeReader struct {
	f *os.File
}

func (r *punchholeReader) Read(p []byte) (n int, err error) {
	n, err = r.f.Read(p)
	return
}
