package pipelines

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"
)

// File that is read in following mode, similar to `tail -f`
//
// When reader is at the EOF the next call to Read blocks until more
// data is available or file is "bounded".
type FollowingFile struct {
	file *os.File

	filePosition int64

	stop      chan struct{}
	isStopped atomic.Bool

	ticker time.Ticker
}

func OpenFollowingFile(filepath string, offset int64) (ff *FollowingFile, err error) {
	file, err := os.Open(filepath)
	if err != nil {
		return
	}
	if offset < 0 {
		panic(fmt.Sprintf("bad offset %v; offset MUST BE >= 0", offset))
	}
	filePosition, err := file.Seek(offset, 0)
	if err != nil {
		return
	}
	ff = &FollowingFile{
		file:         file,
		stop:         make(chan struct{}, 1),
		ticker:       *time.NewTicker(200 * time.Millisecond),
		filePosition: filePosition,
	}
	return
}

func (f *FollowingFile) Read(buf []byte) (read int, err error) {
	return f.ReadContext(context.Background(), buf)
}

func (f *FollowingFile) FilePosition() int64 {
	return f.filePosition
}

func (f *FollowingFile) ReadContext(ctx context.Context, buf []byte) (read int, err error) {
	for {
		read, err = f.file.Read(buf)
		f.filePosition += int64(read)
		if err != nil && err != io.EOF {
			return
		}
		if read > 0 {
			return
		}
		err = nil
		if f.isStopped.Load() {
			err = io.EOF
			return
		}
		select {
		case <-f.ticker.C:
		case <-f.stop:
		case <-ctx.Done():
			err = ctx.Err()
			return
		}
	}
}

func (f *FollowingFile) Stop() {
	if f.isStopped.CompareAndSwap(false, true) {
		f.stop <- struct{}{}
	}
}

func (f *FollowingFile) Close() error {
	return f.file.Close()
}
