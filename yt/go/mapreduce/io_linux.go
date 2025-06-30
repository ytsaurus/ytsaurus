//go:build linux
// +build linux

package mapreduce

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/skiff"
	"go.ytsaurus.tech/yt/go/yson"
)

type skiffReader struct {
	d   *skiff.Decoder
	ctx *jobContext
}

func (r *skiffReader) TableIndex() int {
	return r.d.TableIndex()
}

func (r *skiffReader) KeySwitch() bool {
	return r.d.KeySwitch()
}

func (r *skiffReader) RowIndex() int64 {
	return r.d.RowIndex()
}

func (r *skiffReader) RangeIndex() int {
	return r.d.RangeIndex()
}

func (r *skiffReader) Scan(value any) error {
	return r.d.Scan(value)
}

func (r *skiffReader) MustScan(value any) {
	if err := r.d.Scan(value); err != nil {
		r.ctx.onError(err)
	}
}

func (r *skiffReader) Next() bool {
	return r.d.Next()
}

func (r *skiffReader) Err() error {
	return r.d.Err()
}

func (c *jobContext) initPipes(nOutputPipes int) error {
	c.in = os.Stdin

	// Hide stdin from user code, just in case.
	os.Stdin = nil

	for i := 0; i < nOutputPipes; i++ {
		var pipe *os.File

		fd := uintptr(3*i + 1)
		_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFD, 0)
		if errno != 0 {
			return xerrors.Errorf("output pipe #%d is missing: %w", i, errno)
		}

		if i == 0 {
			pipe = os.Stdout

			// Hide stdout from user code to avoid format errors caused by fmt.Println calls.
			os.Stdout = nil
		} else {
			pipe = os.NewFile(fd, fmt.Sprintf("yt-output-pipe-%d", i))
		}

		c.out = append(c.out, pipe)
	}

	return nil
}

func (c *jobContext) createReader(state *jobState) (Reader, error) {
	if state.InputSkiffFormat == nil {
		return newReader(c.in, c), nil
	}
	in, err := skiff.NewDecoder(c.in, *state.InputSkiffFormat)
	if err != nil {
		return nil, err
	}
	return &skiffReader{d: in, ctx: c}, nil
}

func (c *jobContext) createWriters() []Writer {
	writers := make([]Writer, len(c.out))
	for i, file := range c.out {
		writers[i] = &writer{
			out: file,
			ctx: c,
			writer: yson.NewWriterConfig(file, yson.WriterConfig{
				Format: yson.FormatBinary,
				Kind:   yson.StreamListFragment,
			}),
		}
	}
	return writers
}
