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
	*skiff.Decoder
	ctx *jobContext
}

func (r *skiffReader) MustScan(value any) {
	if err := r.Scan(value); err != nil {
		r.ctx.onError(err)
	}
}

func (c *jobContext) initPipes(state *jobState, nOutputPipes int) error {
	if state.InputSkiffFormat == nil {
		c.in = newReader(os.Stdin, c)
	} else {
		in, err := skiff.NewDecoder(os.Stdin, *state.InputSkiffFormat)
		if err != nil {
			return err
		}
		c.in = &skiffReader{Decoder: in, ctx: c}
	}

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

		c.out = append(c.out, &writer{
			out: pipe,
			ctx: c,
			writer: yson.NewWriterConfig(pipe, yson.WriterConfig{
				Format: yson.FormatBinary,
				Kind:   yson.StreamListFragment,
			}),
		})
	}

	return nil
}
