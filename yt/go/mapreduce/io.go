package mapreduce

import (
	"fmt"
	"io"
	"os"
	"syscall"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/yson"
)

type reader struct {
	in     io.Reader
	reader *yson.Reader
}

func (r *reader) TableIndex() int {
	panic("implement me")
}

func (r *reader) Scan(value interface{}) error {
	panic("implement me")
}

func (r *reader) Next() bool {
	return false
}

type writer struct {
	out    io.WriteCloser
	writer *yson.Writer
}

func (w *writer) Write(value interface{}) error {
	panic("implement me")
}

type jobContext struct {
	in  *reader
	out []*writer
}

func (c *jobContext) initPipes(nOutputPipes int) error {
	c.in = &reader{
		in:     os.Stdin,
		reader: yson.NewReaderKind(os.Stdin, yson.StreamListFragment),
	}

	// Hide stdin from user code, just in case.
	os.Stdin = nil

	for i := 0; i < nOutputPipes; i++ {
		var pipe *os.File

		fd := uintptr(3*i + 1)
		_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFD, 0)
		if errno != 0 {
			return xerrors.Errorf("output pipe #%d is missing: %w", errno)
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
			writer: yson.NewWriterConfig(pipe, yson.WriterConfig{
				Format: yson.FormatBinary,
				Kind:   yson.StreamListFragment,
			}),
		})
	}

	return nil
}

func (c *jobContext) writers() (out []Writer) {
	for _, w := range c.out {
		out = append(out, w)
	}

	return
}

type JobContext interface {
}
