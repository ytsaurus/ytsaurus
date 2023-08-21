package mapreduce

import (
	"io"

	"go.ytsaurus.tech/yt/go/yson"
)

// Writer is single output of mapreduce job.
type Writer interface {
	// Write writes value to output stream.
	//
	// Returns error, only if current row can't be decoded into value.
	// All other errors will terminate current process immediately.
	Write(value any) error

	// MustWrite works like Write, terminates current process in case of an error.
	MustWrite(value any)
}

type writer struct {
	out    io.WriteCloser
	writer *yson.Writer
	ctx    *jobContext

	err error
}

func (w *writer) Write(value any) error {
	if w.err != nil {
		return w.err
	}

	w.writer.Any(value)
	return w.writer.Err()
}

func (w *writer) MustWrite(value any) {
	err := w.Write(value)
	if err != nil {
		w.ctx.onError(err)
	}
}

func (w *writer) Close() error {
	if w.err != nil {
		return w.err
	}

	w.err = w.writer.Finish()
	return w.err
}
