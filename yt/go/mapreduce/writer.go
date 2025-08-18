package mapreduce

import (
	"io"

	"go.ytsaurus.tech/yt/go/skiff"
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

	// Close closes writer.
	Close() error
}

type encoder interface {
	encode(value any) error
	finish() error
}

type ysonEncoder struct {
	writer *yson.Writer
}

func (e *ysonEncoder) encode(value any) error {
	e.writer.Any(value)
	return e.writer.Err()
}

func (e *ysonEncoder) finish() error {
	return e.writer.Finish()
}

type skiffEncoder struct {
	encoder *skiff.Encoder
}

func (e *skiffEncoder) encode(value any) error {
	return e.encoder.Write(value)
}

func (e *skiffEncoder) finish() error {
	return e.encoder.Flush()
}

type writer struct {
	encoder encoder
	out     io.WriteCloser
	ctx     *jobContext
	err     error
}

func newWriter(enc encoder, ctx *jobContext, out io.WriteCloser) Writer {
	return &writer{
		encoder: enc,
		out:     out,
		ctx:     ctx,
	}
}

func (w *writer) Write(value any) error {
	if w.err != nil {
		return w.err
	}

	w.err = w.encoder.encode(value)
	return w.err
}

func (w *writer) MustWrite(value any) {
	if err := w.Write(value); err != nil {
		w.ctx.onError(err)
	}
}

func (w *writer) Close() error {
	if w.err != nil {
		return w.err
	}

	if err := w.encoder.finish(); err != nil {
		w.err = err
		return w.err
	}

	w.err = w.out.Close()
	return w.err
}
