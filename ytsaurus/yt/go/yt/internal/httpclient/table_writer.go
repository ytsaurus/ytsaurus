package httpclient

import (
	"io"

	"go.ytsaurus.tech/yt/go/yson"
)

type tableWriter struct {
	raw        io.WriteCloser
	encoder    *yson.Writer
	cancelFunc func()
	err        error
}

func newTableWriter(w io.WriteCloser, cancelFunc func()) *tableWriter {
	return &tableWriter{
		raw:        w,
		encoder:    yson.NewWriterConfig(w, yson.WriterConfig{Format: yson.FormatBinary, Kind: yson.StreamListFragment}),
		cancelFunc: cancelFunc,
	}
}

func (w *tableWriter) Write(value any) error {
	if w.err != nil {
		return w.err
	}

	w.encoder.Any(value)
	w.err = w.encoder.Err()
	return w.err
}

func (w *tableWriter) Rollback() error {
	// TODO(prime@): this cancellation is asynchronous, violating contract of the Rollback() method.
	//
	// But there is no way to provide synchronous cancellation over HTTP protocol, so we have no other choice.
	//
	// Synchronous Rollback() should be implemented by the high level table writer.

	if w.cancelFunc != nil {
		w.cancelFunc()
	}
	return nil
}

func (w *tableWriter) Commit() error {
	if w.err != nil {
		return w.err
	}

	if w.err = w.encoder.Finish(); w.err != nil {
		return w.err
	}

	w.err = w.raw.Close()
	if w.cancelFunc != nil {
		w.cancelFunc()
	}
	return w.err
}
