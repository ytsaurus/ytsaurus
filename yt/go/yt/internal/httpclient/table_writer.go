package httpclient

import (
	"io"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/skiff"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

type tableWriter struct {
	raw        io.WriteCloser
	cancelFunc func()
	encoder    encoder
	err        error
}

type encoder interface {
	encode(value any) error
	finish() error
}

func newTableWriter(w io.WriteCloser, format any, cancelFunc func()) (tw yt.TableWriter, err error) {
	encoder := newYSONEncoder(w)
	if format != nil {
		if skiffFormat, ok := format.(skiff.Format); ok {
			encoder, err = newSkiffEncoder(w, skiffFormat)
			if err != nil {
				return
			}
		} else {
			err = xerrors.Errorf("unexpected format: %+v", format)
			return
		}
	}
	return &tableWriter{raw: w, encoder: encoder, cancelFunc: cancelFunc}, nil
}

func (w *tableWriter) Write(value any) error {
	if w.err != nil {
		return w.err
	}

	w.err = w.encoder.encode(value)
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

	if w.err = w.encoder.finish(); w.err != nil {
		return w.err
	}

	w.err = w.raw.Close()
	if w.cancelFunc != nil {
		w.cancelFunc()
	}
	return w.err
}

type ysonEncoder struct {
	w *yson.Writer
}

func newYSONEncoder(w io.Writer) encoder {
	return &ysonEncoder{w: yson.NewWriterConfig(w, yson.WriterConfig{Format: yson.FormatBinary, Kind: yson.StreamListFragment})}
}

func (e *ysonEncoder) encode(value any) error {
	e.w.Any(value)
	return e.w.Err()
}

func (e *ysonEncoder) finish() error {
	return e.w.Finish()
}

type skiffEncoder struct {
	encoder *skiff.Encoder
}

func newSkiffEncoder(w io.Writer, skiffFormat skiff.Format) (encoder, error) {
	schema, err := skiff.SingleSchema(&skiffFormat)
	if err != nil {
		return nil, err
	}
	encoder, err := skiff.NewEncoder(w, *schema)
	if err != nil {
		return nil, xerrors.Errorf("failed to create skiff encoder: %w", err)
	}
	return &skiffEncoder{encoder: encoder}, nil
}

func (e *skiffEncoder) encode(value any) error {
	return e.encoder.Write(value)
}

func (e *skiffEncoder) finish() error {
	return e.encoder.Flush()
}
