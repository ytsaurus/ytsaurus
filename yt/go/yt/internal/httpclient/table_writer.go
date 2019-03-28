package httpclient

import (
	"io"

	"a.yandex-team.ru/yt/go/yson"
)

type tableWriter struct {
	raw     io.WriteCloser
	encoder *yson.Writer
	err     error
}

func newTableWriter(w io.WriteCloser) *tableWriter {
	return &tableWriter{
		raw:     w,
		encoder: yson.NewWriterConfig(w, yson.WriterConfig{Format: yson.FormatBinary, Kind: yson.StreamListFragment}),
	}
}

func (w *tableWriter) Write(value interface{}) error {
	if w.err != nil {
		return w.err
	}

	w.encoder.Any(value)
	w.err = w.encoder.Err()
	return w.err
}

func (w *tableWriter) Commit() error {
	if w.err != nil {
		return w.err
	}

	if w.err = w.encoder.Finish(); w.err != nil {
		return w.err
	}

	w.err = w.raw.Close()
	return w.err
}
