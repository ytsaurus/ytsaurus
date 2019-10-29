package yt

import (
	"bytes"
	"context"
	"errors"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
)

var (
	defaultBatchSize = 512 * 1024 * 1024
)

// WithBatchSize sets batch size (in bytes) for WriteTable.
func WithBatchSize(batchSize int) WriteTableOption {
	return func(w *tableWriter) {
		w.batchSize = batchSize
	}
}

type (
	WriteTableOption func(*tableWriter)

	rawTableWriter interface {
		WriteTableRaw(
			ctx context.Context,
			path ypath.YPath,
			options *WriteTableOptions,
			body *bytes.Buffer,
		) (err error)
	}

	tableWriter struct {
		ctx       context.Context
		yc        Client
		rawWriter rawTableWriter
		path      *ypath.Rich

		batchSize   int
		createTable bool

		encoder *yson.Writer
		buffer  *bytes.Buffer
		err     error
	}
)

func (w *tableWriter) Write(value interface{}) error {
	if w.err != nil {
		return w.err
	}

	if w.createTable {
		_, w.err = CreateTable(w.ctx, w.yc, w.path.Path, WithInferredSchema(value))
		if w.err != nil {
			return w.err
		}

		w.createTable = false
	}

	w.encoder.Any(value)
	if w.err = w.encoder.Err(); w.err != nil {
		return w.err
	}

	if w.buffer.Len() > w.batchSize {
		return w.Flush()
	}

	return nil
}

func (w *tableWriter) Commit() error {
	if w.err != nil {
		return w.err
	}

	if err := w.Flush(); err != nil {
		return err
	}

	w.err = errors.New("yt: writer is closed")
	return nil
}

func (w *tableWriter) Rollback() error {
	if w.err != nil {
		return w.err
	}

	w.err = errors.New("yt: writer is closed")
	return nil
}

func (w *tableWriter) Flush() error {
	if w.err != nil {
		return w.err
	}

	if w.err = w.encoder.Finish(); w.err != nil {
		return w.err
	}

	if w.err = w.rawWriter.WriteTableRaw(w.ctx, w.path, nil, w.buffer); w.err != nil {
		return w.err
	}

	w.path.SetAppend()
	w.initBuffer(true)
	return nil
}

func (w *tableWriter) initBuffer(reuse bool) {
	config := yson.WriterConfig{Kind: yson.StreamListFragment, Format: yson.FormatBinary}
	if reuse {
		w.buffer.Reset()
	} else {
		w.buffer = new(bytes.Buffer)
	}
	w.encoder = yson.NewWriterConfig(w.buffer, config)
}

var _ TableWriter = (*tableWriter)(nil)

// WriteTable creates high level table writer.
//
// WriteTable automatically creates table with schema inferred from the first row.
func WriteTable(ctx context.Context, yc Client, path ypath.Path, opts ...WriteTableOption) (TableWriter, error) {
	w := &tableWriter{
		ctx:         ctx,
		yc:          yc,
		batchSize:   defaultBatchSize,
		createTable: true,
	}

	for _, opt := range opts {
		opt(w)
	}

	var ok bool
	if w.rawWriter, ok = yc.(rawTableWriter); !ok {
		return nil, xerrors.Errorf("yt: client %T is not compatible with yt.WriteTable", yc)
	}

	var err error
	w.path, err = ypath.Parse(string(path))
	if err != nil {
		return nil, err
	}

	w.initBuffer(false)
	return w, nil
}
