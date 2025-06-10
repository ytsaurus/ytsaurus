package yt

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/skiff"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
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

// WithCreateOptions disables default behavior of creating table on first Write().
//
// Instead, table is created when WriteTable() is called.
func WithCreateOptions(opts ...CreateTableOption) WriteTableOption {
	return func(w *tableWriter) {
		w.eagerCreate = true
		w.lazyCreate = false
		w.createOptions = opts
	}
}

// WithWriteTableFormat sets YSON-serializable input format. If not specified "yson" will be used.
//
// Possible values:
//   - ​​skiff.Format (see skiff.MustInferFormat).
func WithWriteTableFormat(format any) WriteTableOption {
	return func(w *tableWriter) {
		w.format = format
	}
}

func WithTableWriterConfig(config map[string]any) WriteTableOption {
	return func(w *tableWriter) {
		w.tableWriterConfig = config
	}
}

// WithExistingTable disables automatic table creation.
func WithExistingTable() WriteTableOption {
	return func(w *tableWriter) {
		w.lazyCreate = false
		w.eagerCreate = false
	}
}

// WithAppend adds append attribute to write rows to the end of an existing table.
func WithAppend() WriteTableOption {
	return func(w *tableWriter) {
		w.path.SetAppend()
	}
}

// WithRetries allows to retry flushing several times in case of an error.
func WithRetries(count uint64) WriteTableOption {
	return func(w *tableWriter) {
		w.retryCount = count
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

	encoder interface {
		encode(value any) error
		finish() error
	}

	tableWriter struct {
		ctx       context.Context
		yc        CypressClient
		rawWriter rawTableWriter
		path      *ypath.Rich

		createOptions           []CreateTableOption
		batchSize               int
		retryCount              uint64
		lazyCreate, eagerCreate bool
		format                  any
		tableWriterConfig       map[string]any

		encoder encoder
		buffer  *bytes.Buffer
		err     error
	}
)

func (w *tableWriter) Write(value any) error {
	if w.err != nil {
		return w.err
	}

	if w.lazyCreate {
		_, w.err = CreateTable(
			w.ctx,
			w.yc,
			w.path.Path,
			WithInferredSchema(value))
		if w.err != nil {
			return w.err
		}

		w.lazyCreate = false
	}

	if w.err = w.encoder.encode(value); w.err != nil {
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

	if w.err = w.encoder.finish(); w.err != nil {
		return w.err
	}

	opts := &WriteTableOptions{Format: w.format, TableWriter: w.tableWriterConfig}
	if err := backoff.Retry(func() error {
		if err := w.rawWriter.WriteTableRaw(w.ctx, w.path, opts, w.buffer); err != nil {
			return err
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), w.retryCount)); err != nil {
		w.err = err
		return w.err
	}

	w.path.SetAppend()
	w.err = w.initBuffer(true)
	return w.err
}

func (w *tableWriter) initBuffer(reuse bool) (err error) {
	if reuse {
		w.buffer.Reset()
	} else {
		w.buffer = new(bytes.Buffer)
	}
	w.encoder = newYSONEncoder(w.buffer)
	if w.format != nil {
		if skiffFormat, ok := w.format.(skiff.Format); ok {
			w.encoder, err = newSkiffEncoder(w.buffer, skiffFormat)
		} else {
			err = xerrors.Errorf("unexpected format: %+v", w.format)
		}
	}
	return
}

var _ TableWriter = (*tableWriter)(nil)

type ysonEncoder struct {
	w *yson.Writer
}

func newYSONEncoder(w io.Writer) encoder {
	return &ysonEncoder{w: yson.NewWriterConfig(w, yson.WriterConfig{Kind: yson.StreamListFragment, Format: yson.FormatBinary})}
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

// WriteTable creates high level table writer.
//
// By default, WriteTable overrides existing table, automatically creating table with schema inferred
// from the first row.
func WriteTable(ctx context.Context, yc CypressClient, path ypath.Path, opts ...WriteTableOption) (TableWriter, error) {
	w := &tableWriter{
		ctx:        ctx,
		yc:         yc,
		batchSize:  defaultBatchSize,
		retryCount: 0,
		lazyCreate: true,
	}

	var err error
	w.path, err = ypath.Parse(string(path))
	if err != nil {
		return nil, err
	}

	for _, opt := range opts {
		opt(w)
	}

	if w.eagerCreate {
		if _, err := CreateTable(ctx, yc, path, w.createOptions...); err != nil {
			return nil, err
		}
	}

	var ok bool
	if w.rawWriter, ok = yc.(rawTableWriter); !ok {
		return nil, xerrors.Errorf("yt: client %T is not compatible with yt.WriteTable", yc)
	}

	return w, w.initBuffer(false)
}
