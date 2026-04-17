package yt

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"time"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/skiff"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yterrors"
)

var errYtWriterClosed = errors.New("yt: writer is closed")

const (
	defaultRetryBackoff    = 3 * time.Second
	chunkErrorRetryBackoff = 60 * time.Second
	rateLimitRetryBackoff  = 60 * time.Second
	defaultBatchSize       = 512 * 1024 * 1024
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

// WithCreateTransaction specifies whether to create an internal client transaction for writing table.
//
// This is an advanced option.
// If create is set to false, writer doesn't create internal transaction and doesn't lock table.
//
// WARNING: if `WithCreateTransaction` is `false`, read/write might become non-atomic.
// Change ONLY if you are sure what you are doing!
func WithCreateTransaction(create bool) WriteTableOption {
	return func(w *tableWriter) {
		w.createTransaction = create
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

	cypressWithBeginTx interface {
		CypressClient
		BeginTx(ctx context.Context, options *StartTxOptions) (Tx, error)
	}

	tableWriter struct {
		ctx  context.Context
		yc   cypressWithBeginTx // Holds either root client or active transaction.
		tx   Tx                 // Holds the transaction if such option true.
		path *ypath.Rich

		createOptions           []CreateTableOption
		batchSize               int
		retryCount              uint64
		lazyCreate, eagerCreate bool
		format                  any
		tableWriterConfig       map[string]any
		createTransaction       bool

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
		w.createOptions = append(w.createOptions, WithInferredSchema(value))
		w.err = w.createTable()
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

func (w *tableWriter) Commit() (commitErr error) {
	defer func() {
		if commitErr != nil {
			_ = w.Rollback()
		}
	}()

	if w.err != nil {
		return w.err
	}

	if err := w.Flush(); err != nil {
		return err
	}

	if w.tx != nil {
		if err := w.tx.Commit(); err != nil {
			w.err = xerrors.Errorf("yt: failed to commit write transaction: %w", err)
			return w.err
		}
		w.tx = nil
	}

	w.err = errYtWriterClosed
	return nil
}

func (w *tableWriter) Rollback() error {
	if w.tx != nil {
		_ = w.tx.Abort()
		w.tx = nil
	}

	if w.err != nil {
		return w.err
	}

	w.err = errYtWriterClosed
	return nil
}

func (w *tableWriter) Flush() error {
	if w.err != nil {
		return w.err
	}

	if w.err = w.encoder.finish(); w.err != nil {
		return w.err
	}

	if w.buffer.Len() == 0 {
		return nil
	}

	w.err = w.sendBatchWithRetries()
	if w.err != nil {
		return w.err
	}

	w.path.SetAppend()
	w.err = w.initBuffer(true)
	return w.err
}

// Take lock: shared for append, exclusive for overwrite.
func (w *tableWriter) lockTable() error {
	lockMode := LockExclusive
	if w.path.Append != nil && *w.path.Append {
		lockMode = LockShared
	}

	if _, err := w.tx.LockNode(w.ctx, ypath.Path(w.path.Path), lockMode, nil); err != nil {
		return xerrors.Errorf("yt: failed to lock table: %w", err)
	}
	return nil
}

func (w *tableWriter) sendBatchWithRetries() error {
	var lastErr error
	var retries uint64

	for {
		attemptTx, err := w.yc.BeginTx(w.ctx, nil)
		if err != nil {
			return xerrors.Errorf("yt: failed to start attempt transaction: %w", err)
		}

		attemptWriter := attemptTx.(rawTableWriter)

		opts := &WriteTableOptions{Format: w.format, TableWriter: w.tableWriterConfig}
		err = attemptWriter.WriteTableRaw(w.ctx, w.path, opts, w.buffer)
		if err == nil {
			if commitErr := attemptTx.Commit(); commitErr != nil {
				return xerrors.Errorf("yt: failed to commit attempt transaction: %w", commitErr)
			}
			return nil
		}

		_ = attemptTx.Abort()

		lastErr = err

		backoff := tryGetBackoffDuration(err)
		if backoff == nil {
			return err
		}

		if retries >= w.retryCount {
			break
		}
		retries++

		select {
		case <-time.After(*backoff):
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	}

	return lastErr
}

func (w *tableWriter) createTable() error {
	if _, err := CreateTable(w.ctx, w.yc, w.path.Path, w.createOptions...); err != nil {
		return xerrors.Errorf("yt: failed to create table: %w", err)
	}
	if w.tx != nil {
		if err := w.lockTable(); err != nil {
			return err
		}
	}

	return nil
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

// Error is retriable chunk error only if it doesn't contains non-retriable chunk errors
// and contains at least one chunk error in error tree.
func isRetriableChunkError(errorCodes map[yterrors.ErrorCode]struct{}) bool {
	for _, code := range []yterrors.ErrorCode{
		yterrors.CodeSessionAlreadyExists,
		yterrors.CodeChunkAlreadyExists,
		yterrors.CodeWindowError,
		yterrors.CodeBlockContentMismatch,
		yterrors.CodeInvalidBlockChecksum,
		yterrors.CodeMalformedReadRequest,
		yterrors.CodeMissingExtension,
		yterrors.CodeNoSuchBlock,
		yterrors.CodeNoSuchChunk,
		yterrors.CodeNoSuchChunkList,
		yterrors.CodeNoSuchChunkTree,
		yterrors.CodeNoSuchChunkView,
		yterrors.CodeNoSuchMedium,
	} {
		if _, ok := errorCodes[code]; ok {
			return false
		}
	}

	for code := range errorCodes {
		if code/100 == 7 {
			return true
		}
	}

	return false
}

func tryGetBackoffDuration(err error) *time.Duration {
	errorCodes := yterrors.GetErrorCodes(err)

	if _, ok := errorCodes[yterrors.CodeRequestQueueSizeLimitExceeded]; ok {
		return ptr.T(rateLimitRetryBackoff)
	}

	if _, ok := errorCodes[yterrors.CodeTooManyOperations]; ok {
		return ptr.T(rateLimitRetryBackoff)
	}

	if isRetriableChunkError(errorCodes) {
		return ptr.T(chunkErrorRetryBackoff)
	}

	for _, code := range []yterrors.ErrorCode{
		yterrors.CodeTransportError,
		yterrors.CodeUnavailable,
		yterrors.CodeRetriableArchiveError,
		yterrors.CodeTransientFailure,
		yterrors.CodeCanceled,
		yterrors.CodeTimeout,
	} {
		if _, ok := errorCodes[code]; ok {
			return ptr.T(defaultRetryBackoff)
		}
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		var lookupErr *net.DNSError
		if errors.As(err, &lookupErr) && lookupErr.IsNotFound {
			return nil
		}

		if tcp, ok := opErr.Addr.(*net.TCPAddr); ok && tcp.IP.IsLoopback() {
			return nil
		}

		return ptr.T(defaultRetryBackoff)
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return ptr.T(defaultRetryBackoff)
	}

	return nil
}

// WriteTable creates high level table writer.
//
// By default, WriteTable overrides existing table, automatically creating table with schema inferred
// from the first row.
func WriteTable(ctx context.Context, yc CypressClient, path ypath.Path, opts ...WriteTableOption) (TableWriter, error) {
	w := &tableWriter{
		ctx:               ctx,
		batchSize:         defaultBatchSize,
		retryCount:        10,
		createTransaction: true,
		lazyCreate:        true,
	}

	var ok bool
	// Default: yc is the client.
	if w.yc, ok = yc.(cypressWithBeginTx); !ok {
		return nil, xerrors.Errorf("yt: client %T does not support transactions", yc)
	}
	if _, ok = w.yc.(rawTableWriter); !ok {
		return nil, xerrors.Errorf("yt: client %T is not compatible with yt.WriteTable", yc)
	}

	var err error
	w.path, err = ypath.Parse(string(path))
	if err != nil {
		return nil, err
	}

	for _, opt := range opts {
		opt(w)
	}

	err = w.initBuffer(false)
	if err != nil {
		w.err = err
		return nil, err
	}

	if w.createTransaction {
		tx, err := w.yc.BeginTx(w.ctx, nil)
		if err != nil {
			w.err = xerrors.Errorf("yt: failed to start write transaction: %w", err)
			return nil, w.Rollback()
		}
		// Switch default yc to new transaction.
		// Also we store yc in w.tx (as the Tx interface) to facilitate explicit
		// transaction control (Commit/Abort) without repeated type assertions.
		w.yc = tx
		w.tx = tx
	}

	if w.eagerCreate {
		err = w.createTable()
		if err != nil {
			w.err = err
			return nil, w.Rollback()
		}
	}

	if !w.eagerCreate && !w.lazyCreate && w.tx != nil {
		if err := w.lockTable(); err != nil {
			w.err = err
			return nil, w.Rollback()
		}
	}

	return w, nil
}
