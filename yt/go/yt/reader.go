package yt

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"time"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
)

type (
	ReadFileOption func(*fileReader)

	rawTableReadClient interface {
		ReadTableRaw(ctx context.Context, path ypath.YPath, options *ReadTableOptions) (io.ReadCloser, error)
	}

	fileReader struct {
		ctx        context.Context
		tx         Tx
		snapshotID NodeID
		retryCount uint64

		offset int64
		r      io.ReadCloser
		err    error
	}

	retryingTableReader struct {
		ctx         context.Context
		tx          Tx
		yc          rawTableReadClient
		path        ypath.Rich
		resumeRange ypath.Range
		options     ReadTableOptions
		resumable   bool
		retryCount  uint64

		nextRowIndex *int64
		rowTail      int
		returned     bool
		skip         int
		r            io.ReadCloser
		failure      error
		err          error
	}
)

// WithReadFileRetries allows to retry reading several times in case of an error.
func WithReadFileRetries(count uint64) ReadFileOption {
	return func(r *fileReader) {
		r.retryCount = count
	}
}

func (r *fileReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}

	var retries uint64
	for {
		if r.r == nil {
			rr, err := r.tx.ReadFile(r.ctx, r.snapshotID.YPath(), &ReadFileOptions{Offset: &r.offset})
			if err != nil {
				if !r.backoff(err, &retries) {
					r.err = err
					return 0, err
				}
				continue
			}
			r.r = rr
		}

		n, err := r.r.Read(p)
		if n > 0 {
			r.offset += int64(n)
			return n, nil
		}
		if err == nil {
			continue
		}
		if err == io.EOF {
			r.err = io.EOF
			return 0, io.EOF
		}

		_ = r.r.Close()
		r.r = nil
		if !r.backoff(err, &retries) {
			r.err = err
			return 0, err
		}
	}
}

func (r *fileReader) backoff(err error, retries *uint64) bool {
	d := tryGetBackoffDuration(err)
	if d == nil || *retries >= r.retryCount {
		return false
	}
	*retries++
	select {
	case <-time.After(*d):
		return true
	case <-r.ctx.Done():
		return false
	}
}

func (r *fileReader) Close() error {
	if r.r != nil {
		_ = r.r.Close()
		r.r = nil
	}
	if r.tx != nil {
		err := r.tx.Abort()
		r.tx = nil
		return err
	}
	return nil
}

// ReadFile creates high level file reader.
//
// ReadFile reads the file under a snapshot lock, retrying and resuming from the
// last read offset on transient errors.
func ReadFile(ctx context.Context, yc CypressClient, path ypath.Path, opts ...ReadFileOption) (io.ReadCloser, error) {
	ycTx, ok := yc.(cypressWithBeginTx)
	if !ok {
		return nil, xerrors.Errorf("yt: client %T does not support transactions", yc)
	}

	r := &fileReader{ctx: ctx, retryCount: 10}
	for _, opt := range opts {
		opt(r)
	}

	tx, err := ycTx.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("yt: failed to start read transaction: %w", err)
	}
	r.tx = tx

	lock, err := tx.LockNode(ctx, path, LockSnapshot, nil)
	if err != nil {
		_ = tx.Abort()
		return nil, xerrors.Errorf("yt: failed to lock file: %w", err)
	}
	r.snapshotID = lock.NodeID

	return r, nil
}

func (r *retryingTableReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}

	var retries uint64
	for {
		if r.failure != nil {
			err := r.failure
			r.failure = nil
			if !r.backoff(err, &retries) {
				r.err = err
				return 0, err
			}
		}

		if r.r == nil {
			if err := r.open(); err != nil {
				r.failure = err
				continue
			}
		}

		n, err := r.r.Read(p)
		n = r.consume(p, n)
		if n > 0 && (err == nil || err == io.EOF) {
			return n, nil
		}
		if err == nil {
			continue
		}
		if err == io.EOF {
			if !r.resumable || r.rowTail == 0 {
				r.err = io.EOF
				return 0, io.EOF
			}
			err = xerrors.Errorf("yt: table read ended in the middle of a row: %w", io.ErrUnexpectedEOF)
		}

		_ = r.r.Close()
		r.r = nil
		r.failure = err
		if n > 0 {
			return n, nil
		}
	}
}

func (r *retryingTableReader) open() error {
	path := r.path
	if r.returned {
		rng := r.resumeRange
		rng.Lower = &ypath.ReadLimit{RowIndex: ptr.T(*r.nextRowIndex)}
		path.Ranges = []ypath.Range{rng}
		r.skip = r.rowTail
	}

	rr, err := r.yc.ReadTableRaw(r.ctx, &path, &r.options)
	if err != nil {
		return err
	}
	r.r = rr

	if r.resumable && r.nextRowIndex == nil {
		r.nextRowIndex = startRowIndex(rr, r.path.Ranges)
		r.resumable = r.nextRowIndex != nil
	}
	return nil
}

func (r *retryingTableReader) consume(p []byte, n int) int {
	if r.skip > 0 {
		k := min(n, r.skip)
		n = copy(p, p[k:n])
		r.skip -= k
	}
	if n == 0 {
		return 0
	}
	r.returned = true
	if !r.resumable {
		return n
	}

	data := p[:n]
	if i := bytes.LastIndexByte(data, '\n'); i >= 0 {
		*r.nextRowIndex += int64(bytes.Count(data[:i+1], []byte{'\n'}))
		r.rowTail = n - i - 1
	} else {
		r.rowTail += n
	}
	return n
}

func (r *retryingTableReader) backoff(err error, retries *uint64) bool {
	if r.returned && !r.resumable {
		return false
	}
	d := tryGetBackoffDuration(err)
	if d == nil && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
		d = ptr.T(defaultRetryBackoff)
	}
	if d == nil || *retries >= r.retryCount {
		return false
	}
	*retries++
	select {
	case <-time.After(*d):
		return true
	case <-r.ctx.Done():
		return false
	}
}

func (r *retryingTableReader) Close() error {
	if r.r != nil {
		_ = r.r.Close()
		r.r = nil
	}
	if r.tx != nil {
		err := r.tx.Abort()
		r.tx = nil
		return err
	}
	return nil
}

func startRowIndex(r io.Reader, ranges []ypath.Range) *int64 {
	if rr, ok := r.(interface{ StartRowIndex() (int64, bool) }); ok {
		if index, ok := rr.StartRowIndex(); ok {
			return &index
		}
	}
	if len(ranges) == 0 {
		return ptr.T(int64(0))
	}

	limit := ranges[0].Lower
	if ranges[0].Exact != nil {
		limit = ranges[0].Exact
	}
	if limit == nil {
		return ptr.T(int64(0))
	}
	if limit.RowIndex != nil {
		return ptr.T(*limit.RowIndex)
	}
	return nil
}

func resumeRange(ranges []ypath.Range) ypath.Range {
	if len(ranges) == 0 {
		return ypath.Range{}
	}

	rng := ranges[0]
	switch {
	case rng.Exact == nil:
		return rng
	case rng.Exact.RowIndex != nil:
		return ypath.Range{Upper: &ypath.ReadLimit{RowIndex: ptr.T(*rng.Exact.RowIndex + 1)}}
	case rng.Exact.Key != nil:
		maxKey := yson.ValueWithAttrs{Attrs: map[string]any{"type": "max"}}
		return ypath.Range{Upper: &ypath.ReadLimit{Key: append(slices.Clone(rng.Exact.Key), maxKey)}}
	}
	return rng
}

func isRowPerLineFormat(format any) bool {
	data, err := yson.Marshal(format)
	if err != nil {
		return false
	}
	var f yson.ValueWithAttrs
	if err := yson.Unmarshal(data, &f); err != nil {
		return false
	}

	switch f.Value {
	case "json":
		return slices.Contains([]any{nil, "text"}, f.Attrs["format"])
	case "dsv", "schemaful_dsv":
		return slices.Contains([]any{nil, true}, f.Attrs["enable_escaping"]) &&
			slices.Contains([]any{nil, "\n"}, f.Attrs["record_separator"]) &&
			slices.Contains([]any{nil, false}, f.Attrs["enable_column_names_header"]) &&
			slices.Contains([]any{nil, "fail", "print_sentinel"}, f.Attrs["missing_value_mode"])
	}
	return false
}

func tableReaderKeepsAllRows(config any) bool {
	if config == nil {
		return true
	}
	data, err := yson.Marshal(config)
	if err != nil {
		return false
	}
	var c map[string]any
	if err := yson.Unmarshal(data, &c); err != nil {
		return false
	}
	return c["sampling_rate"] == nil && slices.Contains([]any{nil, "restore", "throw_error"}, c["unavailable_chunk_strategy"])
}

// ReadTableRaw reads the table under a snapshot lock and returns its rows encoded in options.Format.
//
// ReadTableRaw retries transient errors. Once it has returned data, it retries only if it can resume exactly at the
// first row it has not returned yet, as for one ordered range in json; otherwise it returns the error.
func ReadTableRaw(
	ctx context.Context,
	yc CypressClient,
	path ypath.YPath,
	options *ReadTableOptions,
) (io.ReadCloser, error) {
	ycTx, ok := yc.(cypressWithBeginTx)
	if !ok {
		return nil, xerrors.Errorf("yt: client %T does not support transactions", yc)
	}

	var rich ypath.Rich
	switch p := path.(type) {
	case ypath.Path:
		parsed, err := ypath.Parse(p.String())
		if err != nil {
			return nil, err
		}
		rich = *parsed
	case *ypath.Rich:
		rich = *p
	default:
		return nil, xerrors.Errorf("yt: unsupported path type %T", path)
	}

	r := &retryingTableReader{ctx: ctx, retryCount: 10}
	if options != nil {
		r.options = *options
	}

	var parentTx *TransactionOptions
	if r.options.TransactionOptions != nil {
		parentTx = ptr.T(*r.options.TransactionOptions)
	}
	tx, err := ycTx.BeginTx(ctx, &StartTxOptions{TransactionOptions: parentTx})
	if err != nil {
		return nil, xerrors.Errorf("yt: failed to start read transaction: %w", err)
	}
	r.tx = tx
	r.options.TransactionOptions = nil

	if r.yc, ok = tx.(rawTableReadClient); !ok {
		_ = tx.Abort()
		return nil, xerrors.Errorf("yt: client %T does not support raw table reads", yc)
	}

	lock, err := tx.LockNode(ctx, rich.Path, LockSnapshot, nil)
	if err != nil {
		_ = tx.Abort()
		return nil, xerrors.Errorf("yt: failed to lock table: %w", err)
	}
	rich.Path = lock.NodeID.YPath()
	r.path = rich

	var dynamic bool
	if err = tx.GetNode(ctx, rich.Path.Attr("dynamic"), &dynamic, nil); err != nil {
		_ = tx.Abort()
		return nil, xerrors.Errorf("yt: failed to get table type: %w", err)
	}

	r.resumable = !dynamic && len(rich.Ranges) <= 1 && !r.options.Unordered && r.options.ControlAttributes == nil &&
		tableReaderKeepsAllRows(r.options.TableReader) && isRowPerLineFormat(r.options.Format)
	r.resumeRange = resumeRange(rich.Ranges)
	return r, nil
}
