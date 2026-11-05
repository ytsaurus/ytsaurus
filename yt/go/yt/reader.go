package yt

import (
	"context"
	"io"
	"time"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/ypath"
)

type (
	ReadFileOption func(*fileReader)

	fileReader struct {
		ctx        context.Context
		tx         Tx
		snapshotID NodeID
		retryCount uint64

		offset int64
		r      io.ReadCloser
		err    error
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
