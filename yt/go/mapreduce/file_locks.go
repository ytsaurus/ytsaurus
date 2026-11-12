package mapreduce

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/internal/retry"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

const (
	fileLocksTxTimeout   = 2 * time.Minute
	waitLockPollInterval = 5 * time.Second
)

type operationFileLocks struct {
	yc           yt.Client
	parentTx     yt.Tx
	pollInterval time.Duration

	tx     yt.Tx
	cancel context.CancelFunc
}

func (l *operationFileLocks) begin(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.tx != nil {
		return nil
	}

	txCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stop := context.AfterFunc(ctx, cancel)
	timeout := yson.Duration(fileLocksTxTimeout)
	options := &yt.StartTxOptions{Timeout: &timeout}
	var tx yt.Tx
	var err error
	if l.parentTx != nil {
		tx, err = l.parentTx.BeginTx(txCtx, options)
	} else {
		tx, err = l.yc.BeginTx(txCtx, options)
	}
	// Only transaction creation follows the caller's cancellation.
	detached := stop()
	if err != nil {
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction) ||
			(!retry.IsProxyBannedError(err) && !retry.IsRetriableError(err)) {
			return backoff.Permanent(err)
		}
		return err
	}
	l.tx = tx
	l.cancel = cancel
	if !detached {
		l.abort()
		return ctx.Err()
	}
	return nil
}

var errFileTransactionFinished = xerrors.New("mr: file transaction finished before operation start")

func (l *operationFileLocks) finished() <-chan struct{} {
	if l.tx == nil {
		return nil
	}
	return l.tx.Finished()
}

func (l *operationFileLocks) checkFinished() error {
	select {
	case <-l.finished():
		return errFileTransactionFinished
	default:
		return nil
	}
}

func (l *operationFileLocks) contextUntilFinished(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	if finished := l.finished(); finished != nil {
		go func() {
			select {
			case <-finished:
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	return ctx, cancel
}

func (l *operationFileLocks) retry(ctx context.Context, b backoff.BackOff, action func() error) error {
	if b == nil {
		b = backoff.NewExponentialBackOff()
	}
	b.Reset()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := l.checkFinished(); err != nil {
			return err
		}

		err := action()
		if err == nil {
			return l.checkFinished()
		}
		var permanent *backoff.PermanentError
		if xerrors.As(err, &permanent) {
			return permanent.Err
		}
		if finishedErr := l.checkFinished(); finishedErr != nil {
			return finishedErr
		}
		delay := b.NextBackOff()
		if delay == backoff.Stop {
			return err
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		case <-l.finished():
			return errFileTransactionFinished
		}
	}
}

func (l *operationFileLocks) lockPath(ctx context.Context, path ypath.Path) (ypath.Path, error) {
	if err := l.begin(ctx); err != nil {
		return "", err
	}
	if err := l.checkFinished(); err != nil {
		return "", backoff.Permanent(err)
	}

	lockCtx, cancel := l.contextUntilFinished(ctx)
	defer cancel()
	lock, err := l.tx.LockNode(lockCtx, path, yt.LockSnapshot, nil)
	if err != nil {
		if finishedErr := l.checkFinished(); finishedErr != nil {
			return "", backoff.Permanent(finishedErr)
		}
		if yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction) ||
			yterrors.ContainsErrorCode(err, yterrors.CodeAuthorizationError) ||
			(!yterrors.ContainsResolveError(err) && !retry.IsProxyBannedError(err) && !retry.IsRetriableError(err)) {
			return "", backoff.Permanent(err)
		}
		return "", err
	}
	return lock.NodeID.YPath(), nil
}

func (l *operationFileLocks) lock(ctx context.Context, file spec.File) (spec.File, error) {
	lockedPath, err := l.lockPath(ctx, file.CypressPath)
	if err != nil {
		return file, err
	}
	file.OriginalPath = file.CypressPath
	file.CypressPath = lockedPath
	return file, nil
}

func (l *operationFileLocks) abort() {
	if l.tx == nil {
		return
	}

	// Cancel retries before aborting; the pinger can retry the abort in the background.
	if l.cancel != nil {
		l.cancel()
		l.cancel = nil
	}
	_ = l.tx.Abort()
	l.tx = nil
}

func operationNeedsFileLock(state yt.OperationState) bool {
	switch state {
	case yt.StateStarting, yt.StatePending, yt.StateOrphaned,
		yt.StateWaitingForAgent, yt.StateInitializing, yt.StateReviveInitializing:
		return true
	default:
		return false
	}
}

func isRetriableOperationPollError(err error) bool {
	return retry.IsProxyBannedError(err) || retry.IsRetriableError(err)
}

func (l *operationFileLocks) releaseAfterStart(ctx context.Context, operationID yt.OperationID) {
	if l.tx != nil {
		go l.pollOperation(ctx, operationID)
	}
}

func (l *operationFileLocks) pollOperation(ctx context.Context, operationID yt.OperationID) {
	defer l.abort()

	pollInterval := l.pollInterval
	if pollInterval == 0 {
		pollInterval = waitLockPollInterval
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	ctx, cancel := l.contextUntilFinished(context.WithoutCancel(ctx))
	defer cancel()

	for {
		status, err := l.yc.GetOperation(ctx, operationID, &yt.GetOperationOptions{
			Attributes: []string{"state"},
		})
		if err != nil && !isRetriableOperationPollError(err) {
			return
		}
		if err == nil && !operationNeedsFileLock(status.State) {
			return
		}

		select {
		case <-ticker.C:
		case <-l.tx.Finished():
			return
		}
	}
}
