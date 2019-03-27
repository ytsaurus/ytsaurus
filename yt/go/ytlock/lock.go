// package ytlock is high level interface for yt lock.
package ytlock

import (
	"context"
	"time"

	"a.yandex-team.ru/yt/go/yson"

	"a.yandex-team.ru/library/go/core/xerrors"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
)

type WinnerTx struct {
	ID        yt.TxID       `yson:"id"`
	Owner     string        `yson:"owner"`
	StartTime yson.Time     `yson:"start_time"`
	Timeout   yson.Duration `yson:"duration"`
}

type ConflictPolicy int

const (
	Retry ConflictPolicy = iota
	Fail
)

func FindConflictWinner(err error) *WinnerTx {
	if ytErr := yt.FindErrorCode(err, yt.CodeConcurrentTransactionLockConflict); ytErr != nil {
		var winner WinnerTx

		winnerTx, ok := ytErr.Attributes["winner_transaction"]
		if ok {
			rawYson, _ := yson.Marshal(winnerTx)
			_ = yson.Unmarshal(rawYson, &winner)
		}

		return &winner
	}

	return nil
}

const (
	DefaultBackoff = time.Minute
)

type Options struct {
	// LockPath is cypress path used as lock identity.
	LockPath ypath.Path

	// Fail if LockPath is missing.
	FailIfMissing bool

	OnConflict ConflictPolicy

	ConflictBackoff time.Duration
}

func (o *Options) conflictBackoff() time.Duration {
	if o.ConflictBackoff == 0 {
		return DefaultBackoff
	}

	return o.ConflictBackoff
}

func tryLock(ctx context.Context, c yt.Client, options Options, lockCb func(ctx context.Context) error) (finished bool, err error) {
	wrap := func(err error) error {
		return xerrors.Errorf("ytlock: with %q: %w", options.LockPath, err)
	}

	tx, err := c.BeginTx(ctx, nil)
	if err != nil {
		return false, wrap(err)
	}
	defer func() { _ = tx.Abort() }()

	_, err = tx.LockNode(ctx, options.LockPath, yt.LockExclusive, nil)
	if yt.ContainsErrorCode(err, yt.CodeResolveError) && !options.FailIfMissing {
		if _, err = tx.CreateNode(ctx, options.LockPath, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true}); err != nil {
			return false, wrap(err)
		}
	} else if err != nil {
		return false, wrap(err)
	}

	nestedCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	finish := make(chan error, 1)

	go func() {
		finish <- lockCb(nestedCtx)
	}()

	select {
	case <-tx.Finished():
		cancel()
		<-finish
		return true, wrap(xerrors.New("transaction was aborted"))

	case err := <-finish:
		_ = tx.Abort()
		return true, err
	}
}

// With runs lockCb while holding cypress lock.
//
// Unlike sync.Mutex, which is released only by explicit call to Unlock, distributed lock might be lost because of
// network partition or coordination service downtime.
//
// ctx passed to lockCb is canceled when lock is lost.
func With(ctx context.Context, c yt.Client, options Options, lockCb func(ctx context.Context) error) error {
	for {
		finished, err := tryLock(ctx, c, options, lockCb)
		if finished {
			return err
		}

		if err != nil && options.OnConflict == Fail {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-time.After(options.conflictBackoff()):
		}
	}
}
