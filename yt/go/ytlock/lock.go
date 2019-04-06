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
	Fail ConflictPolicy = iota
	Retry
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
	// Fail if LockPath is missing.
	FailIfMissing   bool
	OnConflict      ConflictPolicy
	ConflictBackoff time.Duration
	LockType        yt.LockMode
}

func (o *Options) conflictBackoff() time.Duration {
	if o.ConflictBackoff == 0 {
		return DefaultBackoff
	}

	return o.ConflictBackoff
}

// Lock object represents cypress lock
type Lock struct {
	Path    ypath.Path
	Options Options
	Yc      yt.Client

	// Implementation details
	initialCtx context.Context
	ctx        context.Context
	ctxCancel  context.CancelFunc
}

// NewLock creates Lock object using default Options
func NewLock(ctx context.Context, yc yt.Client, path ypath.Path) (l Lock) {
	// TODO(@iagorsky): pick sane default options
	defaultOptions := Options{
		FailIfMissing:   true,
		OnConflict:      Fail,
		ConflictBackoff: DefaultBackoff,
		LockType:        yt.LockExclusive,
	}
	return NewLockWithOptions(ctx, yc, path, defaultOptions)
}

// NewLockWithOptions creates Lock object with specified options
func NewLockWithOptions(ctx context.Context, yc yt.Client, path ypath.Path, opt Options) (l Lock) {
	l.initialCtx = ctx
	l.Yc = yc
	l.Path = path
	l.Options = opt
	return
}

// wrapError wraps error in a way that is relevant to given lock
func (l Lock) wrapError(err error) error {
	if err != nil {
		return xerrors.Errorf("ytlock: with %q: %w", l.Path, err)
	}

	return nil
}

// setup creates master transaction and tries to acquire lock, i.e. create cypress LockNode.
// It also handles transaction abortion, in concurrent fasion.
func (l Lock) setup() error {
	tx, err := l.Yc.BeginTx(l.ctx, nil)
	if err != nil {
		return l.wrapError(err)
	}
	go func() {
		<-tx.Finished()
		// Transaction finised, i.e. lock got released on server side
		l.ctxCancel()
	}()

	_, err = tx.LockNode(l.ctx, l.Path, yt.LockExclusive, nil)
	if yt.ContainsErrorCode(err, yt.CodeResolveError) && !l.Options.FailIfMissing {
		_, err = tx.CreateNode(l.ctx, l.Path, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	}
	err = l.wrapError(err)
	return err
}

// Acquire acquires cypress lock and returns lock context.
//
// Lock context "doneness" should be checked frequently, due to the fact that it could get closed
// without explicit lock release, which means that distributed lock is lost. This could happen
// is case of transaction being aborted remotely, network partition or coordination service downtime.
//
// If lock context is done, there is no need to call Release explicitly.
func (l *Lock) Acquire() (ctx context.Context, err error) {
	for {
		l.ctx, l.ctxCancel = context.WithCancel(l.initialCtx)
		ctx = l.ctx
		err = l.setup()

		// FIX(@iagorsky): if node creating fails with OnConflict == Retry and !Options.FailIfMissing -> infinite loop
		if err == nil || l.Options.OnConflict == Fail {
			return
		}

		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-time.After(l.Options.conflictBackoff()):
		}
	}
}

// Release closes lock context and releases distributed lock by aborting transaction
func (l Lock) Release() {
	l.ctxCancel()
	<-l.ctx.Done()
}
