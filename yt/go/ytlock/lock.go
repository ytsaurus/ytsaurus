// Package ytlock is high level interface for yt lock.
package ytlock

import (
	"context"
	"sync"

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

// FindConflictWinner returns information about a process holding the lock that caused the conflict.
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

type Options struct {
	// Create map node if path is missing.
	CreateIfMissing bool
	LockMode        yt.LockMode
}

// NewLock creates Lock object using default Options.
func NewLock(yc yt.Client, path ypath.Path) (l *Lock) {
	defaultOptions := Options{
		CreateIfMissing: true,
		LockMode:        yt.LockExclusive,
	}
	return NewLockOptions(yc, path, defaultOptions)
}

// NewLockOptions creates Lock object with specified options.
func NewLockOptions(yc yt.Client, path ypath.Path, opts Options) (l *Lock) {
	return &Lock{
		Yc:      yc,
		Path:    path,
		Options: opts,
	}
}

// Lock object represents cypress lock.
type Lock struct {
	Path    ypath.Path
	Options Options
	Yc      yt.Client

	l         sync.Mutex
	acquiring bool
	tx        yt.Tx
}

func (l *Lock) startAcquire() (err error) {
	l.l.Lock()
	defer l.l.Unlock()

	if l.acquiring {
		return xerrors.New("another acquire is in progress")
	}

	if l.tx != nil {
		select {
		case <-l.tx.Finished():
			l.tx = nil
		default:
			return xerrors.New("lock is already acquired")
		}
	}

	l.acquiring = true
	return nil
}

func (l *Lock) abortAcquire() {
	l.l.Lock()
	defer l.l.Unlock()

	l.acquiring = false
}

func (l *Lock) finishAcquire(tx yt.Tx) {
	l.l.Lock()
	defer l.l.Unlock()

	l.acquiring = false
	l.tx = tx
}

func (l *Lock) startAbort() yt.Tx {
	l.l.Lock()
	defer l.l.Unlock()

	tx := l.tx
	l.tx = nil
	return tx
}

// Acquire acquires cypress lock.
//
// Returned lost channel is closed when lock is lost because of some external event, e.g.
// transaction being aborted remotely, network partition or coordination service downtime.
//
// If lock is lost, there is no need to call Release explicitly.
//
// Lock is automatically released when provided ctx is canceled.
func (l *Lock) Acquire(ctx context.Context) (lost <-chan struct{}, err error) {
	if err = l.startAcquire(); err != nil {
		return
	}

	var tx yt.Tx
	tx, err = l.Yc.BeginTx(ctx, nil)
	if err != nil {
		l.abortAcquire()
		return
	}
	lost = tx.Finished()

	defer func() {
		if tx != nil {
			l.abortAcquire()
			_ = tx.Abort()
		}
	}()

	_, err = tx.LockNode(ctx, l.Path, l.Options.LockMode, nil)
	if yt.ContainsErrorCode(err, yt.CodeResolveError) && l.Options.CreateIfMissing {
		_, err = l.Yc.CreateNode(ctx, l.Path, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true, IgnoreExisting: true})
		if err != nil {
			return
		}

		_, err = tx.LockNode(ctx, l.Path, l.Options.LockMode, nil)
		if err != nil {
			return
		}
	} else if err != nil {
		return
	}

	l.finishAcquire(tx)
	tx = nil

	return
}

// Release releases distributed lock by aborting transaction.
//
// Error might indicate, that we failed to receive acknowledgement from the master, but lock will be released eventually.
func (l *Lock) Release(ctx context.Context) error {
	tx := l.startAbort()
	if tx != nil {
		return tx.Abort()
	}

	return nil
}
