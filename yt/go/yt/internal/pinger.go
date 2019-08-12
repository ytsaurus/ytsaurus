package internal

import (
	"context"
	"sync"
	"time"

	"a.yandex-team.ru/yt/go/yterrors"

	"a.yandex-team.ru/yt/go/yt"
)

type abortCtx struct {
	context.Context
}

func (*abortCtx) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (*abortCtx) Done() <-chan struct{} {
	return nil
}

var _ context.Context = (*abortCtx)(nil)

type pinger struct {
	l                  sync.Mutex
	committed, aborted bool
	yc                 yt.LowLevelTxClient
	ctx                context.Context
	abortCtx           context.Context
	stop               *StopGroup
	txID               yt.TxID
	txTimeout          time.Duration
	finished           chan struct{}
}

func newPinger(ctx context.Context, yc yt.LowLevelTxClient, txID yt.TxID, txTimeout time.Duration, stop *StopGroup) *pinger {
	return &pinger{
		yc:        yc,
		ctx:       ctx,
		abortCtx:  &abortCtx{ctx},
		stop:      stop,
		txID:      txID,
		txTimeout: txTimeout,
		finished:  make(chan struct{}),
	}
}

func (p *pinger) abortBackground() {
	if err := p.tryAbort(); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(p.abortCtx, p.txTimeout)
	defer cancel()

	_ = p.yc.AbortTx(ctx, p.txID, nil)
}

func (p *pinger) check() error {
	p.l.Lock()
	defer p.l.Unlock()

	if p.aborted {
		return yt.ErrTxAborted
	}
	if p.committed {
		return yt.ErrTxCommitted
	}

	return nil
}

func (p *pinger) tryAbort() error {
	p.l.Lock()
	defer p.l.Unlock()

	if p.aborted {
		return yt.ErrTxAborted
	}
	if p.committed {
		return yt.ErrTxCommitted
	}

	p.aborted = true
	close(p.finished)
	return nil
}

func (p *pinger) tryCommit() error {
	p.l.Lock()
	defer p.l.Unlock()

	if p.aborted {
		return yt.ErrTxAborted
	}
	if p.committed {
		return yt.ErrTxCommitted
	}

	p.committed = true
	close(p.finished)
	return nil
}

func (p *pinger) run() {
	defer p.stop.Done()

	ticker := time.NewTicker(p.txTimeout / 4)
	defer ticker.Stop()

	for {
		select {
		case <-p.finished:
			return

		case <-p.ctx.Done():
			p.abortBackground()
			return

		case <-p.stop.C():
			p.abortBackground()
			return

		case <-ticker.C:
			err := p.yc.PingTx(p.ctx, p.txID, nil)
			if yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction) {
				_ = p.tryAbort()
				return
			}
		}
	}
}
