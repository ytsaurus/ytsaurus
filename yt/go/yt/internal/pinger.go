package internal

import (
	"context"
	"sync"
	"time"

	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
)

// abortCtx preserves parent ctx Value()-s, but resets deadline and cancellation state.
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

type Pinger struct {
	l                  sync.Mutex
	committed, aborted bool
	yc                 yt.LowLevelTxClient
	ctx                context.Context
	abortCtx           context.Context
	stop               *StopGroup
	txID               yt.TxID
	config             *yt.Config
	finished           chan struct{}
}

func NewPinger(
	ctx context.Context,
	yc yt.LowLevelTxClient,
	txID yt.TxID,
	config *yt.Config,
	stop *StopGroup,
) *Pinger {
	return &Pinger{
		yc:       yc,
		ctx:      ctx,
		abortCtx: &abortCtx{ctx},
		stop:     stop,
		txID:     txID,
		config:   config,
		finished: make(chan struct{}),
	}
}

func (p *Pinger) abortBackground() {
	if err := p.TryAbort(); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(p.abortCtx, p.config.GetTxTimeout())
	defer cancel()

	_ = p.yc.AbortTx(ctx, p.txID, nil)
}

func (p *Pinger) Check() error {
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

func (p *Pinger) TryAbort() error {
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

func (p *Pinger) TryCommit() error {
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

func (p *Pinger) Run() {
	defer p.stop.Done()

	ticker := time.NewTicker(p.config.GetTxPingPeriod())
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
				_ = p.TryAbort()
				return
			}
		}
	}
}
