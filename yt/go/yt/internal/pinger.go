package internal

import (
	"context"
	"errors"
	"sync"
	"time"

	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
)

var (
	errTxCommitted  = errors.New("transaction is already committed")
	errTxAborted    = errors.New("transaction is already aborted")
	errTxAborting   = errors.New("transaction is aborting")
	errTxCommitting = errors.New("transaction is committing")
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

// Pinger tracks server transaction state and performs periodic pings.
type Pinger struct {
	l sync.Mutex

	committing bool // Commit was started by the client.
	aborting   bool // Synchronous abort was started by the client.
	dead       bool // We know for sure that transaction is dead.

	finished     chan struct{}
	finishFailed chan struct{}

	yc       yt.LowLevelTxClient
	ctx      context.Context
	abortCtx context.Context
	stop     *StopGroup
	txID     yt.TxID
	config   *yt.Config
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

		finished:     make(chan struct{}),
		finishFailed: make(chan struct{}),
	}
}

func (p *Pinger) abortBackground() {
	p.l.Lock()
	if p.dead {
		p.l.Unlock()
		return
	}
	p.aborting = true
	p.l.Unlock()

	ctx, cancel := context.WithTimeout(p.abortCtx, p.config.GetTxTimeout())
	defer cancel()

	_ = p.yc.AbortTx(ctx, p.txID, nil)
	p.finish()
}

func (p *Pinger) checkAlive() error {
	switch {
	case p.aborting && !p.dead:
		return errTxAborting
	case p.aborting && p.dead:
		return errTxAborted
	case p.committing && !p.dead:
		return errTxCommitting
	case p.committing && p.dead:
		return errTxCommitted
	}

	return nil
}

func (p *Pinger) startAbort() error {
	p.l.Lock()
	defer p.l.Unlock()

	if err := p.checkAlive(); err != nil {
		return err
	}

	p.aborting = true
	return nil
}

func (p *Pinger) startCommit() error {
	p.l.Lock()
	defer p.l.Unlock()

	if err := p.checkAlive(); err != nil {
		return err
	}

	p.committing = true
	return nil
}

func (p *Pinger) finish() {
	p.l.Lock()
	defer p.l.Unlock()

	select {
	case <-p.finished:
	default:
		close(p.finished)
	}
}

func (p *Pinger) onFinishError(err error) {
	p.l.Lock()
	defer p.l.Unlock()

	if yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction) {
		p.dead = true
		select {
		case <-p.finished:
		default:
			close(p.finished)
		}
	} else {
		// Launch backgroud abort.
		close(p.finishFailed)
		p.aborting = true
	}
}

func (p *Pinger) CheckAlive() error {
	p.l.Lock()
	defer p.l.Unlock()

	return p.checkAlive()
}

func (p *Pinger) TryAbort(aborter func() error) error {
	if err := p.startAbort(); err != nil {
		return err
	}

	if err := aborter(); err != nil {
		p.onFinishError(err)
		return err
	} else {
		p.finish()
	}

	return nil
}

func (p *Pinger) TryCommit(committer func() error) error {
	if err := p.startCommit(); err != nil {
		return err
	}

	if err := committer(); err != nil {
		p.onFinishError(err)
		return err
	} else {
		p.finish()
	}

	return nil
}

// OnTxError may be optionally invoked to snoop transaction state from failure of other commands.
func (p *Pinger) OnTxError(err error) {
	if !yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction) {
		return
	}

	p.l.Lock()
	defer p.l.Unlock()

	// If tx is already finishing, we ignore error and wait for Commit/Abort reply.
	if p.committing || p.aborting {
		return
	}

	p.aborting = true // We didn't start commit, so it must be aborted by timeout.
	p.dead = true     // It is dead for sure.
	select {
	case <-p.finished:
	default:
		close(p.finished)
	}
}

func (p *Pinger) Run() {
	defer p.stop.Done()

	ticker := time.NewTicker(p.config.GetTxPingPeriod())
	defer ticker.Stop()

	for {
		select {
		case <-p.finished:
			return

		case <-p.finishFailed:
			p.abortBackground()
			return

		case <-p.ctx.Done():
			p.abortBackground()
			return

		case <-p.stop.C():
			p.abortBackground()
			return

		case <-ticker.C:
			err := p.yc.PingTx(p.ctx, p.txID, nil)
			if err != nil {
				p.OnTxError(err)
			}
		}
	}
}
