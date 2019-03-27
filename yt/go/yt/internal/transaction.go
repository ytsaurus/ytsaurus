package internal

import (
	"context"
	"io"
	"sync"
	"time"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/yt"
)

type TxInterceptor struct {
	Encoder

	sync.Mutex
	committed, aborted bool
	finished           chan struct{}
	stop               *StopGroup

	id     yt.TxID
	ctx    context.Context
	client Encoder
}

type TransactionParams interface {
	TransactionOptions() **yt.TransactionOptions
}

func (t *TxInterceptor) checkState() error {
	return t.updateState(false, false)
}

func (t *TxInterceptor) updateState(commit, abort bool) error {
	t.Lock()
	defer t.Unlock()

	if t.committed {
		return yt.ErrTxCommitted
	} else if t.aborted {
		return yt.ErrTxAborted
	}

	if commit {
		t.committed = true
	}

	if abort {
		t.aborted = true
	}

	return nil
}

func (t *TxInterceptor) abort() {
	_ = t.Abort()
	close(t.finished)
}

func (t *TxInterceptor) pinger() {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()
	defer t.stop.Done()

	for {
		select {
		case <-t.stop.C():
			t.abort()
			return

		case <-t.ctx.Done():
			t.abort()
			return

		case <-ticker.C:
			if err := t.checkState(); err != nil {
				close(t.finished)
				return
			}

			_ = t.PingTx(t.ctx, t.id, nil)
		}
	}
}

func NewTx(ctx context.Context, e Encoder, stop *StopGroup, options *yt.StartTxOptions) (*TxInterceptor, error) {
	txID, err := e.StartTx(ctx, options)
	if err != nil {
		return nil, err
	}

	tx := &TxInterceptor{Encoder: e, id: txID, ctx: ctx, client: e, finished: make(chan struct{}), stop: stop}
	tx.Encoder.Invoke = tx.Encoder.Invoke.Wrap(tx.Intercept)
	tx.Encoder.InvokeRead = tx.Encoder.InvokeRead.Wrap(tx.Read)
	tx.Encoder.InvokeWrite = tx.Encoder.InvokeWrite.Wrap(tx.Write)
	tx.Encoder.InvokeReadRow = tx.Encoder.InvokeReadRow.Wrap(tx.ReadRow)
	tx.Encoder.InvokeWriteRow = tx.Encoder.InvokeWriteRow.Wrap(tx.WriteRow)

	if !stop.TryAdd() {
		tx.abort()
		return tx, xerrors.New("client is stopped")
	}

	go tx.pinger()

	return tx, nil
}

func (t *TxInterceptor) ID() yt.TxID {
	return t.id
}

func (t *TxInterceptor) Finished() <-chan struct{} {
	return t.finished
}

func (t *TxInterceptor) BeginTx(ctx context.Context, options *yt.StartTxOptions) (tx yt.Tx, err error) {
	if err = t.checkState(); err != nil {
		return
	}

	if options == nil {
		options = &yt.StartTxOptions{}
	}
	options.ParentID = &t.id

	return NewTx(ctx, t.client, t.stop, options)
}

func (t *TxInterceptor) Abort() (err error) {
	if err = t.updateState(false, true); err != nil {
		return
	}

	return t.client.AbortTx(t.ctx, t.id, nil)
}

func (t *TxInterceptor) Commit() (err error) {
	if err = t.updateState(true, false); err != nil {
		return
	}

	return t.client.CommitTx(t.ctx, t.id, nil)
}

func (t *TxInterceptor) setTx(call *Call) error {
	if err := t.checkState(); err != nil {
		return err
	}

	params, ok := call.Params.(TransactionParams)
	if !ok {
		return xerrors.Errorf("call %s is not transactional", call.Params.HTTPVerb())
	}

	txOpts := params.TransactionOptions()
	*txOpts = &yt.TransactionOptions{TransactionID: t.id}
	return nil
}

func (t *TxInterceptor) Intercept(ctx context.Context, call *Call, invoke CallInvoker) (res *CallResult, err error) {
	if err = t.setTx(call); err != nil {
		return
	}

	return invoke(ctx, call)
}

func (t *TxInterceptor) Read(ctx context.Context, call *Call, invoke ReadInvoker) (r io.ReadCloser, err error) {
	if err = t.setTx(call); err != nil {
		return
	}

	return invoke(ctx, call)
}

func (t *TxInterceptor) Write(ctx context.Context, call *Call, invoke WriteInvoker) (w io.WriteCloser, err error) {
	if err = t.setTx(call); err != nil {
		return
	}

	return invoke(ctx, call)
}

func (t *TxInterceptor) ReadRow(ctx context.Context, call *Call, invoke ReadRowInvoker) (r yt.TableReader, err error) {
	if err = t.setTx(call); err != nil {
		return
	}

	return invoke(ctx, call)
}

func (t *TxInterceptor) WriteRow(ctx context.Context, call *Call, invoke WriteRowInvoker) (w yt.TableWriter, err error) {
	if err = t.setTx(call); err != nil {
		return
	}

	return invoke(ctx, call)
}
