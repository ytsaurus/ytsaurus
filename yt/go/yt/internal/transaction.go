package internal

import (
	"context"
	"io"
	"time"

	"a.yandex-team.ru/yt/go/yson"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/yt"
)

type TxInterceptor struct {
	Encoder
	Client *Encoder
	pinger *pinger
}

type TransactionParams interface {
	TransactionOptions() **yt.TransactionOptions
}

func NewTx(ctx context.Context, e Encoder, stop *StopGroup, options *yt.StartTxOptions) (yt.Tx, error) {
	if options == nil {
		options = &yt.StartTxOptions{}
	}

	updatedOptions := *options
	txTimeout := yson.Duration(yt.DefaultTxTimeout)
	if updatedOptions.Timeout == nil {
		updatedOptions.Timeout = &txTimeout
	}

	txID, err := e.StartTx(ctx, &updatedOptions)
	if err != nil {
		return nil, err
	}

	tx := &TxInterceptor{
		Encoder: e,
		Client:  &e,
		pinger:  newPinger(ctx, &e, txID, time.Duration(*updatedOptions.Timeout), stop),
	}

	tx.Encoder.Invoke = tx.Encoder.Invoke.Wrap(tx.Intercept)
	tx.Encoder.InvokeRead = tx.Encoder.InvokeRead.Wrap(tx.Read)
	tx.Encoder.InvokeWrite = tx.Encoder.InvokeWrite.Wrap(tx.Write)
	tx.Encoder.InvokeReadRow = tx.Encoder.InvokeReadRow.Wrap(tx.ReadRow)
	tx.Encoder.InvokeWriteRow = tx.Encoder.InvokeWriteRow.Wrap(tx.WriteRow)

	if !stop.TryAdd() {
		// In this rare event, leave tx running on the master.
		return nil, xerrors.New("client is stopped")
	}

	go tx.pinger.run()

	return tx, nil
}

func (t *TxInterceptor) ID() yt.TxID {
	return t.pinger.txID
}

func (t *TxInterceptor) Finished() <-chan struct{} {
	return t.pinger.finished
}

func (t *TxInterceptor) BeginTx(ctx context.Context, options *yt.StartTxOptions) (tx yt.Tx, err error) {
	if err = t.pinger.check(); err != nil {
		return
	}

	if options == nil {
		options = &yt.StartTxOptions{}
	}
	options.ParentID = &t.pinger.txID

	return NewTx(ctx, t.Encoder, t.pinger.stop, options)
}

func (t *TxInterceptor) Abort() (err error) {
	if err = t.pinger.tryAbort(); err != nil {
		return
	}

	return t.Client.AbortTx(t.pinger.ctx, t.pinger.txID, nil)
}

func (t *TxInterceptor) Commit() (err error) {
	if err = t.pinger.tryCommit(); err != nil {
		return
	}

	return t.Client.CommitTx(t.pinger.ctx, t.pinger.txID, nil)
}

func (t *TxInterceptor) setTx(call *Call) error {
	if err := t.pinger.check(); err != nil {
		return err
	}

	params, ok := call.Params.(TransactionParams)
	if !ok {
		return xerrors.Errorf("call %s is not transactional", call.Params.HTTPVerb())
	}

	txOpts := params.TransactionOptions()
	*txOpts = &yt.TransactionOptions{TransactionID: t.pinger.txID}
	return nil
}

func (t *TxInterceptor) Intercept(ctx context.Context, call *Call, next CallInvoker) (res *CallResult, err error) {
	if err = t.setTx(call); err != nil {
		return
	}

	return next(ctx, call)
}

func (t *TxInterceptor) Read(ctx context.Context, call *Call, next ReadInvoker) (r io.ReadCloser, err error) {
	if err = t.setTx(call); err != nil {
		return
	}

	return next(ctx, call)
}

func (t *TxInterceptor) Write(ctx context.Context, call *Call, next WriteInvoker) (w io.WriteCloser, err error) {
	if err = t.setTx(call); err != nil {
		return
	}

	return next(ctx, call)
}

func (t *TxInterceptor) ReadRow(ctx context.Context, call *Call, next ReadRowInvoker) (r yt.TableReader, err error) {
	if err = t.setTx(call); err != nil {
		return
	}

	return next(ctx, call)
}

func (t *TxInterceptor) WriteRow(ctx context.Context, call *Call, next WriteRowInvoker) (w yt.TableWriter, err error) {
	if err = t.setTx(call); err != nil {
		return
	}

	return next(ctx, call)
}
