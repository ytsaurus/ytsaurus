package internal

import (
	"context"
	"io"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal/smartreader"
)

type TxInterceptor struct {
	Encoder
	Client Encoder
	log    log.Structured
	pinger *Pinger
}

type TransactionParams interface {
	TransactionOptions() **yt.TransactionOptions
}

func NewTx(
	ctx context.Context,
	e Encoder,
	log log.Structured,
	stop *StopGroup,
	config *yt.Config,
	options *yt.StartTxOptions,
) (yt.Tx, error) {
	if options == nil {
		options = &yt.StartTxOptions{}
	}

	updatedOptions := *options
	txTimeout := yson.Duration(config.GetTxTimeout())
	updatedOptions.Timeout = &txTimeout

	txID, err := e.StartTx(ctx, &updatedOptions)
	if err != nil {
		return nil, err
	}

	tx := &TxInterceptor{
		Encoder: e,
		Client:  e,
		log:     log,
		pinger:  NewPinger(ctx, &e, txID, config, stop),
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

	go tx.pinger.Run()

	return tx, nil
}

func (t *TxInterceptor) ID() yt.TxID {
	return t.pinger.txID
}

func (t *TxInterceptor) Finished() <-chan struct{} {
	return t.pinger.finished
}

func (t *TxInterceptor) BeginTx(ctx context.Context, options *yt.StartTxOptions) (tx yt.Tx, err error) {
	if err = t.pinger.CheckAlive(); err != nil {
		return
	}

	if options == nil {
		options = &yt.StartTxOptions{}
	}
	if options.TransactionOptions == nil {
		options.TransactionOptions = &yt.TransactionOptions{}
	}

	options.TransactionID = t.ID()

	return NewTx(ctx, t.Client, t.log, t.pinger.stop, t.pinger.config, options)
}

func (t *TxInterceptor) Abort() (err error) {
	return t.pinger.TryAbort(func() error {
		return t.Client.AbortTx(t.pinger.ctx, t.pinger.txID, nil)
	})
}

func (t *TxInterceptor) Commit() (err error) {
	return t.pinger.TryCommit(func() error {
		return t.Client.CommitTx(t.pinger.ctx, t.pinger.txID, nil)
	})
}

func (t *TxInterceptor) setTx(call *Call) error {
	if err := t.pinger.CheckAlive(); err != nil {
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

func (t *TxInterceptor) ReadTable(
	ctx context.Context,
	path ypath.YPath,
	options *yt.ReadTableOptions,
) (r yt.TableReader, err error) {
	if options != nil && options.Smart != nil && *options.Smart && !options.Unordered {
		opts := *options
		opts.Smart = nil
		return smartreader.NewReader(ctx, t, false, t.log, path, &opts)
	} else {
		return t.Encoder.ReadTable(ctx, path, options)
	}
}
