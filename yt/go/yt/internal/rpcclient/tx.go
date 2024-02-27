package rpcclient

import (
	"context"

	"github.com/golang/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal"
)

func (c *client) BeginTx(
	ctx context.Context,
	opts *yt.StartTxOptions,
) (tx yt.Tx, err error) {
	return NewTx(ctx, c.Encoder, c.log, c.stop, c.conf, opts)
}

var _ yt.Tx = (*TxInterceptor)(nil)

type TxInterceptor struct {
	Encoder

	Client Encoder

	log    log.Structured
	pinger *internal.Pinger
}

func NewTx(
	ctx context.Context,
	e Encoder,
	log log.Structured,
	stop *internal.StopGroup,
	config *yt.Config,
	opts *yt.StartTxOptions,
) (yt.Tx, error) {
	if opts == nil {
		opts = &yt.StartTxOptions{}
	}

	updatedOptions := *opts
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
		pinger:  internal.NewPinger(ctx, &e, txID, config, stop),
	}

	tx.Encoder.Invoke = tx.Encoder.Invoke.Wrap(tx.Intercept)
	tx.Encoder.InvokeReadRow = tx.Encoder.InvokeReadRow.Wrap(tx.ReadRow)

	if !stop.TryAdd() {
		// In this rare event, leave tx running on the master.
		return nil, xerrors.New("client is stopped")
	}

	go tx.pinger.Run()

	return tx, nil
}

func (t *TxInterceptor) ID() yt.TxID {
	return t.pinger.ID()
}

func (t *TxInterceptor) Finished() <-chan struct{} {
	return t.pinger.Finished()
}

func (t *TxInterceptor) BeginTx(
	ctx context.Context,
	opts *yt.StartTxOptions,
) (tx yt.Tx, err error) {
	if err = t.pinger.CheckAlive(); err != nil {
		return
	}

	if opts == nil {
		opts = &yt.StartTxOptions{}
	}
	if opts.TransactionOptions == nil {
		opts.TransactionOptions = &yt.TransactionOptions{}
	}
	opts.TransactionID = t.ID()

	return NewTx(ctx, t.Client, t.log, t.pinger.Stop(), t.pinger.Config(), opts)
}

func (t *TxInterceptor) Abort() (err error) {
	return t.pinger.TryAbort(func() error {
		return t.Client.AbortTx(t.pinger.Ctx(), t.pinger.ID(), nil)
	})
}

func (t *TxInterceptor) Commit() (err error) {
	return t.pinger.TryAbort(func() error {
		return t.Client.CommitTx(t.pinger.Ctx(), t.pinger.ID(), nil)
	})
}

func (t *TxInterceptor) setTxID(call *Call) error {
	if err := t.pinger.CheckAlive(); err != nil {
		return err
	}

	req, ok := call.Req.(TransactionalRequest)
	if !ok {
		return xerrors.Errorf("call %q is not transactional", call.Method)
	}

	opts := &TransactionOptions{
		TransactionOptions: &yt.TransactionOptions{TransactionID: t.pinger.ID()},
	}

	req.SetTxOptions(opts)
	return nil
}

func (t *TxInterceptor) Intercept(
	ctx context.Context,
	call *Call,
	next CallInvoker,
	rsp proto.Message,
	opts ...bus.SendOption,
) (err error) {
	if err = t.setTxID(call); err != nil {
		return
	}

	return next(ctx, call, rsp, opts...)
}

func (t *TxInterceptor) ReadRow(
	ctx context.Context,
	call *Call,
	next ReadRowInvoker,
	rsp ProtoRowset,
) (r yt.TableReader, err error) {
	if err = t.setTxID(call); err != nil {
		return
	}

	return next(ctx, call, rsp)
}
