package rpcclient

import (
	"context"

	"github.com/golang/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal"
)

type TransactionOptions struct {
	*yt.TransactionOptions
	TxStartTimestamp yt.Timestamp
}

type TransactionalRequest interface {
	SetTxOptions(opts *TransactionOptions)
}

type tabletTx struct {
	Encoder

	txID             yt.TxID
	txStartTimestamp yt.Timestamp

	coordinator string
	c           *client
	ctx         context.Context

	pinger *internal.Pinger
}

func (c *client) BeginTabletTx(
	ctx context.Context,
	opts *yt.StartTabletTxOptions,
) (yt.TabletTx, error) {
	var tx tabletTx

	var err error
	tx.coordinator, err = c.pickRPCProxy(ctx)
	if err != nil {
		return nil, err
	}

	tx.StartCall = c.StartCall
	tx.Invoke = tx.do
	tx.InvokeReadRow = tx.doReadRow

	tx.c = c

	txTimeout := yson.Duration(c.conf.GetTxTimeout())

	startOptions := &yt.StartTabletTxOptions{
		Type:    yt.TxTypeTablet,
		Sticky:  true,
		Timeout: &txTimeout,
	}

	if opts != nil {
		startOptions.Atomicity = opts.Atomicity
	}

	tx.txID, tx.txStartTimestamp, err = tx.startTabletTx(ctx, startOptions)
	tx.ctx = ctx
	tx.pinger = internal.NewPinger(ctx, &tx, tx.txID, c.conf, c.stop)

	go tx.pinger.Run()

	return &tx, err
}

func (tx *tabletTx) do(ctx context.Context, call *Call, rsp proto.Message, opts ...bus.SendOption) (err error) {
	call.RequestedProxy = tx.coordinator

	switch call.Method {
	case MethodStartTransaction:
		return tx.c.Invoke(ctx, call, rsp, opts...)

	case MethodCommitTransaction:
		err = tx.pinger.TryCommit(func() error {
			return tx.c.Invoke(ctx, call, rsp, opts...)
		})
		return

	case MethodAbortTransaction:
		err = tx.pinger.TryAbort(func() error {
			return tx.c.Invoke(ctx, call, rsp, opts...)
		})
		return

	case MethodModifyRows:
		if err = tx.pinger.CheckAlive(); err != nil {
			return
		}
		return tx.doWriteRows(ctx, call, rsp, opts...)

	default:
		if err = tx.pinger.CheckAlive(); err != nil {
			return
		}
		return tx.c.Invoke(ctx, call, rsp, opts...)
	}
}

func (tx *tabletTx) setTxID(call *Call) error {
	req, ok := call.Req.(TransactionalRequest)
	if !ok {
		return xerrors.Errorf("call %q is not transactional", call.Method)
	}

	opts := &TransactionOptions{
		TransactionOptions: &yt.TransactionOptions{TransactionID: tx.ID()},
		TxStartTimestamp:   tx.txStartTimestamp,
	}

	req.SetTxOptions(opts)
	return nil
}

func (tx *tabletTx) doReadRow(ctx context.Context, call *Call, rsp ProtoRowset) (r yt.TableReader, err error) {
	if err = tx.pinger.CheckAlive(); err != nil {
		return
	}

	call.RequestedProxy = tx.coordinator
	call.DisableRetries = true
	if err := tx.setTxID(call); err != nil {
		return nil, err
	}

	return tx.c.InvokeReadRow(ctx, call, rsp)
}

func (tx *tabletTx) doWriteRows(ctx context.Context, call *Call, rsp proto.Message, opts ...bus.SendOption) (err error) {
	if err = tx.pinger.CheckAlive(); err != nil {
		return
	}

	call.RequestedProxy = tx.coordinator
	call.DisableRetries = true
	if err := tx.setTxID(call); err != nil {
		return err
	}

	return tx.c.Invoke(ctx, call, rsp, opts...)
}

func (tx *tabletTx) ID() yt.TxID {
	return tx.txID
}

func (tx *tabletTx) Commit() error {
	return tx.CommitTx(tx.ctx, tx.txID, &yt.CommitTxOptions{Sticky: true})
}

func (tx *tabletTx) Abort() error {
	return tx.AbortTx(tx.ctx, tx.txID, &yt.AbortTxOptions{Sticky: true})
}
