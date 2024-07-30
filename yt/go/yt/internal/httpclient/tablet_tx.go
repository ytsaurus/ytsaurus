package httpclient

import (
	"context"

	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal"
)

type tabletTx struct {
	internal.Encoder

	txID        yt.TxID
	coordinator string
	c           *httpClient
	ctx         context.Context

	pinger *internal.Pinger
}

func (c *httpClient) BeginTabletTx(ctx context.Context, options *yt.StartTabletTxOptions) (yt.TabletTx, error) {
	var tx tabletTx

	var err error
	tx.coordinator, err = c.pickHeavyProxy(ctx)
	if err != nil {
		return nil, err
	}

	tx.StartCall = c.StartCall
	tx.Invoke = tx.do
	tx.InvokeReadRow = tx.doReadRow
	tx.InvokeWriteRow = tx.doWriteRow

	tx.c = c

	txTimeout := yson.Duration(c.config.GetTxTimeout())

	startOptions := &yt.StartTabletTxOptions{
		Type:    yt.TxTypeTablet,
		Sticky:  true,
		Timeout: &txTimeout,
	}

	if options != nil {
		startOptions.Atomicity = options.Atomicity
	}

	tx.txID, err = tx.StartTabletTx(ctx, startOptions)
	tx.ctx = ctx
	tx.pinger = internal.NewPinger(ctx, &tx, tx.txID, c.config, c.stop, nil)

	go tx.pinger.Run()

	return &tx, err
}

func (tx *tabletTx) setTx(call *internal.Call) {
	call.DisableRetries = true
	txOpts := call.Params.(internal.TransactionParams).TransactionOptions()
	*txOpts = &yt.TransactionOptions{TransactionID: tx.txID}
}

func (tx *tabletTx) do(ctx context.Context, call *internal.Call) (res *internal.CallResult, err error) {
	call.RequestedProxy = tx.coordinator
	switch call.Params.HTTPVerb() {
	case internal.VerbStartTransaction:
		return tx.c.Invoke(ctx, call)

	case internal.VerbCommitTransaction:
		err = tx.pinger.TryCommit(func() error {
			res, err = tx.c.Invoke(ctx, call)
			return err
		})
		return

	case internal.VerbAbortTransaction:
		err = tx.pinger.TryAbort(func() error {
			res, err = tx.c.Invoke(ctx, call)
			return err
		})
		return

	default:
		if err = tx.pinger.CheckAlive(); err != nil {
			return
		}

		return tx.c.Invoke(ctx, call)
	}
}

func (tx *tabletTx) doReadRow(ctx context.Context, call *internal.Call) (r yt.TableReader, err error) {
	if err = tx.pinger.CheckAlive(); err != nil {
		return
	}

	call.RequestedProxy = tx.coordinator
	tx.setTx(call)
	return tx.c.InvokeReadRow(ctx, call)
}

func (tx *tabletTx) doWriteRow(ctx context.Context, call *internal.Call) (r yt.TableWriter, err error) {
	if err = tx.pinger.CheckAlive(); err != nil {
		return
	}

	call.RequestedProxy = tx.coordinator
	tx.setTx(call)
	return tx.c.InvokeWriteRow(ctx, call)
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
