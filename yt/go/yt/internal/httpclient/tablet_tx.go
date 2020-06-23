package httpclient

import (
	"context"
	"time"

	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/internal"
)

type tabletTx struct {
	internal.Encoder

	txID           yt.TxID
	coordinatorURL string
	c              *httpClient
	ctx            context.Context
	commitOptions  *yt.CommitTxOptions

	pinger *internal.Pinger
}

func (c *httpClient) BeginTabletTx(ctx context.Context, options *yt.StartTabletTxOptions) (yt.TabletTx, error) {
	var tx tabletTx

	var err error
	tx.coordinatorURL, err = c.pickHeavyProxy(ctx)
	if err != nil {
		return nil, err
	}

	tx.Invoke = tx.do
	tx.InvokeReadRow = tx.doReadRow
	tx.InvokeWriteRow = tx.doWriteRow
	if options != nil {
		tx.commitOptions = options.CommitOptions
	}

	tx.c = c

	txType := "tablet"
	if options != nil && options.Master {
		txType = "master"
	}

	startOptions := &yt.StartTxOptions{
		Type:   &txType,
		Sticky: true,
	}
	if options != nil {
		startOptions.Atomicity = options.Atomicity
		if options.Timeout != nil {
			startOptions.Timeout = options.Timeout
		}
	}

	txTimeout := yt.DefaultTxTimeout
	if startOptions.Timeout != nil {
		txTimeout = time.Duration(*startOptions.Timeout)
	}

	tx.txID, err = tx.StartTx(ctx, startOptions)
	tx.ctx = ctx
	tx.pinger = internal.NewPinger(ctx, c, tx.txID, txTimeout, c.stop)

	go tx.pinger.Run()

	return &tx, err
}

func (tx *tabletTx) setTx(call *internal.Call) {
	txOpts := call.Params.(internal.TransactionParams).TransactionOptions()
	*txOpts = &yt.TransactionOptions{TransactionID: tx.txID}
}

func (tx *tabletTx) do(ctx context.Context, call *internal.Call) (res *internal.CallResult, err error) {
	switch call.Params.HTTPVerb() {
	case internal.VerbStartTransaction:
		break

	case internal.VerbCommitTransaction:
		if err = tx.pinger.TryCommit(); err != nil {
			return
		}

	case internal.VerbAbortTransaction:
		if err = tx.pinger.TryAbort(); err != nil {
			return
		}

	default:
		if err = tx.pinger.Check(); err != nil {
			return
		}
	}

	call.ProxyURL = tx.coordinatorURL
	return tx.c.Invoke(ctx, call)
}

func (tx *tabletTx) doReadRow(ctx context.Context, call *internal.Call) (r yt.TableReader, err error) {
	if err = tx.pinger.Check(); err != nil {
		return
	}

	call.ProxyURL = tx.coordinatorURL
	tx.setTx(call)
	return tx.c.InvokeReadRow(ctx, call)
}

func (tx *tabletTx) doWriteRow(ctx context.Context, call *internal.Call) (r yt.TableWriter, err error) {
	if err = tx.pinger.Check(); err != nil {
		return
	}

	call.ProxyURL = tx.coordinatorURL
	tx.setTx(call)
	return tx.c.InvokeWriteRow(ctx, call)
}

func (tx *tabletTx) ID() yt.TxID {
	return tx.txID
}

func (tx *tabletTx) Commit() error {
	var opts yt.CommitTxOptions
	if tx.commitOptions != nil {
		opts = *tx.commitOptions
	}
	opts.Sticky = true

	return tx.CommitTx(tx.ctx, tx.txID, &opts)
}

func (tx *tabletTx) Abort() error {
	return tx.AbortTx(tx.ctx, tx.txID, &yt.AbortTxOptions{Sticky: true})
}
