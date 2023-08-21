package yt

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
)

type ExecTxRetryOptions backoff.BackOff

const (
	DefaultExecTxRetryCount   = 5
	DefaultExecTxRetryBackoff = time.Second
)

// newDefaultExecTxRetryOptions creates retry options with the following properties:
// - operation is retried up to DefaultExecTxRetryCount times with DefaultExecTxRetryBackoff sleeps in between.
// - retries could be stopped via context cancellation.
func newDefaultExecTxRetryOptions(ctx context.Context) backoff.BackOffContext {
	b := backoff.NewConstantBackOff(DefaultExecTxRetryBackoff)
	return backoff.WithContext(backoff.WithMaxRetries(b, DefaultExecTxRetryCount), ctx)
}

// ExecTxRetryOptionsNone is a fixed retry policy that never retries the operation.
type ExecTxRetryOptionsNone = backoff.StopBackOff

// TxFunc is a callback used in ExecTx function.
type TxFunc func(ctx context.Context, tx Tx) error

type ExecTxOptions struct {
	RetryOptions ExecTxRetryOptions
	*StartTxOptions
}

// ExecTx is a convenience method that creates new master transaction and
// executes commit/abort based on the error returned by the callback function.
//
// In case of nil options default ones are used.
//
// Retries could be stopped with the context cancellation.
//
// If f returns a *backoff.PermanentError, the operation is not retried, and the wrapped error is returned.
func ExecTx(ctx context.Context, yc Client, f TxFunc, opts *ExecTxOptions) (err error) {
	if opts == nil {
		opts = &ExecTxOptions{}
	}

	if opts.RetryOptions == nil {
		opts.RetryOptions = newDefaultExecTxRetryOptions(ctx)
	}

	return backoff.Retry(func() error {
		b := newMasterTxBeginner(yc, opts.StartTxOptions)
		return execTx(ctx, b, func(ctx context.Context, tx any) error {
			return f(ctx, tx.(Tx))
		})
	}, opts.RetryOptions)
}

// TabletTxFunc is a callback used in ExecTabletTx function.
type TabletTxFunc func(ctx context.Context, tx TabletTx) error

type ExecTabletTxOptions struct {
	RetryOptions ExecTxRetryOptions
	*StartTabletTxOptions
}

// ExecTabletTx a convenience method that creates new tablet transaction and executes commit/abort based on the
// error returned by the callback function.
//
// In case of nil options default ones are used.
//
// Retries could be stopped with the context cancellation.
//
// If f returns a *backoff.PermanentError, the operation is not retried, and the wrapped error is returned.
func ExecTabletTx(ctx context.Context, yc Client, f TabletTxFunc, opts *ExecTabletTxOptions) (err error) {
	if opts == nil {
		opts = &ExecTabletTxOptions{}
	}

	if opts.RetryOptions == nil {
		opts.RetryOptions = newDefaultExecTxRetryOptions(ctx)
	}

	return backoff.Retry(func() error {
		b := newTabletTxBeginner(yc, opts.StartTabletTxOptions)
		return execTx(ctx, b, func(ctx context.Context, tx any) error {
			return f(ctx, tx.(TabletTx))
		})
	}, opts.RetryOptions)
}

type txBeginner interface {
	BeginTx(ctx context.Context) (tx, error)
}

type tx interface {
	Commit() error
	Abort() error
}

type masterTxBeginner struct {
	yc   Client
	opts *StartTxOptions
}

func newMasterTxBeginner(yc Client, opts *StartTxOptions) *masterTxBeginner {
	return &masterTxBeginner{yc: yc, opts: opts}
}

func (b *masterTxBeginner) BeginTx(ctx context.Context) (tx, error) {
	return b.yc.BeginTx(ctx, b.opts)
}

type tabletTxBeginner struct {
	yc   Client
	opts *StartTabletTxOptions
}

func newTabletTxBeginner(yc Client, opts *StartTabletTxOptions) *tabletTxBeginner {
	return &tabletTxBeginner{yc: yc, opts: opts}
}

func (b *tabletTxBeginner) BeginTx(ctx context.Context) (tx, error) {
	return b.yc.BeginTabletTx(ctx, b.opts)
}

type txFunc func(ctx context.Context, tx any) error

// execTx starts transaction and executes commit/abort based on the
// error returned by the callback function.
func execTx(ctx context.Context, b txBeginner, f txFunc) (err error) {
	tx, err := b.BeginTx(ctx)
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			_ = tx.Abort()
		} else {
			err = tx.Commit()
		}
	}()

	err = f(ctx, tx)
	return err
}
