package internal

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/cenkalti/backoff/v4"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/go/internal/retry"
	"go.ytsaurus.tech/yt/go/yt"
)

type Retrier struct {
	ProxySet *ProxySet
	Config   *yt.Config

	Log log.Structured
}

type ReadRetryParams interface {
	ReadRetryOptions() **yt.ReadRetryOptions
}

func hasMutationID(params Params) bool {
	mutating, ok := params.(MutatingParams)
	return ok && *mutating.MutatingOptions() != nil
}

func (r *Retrier) shouldRetry(isRead bool, hasStickyProxy bool, err error) bool {
	if retry.IsProxyBannedError(err) {
		return !hasStickyProxy
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		var lookupErr *net.DNSError
		if errors.As(err, &lookupErr) && lookupErr.IsNotFound {
			return false
		}

		if tcp, ok := opErr.Addr.(*net.TCPAddr); ok && tcp.IP.IsLoopback() {
			return false
		}

		if hasStickyProxy {
			return false
		}

		return true
	}

	if isRead && !hasStickyProxy {
		return retry.IsRetriableError(err)
	}

	return retry.IsRetriableHTTPResponseError(err)
}

func (r *Retrier) Intercept(ctx context.Context, call *Call, invoke CallInvoker) (res *CallResult, err error) {
	var cancel func()
	if timeout := r.Config.GetLightRequestTimeout(); timeout != 0 && !call.Params.HTTPVerb().IsHeavy() {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		res, err = invoke(ctx, call)
		if err == nil || call.DisableRetries {
			return
		}

		if hasMutationID(call.Params) {
			return
		}

		_, isRead := call.Params.(ReadRetryParams)
		if !r.shouldRetry(isRead, call.RequestedProxy != "", err) {
			return
		}

		b := call.Backoff.NextBackOff()
		if b == backoff.Stop {
			return
		}

		if r.Log != nil {
			ctxlog.Warn(ctx, r.Log.Logger(), "retrying light request",
				log.String("call_id", call.CallID.String()),
				log.Duration("backoff", b),
				log.Error(err))
		}

		select {
		case <-time.After(b):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (r *Retrier) InterceptInTx(ctx context.Context, call *Call, invoke CallInvoker) (res *CallResult, err error) {
	var cancel func()
	if timeout := r.Config.GetLightRequestTimeout(); timeout != 0 && !call.Params.HTTPVerb().IsHeavy() {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		res, err = invoke(ctx, call)
		if err == nil || call.DisableRetries {
			return
		}

		if !r.shouldRetry(true, call.RequestedProxy != "", err) {
			return
		}

		b := call.Backoff.NextBackOff()
		if b == backoff.Stop {
			return
		}

		if r.Log != nil {
			ctxlog.Warn(ctx, r.Log.Logger(), "retrying request in transaction",
				log.String("call_id", call.CallID.String()),
				log.Duration("backoff", b),
				log.Error(err))
		}

		select {
		case <-time.After(b):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (r *Retrier) Read(ctx context.Context, call *Call, invoke ReadInvoker) (rc io.ReadCloser, err error) {
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		rc, err = invoke(ctx, call)
		if err == nil || call.DisableRetries {
			return
		}

		if !r.shouldRetry(true, call.RequestedProxy != "", err) {
			return
		}

		b := call.Backoff.NextBackOff()
		if b == backoff.Stop {
			return
		}

		if r.Log != nil {
			ctxlog.Warn(ctx, r.Log.Logger(), "retrying heavy read request",
				log.String("call_id", call.CallID.String()),
				log.Duration("backoff", b),
				log.Error(err))
		}

		select {
		case <-time.After(b):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (r *Retrier) Write(ctx context.Context, call *Call, invoke WriteInvoker) (w io.WriteCloser, err error) {
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		w, err = invoke(ctx, call)
		if err == nil || call.DisableRetries {
			return
		}

		// We never actually sent any data to server. In is safe to retry this request like any other read.
		if !r.shouldRetry(true, call.RequestedProxy != "", err) {
			return
		}

		b := call.Backoff.NextBackOff()
		if b == backoff.Stop {
			return
		}

		if r.Log != nil {
			ctxlog.Warn(ctx, r.Log.Logger(), "retrying heavy write request",
				log.String("call_id", call.CallID.String()),
				log.Duration("backoff", b),
				log.Error(err))
		}

		select {
		case <-time.After(b):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
