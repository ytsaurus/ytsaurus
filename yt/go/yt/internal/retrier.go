package internal

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/xerrors"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type Retrier struct {
	ProxySet *ProxySet
	Config   *yt.Config

	Log log.Structured
}

type ReadRetryParams interface {
	ReadRetryOptions() **yt.ReadRetryOptions
}

const (
	CodeBalancerServiceUnavailable = 1000000
)

func isNetError(err error) bool {
	var netErr net.Error
	return xerrors.As(err, &netErr)
}

func (r *Retrier) shouldRetry(isRead bool, err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		var lookupErr *net.DNSError
		if errors.As(err, &lookupErr) && lookupErr.IsNotFound {
			return false
		}

		if tcp, ok := opErr.Addr.(*net.TCPAddr); ok && tcp.IP.IsLoopback() {
			return false
		}

		return true
	}

	if isRead && isNetError(err) {
		return true
	}

	if yterrors.ContainsErrorCode(err, CodeBalancerServiceUnavailable) {
		return true
	}

	if yterrors.ContainsErrorCode(err, yterrors.CodeRetriableArchiveError) {
		return true
	}

	if isProxyBannedError(err) {
		return true
	}

	return false
}

func isProxyBannedError(err error) bool {
	// COMPAT(babenko): drop ProxyBanned in favor of PeerBanned
	if yterrors.ContainsErrorCode(err, yterrors.CodeProxyBanned) {
		return true
	}

	if yterrors.ContainsErrorCode(err, yterrors.CodePeerBanned) {
		return true
	}

	return false
}

func (r *Retrier) Intercept(ctx context.Context, call *Call, invoke CallInvoker) (res *CallResult, err error) {
	var cancel func()
	if timeout := r.Config.GetLightRequestTimeout(); timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	for {
		res, err = invoke(ctx, call)
		if err == nil || call.DisableRetries {
			return
		}

		_, isRead := call.Params.(ReadRetryParams)
		if !r.shouldRetry(isRead, err) {
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

func (r *Retrier) Read(ctx context.Context, call *Call, invoke ReadInvoker) (rc io.ReadCloser, err error) {
	for {
		rc, err = invoke(ctx, call)
		if err == nil || call.DisableRetries {
			return
		}

		if !r.shouldRetry(true, err) {
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
		w, err = invoke(ctx, call)
		if err == nil || call.DisableRetries {
			return
		}

		// We never actually sent any data to server. In is safe to retry this request like any other read.
		if !r.shouldRetry(true, err) {
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
