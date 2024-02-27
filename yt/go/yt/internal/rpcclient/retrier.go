package rpcclient

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type Retrier struct {
	RequestTimeout time.Duration

	Log log.Structured
}

type ReadRetryRequest interface {
	ReadRetryOptions()
}

func (r *Retrier) Intercept(ctx context.Context, call *Call, invoke CallInvoker, rsp proto.Message, opts ...bus.SendOption) (err error) {
	var cancel func()
	if timeout := r.RequestTimeout; timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	for {
		err = invoke(ctx, call, rsp, opts...)
		if err == nil || call.DisableRetries {
			return
		}

		_, isRead := call.Req.(ReadRetryRequest)
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
			return ctx.Err()
		}
	}
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

	if yterrors.ContainsErrorCode(err, yterrors.CodeRetriableArchiveError) {
		return true
	}

	if isProxyBannedError(err) {
		return true
	}

	return false
}
