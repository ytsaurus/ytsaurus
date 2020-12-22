package internal

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/xerrors"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/ctxlog"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
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
	if isRead && isNetError(err) {
		return true
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

		return true
	}

	if yterrors.ContainsErrorCode(err, CodeBalancerServiceUnavailable) {
		return true
	}

	if yterrors.ContainsErrorCode(err, yterrors.CodeOperationProgressOutdated) {
		return true
	}

	if isProxyBannedError(err) {
		return true
	}

	return false
}

func isProxyBannedError(err error) bool {
	var ytErr *yterrors.Error
	if errors.As(err, &ytErr) && ytErr.Message == "This proxy is banned" {
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
			ctxlog.Warn(ctx, r.Log.Logger(), "retrying read request",
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
