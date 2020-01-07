package internal

import (
	"context"
	"net"
	"time"

	"a.yandex-team.ru/library/go/core/log/ctxlog"
	"a.yandex-team.ru/yt/go/yterrors"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/library/go/core/log"

	"a.yandex-team.ru/yt/go/yt"
)

type ReadRetrier struct {
	Backoff BackoffStrategy
	Log     log.Structured
}

type ReadRetryParams interface {
	ReadRetryOptions() **yt.ReadRetryOptions
}

const (
	CodeBalancerServiceUnavailable = 1000000
)

func isTransientError(err error) bool {
	var netErr net.Error
	if ok := xerrors.As(err, &netErr); ok {
		return netErr.Temporary() || netErr.Timeout()
	}

	if yterrors.ContainsErrorCode(err, CodeBalancerServiceUnavailable) {
		return true
	}

	return false
}

func (r *ReadRetrier) Intercept(ctx context.Context, call *Call, invoke CallInvoker) (res *CallResult, err error) {
	if _, ok := call.Params.(ReadRetryParams); ok {
		for i := 0; ; i++ {
			res, err = invoke(ctx, call)
			if err == nil || !isTransientError(err) {
				return
			}

			backoff, ok := r.Backoff.Backoff(i)
			if !ok {
				return
			}

			if r.Log != nil {
				ctxlog.Warn(ctx, r.Log.Logger(), "retrying read request",
					log.String("call_id", call.CallID.String()),
					log.Duration("backoff", backoff),
					log.Error(err))
			}

			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	} else {
		return invoke(ctx, call)
	}
}
