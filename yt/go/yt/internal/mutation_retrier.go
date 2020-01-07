package internal

import (
	"context"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/ctxlog"

	"a.yandex-team.ru/yt/go/guid"

	"a.yandex-team.ru/yt/go/yt"
)

type MutationRetrier struct {
	Backoff BackoffStrategy
	Log     log.Structured
}

type MutatingParams interface {
	MutatingOptions() **yt.MutatingOptions
}

func (r *MutationRetrier) Intercept(ctx context.Context, call *Call, invoke CallInvoker) (res *CallResult, err error) {
	if params, ok := call.Params.(MutatingParams); ok {
		mut := params.MutatingOptions()
		*mut = &yt.MutatingOptions{MutationID: yt.MutationID(guid.New())}

		for i := 0; ; i++ {
			res, err = invoke(ctx, call)
			if err == nil || !isTransientError(err) {
				return
			}

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			(*mut).Retry = true

			backoff, ok := r.Backoff.Backoff(i)
			if !ok {
				return
			}

			if r.Log != nil {
				ctxlog.Warn(ctx, r.Log.Logger(), "retrying mutation",
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
