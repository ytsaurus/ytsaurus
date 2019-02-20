package internal

import (
	"context"
	"time"

	"a.yandex-team.ru/yt/go/yt"
)

type ReadRetrier struct {
	Backoff yt.BackoffStrategy
}

type ReadRetryParams interface {
	ReadRetryOptions() **yt.ReadRetryOptions
}

func (r *ReadRetrier) Intercept(ctx context.Context, call *Call, invoke CallInvoker) (res *CallResult, err error) {
	if _, ok := call.Params.(ReadRetryParams); ok {
		for i := 0; ; i++ {
			res, err = invoke(ctx, call)
			if err == nil {
				return
			}

			select {
			case <-time.After(r.Backoff.Backoff(i)):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	} else {
		return invoke(ctx, call)
	}
}
