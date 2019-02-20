package internal

import (
	"context"
	"time"

	"a.yandex-team.ru/yt/go/guid"

	"a.yandex-team.ru/yt/go/yt"
)

type MutationRetrier struct {
	Backoff yt.BackoffStrategy
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
