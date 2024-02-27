package internal

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/yt"
)

type MutationRetrier struct {
	Log log.Structured
}

type MutatingParams interface {
	MutatingOptions() **yt.MutatingOptions
}

func (r *MutationRetrier) Intercept(ctx context.Context, call *Call, invoke CallInvoker) (res *CallResult, err error) {
	params, ok := call.Params.(MutatingParams)
	if !ok || call.DisableRetries {
		return invoke(ctx, call)
	}

	mut := params.MutatingOptions()
	if *mut == nil {
		*mut = &yt.MutatingOptions{MutationID: yt.MutationID(guid.New())}
	} else {
		*mut = &yt.MutatingOptions{MutationID: (*mut).MutationID, Retry: (*mut).Retry}
	}

	for i := 0; ; i++ {
		res, err = invoke(ctx, call)
		if err == nil || !isNetError(err) {
			return
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		(*mut).Retry = true

		b := call.Backoff.NextBackOff()
		if b == backoff.Stop {
			return
		}

		if r.Log != nil {
			ctxlog.Warn(ctx, r.Log.Logger(), "retrying mutation",
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
