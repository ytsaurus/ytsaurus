package rpcclient

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/internal/retry"
	"go.ytsaurus.tech/yt/go/yt"
)

type MutationRetrier struct {
	Log log.Structured
}

type MutatingRequest interface {
	HasMutatingOptions() bool
	SetMutatingOptions(opts *yt.MutatingOptions)
	SetRetry(retry bool)
}

func (r *MutationRetrier) Intercept(ctx context.Context, call *Call, invoke CallInvoker, rsp proto.Message, opts ...bus.SendOption) (err error) {
	if err = ctx.Err(); err != nil {
		return err
	}

	req, ok := call.Req.(MutatingRequest)
	if !ok || call.DisableRetries {
		return invoke(ctx, call, rsp, opts...)
	}

	generated := !req.HasMutatingOptions()
	if generated {
		req.SetMutatingOptions(
			&yt.MutatingOptions{
				MutationID: yt.MutationID(guid.New()),
				Retry:      false,
			},
		)
	}

	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		err = invoke(ctx, call, rsp, opts...)
		if err == nil {
			return
		}

		if !r.shouldRetry(call.RequestedProxy != "" || call.FixedProxy, err) {
			return
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		b := call.Backoff.NextBackOff()
		if b == backoff.Stop {
			return
		}

		if generated && !retry.IsMutationResultUnknown(err) {
			req.SetMutatingOptions(&yt.MutatingOptions{MutationID: yt.MutationID(guid.New())})
		} else {
			req.SetRetry(true)
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
			return ctx.Err()
		}
	}
}

func (r *MutationRetrier) shouldRetry(hasFixedProxy bool, err error) bool {
	if retry.IsProxyBannedError(err) {
		return !hasFixedProxy
	}

	return retry.IsRetriableError(err) || retry.IsRetriableRPCError(err)
}
