package rpcclient

import (
	"context"
	"net"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang/protobuf/proto"
	"golang.org/x/xerrors"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/guid"
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
	req, ok := call.Req.(MutatingRequest)
	if !ok || call.DisableRetries {
		return invoke(ctx, call, rsp, opts...)
	}

	if !req.HasMutatingOptions() {
		req.SetMutatingOptions(
			&yt.MutatingOptions{
				MutationID: yt.MutationID(guid.New()),
				Retry:      false,
			},
		)
	}

	for i := 0; ; i++ {
		err = invoke(ctx, call, rsp, opts...)
		if err == nil || !isNetError(err) {
			return
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req.SetRetry(true)

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
			return ctx.Err()
		}
	}
}

func isNetError(err error) bool {
	var netErr net.Error
	return xerrors.As(err, &netErr)
}
