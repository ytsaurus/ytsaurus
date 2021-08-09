package rpcclient

import (
	"context"
	"net"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang/protobuf/proto"
	"golang.org/x/xerrors"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/ctxlog"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/yt"
)

type MutationRetrier struct {
	Log log.Structured
}

type MutatingRequest interface {
	SetMutatingOptions(opts *yt.MutatingOptions)
}

func (r *MutationRetrier) Intercept(ctx context.Context, call *Call, invoke CallInvoker, rsp proto.Message) (err error) {
	req, ok := call.Req.(MutatingRequest)
	if !ok || call.DisableRetries {
		return invoke(ctx, call, rsp)
	}

	opts := &yt.MutatingOptions{
		MutationID: yt.MutationID(guid.New()),
		Retry:      false,
	}

	for i := 0; ; i++ {
		req.SetMutatingOptions(opts)

		err = invoke(ctx, call, rsp)
		if err == nil || !isNetError(err) {
			return
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		opts.Retry = true

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
