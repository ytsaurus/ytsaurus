package rpcclient

import (
	"context"
	"testing"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"golang.org/x/xerrors"

	"a.yandex-team.ru/library/go/ptr"
	"a.yandex-team.ru/yt/go/proto/client/api/rpc_proxy"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
)

func TestReadRetrier_readRequest(t *testing.T) {
	r := &Retrier{Config: &yt.Config{}}

	call := &Call{
		Req:     NewGetNodeRequest(&rpc_proxy.TReqGetNode{Path: ptr.String(ypath.Root.String())}),
		Backoff: &backoff.ZeroBackOff{},
	}

	var failed bool

	var rsp rpc_proxy.TRspGetNode
	err := r.Intercept(context.Background(), call, func(context.Context, *Call, proto.Message) error {
		if !failed {
			failed = true
			return xerrors.Errorf("request failed: %w", &netError{timeout: true})
		}

		return nil
	}, &rsp)

	assert.True(t, failed)
	assert.NoError(t, err)
}

func TestReadRetrier_mutating(t *testing.T) {
	r := &Retrier{Config: &yt.Config{}}

	call := &Call{
		Req:     NewSetNodeRequest(&rpc_proxy.TReqSetNode{Path: ptr.String(ypath.Root.String())}),
		Backoff: &backoff.ZeroBackOff{},
	}

	var rsp rpc_proxy.TRspSetNode
	err := r.Intercept(context.Background(), call, func(context.Context, *Call, proto.Message) error {
		return xerrors.New("request failed")
	}, &rsp)

	assert.Error(t, err)
}
