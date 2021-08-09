package rpcclient

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/ptr"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/proto/client/api/rpc_proxy"
	"a.yandex-team.ru/yt/go/proto/core/misc"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
)

type netError struct {
	timeout   bool
	temporary bool
}

func (n *netError) Error() string {
	return fmt.Sprintf("%#v", n)
}

func (n *netError) Timeout() bool {
	return n.timeout
}

func (n *netError) Temporary() bool {
	return n.temporary
}

var _ net.Error = &netError{}

func TestMutationRetrier_notMutating(t *testing.T) {
	r := MutationRetrier{}

	call := &Call{
		Req:     NewGetNodeRequest(&rpc_proxy.TReqGetNode{Path: ptr.String(ypath.Root.String())}),
		Backoff: &backoff.ZeroBackOff{},
	}

	var called bool
	var rsp rpc_proxy.TRspGetNode
	err := r.Intercept(context.Background(), call,
		func(ctx context.Context, call *Call, rsp proto.Message) error {
			if called {
				t.Fatalf("get request retried")
			}

			called = true
			return &netError{timeout: true}
		}, &rsp)

	require.Error(t, err)
}

func TestMutationRetries_mutating(t *testing.T) {
	r := MutationRetrier{}

	call := &Call{
		Req:     NewSetNodeRequest(&rpc_proxy.TReqSetNode{Path: ptr.String(ypath.Root.String())}),
		Backoff: &backoff.ZeroBackOff{},
	}

	var i int
	var id *misc.TGuid

	var rsp rpc_proxy.TRspSetNode
	err := r.Intercept(context.Background(), call,
		func(ctx context.Context, call *Call, rsp proto.Message) error {
			opts := call.Req.(*SetNodeRequest).GetMutatingOptions()

			switch i {
			case 0:
				i++
				assert.NotNil(t, opts)
				assert.False(t, opts.GetRetry())
				zero := convertGUID(guid.GUID(yt.MutationID{}))
				assert.NotEqual(t, zero, opts.GetMutationId())

				id = opts.GetMutationId()
				return &netError{timeout: true}

			case 1:
				i++
				assert.NotNil(t, opts)
				assert.True(t, opts.GetRetry())
				assert.Equal(t, id, opts.GetMutationId())

				return &netError{timeout: true}

			case 2:
				i++

				return nil
			}

			t.Fatalf("retry after successful response")
			return nil

		}, &rsp)

	assert.NoError(t, err)
}
