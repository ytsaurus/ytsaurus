package internal

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
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

func TestReadOnlyMethods(t *testing.T) {
	for _, p := range []any{
		&GetNodeParams{},
		&ListNodeParams{},
		&NodeExistsParams{},

		&GetOperationParams{},
		&ListOperationsParams{},

		&GetFileFromCacheParams{},
	} {
		_, ok := p.(ReadRetryParams)
		assert.True(t, ok, "%T does not implement ReadRetryParams", p)
	}
}

func TestReadRetrierRetriesGet(t *testing.T) {
	r := &Retrier{Config: &yt.Config{}}

	call := &Call{
		Params:  NewGetNodeParams(ypath.Root, nil),
		Backoff: &backoff.ZeroBackOff{},
	}

	var failed bool

	_, err := r.Intercept(context.Background(), call, func(context.Context, *Call) (*CallResult, error) {
		if !failed {
			failed = true
			return &CallResult{}, xerrors.Errorf("request failed: %w", &netError{timeout: true})
		}

		return &CallResult{}, nil
	})

	assert.True(t, failed)
	assert.NoError(t, err)
}

func TestReadRetrierIgnoresMutations(t *testing.T) {
	r := &Retrier{Config: &yt.Config{}}

	call := &Call{
		Params:  NewSetNodeParams(ypath.Root, nil),
		Backoff: &backoff.ZeroBackOff{},
	}

	_, err := r.Intercept(context.Background(), call, func(context.Context, *Call) (*CallResult, error) {
		return &CallResult{}, xerrors.New("request failed")
	})

	assert.Error(t, err)
}
