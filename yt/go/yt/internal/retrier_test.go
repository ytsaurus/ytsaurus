package internal

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestReadRetrierHeavyRequestHasNoLightTimeout(t *testing.T) {
	r := &Retrier{Config: &yt.Config{}} // default LightRequestTimeout is non-zero.

	checkDeadline := func(params Params, wantDeadline bool) {
		call := &Call{
			Params:  params,
			Backoff: &backoff.ZeroBackOff{},
		}
		_, err := r.Intercept(context.Background(), call, func(ctx context.Context, _ *Call) (*CallResult, error) {
			_, hasDeadline := ctx.Deadline()
			assert.Equal(t, wantDeadline, hasDeadline)
			return &CallResult{}, nil
		})
		require.NoError(t, err)
	}

	// Light request gets the light-request timeout.
	checkDeadline(NewGetNodeParams(ypath.Root, nil), true)
	// Heavy request (buffered write_table) must not inherit the light-request timeout.
	checkDeadline(NewWriteTableParams(ypath.Root, nil), false)
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

func TestReadRetrierRetriesMutationOnHTTPGatewayError(t *testing.T) {
	gatewayCodes := []int{
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	}

	for _, code := range gatewayCodes {
		t.Run(fmt.Sprintf("status_%d", code), func(t *testing.T) {
			r := &Retrier{Config: &yt.Config{}}
			call := &Call{
				Params:  NewSetNodeParams(ypath.Root, nil),
				Backoff: &backoff.ZeroBackOff{},
			}

			var attempts int
			_, err := r.Intercept(context.Background(), call, func(context.Context, *Call) (*CallResult, error) {
				attempts++
				if attempts == 1 {
					return nil, NewHTTPError(code, http.Header{}, nil)
				}
				return &CallResult{}, nil
			})

			require.NoError(t, err)
			assert.Equal(t, 2, attempts)
		})
	}
}

func TestReadRetrierRetriesMutationOnAnnotatedHTTPGatewayError(t *testing.T) {
	r := &Retrier{Config: &yt.Config{}}
	call := &Call{
		Params:  NewSetNodeParams(ypath.Root, nil),
		Backoff: &backoff.ZeroBackOff{},
	}

	var attempts int
	ew := &ErrorWrapper{}
	_, err := r.Intercept(context.Background(), call, func(ctx context.Context, c *Call) (*CallResult, error) {
		return ew.Intercept(ctx, c, func(context.Context, *Call) (*CallResult, error) {
			attempts++
			if attempts == 1 {
				return nil, NewHTTPError(http.StatusBadGateway, http.Header{}, nil)
			}
			return &CallResult{}, nil
		})
	})

	require.NoError(t, err)
	assert.Equal(t, 2, attempts)
}
