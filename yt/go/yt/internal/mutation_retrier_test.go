package internal

import (
	"context"
	"testing"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestMutationRetrierIgnoresGet(t *testing.T) {
	r := MutationRetrier{}

	var called bool
	_, err := r.Intercept(context.Background(), &Call{
		Params:  NewGetNodeParams(ypath.Root, nil),
		Backoff: &backoff.ZeroBackOff{},
	},
		func(ctx context.Context, call *Call) (*CallResult, error) {
			if called {
				t.Fatalf("get request retried")
			}

			called = true
			return nil, &netError{timeout: true}
		})
	require.Error(t, err)
}

func TestMutationRetriesSet(t *testing.T) {
	r := MutationRetrier{}

	var i int
	var id yt.MutationID

	_, err := r.Intercept(context.Background(), &Call{
		Params:  NewSetNodeParams(ypath.Root, nil),
		Backoff: &backoff.ZeroBackOff{},
	},
		func(ctx context.Context, call *Call) (*CallResult, error) {
			options := call.Params.(*SetNodeParams).options
			switch i {
			case 0:
				i++
				assert.NotNil(t, options.MutatingOptions)
				assert.False(t, options.Retry)
				assert.NotEqual(t, yt.MutationID{}, options.MutationID)

				id = options.MutationID
				return nil, &netError{timeout: true}

			case 1:
				i++
				assert.NotNil(t, options.MutatingOptions)
				assert.True(t, options.Retry)
				assert.Equal(t, id, options.MutationID)

				return nil, &netError{timeout: true}

			case 2:
				i++

				return &CallResult{}, nil
			}

			t.Fatalf("retry after successful response")
			return nil, nil
		})
	assert.NoError(t, err)
}
