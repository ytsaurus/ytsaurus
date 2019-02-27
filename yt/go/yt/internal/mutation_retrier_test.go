package internal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/yt"

	"github.com/stretchr/testify/assert"
)

func TestMutationRetrierIgnoresGet(t *testing.T) {
	r := MutationRetrier{Backoff: &zeroBackoff{}}

	var called bool
	_, err := r.Intercept(context.Background(), &Call{Params: NewGetNodeParams("", nil)},
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
	r := MutationRetrier{Backoff: &zeroBackoff{}}

	var i int
	var id yt.MutationID

	_, err := r.Intercept(context.Background(), &Call{Params: NewSetNodeParams("", nil)},
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
