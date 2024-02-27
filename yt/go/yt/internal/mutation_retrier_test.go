package internal

import (
	"context"
	"testing"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/guid"
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

type mutationRetriesSetTestCase struct {
	name string
	opts *yt.MutatingOptions
}

func TestMutationRetriesSet(t *testing.T) {
	tests := []mutationRetriesSetTestCase{
		{
			name: "no-options",
			opts: nil,
		},
		{
			name: "no-retry",
			opts: &yt.MutatingOptions{MutationID: yt.MutationID(guid.New()), Retry: false},
		},
		{
			name: "retry",
			opts: &yt.MutatingOptions{MutationID: yt.MutationID(guid.New()), Retry: true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { testMutationRetriesSet(t, tc) })
	}
}

func testMutationRetriesSet(t *testing.T, tc mutationRetriesSetTestCase) {
	r := MutationRetrier{}

	var i int
	var id yt.MutationID

	retryCopy := false
	if tc.opts != nil {
		retryCopy = tc.opts.Retry
	}

	_, err := r.Intercept(
		context.Background(),
		&Call{
			Params: NewSetNodeParams(
				ypath.Root,
				&yt.SetNodeOptions{MutatingOptions: tc.opts},
			),
			Backoff: &backoff.ZeroBackOff{},
		},
		func(ctx context.Context, call *Call) (*CallResult, error) {
			options := call.Params.(*SetNodeParams).options
			switch i {
			case 0:
				i++
				assert.NotNil(t, options.MutatingOptions)
				assert.Equal(t, retryCopy, options.Retry)
				assert.NotEqual(t, yt.MutationID{}, options.MutationID)
				if tc.opts != nil {
					assert.Equal(t, *tc.opts, *options.MutatingOptions)
				}

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
		},
	)

	assert.NoError(t, err)

	if tc.opts != nil {
		assert.Equal(t, retryCopy, tc.opts.Retry)
	}
}
