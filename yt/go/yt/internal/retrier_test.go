package internal

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func checkRetrierAttempts(t *testing.T, call *Call, failure error, retries bool, interceptors ...CallInterceptor) {
	t.Helper()
	call.Backoff = backoff.WithMaxRetries(&backoff.ZeroBackOff{}, 1)
	attempts := 0
	invoke := CallInvoker(func(context.Context, *Call) (*CallResult, error) {
		attempts++
		if attempts == 1 {
			return nil, failure
		}
		return &CallResult{}, nil
	})
	for _, intercept := range interceptors {
		invoke = invoke.Wrap(intercept)
	}
	_, err := invoke(context.Background(), call)
	if retries {
		require.NoError(t, err)
		require.Equal(t, 2, attempts)
	} else {
		require.ErrorIs(t, err, failure)
		require.Equal(t, 1, attempts)
	}
}

func TestRetrierProxyBan(t *testing.T) {
	for _, failure := range []error{
		fmt.Errorf("request failed: %w", yterrors.Err(yterrors.CodeTimeout, yterrors.Err(yterrors.CodeProxyBanned))),
		fmt.Errorf("request failed: %w", yterrors.Err(yterrors.CodeTimeout, yterrors.Err(yterrors.CodePeerBanned))),
		fmt.Errorf("request failed: %w", yterrors.Err("Proxy is banned")),
	} {
		for _, tc := range []struct {
			name    string
			proxy   string
			retries bool
		}{
			{"ordinary", "", true},
			{"sticky", "proxy", false},
		} {
			t.Run(fmt.Sprintf("%s/%v", tc.name, failure), func(t *testing.T) {
				require.Equal(t, tc.retries, (&Retrier{Config: &yt.Config{}}).shouldRetry(false, tc.proxy != "", failure))
				require.Equal(t, tc.retries, (&MutationRetrier{}).shouldRetry(tc.proxy != "", failure))
				call := &Call{
					Params:         NewSetNodeParams(ypath.Root, nil),
					RequestedProxy: tc.proxy,
				}
				checkRetrierAttempts(t, call, failure, tc.retries, (&MutationRetrier{}).Intercept, (&Retrier{Config: &yt.Config{}}).Intercept)
			})
		}
	}
}

func TestRetrierMutationIDOnRetry(t *testing.T) {
	for _, tc := range []struct {
		failure error
		sameID  bool
	}{
		{&netError{timeout: true}, true},
		{yterrors.Err(yterrors.Err(yterrors.CodeTimeout)), true},
		{yterrors.Err(yterrors.CodeUnavailable), true},
		{NewHTTPError(http.StatusBadGateway, http.Header{}, nil), true},
		{yterrors.Err(yterrors.CodeRequestQueueSizeLimitExceeded), false},
	} {
		for _, userID := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/user_id=%t", tc.failure, userID), func(t *testing.T) {
				options := &yt.SetNodeOptions{}
				if userID {
					options.MutatingOptions = &yt.MutatingOptions{MutationID: yt.MutationID(guid.New())}
				}
				call := &Call{
					Params:  NewSetNodeParams(ypath.Root, options),
					Backoff: backoff.WithMaxRetries(&backoff.ZeroBackOff{}, 1),
				}
				var first yt.MutatingOptions
				attempts := 0
				invoke := CallInvoker(func(_ context.Context, call *Call) (*CallResult, error) {
					attempts++
					mut := **call.Params.(MutatingParams).MutatingOptions()
					if attempts == 1 {
						first = mut
						return nil, tc.failure
					}
					sameID := tc.sameID || userID
					require.Equal(t, sameID, mut.MutationID == first.MutationID)
					require.Equal(t, sameID, mut.Retry)
					return &CallResult{}, nil
				}).Wrap((&MutationRetrier{}).Intercept).Wrap((&Retrier{Config: &yt.Config{}}).Intercept)
				_, err := invoke(context.Background(), call)
				require.NoError(t, err)
				require.Equal(t, 2, attempts)
			})
		}
	}
}

func TestRetrierMutationRetryPolicy(t *testing.T) {
	networkFailure := &net.OpError{Op: "read", Err: io.EOF}
	for _, tc := range []struct {
		params              Params
		retriesNetworkError bool
	}{
		{NewAbortQueryParams(yt.QueryID(guid.New()), nil), false},
		{NewStartTxParams(nil), true},
	} {
		for _, failure := range []error{
			yterrors.Err(yterrors.CodeTimeout),
			networkFailure,
		} {
			t.Run(fmt.Sprintf("%T/%v", tc.params, failure), func(t *testing.T) {
				retries := failure != networkFailure || tc.retriesNetworkError
				checkRetrierAttempts(t, &Call{Params: tc.params}, failure, retries, (&MutationRetrier{}).Intercept, (&Retrier{Config: &yt.Config{}}).Intercept)
			})
		}
	}
}

func TestRetrierLeavesMutationWithIDToMutationRetrier(t *testing.T) {
	options := &yt.SetNodeOptions{MutatingOptions: &yt.MutatingOptions{MutationID: yt.MutationID(guid.New())}}
	for _, failure := range []error{
		yterrors.Err(yterrors.CodeTimeout),
		NewHTTPError(http.StatusBadGateway, http.Header{}, nil),
	} {
		checkRetrierAttempts(t, &Call{Params: NewSetNodeParams(ypath.Root, options)}, failure, false, (&Retrier{Config: &yt.Config{}}).Intercept)
	}
}

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
	invoke := CallInvoker(func(ctx context.Context, c *Call) (*CallResult, error) {
		return ew.Intercept(ctx, c, func(context.Context, *Call) (*CallResult, error) {
			attempts++
			if attempts == 1 {
				return nil, NewHTTPError(http.StatusBadGateway, http.Header{}, nil)
			}
			return &CallResult{}, nil
		})
	}).Wrap((&MutationRetrier{}).Intercept).Wrap(r.Intercept)
	_, err := invoke(context.Background(), call)

	require.NoError(t, err)
	assert.Equal(t, 2, attempts)
}
