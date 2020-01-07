package internal

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"a.yandex-team.ru/yt/go/yterrors"

	"github.com/stretchr/testify/assert"
	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/ypath"
)

type zeroBackoff struct{}

func (b *zeroBackoff) Backoff(int) (time.Duration, bool) {
	return 0, true
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

func TestReadTransientErrors(t *testing.T) {
	for _, e := range []error{
		&netError{timeout: true},
		&netError{temporary: true},
		xerrors.Errorf("error: %w", &netError{timeout: true}),
		yterrors.Err(yterrors.ErrorCode(1000000)),
		xerrors.Errorf("error: %w", yterrors.Err(yterrors.ErrorCode(1000000))),
	} {
		assert.True(t, isTransientError(e), "%+v", e)
	}
}

func TestReadFatalErrors(t *testing.T) {
	for _, e := range []error{
		yterrors.Err(yterrors.ErrorCode(500)),
		&netError{},
	} {
		assert.False(t, isTransientError(e), "%+v", e)
	}
}

func TestReadOnlyMethods(t *testing.T) {
	for _, p := range []interface{}{
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
	r := &ReadRetrier{Backoff: &zeroBackoff{}}

	call := &Call{Params: NewGetNodeParams(ypath.Root, nil)}

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
	r := &ReadRetrier{Backoff: &zeroBackoff{}}

	call := &Call{Params: NewSetNodeParams(ypath.Root, nil)}

	_, err := r.Intercept(context.Background(), call, func(context.Context, *Call) (*CallResult, error) {
		return &CallResult{}, xerrors.New("request failed")
	})

	assert.Error(t, err)
}
