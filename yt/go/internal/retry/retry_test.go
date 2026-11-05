package retry

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yterrors"
)

func TestIsRetriableError(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		retries bool
	}{
		{"network", &net.OpError{Op: "read", Err: io.EOF}, true},
		{"dns_not_found", &net.OpError{Op: "dial", Err: &net.DNSError{IsNotFound: true}}, false},
		{"loopback", &net.OpError{Op: "dial", Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}, Err: io.EOF}, false},
		{"http_503", &yterrors.HTTPError{StatusCode: http.StatusServiceUnavailable, Err: io.EOF}, true},
		{"http_400", &yterrors.HTTPError{StatusCode: http.StatusBadRequest, Err: io.EOF}, false},
		{"proxy_banned", yterrors.Err(yterrors.CodeProxyBanned), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.retries, IsRetriableError(tc.err))
		})
	}
}

func TestRetriableErrorTree(t *testing.T) {
	for _, tc := range []struct {
		code    yterrors.ErrorCode
		retries bool
	}{
		{yterrors.CodeTimeout, true},
		{yterrors.CodeAuthorizationError, false},
	} {
		cause := yterrors.Err(tc.code)
		for _, err := range []error{
			fmt.Errorf("request failed: %w", cause),
			yterrors.Err(yterrors.Err(cause), yterrors.Err(yterrors.CodeGeneric)),
			yterrors.Err(yterrors.Err(yterrors.CodeGeneric), yterrors.Err(cause)),
		} {
			require.Equal(t, tc.retries, hasRetriableCode(err), "%v", err)
			require.Equal(t, tc.retries, IsRetriableRPCError(err), "%v", err)
		}
	}

	channel := yterrors.Err(yterrors.CodeProtocolError)
	require.False(t, hasRetriableCode(channel))
	require.True(t, IsRetriableRPCError(channel))
	require.False(t, IsRetriableRPCError(yterrors.Err(channel)))
}

func TestChunkRetryExclusions(t *testing.T) {
	chunk := yterrors.Err(yterrors.CodeAllTargetNodesFailed)
	require.True(t, IsRetriableChunkError(chunk))

	excluded := yterrors.Err(chunk, yterrors.Err(yterrors.CodeSessionAlreadyExists))
	require.False(t, IsRetriableChunkError(excluded))
	require.False(t, hasRetriableCode(excluded))
	require.True(t, hasRetriableCode(yterrors.Err(yterrors.CodeTimeout, excluded)))
}
