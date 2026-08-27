package rpcclient

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestAppendAndSplitAttachments(t *testing.T) {
	body := []byte("proto-body")
	a1 := []byte("rowset-a")
	a2 := []byte("rowset-b")
	payload := appendAttachments(body, [][]byte{a1, nil, a2})

	trailer := metadata.Pairs("yt-message-body-size", "10")
	gotBody, atts, err := splitMessage(payload, trailer)
	require.NoError(t, err)
	require.Equal(t, body, gotBody)
	require.Equal(t, [][]byte{a1, nil, a2}, atts)
}

func TestSplitMessageOmittedAttachment(t *testing.T) {
	var buf []byte
	buf = binary.LittleEndian.AppendUint32(buf, math.MaxUint32)
	atts, err := parseAttachments(buf)
	require.NoError(t, err)
	require.Equal(t, [][]byte{nil}, atts)
}

func TestEnsureGRPCAddr(t *testing.T) {
	require.Equal(t, "host:9014", ensureGRPCAddr("host"))
	require.Equal(t, "host:1234", ensureGRPCAddr("host:1234"))
	require.Equal(t, "", ensureGRPCAddr(""))
}

func TestGRPCSetCredentials(t *testing.T) {
	for _, tc := range []struct {
		name  string
		creds yt.Credentials
		key   string
		value string
	}{
		{"token", &yt.TokenCredentials{Token: "secret"}, "yt-auth-token", "secret"},
		{"bearer", &yt.BearerCredentials{Token: "x"}, "yt-auth-token", "x"},
		{"user ticket", &yt.UserTicketCredentials{Ticket: "ut"}, "yt-auth-user-ticket", "ut"},
		{"service ticket", &yt.ServiceTicketCredentials{Ticket: "st"}, "yt-auth-service-ticket", "st"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			md := metadata.MD{}
			require.NoError(t, grpcSetCredentials(md, tc.creds))
			require.Equal(t, []string{tc.value}, md.Get(tc.key))
		})
	}

	md := metadata.MD{}
	err := grpcSetCredentials(md, yt.CookieCredentials{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported credentials type for grpc")
}

func TestGRPCInjectTracing(t *testing.T) {
	call := &Call{CallID: guid.FromHalves(1, 2)}
	traceID := guid.FromHalves(3, 4)

	t.Run("nil TraceFn adds nothing", func(t *testing.T) {
		tr := newGRPCTransport(&client{conf: &yt.Config{}}, nil)
		md, err := grpcRequestMetadata(call, 0, nil)
		require.NoError(t, err)
		tr.injectTracing(context.Background(), md)
		require.Empty(t, md.Get("yt-tracing-trace-id"))
		require.Empty(t, md.Get("yt-tracing-span-id"))
		require.Empty(t, md.Get("yt-tracing-sampled"))
		require.Empty(t, md.Get("yt-tracing-debug"))
	})

	t.Run("unsampled trace omits flag keys", func(t *testing.T) {
		tr := newGRPCTransport(&client{conf: &yt.Config{
			TraceFn: func(context.Context) (guid.GUID, uint64, byte, bool) {
				return traceID, 42, 0, true
			},
		}}, nil)
		md, err := grpcRequestMetadata(call, 0, nil)
		require.NoError(t, err)
		tr.injectTracing(context.Background(), md)
		require.Equal(t, []string{traceID.String()}, md.Get("yt-tracing-trace-id"))
		require.Equal(t, []string{"42"}, md.Get("yt-tracing-span-id"))
		require.Empty(t, md.Get("yt-tracing-sampled"))
		require.Empty(t, md.Get("yt-tracing-debug"))
	})

	t.Run("sampled and debug flags", func(t *testing.T) {
		tr := newGRPCTransport(&client{conf: &yt.Config{
			TraceFn: func(context.Context) (guid.GUID, uint64, byte, bool) {
				return traceID, 7, traceFlagSampled | traceFlagDebug, true
			},
		}}, nil)
		md, err := grpcRequestMetadata(call, 0, nil)
		require.NoError(t, err)
		tr.injectTracing(context.Background(), md)
		require.Equal(t, []string{traceID.String()}, md.Get("yt-tracing-trace-id"))
		require.Equal(t, []string{"7"}, md.Get("yt-tracing-span-id"))
		require.Equal(t, []string{"1"}, md.Get("yt-tracing-sampled"))
		require.Equal(t, []string{"1"}, md.Get("yt-tracing-debug"))
	})

	t.Run("TraceFn ok=false adds nothing", func(t *testing.T) {
		tr := newGRPCTransport(&client{conf: &yt.Config{
			TraceFn: func(context.Context) (guid.GUID, uint64, byte, bool) {
				return guid.GUID{}, 0, 0, false
			},
		}}, nil)
		md, err := grpcRequestMetadata(call, 0, nil)
		require.NoError(t, err)
		tr.injectTracing(context.Background(), md)
		require.Empty(t, md.Get("yt-tracing-trace-id"))
		require.Empty(t, md.Get("yt-tracing-span-id"))
		require.Empty(t, md.Get("yt-tracing-sampled"))
		require.Empty(t, md.Get("yt-tracing-debug"))
	})
}

func TestParseAttachmentsSharesBackingBuffer(t *testing.T) {
	payload := []byte("abcd")
	buf := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(buf, uint32(len(payload)))
	copy(buf[4:], payload)

	atts, err := parseAttachments(buf)
	require.NoError(t, err)
	require.Equal(t, [][]byte{payload}, atts)

	buf[4] = 'Z'
	require.Equal(t, byte('Z'), atts[0][0])
}

func TestParseAttachmentsOversizedLength(t *testing.T) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf, math.MaxUint32-1)
	_, err := parseAttachments(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "truncated attachment")
}

func TestGRPCTransportStop(t *testing.T) {
	tr := newGRPCTransport(&client{conf: &yt.Config{}}, nil)
	tr.Stop()
	_, err := tr.getConn(context.Background(), "host:9014")
	require.ErrorIs(t, err, errGRPCTransportStopped)

	tr.Stop() // idempotent
	_, err = tr.getConn(context.Background(), "host:9014")
	require.ErrorIs(t, err, errGRPCTransportStopped)
}

func TestGRPCTransportDiscardKeepsConn(t *testing.T) {
	tr := newGRPCTransport(&client{conf: &yt.Config{}}, nil)
	defer tr.Stop()

	conn, err := tr.getConn(context.Background(), "host:9014")
	require.NoError(t, err)

	// Banning a proxy must not tear the shared conn down under the calls still in flight on it.
	tr.Discard("host:9014")

	same, err := tr.getConn(context.Background(), "host:9014")
	require.NoError(t, err)
	require.Same(t, conn, same)
}

func TestGRPCTransportRejectsInjectedDialer(t *testing.T) {
	tr := newGRPCTransport(&client{conf: &yt.Config{}}, nil)
	defer tr.Stop()

	ctx := WithDialer(context.Background(), DefaultDial)
	_, err := tr.getConn(ctx, "host:9014")
	require.ErrorIs(t, err, errGRPCDialerUnsupported)
}

func TestNewGRPCClientRejectsTVMOnlyEndpoint(t *testing.T) {
	_, err := NewGRPCClient(&yt.Config{UseTVMOnlyEndpoint: true, Proxy: "localhost"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "UseTVMOnlyEndpoint")
}

func TestGRPCRequestMetadata(t *testing.T) {
	call := &Call{CallID: guid.FromHalves(1, 2)}
	md, err := grpcRequestMetadata(call, 7, &yt.TokenCredentials{Token: "tok"})
	require.NoError(t, err)
	require.Equal(t, []string{strconv.Itoa(ProtocolVersionMajor) + ".0"}, md.Get("yt-protocol-version"))
	require.Equal(t, []string{"7"}, md.Get("yt-message-body-size"))
	require.Equal(t, []string{call.CallID.String()}, md.Get("yt-request-id"))
	require.Equal(t, []string{"tok"}, md.Get("yt-auth-token"))
}

func TestConvertGRPCError(t *testing.T) {
	t.Run("local ctx error wins", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		<-ctx.Done()

		err := convertGRPCError(ctx, status.Error(codes.DeadlineExceeded, "deadline"))
		require.ErrorIs(t, err, context.DeadlineExceeded)

		cancelCtx, cancelFn := context.WithCancel(context.Background())
		cancelFn()
		err = convertGRPCError(cancelCtx, status.Error(codes.Canceled, "canceled"))
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("unavailable is a retryable net error", func(t *testing.T) {
		raw := status.Error(codes.Unavailable, "connection refused")
		err := convertGRPCError(context.Background(), raw)

		var statusErr *grpcStatusError
		require.ErrorAs(t, err, &statusErr)
		require.True(t, isNetError(err))
		require.True(t, shouldBanProxy(err))

		var netErr net.Error
		require.True(t, errors.As(err, &netErr))
		require.False(t, netErr.Timeout())
		require.True(t, netErr.Temporary())
	})

	t.Run("deadline exceeded is a retryable timeout", func(t *testing.T) {
		raw := status.Error(codes.DeadlineExceeded, "server deadline")
		err := convertGRPCError(context.Background(), raw)

		require.True(t, isNetError(err))
		require.False(t, shouldBanProxy(err))

		var netErr net.Error
		require.True(t, errors.As(err, &netErr))
		require.True(t, netErr.Timeout())
	})

	t.Run("other statuses pass through", func(t *testing.T) {
		raw := status.Error(codes.Unauthenticated, "bad token")
		err := convertGRPCError(context.Background(), raw)
		require.Equal(t, raw, err)
		require.False(t, isNetError(err))
		require.False(t, shouldBanProxy(err))
	})
}

func TestGRPCTransportCredentials(t *testing.T) {
	httpTransport := &http.Transport{TLSClientConfig: &tls.Config{}}

	t.Run("plaintext by default", func(t *testing.T) {
		tr := newGRPCTransport(&client{conf: &yt.Config{}}, httpTransport)
		require.Equal(t, "insecure", tr.transportCredentials("host:9014").Info().SecurityProtocol)
	})

	t.Run("tls when enabled", func(t *testing.T) {
		tr := newGRPCTransport(&client{conf: &yt.Config{UseTLS: true}}, httpTransport)
		require.Equal(t, "tls", tr.transportCredentials("host:9014").Info().SecurityProtocol)
	})

	t.Run("tls ignored without tls config", func(t *testing.T) {
		tr := newGRPCTransport(&client{conf: &yt.Config{UseTLS: true}}, &http.Transport{})
		require.Equal(t, "insecure", tr.transportCredentials("host:9014").Info().SecurityProtocol)
	})

	t.Run("server name resolution", func(t *testing.T) {
		original := &tls.Config{}
		httpTransport := &http.Transport{TLSClientConfig: original}

		conf := &yt.Config{UseTLS: true}
		cfg := grpcTLSConfig(conf, httpTransport, "host.example.com:9014")
		require.NotNil(t, cfg)
		require.Equal(t, "host.example.com", cfg.ServerName)
		require.Empty(t, original.ServerName, "original TLS config must not be mutated")

		confWithAlt := &yt.Config{UseTLS: true, PeerAlternativeHostName: "alt.example.com"}
		cfg = grpcTLSConfig(confWithAlt, httpTransport, "host.example.com:9014")
		require.Equal(t, "alt.example.com", cfg.ServerName)
	})
}
