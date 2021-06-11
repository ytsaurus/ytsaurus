package bus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"a.yandex-team.ru/library/go/ptr"
	"a.yandex-team.ru/yt/go/compression"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/proto/core/misc"
	myservice "a.yandex-team.ru/yt/go/proto/core/rpc/unittests"
	"a.yandex-team.ru/yt/go/yterrors"
	"a.yandex-team.ru/yt/internal/go/tcptest"
)

type MyFeature int32

const (
	MyFeatureCool  MyFeature = 0
	MyFeatureGreat MyFeature = 1
)

func (f MyFeature) String() string {
	switch f {
	case MyFeatureCool:
		return "Cool"
	case MyFeatureGreat:
		return "Great"
	}
	return ""
}

func TestClient_Stop(t *testing.T) {
	defer goleak.VerifyNone(t)

	addr, stop := StartMyService(t)
	defer stop()

	t.Run("StopWithoutActiveRequests", func(t *testing.T) {
		conn, err := NewClient(context.Background(), addr)
		require.NoError(t, err)

		conn.Close()
		<-conn.Done()
		require.Error(t, conn.Err())
	})

	t.Run("StopWithActiveRequests", func(t *testing.T) {
		ctx := context.Background()

		conn, err := NewClient(ctx, addr)
		require.NoError(t, err)

		time.AfterFunc(time.Millisecond*500, func() {
			conn.Close()
		})

		req := &myservice.TReqSlowCall{}
		var rsp myservice.TRspSlowCall
		err = conn.Send(ctx, "MyService", "SlowCall", req, &rsp)
		require.Error(t, err)

		<-conn.Done()
		require.Error(t, conn.Err())
	})

	t.Run("NoMoreRequestsAfterStop", func(t *testing.T) {
		ctx := context.Background()

		conn, err := NewClient(context.Background(), addr)
		require.NoError(t, err)

		conn.Close()
		<-conn.Done()

		req := &myservice.TReqSlowCall{}
		var rsp myservice.TRspSlowCall
		err = conn.Send(ctx, "MyService", "SlowCall", req, &rsp)
		require.Error(t, err)
	})
}

func TestClient_errors(t *testing.T) {
	defer goleak.VerifyNone(t)

	addr, stop := StartMyService(t)
	defer stop()

	c, err := NewMyServiceClient(addr)
	require.NoError(t, err)
	defer c.Close()

	t.Run("OK", func(t *testing.T) {
		req := &myservice.TReqDoNothing{}

		_, err := c.DoNothing(context.Background(), req)
		require.NoError(t, err)
	})

	t.Run("Ack", func(t *testing.T) {
		ctx := context.Background()

		req := &myservice.TReqDoNothing{}

		_, err := c.DoNothing(ctx, req, SendOptionAfterFunc(func(req *clientReq) {
			require.True(t, req.acked.Load())
		}))
		require.NoError(t, err)
	})

	t.Run("NoAck", func(t *testing.T) {
		ctx := context.Background()

		req := &myservice.TReqDoNothing{}

		_, err := c.DoNothing(ctx, req, WithoutRequestAcknowledgement(), SendOptionAfterFunc(func(req *clientReq) {
			require.False(t, req.acked.Load())
		}))
		require.NoError(t, err)
	})

	t.Run("AckTimeout", func(t *testing.T) {
		ctx := context.Background()

		req := &myservice.TReqSlowCall{}
		_, err := c.SlowCall(ctx, req, WithAckTimeout(time.Nanosecond))
		require.Error(t, err)
	})

	t.Run("DialError", func(t *testing.T) {
		addr, err := tcptest.GetFreeAddr()
		require.NoError(t, err)

		_, err = NewMyServiceClient(addr)
		require.Error(t, err)
	})

	t.Run("NoService", func(t *testing.T) {
		ctx := context.Background()

		req := &myservice.TReqDoNothing{}
		var rsp myservice.TRspDoNothing

		err := c.conn.Send(ctx, "NonExistingService", "DoNothing", req, &rsp)
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchService))
	})

	t.Run("NoMethod", func(t *testing.T) {
		ctx := context.Background()

		req := &myservice.TReqDoNothing{}
		var rsp myservice.TRspDoNothing

		err := c.conn.Send(ctx, "MyService", "NonExistingMethod", req, &rsp)
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchMethod))
	})

	t.Run("ClientTimeout", func(t *testing.T) {
		ctx := context.Background()

		largeTimeout := 2000 * time.Millisecond
		smallTimeout := 500 * time.Millisecond

		{
			req := &myservice.TReqSlowCall{}
			_, err := c.SlowCall(ctx, req, WithRequestTimeout(largeTimeout))
			require.NoError(t, err)
		}

		{
			req := &myservice.TReqSlowCall{}
			_, err := c.SlowCall(ctx, req, WithRequestTimeout(smallTimeout))
			require.Error(t, err)
			require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeTimeout))
		}
	})

	t.Run("ContextTimeout", func(t *testing.T) {
		const smallTimeout = 500 * time.Millisecond

		ctx, cancel := context.WithTimeout(context.Background(), smallTimeout)
		defer cancel()

		req := &myservice.TReqSlowCall{}
		_, err := c.SlowCall(ctx, req)
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeTimeout))
	})

	t.Run("ClientCancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		time.AfterFunc(time.Millisecond*500, func() {
			cancel()
		})

		req := &myservice.TReqSlowCanceledCall{}
		_, err := c.SlowCanceledCall(ctx, req)
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeCanceled), err)
	})

	t.Run("NoReply", func(t *testing.T) {
		req := &myservice.TReqNoReply{}
		_, err := c.NoReply(context.Background(), req)
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeUnavailable), err)
	})

	t.Run("CustomErrorMessage", func(t *testing.T) {
		req := &myservice.TReqCustomMessageError{}
		_, err := c.CustomMessageError(context.Background(), req)
		require.Error(t, err)
		yterror, ok := err.(*yterrors.Error)
		require.True(t, ok)
		require.Contains(t, yterror.Message, "Some Error")
	})

	t.Run("ProtocolVersionMismatch", func(t *testing.T) {
		req := &myservice.TReqSomeCall{A: ptr.Int32(42)}
		_, err := c.SomeCall(context.Background(), req, WithProtocolVersionMajor(42))
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeProtocolError), err)
	})

	t.Run("InvalidRequestCodec", func(t *testing.T) {
		req := &myservice.TReqPassCall{}
		_, err := c.PassCall(context.Background(), req, WithRequestCodec(-42))
		require.Error(t, err)
	})

	t.Run("InvalidResponseCodec", func(t *testing.T) {
		req := &myservice.TReqPassCall{}
		_, err := c.PassCall(context.Background(), req, WithResponseCodec(-42))
		require.Error(t, err)
	})
}

func TestClient_features(t *testing.T) {
	defer goleak.VerifyNone(t)

	addr, stop := StartMyService(t)
	defer stop()

	t.Run("RequiredServerFeatureSupported", func(t *testing.T) {
		c, err := NewMyServiceClient(addr)
		require.NoError(t, err)
		defer c.Close()

		req := &myservice.TReqPassCall{}
		_, err = c.PassCall(context.Background(), req,
			WithUser("test-user"),
			WithRequiredServerFeatureIDs(int32(MyFeatureGreat)))

		require.NoError(t, err)
	})

	t.Run("RequiredServerFeatureNotSupported", func(t *testing.T) {
		c, err := NewMyServiceClient(addr, WithFeatureIDFormatter(func(i int32) string {
			return MyFeature(i).String()
		}))
		require.NoError(t, err)
		defer c.Close()

		req := &myservice.TReqPassCall{}
		_, err = c.PassCall(context.Background(), req,
			WithUser("test-user"),
			WithRequiredServerFeatureIDs(int32(MyFeatureCool)))

		require.Error(t, err)
		yterror, ok := err.(*yterrors.Error)
		require.True(t, ok)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeUnsupportedServerFeature), err)
		require.Equal(t, int64(MyFeatureCool), yterror.Attributes[string(AttributeKeyFeatureID)])
		require.Equal(t, MyFeatureCool.String(), yterror.Attributes[string(AttributeKeyFeatureName)], yterror.Attributes)
	})

	t.Run("RequiredClientFeatureSupported", func(t *testing.T) {
		c, err := NewMyServiceClient(addr)
		require.NoError(t, err)
		defer c.Close()

		req := &myservice.TReqRequireCoolFeature{}
		_, err = c.RequireCoolFeature(context.Background(), req, WithDeclaredClientFeatureIDs(int32(MyFeatureCool)))
		require.NoError(t, err)
	})

	t.Run("RequiredClientFeatureNotSupported", func(t *testing.T) {
		c, err := NewMyServiceClient(addr, WithFeatureIDFormatter(func(i int32) string {
			return MyFeature(i).String()
		}))
		require.NoError(t, err)
		defer c.Close()

		req := &myservice.TReqRequireCoolFeature{}
		_, err = c.RequireCoolFeature(context.Background(), req, WithDeclaredClientFeatureIDs(int32(MyFeatureGreat)))

		require.Error(t, err)
		yterror, ok := err.(*yterrors.Error)
		require.True(t, ok)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeUnsupportedClientFeature), err)
		require.Equal(t, int64(MyFeatureCool), yterror.Attributes[string(AttributeKeyFeatureID)])
		require.Equal(t, MyFeatureCool.String(), yterror.Attributes[string(AttributeKeyFeatureName)], yterror.Attributes)
	})
}

func TestMyService(t *testing.T) {
	defer goleak.VerifyNone(t)

	addr, stop := StartMyService(t)
	defer stop()

	c, err := NewMyServiceClient(addr)
	require.NoError(t, err)
	defer c.Close()

	t.Run("Send", func(t *testing.T) {
		req := &myservice.TReqSomeCall{A: ptr.Int32(42)}

		rsp, err := c.SomeCall(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, int32(142), *rsp.B)
	})

	t.Run("UserTag", func(t *testing.T) {
		req := &myservice.TReqPassCall{}

		rsp, err := c.PassCall(context.Background(), req,
			WithUser("test-user"),
			WithUserTag("test-user-tag"))

		require.NoError(t, err)
		require.Equal(t, "test-user", *rsp.User)
		require.Equal(t, "test-user-tag", *rsp.UserTag)
	})

	t.Run("SendSimple", func(t *testing.T) {
		req := &myservice.TReqPassCall{}

		mutationID := misc.NewProtoFromGUID(guid.New())
		rsp, err := c.PassCall(context.Background(), req,
			WithUser("test-user"),
			SendOptionBeforeFunc(func(req *clientReq) {
				req.reqHeader.MutationId = mutationID
				req.reqHeader.Retry = ptr.Bool(true)
			}))

		require.NoError(t, err)
		require.Equal(t, "test-user", *rsp.User)
		require.Nil(t, rsp.UserTag)
		require.Equal(t, mutationID.String(), rsp.MutationId.String())
		require.True(t, *rsp.Retry)
	})

	t.Run("ManyAsyncRequests", func(t *testing.T) {
		wg := sync.WaitGroup{}

		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				req := &myservice.TReqSomeCall{A: ptr.Int32(int32(i))}
				rsp, err := c.SomeCall(context.Background(), req)

				require.NoError(t, err)
				require.Equal(t, int32(i+100), *rsp.B)
			}(i)
		}

		wg.Wait()
	})

	t.Run("RegularAttachments", func(t *testing.T) {
		req := &myservice.TReqRegularAttachments{}

		_, err := c.RegularAttachments(context.Background(), req,
			WithAttachmentStrings("Hello", "from", "TMyProxy"),
			SendOptionAfterFunc(func(req *clientReq) {
				require.Len(t, req.rspAttachments, 3)
				require.Equal(t, []byte("Hello_"), req.rspAttachments[0])
				require.Equal(t, []byte("from_"), req.rspAttachments[1])
				require.Equal(t, []byte("TMyProxy_"), req.rspAttachments[2])
			}))

		require.NoError(t, err)
	})

	t.Run("NullAndEmptyAttachments", func(t *testing.T) {
		req := &myservice.TReqNullAndEmptyAttachments{}

		_, err := c.NullAndEmptyAttachments(context.Background(), req,
			WithAttachments(nil, []byte{}),
			SendOptionAfterFunc(func(req *clientReq) {
				require.Len(t, req.rspAttachments, 2)
				//require.Nil(t, req.rspAttachments[0]) // todo @verytable
				require.Empty(t, req.rspAttachments[0])
				require.NotNil(t, req.rspAttachments[1])
				require.Empty(t, req.rspAttachments[1])
			}))

		require.NoError(t, err)
	})

	t.Run("Compression", func(t *testing.T) {
		req := &myservice.TReqCompression{
			RequestCodec: ptr.Int32(int32(compression.CodecIDLz4HighCompression)),
			Message:      ptr.String("This is a message string."),
		}

		rsp, err := c.Compression(context.Background(), req,
			WithAttachmentStrings(
				"This is an attachment string.",
				"640K ought to be enough for anybody.",
				"According to all known laws of aviation, there is no way that a bee should be able to fly.",
			),
			WithResponseCodec(compression.CodecIDLz4HighCompression),
			WithResponseCodec(compression.CodecIDSnappy),
			SendOptionAfterFunc(func(req *clientReq) {
				require.Len(t, req.rspAttachments, len(req.reqAttachments))

				for i := 0; i < len(req.rspAttachments); i++ {
					require.Equal(t, req.reqAttachments[i], req.rspAttachments[i])
				}
			}))

		require.NoError(t, err)
		require.Equal(t, *req.Message, *rsp.Message)
	})
}

func TestClient_stress(t *testing.T) {
	defer goleak.VerifyNone(t)

	addr, stop := StartMyService(t)
	defer stop()

	c, err := NewMyServiceClient(addr)
	require.NoError(t, err)
	defer c.Close()

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			req := &myservice.TReqSomeCall{A: ptr.Int32(int32(i))}
			rsp, err := c.SomeCall(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, int32(i+100), *rsp.B)
		}(i)
	}

	wg.Wait()
}
