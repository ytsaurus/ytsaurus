package bus

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/compression"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/core/misc"
	testservice "go.ytsaurus.tech/yt/go/proto/core/rpc/unittests"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func protoResponse(msg proto.Message) (*Response, error) {
	body, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return &Response{Body: body}, nil
}

func registerTestService(server *Server) {
	server.RegisterMethod("TestService", "DoNothing", func(ctx context.Context, req *Request) (*Response, error) {
		return protoResponse(&testservice.TRspDoNothing{})
	})

	server.RegisterMethod("TestService", "SomeCall", func(ctx context.Context, req *Request) (*Response, error) {
		var body testservice.TReqSomeCall
		if err := proto.Unmarshal(req.Body, &body); err != nil {
			return nil, err
		}
		return protoResponse(&testservice.TRspSomeCall{B: ptr.Int32(body.GetA() + 100)})
	})

	server.RegisterMethod("TestService", "PassCall", func(ctx context.Context, req *Request) (*Response, error) {
		mutationID := req.Header.MutationId
		if mutationID == nil {
			mutationID = misc.NewProtoFromGUID(guid.GUID{})
		}
		return protoResponse(&testservice.TRspPassCall{
			User:       req.Header.User,
			UserTag:    req.Header.UserTag,
			MutationId: mutationID,
			Retry:      ptr.Bool(req.Header.GetRetry()),
		})
	})

	server.RegisterMethod("TestService", "RegularAttachments", func(ctx context.Context, req *Request) (*Response, error) {
		rsp, err := protoResponse(&testservice.TRspRegularAttachments{})
		if err != nil {
			return nil, err
		}
		for _, a := range req.Attachments {
			attachment := make([]byte, 0, len(a)+1)
			attachment = append(attachment, a...)
			attachment = append(attachment, '_')
			rsp.Attachments = append(rsp.Attachments, attachment)
		}
		return rsp, nil
	})

	server.RegisterMethod("TestService", "NullAndEmptyAttachments", func(ctx context.Context, req *Request) (*Response, error) {
		rsp, err := protoResponse(&testservice.TRspNullAndEmptyAttachments{})
		if err != nil {
			return nil, err
		}
		rsp.Attachments = req.Attachments
		return rsp, nil
	})

	server.RegisterMethod("TestService", "Compression", func(ctx context.Context, req *Request) (*Response, error) {
		var body testservice.TReqCompression
		if err := proto.Unmarshal(req.Body, &body); err != nil {
			return nil, err
		}
		rsp, err := protoResponse(&testservice.TRspCompression{Message: body.Message})
		if err != nil {
			return nil, err
		}
		rsp.Attachments = req.Attachments
		return rsp, nil
	})

	server.RegisterMethod("TestService", "SlowCall", func(ctx context.Context, req *Request) (*Response, error) {
		select {
		case <-time.After(time.Second):
			return protoResponse(&testservice.TRspSlowCall{})
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	server.RegisterMethod("TestService", "SlowCanceledCall", func(ctx context.Context, req *Request) (*Response, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})

	server.RegisterMethod("TestService", "CustomMessageError", func(ctx context.Context, req *Request) (*Response, error) {
		return nil, yterrors.Err(
			yterrors.ErrorCode(42),
			"Some Error",
			yterrors.Attr("attr_key", "attr_value"),
			yterrors.Err(yterrors.CodeGeneric, "Inner Error"))
	})

	server.RegisterMethod("TestService", "RequireCoolFeature", func(ctx context.Context, req *Request) (*Response, error) {
		for _, id := range req.Header.DeclaredClientFeatureIds {
			if id == int32(TestFeatureCool) {
				return protoResponse(&testservice.TRspRequireCoolFeature{})
			}
		}
		return nil, yterrors.Err(
			yterrors.CodeUnsupportedClientFeature,
			"Client does not support the feature required by server",
			yterrors.Attr(string(AttributeKeyFeatureID), int64(TestFeatureCool)),
			yterrors.Attr(string(AttributeKeyFeatureName), TestFeatureCool.String()))
	})

	server.RegisterMethod("TestService", "Panic", func(ctx context.Context, req *Request) (*Response, error) {
		panic("no way")
	})
}

func startTestServer(t *testing.T, server *Server) (addr string, stop func()) {
	t.Helper()

	lsn, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- server.Serve(lsn)
	}()

	stop = func() {
		server.Close()
		require.ErrorIs(t, <-done, ErrServerStopped)
	}

	return lsn.Addr().String(), stop
}

func startGoTestService(t *testing.T, opts ...ServerOption) (addr string, stop func()) {
	t.Helper()

	server := NewServer(opts...)
	server.DeclareServerFeature(int32(TestFeatureGreat))
	registerTestService(server)

	return startTestServer(t, server)
}

func TestServer_calls(t *testing.T) {
	defer goleak.VerifyNone(t)

	addr, stop := startGoTestService(t)
	defer stop()

	c := NewTestServiceClient(addr)
	defer func() {
		c.Close()
		<-c.conn.Done()
	}()

	t.Run("Send", func(t *testing.T) {
		req := &testservice.TReqSomeCall{A: ptr.Int32(42)}

		rsp, err := c.SomeCall(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, int32(142), *rsp.B)
	})

	t.Run("UserTag", func(t *testing.T) {
		req := &testservice.TReqPassCall{}

		rsp, err := c.PassCall(context.Background(), req,
			WithUser("test-user"),
			WithUserTag("test-user-tag"))

		require.NoError(t, err)
		require.Equal(t, "test-user", *rsp.User)
		require.Equal(t, "test-user-tag", *rsp.UserTag)
	})

	t.Run("SendSimple", func(t *testing.T) {
		req := &testservice.TReqPassCall{}

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

	t.Run("RegularAttachments", func(t *testing.T) {
		req := &testservice.TReqRegularAttachments{}

		_, err := c.RegularAttachments(context.Background(), req,
			WithAttachmentStrings("Hello", "from", "TTestProxy"),
			SendOptionAfterFunc(func(req *clientReq) {
				require.Len(t, req.rspAttachments, 3)
				require.Equal(t, []byte("Hello_"), req.rspAttachments[0])
				require.Equal(t, []byte("from_"), req.rspAttachments[1])
				require.Equal(t, []byte("TTestProxy_"), req.rspAttachments[2])
			}))

		require.NoError(t, err)
	})

	t.Run("NullAndEmptyAttachments", func(t *testing.T) {
		req := &testservice.TReqNullAndEmptyAttachments{}

		_, err := c.NullAndEmptyAttachments(context.Background(), req,
			WithAttachments(nil, []byte{}),
			SendOptionAfterFunc(func(req *clientReq) {
				require.Len(t, req.rspAttachments, 2)
				require.Nil(t, req.rspAttachments[0])
				require.NotNil(t, req.rspAttachments[1])
				require.Empty(t, req.rspAttachments[1])
			}))

		require.NoError(t, err)
	})

	t.Run("Compression", func(t *testing.T) {
		codecs := []compression.CodecID{
			compression.CodecIDNone,
			compression.CodecIDSnappy,
			compression.CodecIDLz4,
			compression.CodecIDLz4HighCompression,
			compression.CodecIDBrotli3,
			compression.CodecIDZlib6,
			compression.CodecIDZstd1,
		}

		for _, codecID := range codecs {
			t.Run(codecID.String(), func(t *testing.T) {
				req := &testservice.TReqCompression{
					RequestCodec: ptr.Int32(int32(codecID)),
					Message:      ptr.String("This is a message string."),
				}

				rsp, err := c.Compression(context.Background(), req,
					WithAttachmentStrings(
						"This is an attachment string.",
						"640K ought to be enough for anybody.",
					),
					WithRequestCodec(codecID),
					WithResponseCodec(codecID),
					SendOptionAfterFunc(func(req *clientReq) {
						require.Equal(t, req.reqAttachments, req.rspAttachments)
					}))

				require.NoError(t, err)
				require.Equal(t, *req.Message, *rsp.Message)
			})
		}
	})

	t.Run("MixedCodecs", func(t *testing.T) {
		req := &testservice.TReqCompression{
			RequestCodec: ptr.Int32(int32(compression.CodecIDLz4)),
			Message:      ptr.String("This is a message string."),
		}

		rsp, err := c.Compression(context.Background(), req,
			WithAttachmentStrings("This is an attachment string."),
			WithRequestCodec(compression.CodecIDLz4),
			WithResponseCodec(compression.CodecIDSnappy),
			SendOptionAfterFunc(func(req *clientReq) {
				require.Equal(t, req.reqAttachments, req.rspAttachments)
			}))

		require.NoError(t, err)
		require.Equal(t, *req.Message, *rsp.Message)
	})
}

func TestServer_errors(t *testing.T) {
	defer goleak.VerifyNone(t)

	addr, stop := startGoTestService(t)
	defer stop()

	c := NewTestServiceClient(addr)
	defer func() {
		c.Close()
		<-c.conn.Done()
	}()

	t.Run("NoService", func(t *testing.T) {
		req := &testservice.TReqDoNothing{}
		var rsp testservice.TRspDoNothing

		err := c.conn.Send(context.Background(), "NonExistingService", "DoNothing", req, &rsp)
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchService))
	})

	t.Run("NoMethod", func(t *testing.T) {
		req := &testservice.TReqDoNothing{}
		var rsp testservice.TRspDoNothing

		err := c.conn.Send(context.Background(), "TestService", "NonExistingMethod", req, &rsp)
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchMethod))
	})

	t.Run("CustomErrorMessage", func(t *testing.T) {
		req := &testservice.TReqCustomMessageError{}
		_, err := c.CustomMessageError(context.Background(), req)
		require.Error(t, err)

		yterror, ok := err.(*yterrors.Error)
		require.True(t, ok)
		require.Equal(t, yterrors.ErrorCode(42), yterror.Code)
		require.Contains(t, yterror.Message, "Some Error")
		require.Equal(t, "attr_value", yterror.Attributes["attr_key"])
		require.Len(t, yterror.InnerErrors, 1)
		require.Equal(t, "Inner Error", yterror.InnerErrors[0].Message)
	})

	t.Run("InvalidRequestCodec", func(t *testing.T) {
		req := &testservice.TReqPassCall{}
		_, err := c.PassCall(context.Background(), req, WithRequestCodec(-42))
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeProtocolError))
	})

	t.Run("InvalidResponseCodec", func(t *testing.T) {
		req := &testservice.TReqPassCall{}
		_, err := c.PassCall(context.Background(), req, WithResponseCodec(-42))
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeProtocolError))
	})

	t.Run("HandlerPanic", func(t *testing.T) {
		req := &testservice.TReqDoNothing{}
		var rsp testservice.TRspDoNothing

		err := c.conn.Send(context.Background(), "TestService", "Panic", req, &rsp)
		require.Error(t, err)

		yterror, ok := err.(*yterrors.Error)
		require.True(t, ok)
		require.Contains(t, yterror.Message, "panicked")

		// The connection survives a handler panic.
		_, err = c.DoNothing(context.Background(), &testservice.TReqDoNothing{})
		require.NoError(t, err)
	})
}

func TestServer_acks(t *testing.T) {
	defer goleak.VerifyNone(t)

	addr, stop := startGoTestService(t)
	defer stop()

	c := NewTestServiceClient(addr)
	defer func() {
		c.Close()
		<-c.conn.Done()
	}()

	t.Run("Ack", func(t *testing.T) {
		req := &testservice.TReqDoNothing{}

		_, err := c.DoNothing(context.Background(), req, SendOptionAfterFunc(func(req *clientReq) {
			require.True(t, req.acked.Load())
		}))
		require.NoError(t, err)
	})

	t.Run("NoAck", func(t *testing.T) {
		req := &testservice.TReqDoNothing{}

		_, err := c.DoNothing(context.Background(), req, WithoutRequestAcknowledgement(), SendOptionAfterFunc(func(req *clientReq) {
			require.False(t, req.acked.Load())
		}))
		require.NoError(t, err)
	})

	t.Run("AckTimeout", func(t *testing.T) {
		req := &testservice.TReqSlowCall{}
		_, err := c.SlowCall(context.Background(), req, WithAckTimeout(time.Nanosecond))
		require.Error(t, err)
	})
}

func TestServer_features(t *testing.T) {
	defer goleak.VerifyNone(t)

	addr, stop := startGoTestService(t)
	defer stop()

	t.Run("RequiredServerFeatureSupported", func(t *testing.T) {
		c := NewTestServiceClient(addr)
		defer func() {
			c.Close()
			<-c.conn.Done()
		}()

		req := &testservice.TReqPassCall{}
		_, err := c.PassCall(context.Background(), req,
			WithUser("test-user"),
			WithRequiredServerFeatureIDs(int32(TestFeatureGreat)))

		require.NoError(t, err)
	})

	t.Run("RequiredServerFeatureNotSupported", func(t *testing.T) {
		c := NewTestServiceClient(addr, WithFeatureIDFormatter(func(i int32) string {
			return TestFeature(i).String()
		}))
		defer func() {
			c.Close()
			<-c.conn.Done()
		}()

		req := &testservice.TReqPassCall{}
		_, err := c.PassCall(context.Background(), req,
			WithUser("test-user"),
			WithRequiredServerFeatureIDs(int32(TestFeatureCool)))

		require.Error(t, err)
		yterror, ok := err.(*yterrors.Error)
		require.True(t, ok)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeUnsupportedServerFeature), err)
		require.Equal(t, int64(TestFeatureCool), yterror.Attributes[string(AttributeKeyFeatureID)])
		require.Equal(t, TestFeatureCool.String(), yterror.Attributes[string(AttributeKeyFeatureName)], yterror.Attributes)
	})

	t.Run("RequiredClientFeatureSupported", func(t *testing.T) {
		c := NewTestServiceClient(addr)
		defer func() {
			c.Close()
			<-c.conn.Done()
		}()

		req := &testservice.TReqRequireCoolFeature{}
		_, err := c.RequireCoolFeature(context.Background(), req, WithDeclaredClientFeatureIDs(int32(TestFeatureCool)))
		require.NoError(t, err)
	})

	t.Run("RequiredClientFeatureNotSupported", func(t *testing.T) {
		c := NewTestServiceClient(addr)
		defer func() {
			c.Close()
			<-c.conn.Done()
		}()

		req := &testservice.TReqRequireCoolFeature{}
		_, err := c.RequireCoolFeature(context.Background(), req, WithDeclaredClientFeatureIDs(int32(TestFeatureGreat)))

		require.Error(t, err)
		yterror, ok := err.(*yterrors.Error)
		require.True(t, ok)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeUnsupportedClientFeature), err)
		require.Equal(t, int64(TestFeatureCool), yterror.Attributes[string(AttributeKeyFeatureID)])
		require.Equal(t, TestFeatureCool.String(), yterror.Attributes[string(AttributeKeyFeatureName)], yterror.Attributes)
	})
}

func TestServer_cancelation(t *testing.T) {
	defer goleak.VerifyNone(t)

	handlerErrs := make(chan error, 2)

	server := NewServer()
	server.RegisterMethod("TestService", "Wait", func(ctx context.Context, req *Request) (*Response, error) {
		<-ctx.Done()
		handlerErrs <- ctx.Err()
		return nil, ctx.Err()
	})

	addr, stop := startTestServer(t, server)
	defer stop()

	conn := NewClient(context.Background(), addr)
	defer func() {
		conn.Close()
		<-conn.Done()
	}()

	t.Run("ClientCancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		time.AfterFunc(100*time.Millisecond, cancel)

		var rsp testservice.TRspDoNothing
		err := conn.Send(ctx, "TestService", "Wait", &testservice.TReqDoNothing{}, &rsp)
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeCanceled), err)

		select {
		case err := <-handlerErrs:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(10 * time.Second):
			t.Fatal("handler was not canceled")
		}
	})

	t.Run("ServerSideTimeout", func(t *testing.T) {
		// The client deadline is far away, so the timeout error must be
		// produced by the server.
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		var rsp testservice.TRspDoNothing
		err := conn.Send(ctx, "TestService", "Wait", &testservice.TReqDoNothing{}, &rsp,
			WithRequestTimeout(100*time.Millisecond))
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeTimeout), err)

		select {
		case err := <-handlerErrs:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(10 * time.Second):
			t.Fatal("handler was not canceled")
		}
	})
}

func TestServer_TLS(t *testing.T) {
	defer goleak.VerifyNone(t)

	certPEM, keyPEM, err := generateSelfSignedECDSA()
	require.NoError(t, err)

	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	rootCAs := x509.NewCertPool()
	require.True(t, rootCAs.AppendCertsFromPEM(certPEM))

	clientTLSConfig := &tls.Config{
		ServerName: "localhost",
		RootCAs:    rootCAs,
	}
	serverTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{certificate},
	}

	tests := []struct {
		name                       string
		encryptedConn              bool
		handshakeFailure           bool
		clientMode, serverMode     EncryptionMode
		clientConfig, serverConfig *tls.Config
	}{
		{"Disabled-Disabled", false, false, EncryptionModeDisabled, EncryptionModeDisabled, nil, nil},
		{"Disabled-Required", false, true, EncryptionModeDisabled, EncryptionModeRequired, nil, serverTLSConfig},
		{"Required-Disabled", false, true, EncryptionModeRequired, EncryptionModeDisabled, clientTLSConfig, nil},
		{"Optional-Optional", false, false, EncryptionModeOptional, EncryptionModeOptional, clientTLSConfig, serverTLSConfig},
		{"Optional-Required", true, false, EncryptionModeOptional, EncryptionModeRequired, clientTLSConfig, serverTLSConfig},
		{"Required-Optional", true, false, EncryptionModeRequired, EncryptionModeOptional, clientTLSConfig, serverTLSConfig},
		{"Required-Required", true, false, EncryptionModeRequired, EncryptionModeRequired, clientTLSConfig, serverTLSConfig},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			addr, stop := startGoTestService(t,
				WithServerEncryptionMode(test.serverMode),
				WithServerTLSConfig(test.serverConfig))
			defer stop()

			c := NewTestServiceClient(addr,
				WithEncryptionMode(test.clientMode),
				WithTLSConfig(test.clientConfig))
			defer func() {
				c.Close()
				<-c.conn.Done()
			}()

			if test.handshakeFailure {
				<-c.conn.Done()
				require.Error(t, c.conn.Err())
				return
			}

			rsp, err := c.SomeCall(context.Background(), &testservice.TReqSomeCall{A: ptr.Int32(42)})
			require.NoError(t, err)
			require.Equal(t, int32(142), *rsp.B)

			if tlsConn, ok := c.conn.bus.conn.(*tls.Conn); ok {
				require.True(t, test.encryptedConn)
				require.True(t, tlsConn.ConnectionState().HandshakeComplete)
			} else {
				require.False(t, test.encryptedConn)
			}
		})
	}
}

func TestServer_concurrency(t *testing.T) {
	defer goleak.VerifyNone(t)

	addr, stop := startGoTestService(t)
	defer stop()

	var wg sync.WaitGroup
	for clientIndex := 0; clientIndex < 4; clientIndex++ {
		c := NewTestServiceClient(addr)
		defer func() {
			c.Close()
			<-c.conn.Done()
		}()

		for i := 0; i < 250; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				req := &testservice.TReqSomeCall{A: ptr.Int32(int32(i))}
				rsp, err := c.SomeCall(context.Background(), req)
				require.NoError(t, err)
				require.Equal(t, int32(i+100), *rsp.B)
			}(i)
		}
	}

	wg.Wait()
}

func TestServer_close(t *testing.T) {
	defer goleak.VerifyNone(t)

	started := make(chan struct{})

	server := NewServer()
	server.RegisterMethod("TestService", "Wait", func(ctx context.Context, req *Request) (*Response, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})

	lsn, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(lsn)
	}()

	conn := NewClient(context.Background(), lsn.Addr().String())
	defer func() {
		conn.Close()
		<-conn.Done()
	}()

	sendDone := make(chan error, 1)
	go func() {
		var rsp testservice.TRspDoNothing
		sendDone <- conn.Send(context.Background(), "TestService", "Wait", &testservice.TReqDoNothing{}, &rsp)
	}()

	<-started
	server.Close()

	require.Error(t, <-sendDone)
	require.ErrorIs(t, <-serveDone, ErrServerStopped)

	// Serve on a stopped server closes the listener and returns immediately.
	lsn, err = net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	require.ErrorIs(t, server.Serve(lsn), ErrServerStopped)
}

func TestServer_errorProtoConversion(t *testing.T) {
	err := yterrors.Err(
		yterrors.CodeTimeout,
		"Outer Error",
		yterrors.Attr("string_attr", "value"),
		yterrors.Attr("int_attr", int64(42)),
		yterrors.Err(yterrors.CodeCanceled, "Inner Error"))

	converted := misc.NewErrorFromProto(misc.NewProtoFromError(err))
	require.Error(t, converted)

	yterror, ok := converted.(*yterrors.Error)
	require.True(t, ok)
	require.Equal(t, yterrors.CodeTimeout, yterror.Code)
	require.Equal(t, "Outer Error", yterror.Message)
	require.Equal(t, "value", yterror.Attributes["string_attr"])
	require.Equal(t, int64(42), yterror.Attributes["int_attr"])
	require.Len(t, yterror.InnerErrors, 1)
	require.Equal(t, yterrors.CodeCanceled, yterror.InnerErrors[0].Code)
	require.Equal(t, "Inner Error", yterror.InnerErrors[0].Message)
}
