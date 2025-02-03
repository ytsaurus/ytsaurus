package bus

import (
	"context"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yson"

	"go.ytsaurus.tech/yt/go/bus/tcptest"
	testservice "go.ytsaurus.tech/yt/go/proto/core/rpc/unittests"
)

type BusServerConfig struct {
	Port int `yson:"port"`
}

func StartTestServiceWithConfig(t *testing.T, config BusServerConfig) (addr string, stop func()) {
	t.Helper()

	binary := GetTestServiceBinary(t)

	port, err := tcptest.GetFreePort()
	require.NoError(t, err, "unable to get free port")
	addr = net.JoinHostPort("localhost", strconv.Itoa(port))

	config.Port = port

	configText, err := yson.Marshal(&config)
	require.NoError(t, err, "cannot marshal config")

	cmd := exec.Command(binary, string(configText))
	cmd.Stdout = nil
	cmd.Stderr = os.Stderr

	require.NoError(t, cmd.Start())

	done := make(chan error)
	go func() {
		done <- cmd.Wait()
	}()

	stop = func() {
		_ = cmd.Process.Kill()
		<-done
	}

	if err = tcptest.WaitForPort(port, time.Second*30); err != nil {
		stop()
	}

	require.NoError(t, err)
	t.Logf("started service on port %d", port)

	return
}

func StartTestService(t *testing.T) (addr string, stop func()) {
	return StartTestServiceWithConfig(t, BusServerConfig{})
}

type TestServiceClient interface {
	SomeCall(ctx context.Context, req *testservice.TReqSomeCall, opts ...SendOption) (*testservice.TRspSomeCall, error)
	PassCall(ctx context.Context, req *testservice.TReqPassCall, opts ...SendOption) (*testservice.TRspPassCall, error)
	RegularAttachments(ctx context.Context, req *testservice.TReqRegularAttachments, opts ...SendOption) (*testservice.TRspRegularAttachments, error)
	NullAndEmptyAttachments(ctx context.Context, req *testservice.TReqNullAndEmptyAttachments, opts ...SendOption) (*testservice.TRspNullAndEmptyAttachments, error)
	Compression(ctx context.Context, req *testservice.TReqCompression, opts ...SendOption) (*testservice.TRspCompression, error)
	DoNothing(ctx context.Context, req *testservice.TReqDoNothing, opts ...SendOption) (*testservice.TRspDoNothing, error)
	CustomMessageError(ctx context.Context, req *testservice.TReqCustomMessageError, opts ...SendOption) (*testservice.TRspCustomMessageError, error)
	//NotRegistered(ctx context.Context, req *testservice.TRspNotRegistemethodred, opts ...SendOption) (*testservice.TRspNotRegistered, error)
	SlowCall(ctx context.Context, req *testservice.TReqSlowCall, opts ...SendOption) (*testservice.TRspSlowCall, error)
	SlowCanceledCall(ctx context.Context, req *testservice.TReqSlowCanceledCall, opts ...SendOption) (*testservice.TRspSlowCanceledCall, error)
	NoReply(ctx context.Context, req *testservice.TReqNoReply, opts ...SendOption) (*testservice.TRspNoReply, error)
	//FlakyCall(ctx context.Context, req *testservice.TReqFlakyCall, opts ...SendOption) (*testservice.TRspFlakyCall, error)
	RequireCoolFeature(ctx context.Context, req *testservice.TReqRequireCoolFeature, opts ...SendOption) (*testservice.TRspRequireCoolFeature, error)
}

var _ TestServiceClient = (*testServiceClient)(nil)

type testServiceClient struct {
	conn *ClientConn
}

func NewTestServiceClient(addr string, opts ...ClientOption) *testServiceClient {
	options := append([]ClientOption{WithDefaultProtocolVersionMajor(1)}, opts...)

	ctx := context.Background()
	conn := NewClient(ctx, addr, options...)
	return &testServiceClient{conn: conn}
}

func (c *testServiceClient) send(ctx context.Context, method string, req, rsp proto.Message, opts ...SendOption) error {
	options := opts
	if len(opts) == 0 {
		options = []SendOption{WithProtocolVersionMajor(1)}
	}
	return c.conn.Send(ctx, "TestService", method, req, rsp, options...)
}

func (c *testServiceClient) SomeCall(ctx context.Context, req *testservice.TReqSomeCall, opts ...SendOption) (*testservice.TRspSomeCall, error) {
	var rsp testservice.TRspSomeCall
	err := c.send(ctx, "SomeCall", req, &rsp, opts...)
	return &rsp, err
}

func (c *testServiceClient) PassCall(ctx context.Context, req *testservice.TReqPassCall, opts ...SendOption) (*testservice.TRspPassCall, error) {
	var rsp testservice.TRspPassCall
	err := c.send(ctx, "PassCall", req, &rsp, opts...)
	return &rsp, err
}

func (c *testServiceClient) RegularAttachments(ctx context.Context, req *testservice.TReqRegularAttachments, opts ...SendOption) (*testservice.TRspRegularAttachments, error) {
	var rsp testservice.TRspRegularAttachments
	err := c.send(ctx, "RegularAttachments", req, &rsp, opts...)
	return &rsp, err
}

func (c *testServiceClient) NullAndEmptyAttachments(ctx context.Context, req *testservice.TReqNullAndEmptyAttachments, opts ...SendOption) (*testservice.TRspNullAndEmptyAttachments, error) {
	var rsp testservice.TRspNullAndEmptyAttachments
	err := c.send(ctx, "NullAndEmptyAttachments", req, &rsp, opts...)
	return &rsp, err
}

func (c *testServiceClient) Compression(ctx context.Context, req *testservice.TReqCompression, opts ...SendOption) (*testservice.TRspCompression, error) {
	var rsp testservice.TRspCompression
	err := c.send(ctx, "Compression", req, &rsp, opts...)
	return &rsp, err
}

func (c *testServiceClient) DoNothing(ctx context.Context, req *testservice.TReqDoNothing, opts ...SendOption) (*testservice.TRspDoNothing, error) {
	var rsp testservice.TRspDoNothing
	err := c.send(ctx, "DoNothing", req, &rsp, opts...)
	return &rsp, err
}

func (c *testServiceClient) CustomMessageError(ctx context.Context, req *testservice.TReqCustomMessageError, opts ...SendOption) (*testservice.TRspCustomMessageError, error) {
	var rsp testservice.TRspCustomMessageError
	err := c.send(ctx, "CustomMessageError", req, &rsp, opts...)
	return &rsp, err
}

func (c *testServiceClient) SlowCall(ctx context.Context, req *testservice.TReqSlowCall, opts ...SendOption) (*testservice.TRspSlowCall, error) {
	var rsp testservice.TRspSlowCall
	err := c.send(ctx, "SlowCall", req, &rsp, opts...)
	return &rsp, err
}

func (c *testServiceClient) SlowCanceledCall(ctx context.Context, req *testservice.TReqSlowCanceledCall, opts ...SendOption) (*testservice.TRspSlowCanceledCall, error) {
	var rsp testservice.TRspSlowCanceledCall
	err := c.send(ctx, "SlowCanceledCall", req, &rsp, opts...)
	return &rsp, err
}

func (c *testServiceClient) NoReply(ctx context.Context, req *testservice.TReqNoReply, opts ...SendOption) (*testservice.TRspNoReply, error) {
	var rsp testservice.TRspNoReply
	err := c.send(ctx, "NoReply", req, &rsp, opts...)
	return &rsp, err
}

func (c *testServiceClient) RequireCoolFeature(ctx context.Context, req *testservice.TReqRequireCoolFeature, opts ...SendOption) (*testservice.TRspRequireCoolFeature, error) {
	var rsp testservice.TRspRequireCoolFeature
	err := c.send(ctx, "RequireCoolFeature", req, &rsp, opts...)
	return &rsp, err
}

func (c *testServiceClient) Close() {
	c.conn.Close()
}
