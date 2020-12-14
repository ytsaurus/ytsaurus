package bus

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/test/yatest"
	myservice "a.yandex-team.ru/yt/go/proto/core/rpc/unittests"
)

func StartMyService(t *testing.T) (port string, stop func()) {
	t.Helper()

	binary, err := yatest.BinaryPath("yt/yt/core/rpc/unittests/bin/bin")
	require.NoError(t, err)

	port, err = GetFreePort()
	require.NoError(t, err, "unable to get free port")

	cmd := exec.Command(binary, port)
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

	if err = WaitForPort(t, time.Second*30, port); err != nil {
		stop()
	}

	require.NoError(t, err)
	t.Logf("started service on port %s", port)

	return
}

// GetFreePort returns free local tcp port.
func GetFreePort() (string, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return "", err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return "", err
	}
	defer func() { _ = l.Close() }()

	p := l.Addr().(*net.TCPAddr).Port

	return strconv.Itoa(p), nil
}

// WaitForPort tries to connect to given local port with constant backoff.
//
// Returns error if port is not ready after timeout.
func WaitForPort(t *testing.T, timeout time.Duration, port string) error {
	t.Helper()

	stopTimer := time.NewTimer(timeout)
	defer stopTimer.Stop()

	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	for {
		select {
		case <-stopTimer.C:
			return fmt.Errorf("no server started listening on port %s after timeout %s", port, timeout)
		case <-ticker.C:
			if err := portIsReady(port); err != nil {
				t.Logf("waiting for port: %s\n", err)
				break
			}
			return nil
		}
	}
}

func portIsReady(port string) error {
	conn, err := net.Dial("tcp", net.JoinHostPort("localhost", port))
	if err != nil {
		return err
	}
	return conn.Close()
}

type MyServiceClient interface {
	SomeCall(ctx context.Context, req *myservice.TReqSomeCall, opts ...SendOption) (*myservice.TRspSomeCall, error)
	PassCall(ctx context.Context, req *myservice.TReqPassCall, opts ...SendOption) (*myservice.TRspPassCall, error)
	RegularAttachments(ctx context.Context, req *myservice.TReqRegularAttachments, opts ...SendOption) (*myservice.TRspRegularAttachments, error)
	NullAndEmptyAttachments(ctx context.Context, req *myservice.TReqNullAndEmptyAttachments, opts ...SendOption) (*myservice.TRspNullAndEmptyAttachments, error)
	Compression(ctx context.Context, req *myservice.TReqCompression, opts ...SendOption) (*myservice.TRspCompression, error)
	DoNothing(ctx context.Context, req *myservice.TReqDoNothing, opts ...SendOption) (*myservice.TRspDoNothing, error)
	CustomMessageError(ctx context.Context, req *myservice.TReqCustomMessageError, opts ...SendOption) (*myservice.TRspCustomMessageError, error)
	//NotRegistered(ctx context.Context, req *myservice.TRspNotRegistemethodred, opts ...SendOption) (*myservice.TRspNotRegistered, error)
	SlowCall(ctx context.Context, req *myservice.TReqSlowCall, opts ...SendOption) (*myservice.TRspSlowCall, error)
	SlowCanceledCall(ctx context.Context, req *myservice.TReqSlowCanceledCall, opts ...SendOption) (*myservice.TRspSlowCanceledCall, error)
	NoReply(ctx context.Context, req *myservice.TReqNoReply, opts ...SendOption) (*myservice.TRspNoReply, error)
	//FlakyCall(ctx context.Context, req *myservice.TReqFlakyCall, opts ...SendOption) (*myservice.TRspFlakyCall, error)
	RequireCoolFeature(ctx context.Context, req *myservice.TReqRequireCoolFeature, opts ...SendOption) (*myservice.TRspRequireCoolFeature, error)
}

var _ MyServiceClient = (*myServiceClient)(nil)

type myServiceClient struct {
	conn *ClientConn
}

func NewMyServiceClient(addr string, opts ...ClientOption) (*myServiceClient, error) {
	options := append([]ClientOption{WithDefaultProtocolVersionMajor(1)}, opts...)

	ctx := context.Background()
	conn, err := NewClient(ctx, addr, options...)
	if err != nil {
		return nil, err
	}
	return &myServiceClient{conn: conn}, nil
}

func (c *myServiceClient) send(ctx context.Context, method string, req, rsp proto.Message, opts ...SendOption) error {
	options := opts
	if len(opts) == 0 {
		options = []SendOption{WithProtocolVersionMajor(1)}
	}
	return c.conn.Send(ctx, "MyService", method, req, rsp, options...)
}

func (c *myServiceClient) SomeCall(ctx context.Context, req *myservice.TReqSomeCall, opts ...SendOption) (*myservice.TRspSomeCall, error) {
	var rsp myservice.TRspSomeCall
	err := c.send(ctx, "SomeCall", req, &rsp, opts...)
	return &rsp, err
}

func (c *myServiceClient) PassCall(ctx context.Context, req *myservice.TReqPassCall, opts ...SendOption) (*myservice.TRspPassCall, error) {
	var rsp myservice.TRspPassCall
	err := c.send(ctx, "PassCall", req, &rsp, opts...)
	return &rsp, err
}

func (c *myServiceClient) RegularAttachments(ctx context.Context, req *myservice.TReqRegularAttachments, opts ...SendOption) (*myservice.TRspRegularAttachments, error) {
	var rsp myservice.TRspRegularAttachments
	err := c.send(ctx, "RegularAttachments", req, &rsp, opts...)
	return &rsp, err
}

func (c *myServiceClient) NullAndEmptyAttachments(ctx context.Context, req *myservice.TReqNullAndEmptyAttachments, opts ...SendOption) (*myservice.TRspNullAndEmptyAttachments, error) {
	var rsp myservice.TRspNullAndEmptyAttachments
	err := c.send(ctx, "NullAndEmptyAttachments", req, &rsp, opts...)
	return &rsp, err
}

func (c *myServiceClient) Compression(ctx context.Context, req *myservice.TReqCompression, opts ...SendOption) (*myservice.TRspCompression, error) {
	var rsp myservice.TRspCompression
	err := c.send(ctx, "Compression", req, &rsp, opts...)
	return &rsp, err
}

func (c *myServiceClient) DoNothing(ctx context.Context, req *myservice.TReqDoNothing, opts ...SendOption) (*myservice.TRspDoNothing, error) {
	var rsp myservice.TRspDoNothing
	err := c.send(ctx, "DoNothing", req, &rsp, opts...)
	return &rsp, err
}

func (c *myServiceClient) CustomMessageError(ctx context.Context, req *myservice.TReqCustomMessageError, opts ...SendOption) (*myservice.TRspCustomMessageError, error) {
	var rsp myservice.TRspCustomMessageError
	err := c.send(ctx, "CustomMessageError", req, &rsp, opts...)
	return &rsp, err
}

func (c *myServiceClient) SlowCall(ctx context.Context, req *myservice.TReqSlowCall, opts ...SendOption) (*myservice.TRspSlowCall, error) {
	var rsp myservice.TRspSlowCall
	err := c.send(ctx, "SlowCall", req, &rsp, opts...)
	return &rsp, err
}

func (c *myServiceClient) SlowCanceledCall(ctx context.Context, req *myservice.TReqSlowCanceledCall, opts ...SendOption) (*myservice.TRspSlowCanceledCall, error) {
	var rsp myservice.TRspSlowCanceledCall
	err := c.send(ctx, "SlowCanceledCall", req, &rsp, opts...)
	return &rsp, err
}

func (c *myServiceClient) NoReply(ctx context.Context, req *myservice.TReqNoReply, opts ...SendOption) (*myservice.TRspNoReply, error) {
	var rsp myservice.TRspNoReply
	err := c.send(ctx, "NoReply", req, &rsp, opts...)
	return &rsp, err
}

func (c *myServiceClient) RequireCoolFeature(ctx context.Context, req *myservice.TReqRequireCoolFeature, opts ...SendOption) (*myservice.TRspRequireCoolFeature, error) {
	var rsp myservice.TRspRequireCoolFeature
	err := c.send(ctx, "RequireCoolFeature", req, &rsp, opts...)
	return &rsp, err
}

func (c *myServiceClient) Close() {
	c.conn.Close()
}
