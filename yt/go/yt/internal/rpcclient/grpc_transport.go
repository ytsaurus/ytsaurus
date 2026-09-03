package rpcclient

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/proto/core/misc"
	"go.ytsaurus.tech/yt/go/yt"
)

const (
	grpcAPIServicePrefix = "/ApiService/"
	grpcDefaultPort      = 9014
	omitAttachment       = math.MaxUint32

	traceFlagSampled = 1
	traceFlagDebug   = 2
)

var grpcProtocolVersion = strconv.Itoa(ProtocolVersionMajor) + ".0"

var (
	errGRPCTransportStopped  = xerrors.New("grpc transport is stopped")
	errGRPCDialerUnsupported = xerrors.New("rpcclient.WithDialer is not supported by the grpc transport")
)

type grpcTransport struct {
	conf          *yt.Config
	httpTransport *http.Transport

	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	stopped bool
}

var _ transport = (*grpcTransport)(nil)

func newGRPCTransport(c *client, httpTransport *http.Transport) *grpcTransport {
	return &grpcTransport{
		conf:          c.conf,
		httpTransport: httpTransport,
		conns:         make(map[string]*grpc.ClientConn),
	}
}

func (t *grpcTransport) ProxyType() string { return "grpc" }

func (t *grpcTransport) Discard(addr string) {}

func (t *grpcTransport) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
	for addr, conn := range t.conns {
		_ = conn.Close()
		delete(t.conns, addr)
	}
}

func (t *grpcTransport) Send(
	ctx context.Context,
	addr string,
	call *Call,
	rsp proto.Message,
	creds yt.Credentials,
) ([][]byte, error) {
	body, err := proto.Marshal(call.Req)
	if err != nil {
		return nil, xerrors.Errorf("marshal grpc request: %w", err)
	}

	md, err := grpcRequestMetadata(call, len(body), creds)
	if err != nil {
		return nil, err
	}
	t.injectTracing(ctx, md)
	ctx = metadata.NewOutgoingContext(ctx, md)

	conn, err := t.getConn(ctx, addr)
	if err != nil {
		return nil, err
	}

	payload := appendAttachments(body, call.Attachments)

	var trailer metadata.MD
	var raw []byte
	err = conn.Invoke(ctx, grpcAPIServicePrefix+string(call.Method), payload, &raw,
		grpc.ForceCodec(rawCodec{}),
		grpc.Trailer(&trailer),
	)
	if err != nil {
		if ytErr := errorFromTrailer(trailer); ytErr != nil {
			return nil, ytErr
		}
		return nil, convertGRPCError(ctx, err)
	}

	rspBody, attachments, err := splitMessage(raw, trailer)
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(rspBody, rsp); err != nil {
		return nil, xerrors.Errorf("unmarshal grpc response: %w", err)
	}
	return attachments, nil
}

func (t *grpcTransport) getConn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	if _, ok := GetDialer(ctx); ok {
		return nil, errGRPCDialerUnsupported
	}

	addr = ensureGRPCAddr(addr)

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.stopped {
		return nil, errGRPCTransportStopped
	}

	if conn, ok := t.conns[addr]; ok {
		return conn, nil
	}

	network := t.conf.GetIPVersion().Network()
	dialer := &net.Dialer{}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(t.transportCredentials(addr)),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		}),
		grpc.WithDefaultCallOptions(
			// YT packs body and rowset attachments into a single message, so the default 4MiB limit is too small.
			grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32),
		),
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	t.conns[addr] = conn
	return conn, nil
}

func (t *grpcTransport) transportCredentials(addr string) credentials.TransportCredentials {
	if tlsConfig := grpcTLSConfig(t.conf, t.httpTransport, addr); tlsConfig != nil {
		return credentials.NewTLS(tlsConfig)
	}
	return insecure.NewCredentials()
}

func grpcTLSConfig(conf *yt.Config, httpTransport *http.Transport, addr string) *tls.Config {
	if !conf.UseTLS || httpTransport == nil || httpTransport.TLSClientConfig == nil {
		return nil
	}
	tlsConfig := httpTransport.TLSClientConfig.Clone()
	tlsConfig.ServerName = resolveServerName(conf, addr)
	return tlsConfig
}

func (t *grpcTransport) injectTracing(ctx context.Context, md metadata.MD) {
	if t.conf.TraceFn == nil {
		return
	}

	traceID, spanID, flags, ok := t.conf.TraceFn(ctx)
	if !ok {
		return
	}

	md.Set("yt-tracing-trace-id", traceID.String())
	md.Set("yt-tracing-span-id", strconv.FormatUint(spanID, 10))
	if flags&traceFlagSampled != 0 {
		md.Set("yt-tracing-sampled", "1")
	}
	if flags&traceFlagDebug != 0 {
		md.Set("yt-tracing-debug", "1")
	}
}

func grpcRequestMetadata(call *Call, bodySize int, creds yt.Credentials) (metadata.MD, error) {
	md := metadata.Pairs(
		"yt-protocol-version", grpcProtocolVersion,
		"yt-message-body-size", strconv.Itoa(bodySize),
		"yt-request-id", call.CallID.String(),
	)
	if creds == nil {
		return md, nil
	}
	if err := grpcSetCredentials(md, creds); err != nil {
		return nil, err
	}
	return md, nil
}

func grpcSetCredentials(md metadata.MD, creds yt.Credentials) error {
	switch c := creds.(type) {
	case *yt.TokenCredentials:
		md.Set("yt-auth-token", c.Token)
	case *yt.BearerCredentials:
		md.Set("yt-auth-token", c.Token)
	case *yt.UserTicketCredentials:
		md.Set("yt-auth-user-ticket", c.Ticket)
	case *yt.ServiceTicketCredentials:
		md.Set("yt-auth-service-ticket", c.Ticket)
	default:
		return xerrors.Errorf("unsupported credentials type for grpc: %T", creds)
	}
	return nil
}

// grpcStatusError wraps a gRPC status so retriers treat it as a net.Error.
type grpcStatusError struct {
	code codes.Code
	err  error
}

func (e *grpcStatusError) Error() string { return e.err.Error() }
func (e *grpcStatusError) Unwrap() error { return e.err }
func (e *grpcStatusError) Timeout() bool { return e.code == codes.DeadlineExceeded }
func (e *grpcStatusError) Temporary() bool {
	switch e.code {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return true
	default:
		return false
	}
}

var _ net.Error = (*grpcStatusError)(nil)

func convertGRPCError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	switch code := status.Code(err); code {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return &grpcStatusError{code: code, err: err}
	default:
		return err
	}
}

func errorFromTrailer(trailer metadata.MD) error {
	bins := trailer.Get("yt-error-bin")
	if len(bins) == 0 {
		return nil
	}
	var te misc.TError
	if err := proto.Unmarshal([]byte(bins[0]), &te); err != nil {
		return xerrors.Errorf("yt-error-bin unmarshal: %w", err)
	}
	return misc.NewErrorFromProto(&te)
}

func appendAttachments(buf []byte, attachments [][]byte) []byte {
	total := len(buf)
	for _, a := range attachments {
		total += 4
		if a != nil {
			total += len(a)
		}
	}
	if cap(buf) < total {
		grown := make([]byte, len(buf), total)
		copy(grown, buf)
		buf = grown
	}
	for _, a := range attachments {
		if a == nil {
			buf = binary.LittleEndian.AppendUint32(buf, omitAttachment)
			continue
		}
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(a)))
		buf = append(buf, a...)
	}
	return buf
}

func splitMessage(raw []byte, trailer metadata.MD) (body []byte, attachments [][]byte, err error) {
	size := len(raw)
	if vals := trailer.Get("yt-message-body-size"); len(vals) > 0 {
		n, convErr := strconv.Atoi(vals[0])
		if convErr != nil {
			return nil, nil, xerrors.Errorf("yt-message-body-size: %w", convErr)
		}
		size = n
	}
	if size < 0 || size > len(raw) {
		return nil, nil, xerrors.Errorf("yt-message-body-size %d is outside payload %d", size, len(raw))
	}
	body = raw[:size]
	attachments, err = parseAttachments(raw[size:])
	return body, attachments, err
}

func parseAttachments(buf []byte) ([][]byte, error) {
	var out [][]byte
	for len(buf) > 0 {
		if len(buf) < 4 {
			return nil, xerrors.New("truncated attachment header")
		}
		n := binary.LittleEndian.Uint32(buf[:4])
		buf = buf[4:]
		if n == omitAttachment {
			out = append(out, nil)
			continue
		}
		if uint64(n) > uint64(len(buf)) {
			return nil, xerrors.Errorf("truncated attachment: want %d have %d", n, len(buf))
		}
		out = append(out, buf[:n])
		buf = buf[n:]
	}
	return out, nil
}

func ensureGRPCAddr(addr string) string {
	if addr == "" {
		return addr
	}
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return addr
	}
	return net.JoinHostPort(addr, strconv.Itoa(grpcDefaultPort))
}

type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	switch m := v.(type) {
	case []byte:
		return m, nil
	case *[]byte:
		return *m, nil
	default:
		return nil, xerrors.Errorf("rawCodec.Marshal: unexpected type %T", v)
	}
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	dst, ok := v.(*[]byte)
	if !ok {
		return xerrors.Errorf("rawCodec.Unmarshal: unexpected type %T", v)
	}
	*dst = append([]byte(nil), data...)
	return nil
}

func (rawCodec) Name() string { return "proto" }

var _ encoding.Codec = rawCodec{}
