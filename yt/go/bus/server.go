package bus

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"

	"google.golang.org/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/nop"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/compression"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/core/misc"
	"go.ytsaurus.tech/yt/go/proto/core/rpc"
	"go.ytsaurus.tech/yt/go/yterrors"
)

var ErrServerStopped = xerrors.NewSentinel("server stopped")

type Request struct {
	Header      *rpc.TRequestHeader
	Body        []byte
	Attachments [][]byte
}

type Response struct {
	Body        []byte
	Attachments [][]byte
}

// Handler processes a single RPC call.
//
// ctx is canceled when the client cancels the request, the request times
// out or the connection is closed.
type Handler func(ctx context.Context, req *Request) (*Response, error)

type ServerOption func(server *Server)

func WithServerLogger(l log.Logger) ServerOption {
	return func(server *Server) {
		server.log = l
	}
}

func WithServerEncryptionMode(em EncryptionMode) ServerOption {
	return func(server *Server) {
		server.encryptionMode = em
	}
}

func WithServerTLSConfig(c *tls.Config) ServerOption {
	return func(server *Server) {
		server.tlsConfig = c
	}
}

// Server is a bus RPC server.
//
// All methods must be registered and all features declared before the
// first call to Serve.
type Server struct {
	handlers map[string]map[string]Handler
	features map[int32]struct{}

	log log.Logger

	encryptionMode EncryptionMode
	tlsConfig      *tls.Config

	// l guards the following fields.
	l         sync.Mutex
	stopped   bool
	listeners map[net.Listener]struct{}
	conns     map[*serverConn]struct{}

	wg sync.WaitGroup
}

func NewServer(opts ...ServerOption) *Server {
	server := &Server{
		handlers:  make(map[string]map[string]Handler),
		features:  make(map[int32]struct{}),
		log:       &nop.Logger{},
		listeners: make(map[net.Listener]struct{}),
		conns:     make(map[*serverConn]struct{}),
	}

	for _, opt := range opts {
		opt(server)
	}

	return server
}

func (s *Server) RegisterMethod(service, method string, handler Handler) {
	methods := s.handlers[service]
	if methods == nil {
		methods = make(map[string]Handler)
		s.handlers[service] = methods
	}
	methods[method] = handler
}

// DeclareServerFeature marks the feature id as supported.
func (s *Server) DeclareServerFeature(id int32) {
	s.features[id] = struct{}{}
}

func (s *Server) Serve(lsn net.Listener) error {
	if !s.registerListener(lsn) {
		_ = lsn.Close()
		return ErrServerStopped
	}
	defer s.unregisterListener(lsn)

	for {
		rawConn, err := lsn.Accept()
		if err != nil {
			if s.isStopped() {
				return ErrServerStopped
			}
			return err
		}

		conn := newServerConn(s, rawConn)
		if !s.registerConn(conn) {
			conn.close()
			return ErrServerStopped
		}

		go func() {
			defer s.wg.Done()
			defer s.unregisterConn(conn)
			conn.run()
		}()
	}
}

// Close stops accepting new connections, closes all active connections
// and waits until all inflight requests finish.
func (s *Server) Close() {
	s.l.Lock()
	if !s.stopped {
		s.stopped = true

		for lsn := range s.listeners {
			_ = lsn.Close()
		}
		for conn := range s.conns {
			conn.close()
		}
	}
	s.l.Unlock()

	s.wg.Wait()
}

func (s *Server) registerListener(lsn net.Listener) bool {
	s.l.Lock()
	defer s.l.Unlock()

	if s.stopped {
		return false
	}

	s.listeners[lsn] = struct{}{}
	return true
}

func (s *Server) unregisterListener(lsn net.Listener) {
	s.l.Lock()
	defer s.l.Unlock()

	delete(s.listeners, lsn)
}

func (s *Server) registerConn(conn *serverConn) bool {
	s.l.Lock()
	defer s.l.Unlock()

	if s.stopped {
		return false
	}

	s.conns[conn] = struct{}{}
	s.wg.Add(1)
	return true
}

func (s *Server) unregisterConn(conn *serverConn) {
	s.l.Lock()
	defer s.l.Unlock()

	delete(s.conns, conn)
}

func (s *Server) isStopped() bool {
	s.l.Lock()
	defer s.l.Unlock()

	return s.stopped
}

func (s *Server) lookupHandler(service, method string) (Handler, error) {
	methods, ok := s.handlers[service]
	if !ok {
		return nil, yterrors.Err(
			yterrors.CodeNoSuchService,
			"Service is not registered",
			yterrors.Attr("service", service))
	}

	handler, ok := methods[method]
	if !ok {
		return nil, yterrors.Err(
			yterrors.CodeNoSuchMethod,
			"Method is not registered",
			yterrors.Attr("service", service),
			yterrors.Attr("method", method))
	}

	return handler, nil
}

// serverConn services a single accepted connection: it runs the receive
// loop and tracks inflight requests for cancelation.
type serverConn struct {
	server *Server
	bus    *Bus
	log    log.Logger

	rawConn net.Conn

	// sendMu serializes packet writes from concurrent request handlers.
	sendMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	// mu guards cancels.
	mu      sync.Mutex
	cancels map[guid.GUID]context.CancelFunc

	handlers sync.WaitGroup
}

func newServerConn(server *Server, rawConn net.Conn) *serverConn {
	bus := NewBus(rawConn, Options{
		Logger:         server.log,
		EncryptionMode: server.encryptionMode,
		TLSConfig:      server.tlsConfig,
	})
	bus.isServer = true

	ctx, cancel := context.WithCancel(context.Background())

	return &serverConn{
		server:  server,
		bus:     bus,
		log:     bus.logger,
		rawConn: rawConn,
		ctx:     ctx,
		cancel:  cancel,
		cancels: make(map[guid.GUID]context.CancelFunc),
	}
}

func (c *serverConn) close() {
	c.cancel()
	_ = c.rawConn.Close()
}

func (c *serverConn) run() {
	defer c.handlers.Wait()
	defer c.close()
	defer c.bus.Close()

	if err := c.bus.establishEncryption(true); err != nil {
		c.log.Error("Unable to establish encryption", log.Error(err))
		return
	}

	for {
		msg, err := c.bus.Receive()
		if err != nil {
			return
		}

		if err := c.handleMsg(msg); err != nil {
			c.log.Error("Unable to handle message", log.Error(err))
			return
		}
	}
}

func (c *serverConn) send(packetID guid.GUID, parts [][]byte) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	return c.bus.Send(packetID, parts, &busSendOptions{})
}

func (c *serverConn) sendAck(packetID guid.GUID) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	return c.bus.sendAck(packetID)
}

func (c *serverConn) handleMsg(msg busMsg) error {
	if msg.fixHeader.typ != packetMessage {
		return nil
	}

	if msg.fixHeader.flags&packetFlagsRequestAcknowledgement != 0 {
		if err := c.sendAck(msg.fixHeader.packetID); err != nil {
			return err
		}
	}

	if len(msg.parts) == 0 || len(msg.parts[0]) < 4 {
		return fmt.Errorf("bus: message type is missing")
	}

	switch typ := msgType(binary.LittleEndian.Uint32(msg.parts[0][:4])); typ {
	case msgRequest:
		return c.handleRequest(msg.parts)

	case msgCancel:
		return c.handleCancel(msg.parts)

	default:
		c.log.Warnf("ignoring message of unexpected type: %x", typ)
		return nil
	}
}

func (c *serverConn) handleRequest(parts [][]byte) error {
	header := &rpc.TRequestHeader{}
	if err := proto.Unmarshal(parts[0][4:], header); err != nil {
		return fmt.Errorf("bus: error unmarshaling request header: %w", err)
	}

	if header.RequestId == nil {
		return fmt.Errorf("bus: request is missing request_id")
	}

	if len(parts) < 2 {
		return fmt.Errorf("bus: request body is missing")
	}

	requestID := misc.NewGUIDFromProto(header.RequestId)
	c.log.Trace("Request received",
		log.String("request_id", requestID.String()),
		log.String("service", header.GetService()),
		log.String("method", header.GetMethod()))

	// Register for cancelation before starting the handler so that a
	// cancelation message arriving right after the request cannot miss it.
	ctx, finish := c.beginRequest(requestID, header)

	c.handlers.Add(1)
	go func() {
		defer c.handlers.Done()
		defer finish()
		c.processRequest(ctx, header, parts[1], parts[2:])
	}()

	return nil
}

func (c *serverConn) handleCancel(parts [][]byte) error {
	header := &rpc.TRequestCancelationHeader{}
	if err := proto.Unmarshal(parts[0][4:], header); err != nil && header.RequestId == nil {
		c.log.Warn("Error unmarshaling cancelation header", log.Error(err))
		return nil
	}

	if header.RequestId == nil {
		return nil
	}

	requestID := misc.NewGUIDFromProto(header.RequestId)
	c.log.Trace("Cancelation received", log.String("request_id", requestID.String()))

	c.cancelRequest(requestID)
	return nil
}

func (c *serverConn) beginRequest(requestID guid.GUID, header *rpc.TRequestHeader) (context.Context, func()) {
	var ctx context.Context
	var cancel context.CancelFunc
	if header.Timeout != nil {
		ctx, cancel = context.WithTimeout(c.ctx, microsecondsToDuration(header.GetTimeout()))
	} else {
		ctx, cancel = context.WithCancel(c.ctx)
	}

	c.mu.Lock()
	c.cancels[requestID] = cancel
	c.mu.Unlock()

	finish := func() {
		c.mu.Lock()
		delete(c.cancels, requestID)
		c.mu.Unlock()

		cancel()
	}

	return ctx, finish
}

func (c *serverConn) cancelRequest(requestID guid.GUID) {
	c.mu.Lock()
	cancel := c.cancels[requestID]
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

func (c *serverConn) processRequest(ctx context.Context, header *rpc.TRequestHeader, body []byte, attachments [][]byte) {
	rsp, err := c.callHandler(ctx, header, body, attachments)

	var parts [][]byte
	if err == nil {
		parts, err = compressResponse(header, rsp)
	}

	rspHeader := &rpc.TResponseHeader{
		RequestId: header.RequestId,
		Service:   header.Service,
		Method:    header.Method,
	}

	if err != nil {
		// NOTE: Error responses consist of a single header part.
		rspHeader.Error = errorToProto(err)
		parts = nil
	} else if header.ResponseCodec != nil {
		rspHeader.Codec = ptr.Int32(header.GetResponseCodec())
	}

	rawHeader, err := proto.Marshal(rspHeader)
	if err != nil {
		c.log.Error("Unable to marshal response header", log.Error(err))
		return
	}

	msg := make([][]byte, 0, 1+len(parts))

	head := make([]byte, 4, 4+len(rawHeader))
	binary.LittleEndian.PutUint32(head[0:4], uint32(msgResponse))
	msg = append(msg, append(head, rawHeader...))
	msg = append(msg, parts...)

	packetID := guid.New()
	c.log.Trace("Sending response",
		log.String("id", packetID.String()),
		log.String("request_id", misc.NewGUIDFromProto(header.RequestId).String()))

	if err := c.send(packetID, msg); err != nil {
		// The connection is broken; close it to terminate the receive loop.
		c.close()
	}
}

func (c *serverConn) callHandler(ctx context.Context, header *rpc.TRequestHeader, body []byte, attachments [][]byte) (rsp *Response, err error) {
	defer func() {
		if p := recover(); p != nil {
			rsp, err = nil, yterrors.Err(
				"Request handler panicked",
				yterrors.Attr("panic_value", fmt.Sprint(p)))
		}
	}()

	if err := validateCodec(header.GetRequestCodec()); err != nil {
		return nil, err
	}
	if err := validateCodec(header.GetResponseCodec()); err != nil {
		return nil, err
	}

	for _, id := range header.RequiredServerFeatureIds {
		if _, ok := c.server.features[id]; !ok {
			return nil, yterrors.Err(
				yterrors.CodeUnsupportedServerFeature,
				"Server does not support the feature demanded by request",
				yterrors.Attr(string(AttributeKeyFeatureID), int64(id)))
		}
	}

	handler, err := c.server.lookupHandler(header.GetService(), header.GetMethod())
	if err != nil {
		return nil, err
	}

	codec := compression.NewCodec(compression.CodecID(header.GetRequestCodec()))

	reqBody, err := codec.Decompress(body)
	if err != nil {
		return nil, yterrors.Err(yterrors.CodeProtocolError, "Error decompressing request body", err)
	}

	reqAttachments := make([][]byte, 0, len(attachments))
	for _, a := range attachments {
		decompressed, err := codec.Decompress(a)
		if err != nil {
			return nil, yterrors.Err(yterrors.CodeProtocolError, "Error decompressing request attachment", err)
		}
		reqAttachments = append(reqAttachments, decompressed)
	}

	return handler(ctx, &Request{
		Header:      header,
		Body:        reqBody,
		Attachments: reqAttachments,
	})
}

func compressResponse(header *rpc.TRequestHeader, rsp *Response) ([][]byte, error) {
	if rsp == nil {
		rsp = &Response{}
	}

	codec := compression.NewCodec(compression.CodecID(header.GetResponseCodec()))

	body, err := codec.Compress(rsp.Body)
	if err != nil {
		return nil, err
	}

	parts := make([][]byte, 0, 1+len(rsp.Attachments))
	parts = append(parts, body)

	for _, a := range rsp.Attachments {
		compressed, err := codec.Compress(a)
		if err != nil {
			return nil, err
		}
		parts = append(parts, compressed)
	}

	return parts, nil
}

func validateCodec(id int32) error {
	if id >= 0 && id <= 127 && compression.CodecID(id).String() != "" {
		return nil
	}

	return yterrors.Err(
		yterrors.CodeProtocolError,
		fmt.Sprintf("Codec %d is not supported", id))
}

func errorToProto(err error) *misc.TError {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		err = yterrors.Err(yterrors.CodeTimeout, "Request timed out")
	case errors.Is(err, context.Canceled):
		err = yterrors.Err(yterrors.CodeCanceled, "Request canceled")
	}

	return misc.NewProtoFromError(err)
}
