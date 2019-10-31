package bus

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/proto/core/misc"
	"a.yandex-team.ru/yt/go/proto/core/rpc"
)

type clientReq struct {
	id guid.GUID

	reqHeader rpc.TRequestHeader
	rspHeader rpc.TResponseHeader

	request, reply proto.Message

	reqAttachments [][]byte
	rspAttachments [][]byte

	done chan error
}

type msgType uint32

const (
	maxInFlightRequests = 1000

	msgRequest        msgType = 0x69637072
	msgCancel         msgType = 0x63637072
	msgResponse       msgType = 0x6f637072
	msgStreamPayload  msgType = 0x70637072
	msgStreamFeedback msgType = 0x66637072
)

var (
	ytGoClient       = "YT go client"
	codecNone  int32 = 0
)

type ClientConn struct {
	l    sync.Mutex
	err  error
	stop chan struct{}
	reqs map[guid.GUID]*clientReq

	sendQueue, cancelQueue chan *clientReq

	bus *Bus
	log log.Logger
}

type SendOption interface {
	before(req *clientReq)
	after(req *clientReq)
}

func (c *ClientConn) Close() {
	c.bus.Close()
}

func (c *ClientConn) Done() <-chan struct{} {
	return c.stop
}

func (c *ClientConn) Err() (err error) {
	c.l.Lock()
	err = c.err
	c.l.Unlock()
	return
}

func (c *ClientConn) fail(err error) {
	c.l.Lock()
	if c.err == nil {
		c.err = err
		close(c.stop)
	}
	c.l.Unlock()
}

func (c *ClientConn) runSender() {
	for {
		select {
		case req := <-c.cancelQueue:
			c.l.Lock()
			delete(c.reqs, req.id)
			c.l.Unlock()

			// TODO(prime@):

		case req := <-c.sendQueue:
			msg := make([][]byte, 2+len(req.reqAttachments))

			var err error
			var protoHeader []byte
			if protoHeader, err = proto.Marshal(&req.reqHeader); err != nil {
				req.done <- err
				continue
			}

			msg[0] = make([]byte, 4, 4+len(protoHeader))
			binary.LittleEndian.PutUint32(msg[0], uint32(msgRequest))
			msg[0] = append(msg[0], protoHeader...)

			if msg[1], err = proto.Marshal(req.request); err != nil {
				req.done <- err
				continue
			}

			for i, blob := range req.reqAttachments {
				msg[2+i] = blob
			}

			c.l.Lock()
			c.reqs[req.id] = req
			c.l.Unlock()

			if err := c.bus.Send(msg); err != nil {
				c.fail(err)
				return
			}

		case <-c.stop:
			return
		}
	}
}

func (c *ClientConn) handleMsg(msg [][]byte) error {
	if len(msg) == 0 {
		return fmt.Errorf("bus: received empty message")
	}

	if len(msg[0]) < 4 {
		return fmt.Errorf("bus: message type is missing")
	}

	if msgType(binary.LittleEndian.Uint32(msg[0][:4])) != msgResponse {
		// TODO(prime@): log
		return nil
	}

	var rsp rpc.TResponseHeader
	if err := proto.Unmarshal(msg[0][4:], &rsp); err != nil {
		return err
	}

	if rsp.RequestId == nil {
		return fmt.Errorf("bus: response is missing request_id")
	}

	reqID := misc.NewGUIDFromProto(rsp.RequestId)

	c.l.Lock()
	req, ok := c.reqs[reqID]
	delete(c.reqs, reqID)
	c.l.Unlock()

	if !ok {
		// TODO(prime@): log
		return nil
	}

	req.rspHeader = rsp
	if replyErr := misc.NewErrorFromProto(req.rspHeader.Error); replyErr != nil {
		req.done <- replyErr
		return nil
	}

	if len(msg) < 2 {
		req.done <- fmt.Errorf("bus: reply body is missing")
		return nil
	}

	if err := proto.Unmarshal(msg[1], req.reply); err != nil {
		req.done <- err
		return nil
	}

	req.rspAttachments = msg[2:]
	req.done <- nil
	return nil
}

func (c *ClientConn) runReceiver() {
	for {
		msg, err := c.bus.Receive()
		if err != nil {
			c.fail(err)
			return
		}

		if err := c.handleMsg(msg); err != nil {
			c.fail(err)
			return
		}
	}
}

func NewClient(ctx context.Context, address string) (*ClientConn, error) {
	bus, err := Dial(ctx, Options{Address: address})
	if err != nil {
		return nil, err
	}

	client := &ClientConn{
		reqs:        make(map[guid.GUID]*clientReq, maxInFlightRequests),
		sendQueue:   make(chan *clientReq, maxInFlightRequests),
		cancelQueue: make(chan *clientReq, maxInFlightRequests),
		stop:        make(chan struct{}),

		bus: bus,
	}

	go client.runSender()
	go client.runReceiver()

	return client, nil
}

func (c *ClientConn) Send(
	ctx context.Context,
	service, method string,
	request, reply proto.Message,
	opts ...SendOption,
) error {
	reqID := guid.New()
	req := &clientReq{
		id:      reqID,
		request: request,
		reply:   reply,

		reqHeader: rpc.TRequestHeader{
			RequestId: misc.NewProtoFromGUID(reqID),
			Service:   &service,
			Method:    &method,
			UserAgent: &ytGoClient,

			RequestCodec:  &codecNone,
			ResponseCodec: &codecNone,
		},

		done: make(chan error, 1),
	}

	for _, opt := range opts {
		opt.before(req)
	}

	select {
	case c.sendQueue <- req:

	case <-c.stop:
		return c.err

	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-c.stop:
		return c.err

	case err := <-req.done:
		if err != nil {
			return err
		}

	case <-ctx.Done():
		select {
		case c.cancelQueue <- req:
		default:
			c.l.Lock()
			delete(c.reqs, req.id)
			c.l.Unlock()
		}
		return ctx.Err()
	}

	for _, opt := range opts {
		opt.after(req)
	}

	return nil
}
