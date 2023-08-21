package bus

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/nop"
	"go.ytsaurus.tech/yt/go/crc64"
	"go.ytsaurus.tech/yt/go/guid"
)

type AttributeKey string

const (
	AttributeKeyFeatureID   AttributeKey = "feature_id"
	AttributeKeyFeatureName AttributeKey = "feature_name"
)

type Options struct {
	Address string
	Logger  log.Logger
}

type packetType int16
type packetFlags int16

const (
	packetMessage = packetType(0)
	packetAck     = packetType(1)

	packetFlagsNone                   = packetFlags(0x0000)
	packetFlagsRequestAcknowledgement = packetFlags(0x0001)

	packetSignature = uint32(0x78616d4f)
	nullPartSize    = uint32(0xffffffff)
	maxPartSize     = 512 * 1024 * 1024
	maxPartCount    = 64
	fixHeaderSize   = 36
)

type fixedHeader struct {
	signature uint32
	typ       packetType
	flags     packetFlags
	packetID  guid.GUID
	partCount uint32
	checksum  uint64
}

func (p *fixedHeader) data(computeCRC bool) []byte {
	res := make([]byte, fixHeaderSize)
	binary.LittleEndian.PutUint32(res[0:4], p.signature)
	binary.LittleEndian.PutUint16(res[4:6], uint16(p.typ))
	binary.LittleEndian.PutUint16(res[6:8], uint16(p.flags))

	a, b, c, d := p.packetID.Parts()
	binary.LittleEndian.PutUint32(res[8:12], a)
	binary.LittleEndian.PutUint32(res[12:16], b)
	binary.LittleEndian.PutUint32(res[16:20], c)
	binary.LittleEndian.PutUint32(res[20:24], d)

	binary.LittleEndian.PutUint32(res[24:28], p.partCount)

	if computeCRC {
		binary.LittleEndian.PutUint64(res[28:36], crc64.Checksum(res[0:28]))
	}

	return res
}

type variableHeader struct {
	sizes          []uint32
	checksums      []uint64
	headerChecksum uint64
}

func (p *variableHeader) dataSize() int {
	return (8+4)*len(p.sizes) + 8
}

func (p *variableHeader) data(computeCRC bool) []byte {
	res := make([]byte, p.dataSize())
	c := 0
	for _, p := range p.sizes {
		binary.LittleEndian.PutUint32(res[c:c+4], p)
		c = c + 4
	}

	for _, p := range p.checksums {
		binary.LittleEndian.PutUint64(res[c:c+8], p)
		c = c + 8
	}

	if computeCRC {
		binary.LittleEndian.PutUint64(res[c:c+8], crc64.Checksum(res[:c]))
	}
	return res
}

type busMsg struct {
	fixHeader fixedHeader
	varHeader variableHeader
	parts     [][]byte
}

func newMessagePacket(id guid.GUID, data [][]byte, flags packetFlags, computeCRC bool) busMsg {
	hdr := fixedHeader{
		typ:       packetMessage,
		flags:     flags,
		partCount: uint32(len(data)),
		packetID:  id,
		checksum:  0,
		signature: packetSignature,
	}

	varHdr := variableHeader{
		checksums: make([]uint64, len(data)),
		sizes:     make([]uint32, len(data)),
	}

	for i, p := range data {
		if p == nil {
			varHdr.sizes[i] = nullPartSize
		} else {
			varHdr.sizes[i] = uint32(len(p))
		}

		if computeCRC {
			varHdr.checksums[i] = crc64.Checksum(p)
		}
	}

	return busMsg{
		fixHeader: hdr,
		varHeader: varHdr,
		parts:     data,
	}
}

func (p *busMsg) writeTo(w io.Writer) (int, error) {
	n := 0

	l, err := w.Write(p.fixHeader.data(true))
	if err != nil {
		return n + l, err
	}
	n += l

	l, err = w.Write(p.varHeader.data(true))
	if err != nil {
		return n + l, err
	}
	n += l

	for _, p := range p.parts {
		l, err := w.Write(p)
		if err != nil {
			return n + l, err
		}
		n += l
	}

	return n, nil
}

type Bus struct {
	id      guid.GUID
	options Options
	conn    net.Conn
	logger  log.Logger
	once    sync.Once
}

func NewBus(conn net.Conn, options Options) *Bus {
	bus := &Bus{
		id:      guid.New(),
		options: options,
		conn:    conn,
	}

	logger := options.Logger
	if logger == nil {
		logger = &nop.Logger{}
	}
	bus.setLogger(logger)

	return bus
}

func (c *Bus) setLogger(l log.Logger) {
	c.logger = log.With(l, log.Any("busID", c.id))
}

func (c *Bus) Close() {
	c.once.Do(func() {
		if err := c.conn.Close(); err != nil {
			c.logger.Error("Connection close error", log.Error(err))
		}

		c.logger.Debug("Bus closed")
	})
}

type DeliveryTrackingLevel int

const (
	DeliveryTrackingLevelNone      DeliveryTrackingLevel = 0
	DeliveryTrackingLevelErrorOnly DeliveryTrackingLevel = 1
	DeliveryTrackingLevelFull      DeliveryTrackingLevel = 2
)

type busSendOptions struct {
	DeliveryTrackingLevel DeliveryTrackingLevel
}

func (c *Bus) Send(packetID guid.GUID, packetData [][]byte, opts *busSendOptions) error {
	flags := packetFlagsNone
	if opts.DeliveryTrackingLevel == DeliveryTrackingLevelFull {
		flags = packetFlagsRequestAcknowledgement
	}

	packet := newMessagePacket(packetID, packetData, flags, true)
	l, err := packet.writeTo(c.conn)
	if err != nil {
		c.logger.Error("Unable to send packet", log.Error(err))
		return err
	}

	c.logger.Debug("Packet sent",
		log.String("id", packet.fixHeader.packetID.String()),
		log.Any("bytes", l))

	return nil
}

func (c *Bus) Receive() (busMsg, error) {
	packet, err := c.receive(c.conn)
	if err != nil {
		c.logger.Error("Receive error", log.Error(err))
		return busMsg{}, err
	}

	c.logger.Debug("Packet received",
		log.String("id", packet.fixHeader.packetID.String()),
		log.Any("type", packet.fixHeader.typ),
		log.Int16("flags", int16(packet.fixHeader.flags)))

	return packet, nil
}

func (c *Bus) receive(message io.Reader) (busMsg, error) {
	rawFixHeader := make([]byte, fixHeaderSize)
	if _, err := io.ReadFull(message, rawFixHeader); err != nil {
		return busMsg{}, fmt.Errorf("bus: error reading fix header: %w", err)
	}

	fixHeader := fixedHeader{}
	fixHeader.signature = binary.LittleEndian.Uint32(rawFixHeader[0:4])
	if fixHeader.signature != packetSignature {
		return busMsg{}, errors.New("bus: signature mismatch")
	}

	fixHeader.typ = packetType(binary.LittleEndian.Uint16(rawFixHeader[4:6]))
	fixHeader.flags = packetFlags(binary.LittleEndian.Uint16(rawFixHeader[6:8]))
	fixHeader.packetID = guid.FromParts(
		binary.LittleEndian.Uint32(rawFixHeader[8:12]),
		binary.LittleEndian.Uint32(rawFixHeader[12:16]),
		binary.LittleEndian.Uint32(rawFixHeader[16:20]),
		binary.LittleEndian.Uint32(rawFixHeader[20:24]),
	)
	fixHeader.partCount = binary.LittleEndian.Uint32(rawFixHeader[24:28])
	fixHeader.checksum = binary.LittleEndian.Uint64(rawFixHeader[28:])

	if fixHeader.checksum != crc64.Checksum(rawFixHeader[:28]) {
		return busMsg{}, fmt.Errorf("bus: fixed header checksum mismatch")
	}

	if fixHeader.partCount > maxPartCount {
		return busMsg{}, fmt.Errorf("bus: too many parts: %d > %d", fixHeader.partCount, maxPartCount)
	}

	if fixHeader.partCount == 0 {
		return busMsg{
			parts:     nil,
			fixHeader: fixHeader,
			varHeader: variableHeader{},
		}, nil
	}

	// variableHeader
	varHeader := variableHeader{
		sizes:     make([]uint32, fixHeader.partCount),
		checksums: make([]uint64, fixHeader.partCount),
	}

	rawVarHeader := make([]byte, int((4+8)*fixHeader.partCount+8))
	if _, err := io.ReadFull(message, rawVarHeader); err != nil {
		return busMsg{}, fmt.Errorf("bus: error reading var header: %w", err)
	}

	p := 0
	for i := range varHeader.sizes {
		varHeader.sizes[i] = binary.LittleEndian.Uint32(rawVarHeader[p : p+4])
		p = p + 4
	}
	for i := range varHeader.checksums {
		varHeader.checksums[i] = binary.LittleEndian.Uint64(rawVarHeader[p : p+8])
		p = p + 8
	}
	varHeader.headerChecksum = binary.LittleEndian.Uint64(rawVarHeader[p:])

	if varHeader.headerChecksum != crc64.Checksum(rawVarHeader[:p]) {
		return busMsg{}, fmt.Errorf("bus: variabled header checksum mismatch, expected %x got %x",
			varHeader.headerChecksum, crc64.Checksum(rawVarHeader[:p]))
	}

	parts := make([][]byte, fixHeader.partCount)
	for i, partSize := range varHeader.sizes {
		var part []byte
		if partSize != nullPartSize {
			if partSize > maxPartSize {
				return busMsg{}, fmt.Errorf("bus: part is to big, max %v, actual %v", maxPartSize, partSize)
			}

			part = make([]byte, partSize)
			if _, err := io.ReadFull(message, part); err != nil {
				return busMsg{}, fmt.Errorf("bus: error reading part %d: %w", i, err)
			}
			parts[i] = part
		}

		if varHeader.checksums[i] != crc64.Checksum(part) {
			return busMsg{}, fmt.Errorf("bus: part checksum mismatch")
		}
	}

	return busMsg{
		parts:     parts,
		fixHeader: fixHeader,
		varHeader: varHeader,
	}, nil
}

func Dial(ctx context.Context, options Options) (*Bus, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", options.Address)
	if err != nil {
		return nil, err
	}

	return NewBus(conn, options), nil
}
