package bus

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"a.yandex-team.ru/library/go/core/log/nop"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/crc64"
	"a.yandex-team.ru/yt/go/guid"
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

	packetFlagsNone = packetFlags(0x0000)

	packetSignature = uint32(0x78616d4f)
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
		binary.LittleEndian.PutUint64(res[28:36], crc64.Checksum(res))
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
		binary.LittleEndian.PutUint64(res[c:c+8], crc64.Checksum(res))
	}
	return res
}

type tcpPacket struct {
	fixHeader fixedHeader
	varHeader variableHeader
	data      [][]byte
}

func newPacket(data [][]byte, computeCRC bool) tcpPacket {
	hdr := fixedHeader{
		typ:       packetMessage,
		flags:     packetFlagsNone,
		partCount: uint32(len(data)),
		packetID:  guid.New(),
		checksum:  0,
		signature: packetSignature,
	}

	varHdr := variableHeader{
		checksums: make([]uint64, len(data)),
		sizes:     make([]uint32, len(data)),
	}

	for i, p := range data {
		varHdr.sizes[i] = uint32(len(p))

		if computeCRC {
			varHdr.checksums[i] = crc64.Checksum(p)
		}
	}

	return tcpPacket{
		fixHeader: hdr,
		varHeader: varHdr,
		data:      data,
	}
}

func (p *tcpPacket) writeTo(w io.Writer) (int, error) {
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

	for _, p := range p.data {
		l, err := w.Write(p)
		if err != nil {
			return n + l, err
		}
		n += l
	}

	return n, nil
}

type Bus struct {
	options Options
	conn    net.Conn
	logger  log.Logger
	once    sync.Once
}

func (c *Bus) Close() {
	c.once.Do(func() {
		if err := c.conn.Close(); err != nil {
			c.logger.Error("Connection close error", log.Error(err))
		}

		c.logger.Debug("Bus closed")
	})
}

func (c *Bus) Send(data [][]byte) error {
	packet := newPacket(data, true)
	l, err := packet.writeTo(c.conn)
	if err != nil {
		c.logger.Error("Unable to send packet", log.Error(err))
		return err
	}

	c.logger.Debug("Packet sent", log.Any("bytes", l))
	return nil
}

func (c *Bus) Receive() ([][]byte, error) {
	for {
		packet, err := c.receive(c.conn)
		if err != nil {
			c.logger.Error("Receive error", log.Error(err))
			return nil, err
		}

		if packet.fixHeader.typ == packetAck {
			c.logger.Debug("Receive ack", log.Any("id", packet.fixHeader.packetID.String()))
			continue
		}

		return packet.data, nil
	}
}

func (c *Bus) receive(message io.Reader) (tcpPacket, error) {
	rawFHdr := make([]byte, fixHeaderSize)
	if _, err := io.ReadFull(message, rawFHdr); err != nil {
		return tcpPacket{}, err
	}

	fhdr := fixedHeader{}
	fhdr.signature = binary.LittleEndian.Uint32(rawFHdr[0:4])
	if fhdr.signature != packetSignature {
		return tcpPacket{}, errors.New("signature mismatch")
	}

	fhdr.typ = packetType(binary.LittleEndian.Uint16(rawFHdr[4:6]))
	fhdr.flags = packetFlags(binary.LittleEndian.Uint16(rawFHdr[6:8]))
	fhdr.packetID = guid.FromParts(
		binary.LittleEndian.Uint32(rawFHdr[8:12]),
		binary.LittleEndian.Uint32(rawFHdr[12:16]),
		binary.LittleEndian.Uint32(rawFHdr[16:20]),
		binary.LittleEndian.Uint32(rawFHdr[20:24]),
	)
	fhdr.partCount = binary.LittleEndian.Uint32(rawFHdr[24:28])
	fhdr.checksum = binary.LittleEndian.Uint64(rawFHdr[28:])

	if fhdr.partCount > maxPartCount {
		return tcpPacket{}, fmt.Errorf("bus: too many parts: %d > %d", fhdr.partCount, maxPartCount)
	}

	// variableHeader
	varHdr := variableHeader{
		checksums: make([]uint64, fhdr.partCount),
		sizes:     make([]uint32, fhdr.partCount),
	}

	vSize := int((4+8)*fhdr.partCount + 8)
	rV := make([]byte, vSize)
	if _, err := io.ReadFull(message, rV); err != nil {
		return tcpPacket{}, err
	}

	p := 0
	for i := range varHdr.sizes {
		varHdr.sizes[i] = binary.LittleEndian.Uint32(rV[p : p+4])
		p = p + 4
	}
	for i := range varHdr.checksums {
		varHdr.checksums[i] = binary.LittleEndian.Uint64(rawFHdr[p : p+8])
		p = p + 8
	}

	res := make([][]byte, fhdr.partCount)
	for i, d := range varHdr.sizes {
		if d > maxPartSize {
			return tcpPacket{}, fmt.Errorf("bus: part is to big, max %v, actual %v", maxPartSize, d)
		}
		partB := make([]byte, d)
		if _, err := io.ReadFull(message, partB); err != nil {
			return tcpPacket{}, err
		}
		res[i] = partB
	}

	return tcpPacket{
		data:      res,
		fixHeader: fhdr,
		varHeader: varHdr,
	}, nil
}

func NewBus(conn net.Conn, options Options) *Bus {
	logger := options.Logger
	if logger == nil {
		logger = &nop.Logger{}
	}

	return &Bus{
		options: options,
		conn:    conn,
		logger:  log.With(logger, log.Any("busID", guid.New().String())),
		once:    sync.Once{},
	}
}

func Dial(ctx context.Context, options Options) (*Bus, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", options.Address)
	if err != nil {
		return nil, err
	}

	return NewBus(conn, options), nil
}
