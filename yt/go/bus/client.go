package bus

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/crc64"
	"a.yandex-team.ru/yt/go/guid"
)

type Config struct {
	Address string
	Logger  log.Logger
}

type packetType int16
type packetFlags int16

const (
	packetMessage = packetType(0)
	packetAck     = packetType(1)
)

const (
	packetFlagsNone = packetFlags(0x0000)
)

const (
	packetSignature = uint32(0x78616d4f)
)

const (
	maxPartSize   = math.MaxInt32
	fixHeaderSize = 36
)

type fixedHeader struct {
	signature uint32
	typ       packetType
	flags     packetFlags
	packetID  guid.GUID
	partCount uint32
	checksum  uint64
}

func (p *fixedHeader) data() []byte {
	res := make([]byte, 36)
	binary.LittleEndian.PutUint32(res[0:4], p.signature)
	binary.LittleEndian.PutUint16(res[4:6], uint16(p.typ))
	binary.LittleEndian.PutUint16(res[6:8], uint16(p.flags))

	a, b, c, d := p.packetID.Parts()
	binary.LittleEndian.PutUint32(res[8:12], a)
	binary.LittleEndian.PutUint32(res[12:16], b)
	binary.LittleEndian.PutUint32(res[16:20], c)
	binary.LittleEndian.PutUint32(res[20:24], d)

	binary.LittleEndian.PutUint32(res[24:28], p.partCount)
	binary.LittleEndian.PutUint64(res[28:36], p.checksum)
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

func (p *variableHeader) data() []byte {
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

	binary.LittleEndian.PutUint64(res[c:c+8], p.headerChecksum)
	return res
}

type tcpPacket struct {
	fixHeader fixedHeader
	varHeader variableHeader
	data      [][]byte
}

func newPacket(data [][]byte) tcpPacket {
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
		d := crc64.New()
		if _, err := d.Write(p); err != nil {
			varHdr.checksums[i] = uint64(0)
		} else {
			varHdr.checksums[i] = d.Sum64()
		}
	}

	return tcpPacket{
		fixHeader: hdr,
		varHeader: varHdr,
		data:      data,
	}
}

func (p *tcpPacket) writeTo(w io.Writer) (int, error) {
	r := 0
	l, err := w.Write(p.fixHeader.data())
	if err != nil {
		return l, err
	}
	r += l
	l, err = w.Write(p.varHeader.data())
	if err != nil {
		return l, err
	}
	r += l

	// Data
	for _, p := range p.data {
		l, err := w.Write(p)
		if err != nil {
			return l, err
		}
		r += l
	}

	return r, nil
}

type Сlient struct {
	config Config
	conn   net.Conn
	logger log.Logger
	once   sync.Once
}

func (c *Сlient) Close() {
	c.once.Do(func() {
		if err := c.conn.Close(); err != nil {
			c.logger.Error("Connection close error", log.Error(err))
		}

		c.logger.Debug("Bus closed")
	})
}

func (c *Сlient) Send(data [][]byte) error {
	packet := newPacket(data)
	l, err := packet.writeTo(c.conn)
	if err != nil {
		c.logger.Error("Unable to send packet", log.Error(err))
		return err
	}

	c.logger.Debug("Packet Send", log.Any("bytes", l))
	return nil
}

func (c *Сlient) Receive() ([][]byte, error) {
	packet, err := c.receive(c.conn)
	if err != nil {
		c.logger.Error("Receive error", log.Error(err))
		return nil, err
	}

	if packet.fixHeader.typ == packetAck {
		c.logger.Debug("Receive ack", log.Any("id", packet.fixHeader.packetID.String()))
		return c.Receive()
	}

	return packet.data, nil
}

func (c *Сlient) receive(message io.Reader) (tcpPacket, error) {
	// FixedHeader
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
			return tcpPacket{}, fmt.Errorf(
				"part is to big to receive, max %v, actual %v",
				maxPartSize,
				d,
			)
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

func DialBus(ctx context.Context, config Config) (*Сlient, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", config.Address)
	if err != nil {
		return nil, err
	}

	cl := Сlient{
		config: config,
		conn:   conn,
		logger: log.With(config.Logger, log.Any("busID", guid.New().String())),
		once:   sync.Once{},
	}

	return &cl, nil
}
