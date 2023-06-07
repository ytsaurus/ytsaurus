package skiff

import (
	"encoding/binary"
	"io"
	"math"

	"go.ytsaurus.tech/library/go/core/xerrors"
)

const (
	stringSizeLimit   = 128 * 1024 * 1024 // 128MB is hard limit for row size in YT
	initialBufferSize = 16 * 1024
)

type reader struct {
	underlying io.Reader

	buf      []byte
	pos, end int
	keep     int

	err error
}

func newReader(r io.Reader) *reader {
	return &reader{
		underlying: r,
		buf:        make([]byte, initialBufferSize),
	}
}

func (r *reader) readDouble() float64 {
	return math.Float64frombits(r.readUint64())
}

func (r *reader) readInt64() int64 {
	return int64(r.readUint64())
}

func (r *reader) readUint64() uint64 {
	return binary.LittleEndian.Uint64(r.pull(8))
}

func (r *reader) readUint32() uint32 {
	return binary.LittleEndian.Uint32(r.pull(4))
}

func (r *reader) readUint16() uint16 {
	return binary.LittleEndian.Uint16(r.pull(2))
}

func (r *reader) readUint8() uint8 {
	return r.pull(1)[0]
}

func (r *reader) cleanEOF() {
	if r.err == io.EOF && r.pos == r.end {
		r.err = nil
	}
}

func (r *reader) checkpoint() {
	r.keep = r.pos
}

func (r *reader) backup() {
	r.pos = r.keep
}

func (r *reader) readBytes() []byte {
	size := int(r.readUint32())
	if size > stringSizeLimit {
		r.err = xerrors.Errorf("skiff: blob size is too large %d > %d", size, stringSizeLimit)
		return nil
	}

	return r.pull(size)
}

// Pull reads n bytes from input stream and advances input forward.
//
// Returned slice points directly into decoder buffer and will be overwritten by successive Pull calls.
//
// If error occurs, silently returns slice filled with zeroes. Use Err() method to check for error.
func (r *reader) pull(n int) []byte {
	if r.end-r.pos < n {
		if r.end-r.keep < len(r.buf)/16 {
			copy(r.buf[:r.end-r.keep], r.buf[r.keep:r.end])
			r.end = r.end - r.keep
			r.pos = r.pos - r.keep
			r.keep = 0
		}

		size := len(r.buf)
		for r.pos+n > size {
			size *= 2
		}

		if size != len(r.buf) {
			r.buf = append(r.buf, make([]byte, size-len(r.buf))...)
		}

		var read int
		read, r.err = io.ReadAtLeast(r.underlying, r.buf[r.end:], n-(r.end-r.pos))
		r.end += read
	}

	slice := r.buf[r.pos : r.pos+n]
	if r.err == nil {
		r.pos += n
	}
	return slice
}
