package skiff

import (
	"encoding/binary"
	"math"
)

func (r *Reader) Double() float64 {
	return math.Float64frombits(r.Uint64())
}

func (r *Reader) Int64() int64 {
	return int64(r.Uint64())
}

func (r *Reader) Uint64() uint64 {
	return binary.LittleEndian.Uint64(r.Pull(8))
}

func (r *Reader) Uint32() uint32 {
	return binary.LittleEndian.Uint32(r.Pull(4))
}

func (r *Reader) Uint16() uint16 {
	return binary.LittleEndian.Uint16(r.Pull(2))
}

func (r *Reader) Uint8() uint8 {
	return r.Pull(1)[0]
}

type Reader struct {
	err error
}

// Pull reads n bytes from input stream and advances input forward.
//
// Returned slice points directly into decoder buffer and will be overwritten by successive Pull calls.
//
// If error occurs, silently returns slice filled with zeroes. Use Err() method to check for error.
func (r *Reader) Pull(n uint32) []byte {
	return nil
}

func (r *Reader) Err() error {
	return r.err
}

type Unmarshaler interface {
	UnmarhsalSkiff(r *Reader) error
}
