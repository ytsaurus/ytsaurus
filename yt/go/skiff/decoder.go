package skiff

import (
	"encoding/binary"
	"math"
)

func (d *Decoder) Double() float64 {
	return math.Float64frombits(d.Uint64())
}

func (d *Decoder) Int64() int64 {
	return int64(d.Uint64())
}

func (d *Decoder) Uint64() uint64 {
	return binary.LittleEndian.Uint64(d.Pull(8))
}

func (d *Decoder) Uint32() uint32 {
	return binary.LittleEndian.Uint32(d.Pull(4))
}

func (d *Decoder) Uint16() uint16 {
	return binary.LittleEndian.Uint16(d.Pull(2))
}

func (d *Decoder) Uint8() uint8 {
	return d.Pull(1)[0]
}

type Decoder struct {
	err error
}

// Pull reads n bytes from input stream and advances input forward.
//
// Returned slice points directly into decoder buffer and will be overwritten by successive Pull calls.
//
// If error occurs, silently returns slice filled with zeroes. Use Err() method to check for error.
func (d *Decoder) Pull(n uint32) []byte {
	return nil
}

func (d *Decoder) Checkpoint() {

}

func (d *Decoder) Restore() {

}

func (d *Decoder) Err() error {
	return d.err
}

type Unmarshaler interface {
	UnmarhsalSkiff(d *Decoder) error
}
