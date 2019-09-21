package skiff

import (
	"bufio"
	"encoding/binary"
	"io"
	"math"
)

type writer struct {
	w   *bufio.Writer
	err error
}

func newWriter(w io.Writer) *writer {
	return &writer{
		w: bufio.NewWriter(w),
	}
}

func (w *writer) writeByte(b byte) {
	if w.err != nil {
		return
	}

	w.err = w.w.WriteByte(b)
}

func (w *writer) writeInt64(i int64) {
	if w.err != nil {
		return
	}

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(i))
	_, w.err = w.w.Write(buf[:])
}

func (w *writer) writeUint64(i uint64) {
	if w.err != nil {
		return
	}

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], i)
	_, w.err = w.w.Write(buf[:])
}

func (w *writer) writeDouble(d float64) {
	if w.err != nil {
		return
	}

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(d))
	_, w.err = w.w.Write(buf[:])
}

func (w *writer) writeUint32(i uint32) {
	if w.err != nil {
		return
	}

	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], i)
	_, w.err = w.w.Write(buf[:])
}

func (w *writer) writeUint16(i uint16) {
	if w.err != nil {
		return
	}

	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], i)
	_, w.err = w.w.Write(buf[:])
}

func (w *writer) writeBytes(b []byte) {
	if w.err != nil {
		return
	}

	w.writeUint32(uint32(len(b)))
	if w.err != nil {
		return
	}

	_, w.err = w.w.Write(b)
}
