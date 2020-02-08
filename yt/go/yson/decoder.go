package yson

import (
	"io"
)

type Decoder struct {
	R *Reader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{R: NewReader(r)}
}

func NewDecoderFromBytes(b []byte) *Decoder {
	return &Decoder{R: NewReaderFromBytes(b)}
}

func (d *Decoder) Decode(v interface{}) error {
	return decodeAny(d.R, v)
}

func (d *Decoder) CheckFinish() error {
	return d.R.CheckFinish()
}
