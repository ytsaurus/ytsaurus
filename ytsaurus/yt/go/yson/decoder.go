package yson

import (
	"io"
)

type DecoderOptions struct {
	SupportYPAPIMaps bool
}

type Decoder struct {
	opts *DecoderOptions

	R *Reader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{R: NewReader(r)}
}

func NewDecoderFromBytes(b []byte) *Decoder {
	return &Decoder{R: NewReaderFromBytes(b)}
}

func (d *Decoder) Decode(v any) error {
	return decodeAny(d.R, v, d.opts)
}

func (d *Decoder) CheckFinish() error {
	return d.R.CheckFinish()
}
