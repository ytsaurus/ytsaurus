package yson

import (
	"io"
	"reflect"
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

func zeroInitialize(v interface{}) {
	value := reflect.ValueOf(v)
	if value.Kind() != reflect.Ptr {
		return
	}

	value = value.Elem()
	value.Set(reflect.Zero(value.Type()))
}

func (d *Decoder) Decode(v interface{}) error {
	zeroInitialize(v)
	return decodeAny(d.R, v)
}

func (d *Decoder) CheckFinish() error {
	return d.R.CheckFinish()
}
