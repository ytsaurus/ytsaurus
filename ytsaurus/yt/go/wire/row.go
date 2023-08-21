package wire

import (
	"fmt"
	"math"
)

type ValueType uint8

const (
	typeOffset = 0x02

	// Subtract 2 from all constants, such that Null is represented by zero.
	TypeNull    ValueType = 0x02 - typeOffset
	TypeInt64   ValueType = 0x03 - typeOffset
	TypeUint64  ValueType = 0x04 - typeOffset
	TypeFloat64 ValueType = 0x05 - typeOffset
	TypeBool    ValueType = 0x06 - typeOffset
	TypeBytes   ValueType = 0x10 - typeOffset
	TypeAny     ValueType = 0x11 - typeOffset
)

func (t ValueType) String() string {
	switch t {
	case TypeNull:
		return "null"
	case TypeInt64:
		return "int64"
	case TypeUint64:
		return "uint64"
	case TypeFloat64:
		return "float64"
	case TypeBool:
		return "bool"
	case TypeBytes:
		return "bytes"
	case TypeAny:
		return "any"
	default:
		return ""
	}
}

type Value struct {
	ID        uint16
	Type      ValueType
	Aggregate bool

	scalar uint64
	blob   []byte
}

func (v *Value) Bool() bool {
	return v.scalar != 0
}

func (v *Value) Int64() int64 {
	return int64(v.scalar)
}

func (v *Value) Uint64() uint64 {
	return v.scalar
}

func (v *Value) Float64() float64 {
	return math.Float64frombits(v.scalar)
}

func (v *Value) Bytes() []byte {
	return v.blob
}

func (v *Value) Any() []byte {
	return v.blob
}

func NewNull(id uint16) (v Value) {
	v.ID = id
	return
}

func NewInt64(id uint16, i int64) (v Value) {
	v.ID = id
	v.Type = TypeInt64
	v.scalar = uint64(i)
	return
}

func NewUint64(id uint16, i uint64) (v Value) {
	v.ID = id
	v.Type = TypeUint64
	v.scalar = i
	return
}

func NewFloat64(id uint16, f float64) (v Value) {
	v.ID = id
	v.Type = TypeFloat64
	v.scalar = math.Float64bits(f)
	return
}

func NewBool(id uint16, b bool) (v Value) {
	v.ID = id
	v.Type = TypeBool
	if b {
		v.scalar = 1
	} else {
		v.scalar = 0
	}
	return
}

func NewBytes(id uint16, b []byte) (v Value) {
	v.ID = id
	v.Type = TypeBytes
	v.blob = b
	return
}

func NewAny(id uint16, b []byte) (v Value) {
	v.ID = id
	v.Type = TypeAny
	v.blob = b
	return
}

func (v *Value) Validate() error {
	switch v.Type {
	case TypeAny, TypeBytes:
		if len(v.blob) > maxValueLength {
			return fmt.Errorf("value is too long: actual=%d, limit=%d", len(v.blob), maxValueLength)
		}
	default:
	}

	return nil
}

func (v *Value) wireSize() int {
	size := 8
	switch v.Type {
	case TypeNull:
	case TypeAny, TypeBytes:
		size += len(v.blob) + computePadding(len(v.blob))
	default:
		size += 8
	}
	return size
}

type Row []Value
