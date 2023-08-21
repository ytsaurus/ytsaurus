// Package yson implements encoding and decoding of YSON.
//
// See https://wiki.yandex-team.ru/yt/userdoc/yson/ for introduction to YSON.
//
// Package provides interface similar to encoding/json package.
//
//	type myValue struct {
//	     A int    `yson:"a"`
//	     B string `yson:"b"`
//	}
//
//	func Example() error {
//	    var v myValue
//	    b := []byte("{a=1;b=c;}")
//
//	    if err := yson.Unmarshal(b, &v); err != nil {
//	        return err
//	    }
//
//	    if err, b = yson.Marshal(v); err != nil {
//	        return err
//	    }
//
//	    return nil
//	}
package yson

import (
	"encoding"
	"errors"
	"math"
	"reflect"
	"strconv"
	"time"
)

// Unmarshaler is an interface implemented by types that can unmarshal themselves from YSON.
// Input can be assumed to be a valid YSON value.
type Unmarshaler interface {
	UnmarshalYSON([]byte) error
}

// StreamUnmarshaler is an interface implemented by types that can unmarshal themselves from YSON reader.
type StreamUnmarshaler interface {
	UnmarshalYSON(*Reader) error
}

func nextLiteral(r *Reader) error {
	event, err := r.Next(true)
	if err != nil {
		return err
	}

	// Type error is checked in callee.
	if event == EventBeginMap {
		r.currentType = TypeMap
	} else if event == EventBeginList {
		r.currentType = TypeList
	}

	return nil
}

var ErrIntegerOverflow = errors.New("yson: integer overflow")

func decodeInt(r *Reader, bits int) (i int64, err error) {
	if err = nextLiteral(r); err != nil {
		return
	}

	switch r.currentType {
	case TypeInt64:
		switch bits {
		case 8:
			if r.currentInt > math.MaxInt8 || r.currentInt < math.MinInt8 {
				return 0, ErrIntegerOverflow
			}
		case 16:
			if r.currentInt > math.MaxInt16 || r.currentInt < math.MinInt16 {
				return 0, ErrIntegerOverflow
			}
		case 32:
			if r.currentInt > math.MaxInt32 || r.currentInt < math.MinInt32 {
				return 0, ErrIntegerOverflow
			}
		}

		i = r.currentInt
	case TypeUint64:
		switch bits {
		case 8:
			if r.currentUint > math.MaxInt8 {
				return 0, ErrIntegerOverflow
			}
		case 16:
			if r.currentUint > math.MaxInt16 {
				return 0, ErrIntegerOverflow
			}
		case 32:
			if r.currentUint > math.MaxInt32 {
				return 0, ErrIntegerOverflow
			}
		case 64:
			if r.currentUint > math.MaxInt64 {
				return 0, ErrIntegerOverflow
			}
		}

		i = int64(r.currentUint)
	default:
		return 0, &TypeError{UserType: reflect.TypeOf(i), YSONType: r.currentType}
	}

	return
}

func decodeUint(r *Reader, bits int) (u uint64, err error) {
	if err = nextLiteral(r); err != nil {
		return
	}

	switch r.currentType {
	case TypeInt64:
		if r.currentInt < 0 {
			return 0, ErrIntegerOverflow
		}

		switch bits {
		case 8:
			if r.currentInt > math.MaxUint8 {
				return 0, ErrIntegerOverflow
			}
		case 16:
			if r.currentInt > math.MaxUint16 {
				return 0, ErrIntegerOverflow
			}
		case 32:
			if r.currentInt > math.MaxUint32 {
				return 0, ErrIntegerOverflow
			}
		}

		u = uint64(r.currentInt)
	case TypeUint64:
		switch bits {
		case 8:
			if r.currentUint > math.MaxUint8 {
				return 0, ErrIntegerOverflow
			}
		case 16:
			if r.currentUint > math.MaxUint16 {
				return 0, ErrIntegerOverflow
			}
		case 32:
			if r.currentUint > math.MaxUint32 {
				return 0, ErrIntegerOverflow
			}
		}

		u = r.currentUint
	default:
		return 0, &TypeError{UserType: reflect.TypeOf(u), YSONType: r.currentType}
	}

	return
}

func decodeBool(r *Reader) (b bool, err error) {
	if err = nextLiteral(r); err != nil {
		return
	}

	if r.currentType != TypeBool {
		err = &TypeError{UserType: reflect.TypeOf(b), YSONType: r.currentType}
		return
	}

	b = r.currentBool
	return
}

func decodeFloat(r *Reader) (f float64, err error) {
	if err = nextLiteral(r); err != nil {
		return
	}

	switch r.currentType {
	case TypeFloat64:
		f = r.currentFloat
	case TypeInt64:
		f = float64(r.currentInt)
	case TypeUint64:
		f = float64(r.currentUint)
	default:
		err = &TypeError{UserType: reflect.TypeOf(f), YSONType: r.currentType}
	}

	return
}

func decodeString(r *Reader) (b []byte, err error) {
	if err = nextLiteral(r); err != nil {
		return
	}

	if r.currentType == TypeEntity {
		return
	}

	if r.currentType != TypeString {
		err = &TypeError{UserType: reflect.TypeOf(b), YSONType: r.currentType}
		return
	}

	b = r.currentString
	return
}

func decodeAny(r *Reader, v any, opts *DecoderOptions) (err error) {
	var i int64
	var u uint64
	var f float64

	if v == nil {
		return &UnsupportedTypeError{}
	}

	switch vv := v.(type) {
	case *int:
		i, err = decodeInt(r, strconv.IntSize)
		*vv = int(i)
	case *int8:
		i, err = decodeInt(r, 8)
		*vv = int8(i)
	case *int16:
		i, err = decodeInt(r, 16)
		*vv = int16(i)
	case *int32:
		i, err = decodeInt(r, 32)
		*vv = int32(i)
	case *int64:
		*vv, err = decodeInt(r, 64)
	case *uint:
		u, err = decodeUint(r, strconv.IntSize)
		*vv = uint(u)
	case *uint8:
		u, err = decodeUint(r, 8)
		*vv = uint8(u)
	case *uint16:
		u, err = decodeUint(r, 16)
		*vv = uint16(u)
	case *uint32:
		u, err = decodeUint(r, 32)
		*vv = uint32(u)
	case *uint64:
		*vv, err = decodeUint(r, 64)
	case *bool:
		*vv, err = decodeBool(r)
	case *float32:
		f, err = decodeFloat(r)
		*vv = float32(f)
	case *float64:
		*vv, err = decodeFloat(r)
	case *string:
		var b []byte
		b, err = decodeString(r)
		*vv = string(b)
	case *[]byte:
		var b []byte
		b, err = decodeString(r)

		if b == nil {
			*vv = nil
		} else {
			*vv = make([]byte, len(b))
			copy(*vv, b)
		}
	case *RawValue:
		var raw []byte
		raw, err = r.NextRawValue()
		if err != nil {
			return
		}

		*vv = make([]byte, len(raw))
		copy(*vv, raw)

	case *Duration:
		i, err = decodeInt(r, 64)
		*vv = Duration(time.Millisecond * time.Duration(i))

	case StreamUnmarshaler:
		err = vv.UnmarshalYSON(r)
	case Unmarshaler:
		var raw []byte
		raw, err = r.NextRawValue()
		if err != nil {
			return
		}

		err = vv.UnmarshalYSON(raw)

	case encoding.TextUnmarshaler:
		var b []byte
		b, err = decodeString(r)
		if err != nil {
			return
		}
		err = vv.UnmarshalText(b)

	case encoding.BinaryUnmarshaler:
		var b []byte
		b, err = decodeString(r)
		if err != nil {
			return
		}
		err = vv.UnmarshalBinary(b)

	case *map[string]any:
		err = decodeMap(r, vv)

	case *any:
		err = decodeGeneric(r, vv)

	default:
		err = decodeReflect(r, reflect.ValueOf(v), opts)
	}

	return
}

// Unmarshal parses YSON-encoded data and stores result in the value pointed by v. If v is nil or not a pointer, Unmarshal
// returns UnsupportedTypeError.
//
// Unmarshal works similar to encoding/json package.
//
// Mapping between YSON types and go objects is the same as in Marshal().
func Unmarshal(data []byte, v any) error {
	d := NewDecoderFromBytes(data)
	if err := d.Decode(v); err != nil {
		return err
	}
	return d.CheckFinish()
}

func UnmarshalOptions(data []byte, v any, opts *DecoderOptions) error {
	d := NewDecoderFromBytes(data)
	d.opts = opts
	if err := d.Decode(v); err != nil {
		return err
	}
	return d.CheckFinish()
}
