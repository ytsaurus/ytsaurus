package skiff

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/yson"
)

type Encoder struct {
	w      *writer
	cache  opCache
	schema *Schema
}

// NewEncoder creates encoder for writing rows into w.
func NewEncoder(w io.Writer, schema Schema) (*Encoder, error) {
	e := &Encoder{
		w:      newWriter(w),
		cache:  make(opCache),
		schema: &schema,
	}

	return e, nil
}

// TODO(prime@): replace with reflect.Value.IsZero() when 1.13 is out.
func isZeroValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

func emitZero(w *writer, isOptional bool, wt WireType) {
	if isOptional {
		w.writeByte(0x0)
		return
	}
	switch wt {
	case TypeInt8:
		w.writeInt8(0)
	case TypeInt16:
		w.writeInt16(0)
	case TypeInt32:
		w.writeInt32(0)
	case TypeInt64:
		w.writeInt64(0)
	case TypeUint8:
		w.writeUint8(0)
	case TypeUint16:
		w.writeUint16(0)
	case TypeUint32:
		w.writeUint32(0)
	case TypeUint64:
		w.writeUint64(0)
	case TypeDouble:
		w.writeDouble(0.0)
	case TypeBoolean:
		w.writeByte(0)
	case TypeString32:
		w.writeUint32(0)
	case TypeYSON32:
		w.writeUint32(1)
		w.writeByte('#')
	}
}

func (e *Encoder) encodeStruct(ops []fieldOp, value reflect.Value) error {
	for _, op := range ops {
		if op.unused {
			emitZero(e.w, op.optional, op.schema.Type)
		} else {
			f, ok := fieldByIndex(value, op.index, false)
			if ok && !(op.omitempty && isZeroValue(f)) {
				if op.optional {
					e.w.writeByte(1)
				}

				switch wt := op.schema.Type; wt {
				case TypeInt8, TypeInt16, TypeInt32, TypeInt64:
					if e.w.err = e.encodeIntValue(f, wt); e.w.err != nil {
						return e.w.err
					}
				case TypeUint8, TypeUint16, TypeUint32, TypeUint64:
					if e.w.err = e.encodeUint(f.Uint(), wt); e.w.err != nil {
						return e.w.err
					}
				case TypeDouble:
					e.w.writeDouble(f.Float())
				case TypeBoolean:
					if f.Bool() {
						e.w.writeByte(1)
					} else {
						e.w.writeByte(0)
					}
				case TypeString32:
					if f.Kind() == reflect.String {
						e.w.writeString(f.String())
					} else {
						e.w.writeBytes(f.Bytes())
					}

				case TypeYSON32:
					var bw bytes.Buffer
					w := yson.NewWriterFormat(&bw, yson.FormatBinary)
					if e.w.err = yson.NewEncoderWriter(w).Encode(f.Interface()); e.w.err != nil {
						return e.w.err
					}
					e.w.writeBytes(bw.Bytes())
				}
			} else {
				emitZero(e.w, op.optional, op.schema.Type)
			}
		}

		if e.w.err != nil {
			return e.w.err
		}
	}

	return nil
}

func (e *Encoder) startRow() error {
	e.w.writeUint16(0) // variant tag
	return e.w.err
}

func (e *Encoder) encodeIntValue(v reflect.Value, wt WireType) error {
	i := v.Int()
	if v.Type() == ysonDurationType {
		i /= int64(time.Millisecond)
	}
	return e.encodeInt(i, wt)
}

func (e *Encoder) encodeInt(i int64, wt WireType) error {
	switch wt {
	case TypeInt8:
		if math.MinInt8 <= i && i <= math.MaxInt8 {
			e.w.writeInt8(int8(i))
			return e.w.err
		}
	case TypeInt16:
		if math.MinInt16 <= i && i <= math.MaxInt16 {
			e.w.writeInt16(int16(i))
			return e.w.err
		}
	case TypeInt32:
		if math.MinInt32 <= i && i <= math.MaxInt32 {
			e.w.writeInt32(int32(i))
			return e.w.err
		}
	case TypeInt64:
		e.w.writeInt64(i)
		return e.w.err
	default:
		panic(fmt.Sprintf("encodeInt: unexpected wire type %q", wt.String()))
	}

	return xerrors.Errorf("value %d overflows type %s", i, wt.String())
}

func (e *Encoder) encodeUint(i uint64, wt WireType) error {
	switch wt {
	case TypeUint8:
		if i <= math.MaxUint8 {
			e.w.writeUint8(uint8(i))
			return e.w.err
		}
	case TypeUint16:
		if i <= math.MaxUint16 {
			e.w.writeUint16(uint16(i))
			return e.w.err
		}
	case TypeUint32:
		if i <= math.MaxUint32 {
			e.w.writeUint32(uint32(i))
			return e.w.err
		}
	case TypeUint64:
		e.w.writeUint64(i)
		return e.w.err
	default:
		panic(fmt.Sprintf("encodeUint: unexpected wire type %q", wt.String()))
	}

	return xerrors.Errorf("value %d overflows type %s", i, wt.String())
}

func (e *Encoder) WriteRow(cols []any) error {
	if e.schema == nil || len(cols) != len(e.schema.Children) {
		return xerrors.Errorf("skiff: can't encode row, col count mismatch, expected %v actual %v", len(e.schema.Children), len(cols))
	}
	if err := e.startRow(); err != nil {
		return err
	}
	for i, v := range cols {
		schema, isOpt := unpackOptional(&e.schema.Children[i])
		if v == nil {
			emitZero(e.w, isOpt, schema.Type)
			continue
		}
		if isOpt {
			e.w.writeByte(1)
		}
		switch wt := schema.Type; wt {
		case TypeInt8, TypeInt16, TypeInt32, TypeInt64:
			var i int64
			switch vv := v.(type) {
			case int, int8, int16, int32, int64:
				i = convertIntTo[int64](vv)
			case yson.Duration:
				i = int64(vv) / int64(time.Millisecond)
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
			if err := e.encodeInt(i, wt); err != nil {
				return err
			}
		case TypeUint8, TypeUint16, TypeUint32, TypeUint64:
			switch vv := v.(type) {
			case uint, uint8, uint16, uint32, uint64:
				if err := e.encodeUint(convertIntTo[uint64](vv), wt); err != nil {
					return err
				}
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
		case TypeDouble:
			switch vv := v.(type) {
			case float64:
				e.w.writeDouble(vv)
			case float32:
				e.w.writeDouble(float64(vv))
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
		case TypeBoolean:
			switch vv := v.(type) {
			case bool:
				if vv {
					e.w.writeByte(1)
				} else {
					e.w.writeByte(0)
				}
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
		case TypeString32:
			switch vv := v.(type) {
			case []byte:
				e.w.writeBytes(vv)
			case string:
				e.w.writeString(vv)
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
		case TypeYSON32:
			var bw bytes.Buffer
			w := yson.NewWriterFormat(&bw, yson.FormatBinary)
			if e.w.err = yson.NewEncoderWriter(w).Encode(v); e.w.err != nil {
				return e.w.err
			}
			e.w.writeBytes(bw.Bytes())
		}

		if e.w.err != nil {
			return e.w.err
		}
	}
	return nil
}

func (e *Encoder) encodeMap(ops []fieldOp, value reflect.Value) error {
	for _, op := range ops {
		key := reflect.ValueOf(op.schemaName)
		f := value.MapIndex(key)

		if f.Kind() == reflect.Interface {
			f = reflect.ValueOf(f.Interface())
		}

		if f.Kind() == reflect.Ptr {
			f = f.Elem()
		}

		if f.IsValid() {
			if op.optional {
				e.w.writeByte(1)
			}

			if err := checkTypes(f.Type(), op.schema.Type); err != nil {
				return xerrors.Errorf("skiff: can't encode field %q: %w", key, err)
			}

			switch wt := op.schema.Type; wt {
			case TypeInt8, TypeInt16, TypeInt32, TypeInt64:
				if e.w.err = e.encodeIntValue(f, wt); e.w.err != nil {
					return e.w.err
				}
			case TypeUint8, TypeUint16, TypeUint32, TypeUint64:
				if e.w.err = e.encodeUint(f.Uint(), wt); e.w.err != nil {
					return e.w.err
				}
			case TypeDouble:
				e.w.writeDouble(f.Float())
			case TypeBoolean:
				if f.Bool() {
					e.w.writeByte(1)
				} else {
					e.w.writeByte(0)
				}
			case TypeString32:
				if f.Kind() == reflect.String {
					e.w.writeString(f.String())
				} else {
					e.w.writeBytes(f.Bytes())
				}
			case TypeYSON32:
				var bw bytes.Buffer
				w := yson.NewWriterFormat(&bw, yson.FormatBinary)
				if e.w.err = yson.NewEncoderWriter(w).Encode(f.Interface()); e.w.err != nil {
					return e.w.err
				}
				e.w.writeBytes(bw.Bytes())
			}
		} else {
			emitZero(e.w, op.optional, op.schema.Type)
		}

		if e.w.err != nil {
			return e.w.err
		}
	}

	return nil
}

func (e *Encoder) getTranscoder(typ reflect.Type) (ops []fieldOp, err error) {
	ops, ok := e.cache[typ]
	if ok {
		return
	}

	ops, err = newTranscoder(e.schema, nil, typ)
	if err != nil {
		return
	}

	e.cache[typ] = ops
	return
}

func (e *Encoder) Write(value any) error {
	e.w.writeUint16(0) // variant tag
	if e.w.err != nil {
		return e.w.err
	}

	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	typ := v.Type()
	switch typ.Kind() {
	case reflect.Struct:
		ops, err := e.getTranscoder(typ)
		if err != nil {
			return err
		}
		return e.encodeStruct(ops, v)

	case reflect.Map:
		if typ.Key().Kind() != reflect.String {
			return xerrors.Errorf("skiff: type %v is not supported", typ)
		}

		ops, err := e.getTranscoder(emptyStructType)
		if err != nil {
			return err
		}
		return e.encodeMap(ops, v)

	default:
		return xerrors.Errorf("skiff: type %v is not supported", typ)
	}
}

func (e *Encoder) Flush() error {
	return e.w.w.Flush()
}

type encodeFieldError struct {
	fieldName string
	fieldType reflect.Type
	wireType  WireType
}

func (e *encodeFieldError) Error() string {
	return fmt.Sprintf("skiff: field %s of type %q cannot be encoded as wireType %q", e.fieldName, e.fieldType, e.wireType)
}

type signed interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

type unsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

func convertIntTo[T signed | unsigned](v any) T {
	switch vv := v.(type) {
	case int:
		return T(vv)
	case int8:
		return T(vv)
	case int16:
		return T(vv)
	case int32:
		return T(vv)
	case int64:
		return T(vv)
	case uint:
		return T(vv)
	case uint8:
		return T(vv)
	case uint16:
		return T(vv)
	case uint32:
		return T(vv)
	case uint64:
		return T(vv)
	default:
		panic(fmt.Sprintf("unsupported type: %T", v))
	}
}
