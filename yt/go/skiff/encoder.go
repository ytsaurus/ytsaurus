package skiff

import (
	"bytes"
	"fmt"
	"io"
	"reflect"

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

func emitZero(w *writer, op fieldOp) {
	if op.optional {
		w.writeByte(0x0)
	} else {
		switch op.schema.Type {
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
}

func (e *Encoder) encodeStruct(ops []fieldOp, value reflect.Value) error {
	for _, op := range ops {
		if op.unused {
			emitZero(e.w, op)
		} else {
			f, ok := fieldByIndex(value, op.index, false)
			if ok && !(op.omitempty && isZeroValue(f)) {
				if op.optional {
					e.w.writeByte(1)
				}

				switch op.schema.Type {
				case TypeInt8:
					e.w.writeInt8(int8(f.Int()))
				case TypeInt16:
					e.w.writeInt16(int16(f.Int()))
				case TypeInt32:
					e.w.writeInt32(int32(f.Int()))
				case TypeInt64:
					e.w.writeInt64(f.Int())
				case TypeUint8:
					e.w.writeUint8(uint8(f.Uint()))
				case TypeUint16:
					e.w.writeUint16(uint16(f.Uint()))
				case TypeUint32:
					e.w.writeUint32(uint32(f.Uint()))
				case TypeUint64:
					e.w.writeUint64(f.Uint())
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
						e.w.writeBytes([]byte(f.String()))
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
				emitZero(e.w, op)
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

func (e *Encoder) writeInt8(v int8, opt bool) error {
	if opt {
		e.w.writeByte(1)
	}
	e.w.writeInt8(v)
	return e.w.err
}

func (e *Encoder) writeInt16(v int16, opt bool) error {
	if opt {
		e.w.writeByte(1)
	}
	e.w.writeInt16(v)
	return e.w.err
}

func (e *Encoder) writeInt32(v int32, opt bool) error {
	if opt {
		e.w.writeByte(1)
	}
	e.w.writeInt32(v)
	return e.w.err
}

func (e *Encoder) writeInt64(v int64, opt bool) error {
	if opt {
		e.w.writeByte(1)
	}
	e.w.writeInt64(v)
	return e.w.err
}

func (e *Encoder) writeUint8(v uint8, opt bool) error {
	if opt {
		e.w.writeByte(1)
	}
	e.w.writeUint8(v)
	return e.w.err
}

func (e *Encoder) writeUint16(v uint16, opt bool) error {
	if opt {
		e.w.writeByte(1)
	}
	e.w.writeUint16(v)
	return e.w.err
}

func (e *Encoder) writeUint32(v uint32, opt bool) error {
	if opt {
		e.w.writeByte(1)
	}
	e.w.writeUint32(v)
	return e.w.err
}

func (e *Encoder) writeUint64(v uint64, opt bool) error {
	if opt {
		e.w.writeByte(1)
	}
	e.w.writeUint64(v)
	return e.w.err
}

func (e *Encoder) writeDouble(v float64, opt bool) error {
	if opt {
		e.w.writeByte(1)
	}
	e.w.writeDouble(v)
	return e.w.err
}

func (e *Encoder) writeBool(v bool, opt bool) error {
	if opt {
		e.w.writeByte(1)
	}
	if v {
		e.w.writeByte(1)
	} else {
		e.w.writeByte(0)
	}
	return e.w.err
}

func (e *Encoder) writeString(v []byte, opt bool) error {
	if opt {
		e.w.writeByte(1)
	}
	e.w.writeBytes(v)
	return e.w.err
}

func (e *Encoder) writeAny(v any, opt bool) error {
	if opt {
		e.w.writeByte(1)
	}
	w := yson.NewWriterFormat(e.w.w, yson.FormatBinary)
	if e.w.err = yson.NewEncoderWriter(w).Encode(v); e.w.err != nil {
		return e.w.err
	}
	return e.w.err
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
		switch wt := schema.Type; wt {
		case TypeInt8:
			switch vv := v.(type) {
			case int8:
				if err := e.writeInt8(int8(vv), isOpt); err != nil {
					return err
				}
			case nil:
				if isOpt {
					e.w.writeByte(0x0)
				} else {
					e.w.writeInt8(0)
				}
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
		case TypeInt16:
			switch vv := v.(type) {
			case int8, int16:
				if err := e.writeInt16(convertIntTo[int16](vv), isOpt); err != nil {
					return err
				}
			case nil:
				if isOpt {
					e.w.writeByte(0x0)
				} else {
					e.w.writeInt16(0)
				}
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
		case TypeInt32:
			switch vv := v.(type) {
			case int8, int16, int32:
				if err := e.writeInt32(convertIntTo[int32](vv), isOpt); err != nil {
					return err
				}
			case nil:
				if isOpt {
					e.w.writeByte(0x0)
				} else {
					e.w.writeInt32(0)
				}
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
		case TypeInt64:
			switch vv := v.(type) {
			case int, int8, int16, int32, int64:
				if err := e.writeInt64(convertIntTo[int64](vv), isOpt); err != nil {
					return err
				}
			case nil:
				if isOpt {
					e.w.writeByte(0x0)
				} else {
					e.w.writeInt64(0)
				}
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
		case TypeUint8:
			switch vv := v.(type) {
			case uint8:
				if err := e.writeUint8(uint8(vv), isOpt); err != nil {
					return err
				}
			case nil:
				if isOpt {
					e.w.writeByte(0x0)
				} else {
					e.w.writeUint8(0)
				}
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
		case TypeUint16:
			switch vv := v.(type) {
			case uint8, uint16:
				if err := e.writeUint16(convertIntTo[uint16](vv), isOpt); err != nil {
					return err
				}
			case nil:
				if isOpt {
					e.w.writeByte(0x0)
				} else {
					e.w.writeUint16(0)
				}
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
		case TypeUint32:
			switch vv := v.(type) {
			case uint8, uint16, uint32:
				if err := e.writeUint32(convertIntTo[uint32](vv), isOpt); err != nil {
					return err
				}
			case nil:
				if isOpt {
					e.w.writeByte(0x0)
				} else {
					e.w.writeUint32(0)
				}
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
		case TypeUint64:
			switch vv := v.(type) {
			case uint, uint8, uint16, uint32, uint64:
				if err := e.writeUint64(convertIntTo[uint64](vv), isOpt); err != nil {
					return err
				}
			case nil:
				if isOpt {
					e.w.writeByte(0x0)
				} else {
					e.w.writeUint64(0)
				}
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
		case TypeDouble:
			switch vv := v.(type) {
			case float64:
				if err := e.writeDouble(vv, isOpt); err != nil {
					return err
				}
			case float32:
				if err := e.writeDouble(float64(vv), isOpt); err != nil {
					return err
				}
			case nil:
				if isOpt {
					e.w.writeByte(0x0)
				} else {
					e.w.writeDouble(0)
				}
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
		case TypeBoolean:
			switch vv := v.(type) {
			case bool:
				if err := e.writeBool(vv, isOpt); err != nil {
					return err
				}
			case nil:
				if isOpt {
					e.w.writeByte(0x0)
				} else {
					e.w.writeByte(0)
				}
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
		case TypeString32:
			switch vv := v.(type) {
			case []byte:
				if err := e.writeString(vv, isOpt); err != nil {
					return err
				}
			case string:
				if err := e.writeString([]byte(vv), isOpt); err != nil {
					return err
				}
			case nil:
				if isOpt {
					e.w.writeByte(0x0)
				} else {
					e.w.writeUint32(0)
				}
			default:
				return &encodeFieldError{fieldName: e.schema.Children[i].Name, fieldType: reflect.TypeOf(v), wireType: wt}
			}
		case TypeYSON32:
			if err := e.writeAny(v, isOpt); err != nil {
				return err
			}
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

		if f.IsValid() {
			if op.optional {
				e.w.writeByte(1)
			}

			if err := checkTypes(f.Type(), op.schema.Type); err != nil {
				return xerrors.Errorf("skiff: can't encode field %q: %w", key, err)
			}

			switch op.schema.Type {
			case TypeInt8:
				e.w.writeInt8(int8(f.Int()))
			case TypeInt16:
				e.w.writeInt16(int16(f.Int()))
			case TypeInt32:
				e.w.writeInt32(int32(f.Int()))
			case TypeInt64:
				e.w.writeInt64(f.Int())
			case TypeUint8:
				e.w.writeUint8(uint8(f.Uint()))
			case TypeUint16:
				e.w.writeUint16(uint16(f.Uint()))
			case TypeUint32:
				e.w.writeUint32(uint32(f.Uint()))
			case TypeUint64:
				e.w.writeUint64(f.Uint())
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
					e.w.writeBytes([]byte(f.String()))
				} else {
					e.w.writeBytes(f.Bytes())
				}
			case TypeYSON32:
				w := yson.NewWriterFormat(e.w.w, yson.FormatBinary)
				if e.w.err = yson.NewEncoderWriter(w).Encode(f.Interface()); e.w.err != nil {
					return e.w.err
				}
			}
		} else {
			emitZero(e.w, op)
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
