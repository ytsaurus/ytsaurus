package skiff

import (
	"io"
	"reflect"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/yson"
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

func (e *Encoder) encodeStruct(ops []fieldOp, value reflect.Value) error {
	for _, op := range ops {
		emitZero := func() {
			if op.optional {
				e.w.writeByte(0x0)
			} else {
				switch op.wt {
				case TypeInt64:
					e.w.writeInt64(0)
				case TypeUint64:
					e.w.writeUint64(0)
				case TypeDouble:
					e.w.writeDouble(0.0)
				case TypeBoolean:
					e.w.writeByte(0)
				case TypeString32:
					e.w.writeUint32(0)
				case TypeYSON32:
					e.w.writeUint32(1)
					e.w.writeByte('#')
				}
			}
		}

		if op.unused {
			emitZero()
		} else {
			f, ok := fieldByIndex(value, op.index, false)
			if ok && !(op.omitempty && isZeroValue(f)) {
				if op.optional {
					e.w.writeByte(1)
				}

				switch op.wt {
				case TypeInt64:
					e.w.writeInt64(f.Int())
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
				emitZero()
			}
		}

		if e.w.err != nil {
			return e.w.err
		}
	}

	return nil
}

func (e *Encoder) Write(value interface{}) error {
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	typ := v.Type()

	if typ.Kind() != reflect.Struct {
		return xerrors.Errorf("skiff: type %v is not supported", typ)
	}

	e.w.writeUint16(0)
	if e.w.err != nil {
		return e.w.err
	}

	ops, ok := e.cache[typ]
	if !ok {
		var err error
		ops, err = newTranscoder(e.schema, typ)
		if err != nil {
			return err
		}
		e.cache[typ] = ops
	}

	return e.encodeStruct(ops, v)
}

func (e *Encoder) Flush() error {
	return e.w.w.Flush()
}
