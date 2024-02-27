package skiff

import (
	"bytes"
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
		switch op.wt {
		case TypeInt64:
			w.writeInt64(0)
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

func (e *Encoder) writeInt64(v int64, opt bool) error {
	if opt {
		e.w.writeByte(1)
	}
	e.w.writeInt64(v)
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
		wt, isOpt, err := unpackSimpleVariant(&e.schema.Children[i])
		if err != nil {
			return err
		}
		switch wt {
		case TypeInt64:
			switch vv := v.(type) {
			case int:
				if err := e.writeInt64(int64(vv), isOpt); err != nil {
					return err
				}
			case uint64:
				if err := e.writeInt64(int64(vv), isOpt); err != nil {
					return err
				}
			case uint32:
				if err := e.writeInt64(int64(vv), isOpt); err != nil {
					return err
				}
			case uint16:
				if err := e.writeInt64(int64(vv), isOpt); err != nil {
					return err
				}
			case int64:
				if err := e.writeInt64(int64(vv), isOpt); err != nil {
					return err
				}
			case int32:
				if err := e.writeInt64(int64(vv), isOpt); err != nil {
					return err
				}
			case int16:
				if err := e.writeInt64(int64(vv), isOpt); err != nil {
					return err
				}
			case nil:
				if isOpt {
					e.w.writeByte(0x0)
				} else {
					e.w.writeInt64(0)
				}
			default:
				return xerrors.Errorf("skiff: can't encode field %v: type mismatch %v", e.schema.Children[i].Name, wt)
			}
		case TypeUint64:
			switch vv := v.(type) {
			case int:
				if err := e.writeUint64(uint64(vv), isOpt); err != nil {
					return err
				}
			case uint64:
				if err := e.writeUint64(uint64(vv), isOpt); err != nil {
					return err
				}
			case uint32:
				if err := e.writeUint64(uint64(vv), isOpt); err != nil {
					return err
				}
			case uint16:
				if err := e.writeUint64(uint64(vv), isOpt); err != nil {
					return err
				}
			case int64:
				if err := e.writeUint64(uint64(vv), isOpt); err != nil {
					return err
				}
			case int32:
				if err := e.writeUint64(uint64(vv), isOpt); err != nil {
					return err
				}
			case int16:
				if err := e.writeUint64(uint64(vv), isOpt); err != nil {
					return err
				}
			case nil:
				if isOpt {
					e.w.writeByte(0x0)
				} else {
					e.w.writeUint64(0)
				}
			default:
				return xerrors.Errorf("skiff: can't encode field %v: type mismatch %v", e.schema.Children[i].Name, wt)
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
				return xerrors.Errorf("skiff: can't encode field %v: type mismatch %v", e.schema.Children[i].Name, wt)
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
				return xerrors.Errorf("skiff: can't encode field %v: type mismatch %v", e.schema.Children[i].Name, wt)
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
				return xerrors.Errorf("skiff: can't encode field %v: type mismatch %v", e.schema.Children[i].Name, wt)
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

			if err := checkTypes(f.Type(), op.wt); err != nil {
				return xerrors.Errorf("skiff: can't encode field %q: %w", key, err)
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

	ops, err = newTranscoder(e.schema, typ)
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
