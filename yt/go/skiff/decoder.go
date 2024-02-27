// Package skiff implements YT skiff format.
//
// Skiff provides very efficient encoding and decoding, but requires schema negotiation
// and does not support schema evolution.
package skiff

import (
	"fmt"
	"io"
	"reflect"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/yson"
)

type opCache map[reflect.Type][]fieldOp

// Decoder reads stream of skiff tuples.
//
// Streaming format is specific for YT job input.
type Decoder struct {
	opCaches []opCache
	schemas  []*Schema
	r        *reader

	hasValue, valueRead    bool
	tableIndex, rangeIndex int
	rowIndex               int64
	keySwitch              bool
	keySwitchCol           bool
}

// NewDecoder creates decoder for reading rows from input stream formatted by format.
//
// Each table schema in format must start with three system columns.
func NewDecoder(r io.Reader, format Format) (*Decoder, error) {
	d := &Decoder{
		opCaches: make([]opCache, len(format.TableSchemas)),
		schemas:  make([]*Schema, len(format.TableSchemas)),
		r:        newReader(r),
	}

	schemaCaches := map[string]map[reflect.Type][]fieldOp{}

	for i, s := range format.TableSchemas {
		switch v := s.(type) {
		case string:
			if len(v) == 0 || v[0] != '$' {
				return nil, xerrors.Errorf("skiff: invalid schema key %q", v)
			}

			d.schemas[i] = format.SchemaRegistry[v[1:]]
			if schemaCaches[v] == nil {
				schemaCaches[v] = make(opCache)
			}
			d.opCaches[i] = schemaCaches[v]

		case *Schema:
			d.schemas[i] = v
			d.opCaches[i] = make(opCache)
		}

		// System columns are decoded by hand.
		// TODO(prime@): validate schema is starting with system columns.
		s := *d.schemas[i]
		sysCols := 0
		for _, col := range s.Children {
			if col.IsSystem() {
				sysCols++
			}
		}
		d.keySwitchCol = sysCols > 0
		s.Children = s.Children[sysCols:]
		d.schemas[i] = &s
	}

	return d, nil
}

func (d *Decoder) Next() bool {
	if d.hasValue && !d.valueRead {
		_ = d.Scan(&struct{}{})
	}

	if d.Err() != nil {
		return false
	}

	d.tableIndex = int(d.r.readUint16())
	if d.r.err != nil {
		d.r.cleanEOF()
		return false
	}

	if d.tableIndex >= len(d.schemas) {
		d.r.err = xerrors.Errorf("skiff: table index %d >= %d", d.tableIndex, len(d.schemas))
		return false
	}

	if d.keySwitchCol {
		d.keySwitch = d.r.readUint8() != 0

		if d.r.readUint8() == 1 {
			d.rowIndex = d.r.readInt64()
		} else {
			d.rowIndex++
		}

		if d.r.readUint8() == 1 {
			d.rangeIndex = int(d.r.readInt64())
		}
	} else {
		d.rowIndex++
	}

	if d.Err() != nil {
		return false
	}

	d.r.checkpoint()
	d.hasValue = true
	d.valueRead = false
	return true
}

func (d *Decoder) Err() error {
	return d.r.err
}

func (d *Decoder) KeySwitch() bool {
	if !d.hasValue {
		panic("KeySwitch() called out of sequence")
	}

	return d.keySwitch
}

func (d *Decoder) RowIndex() int64 {
	if !d.hasValue {
		panic("RowIndex() called out of sequence")
	}

	return d.rowIndex
}

func (d *Decoder) RangeIndex() int {
	if !d.hasValue {
		panic("RangeIndex() called out of sequence")
	}

	return d.rangeIndex
}

func (d *Decoder) TableIndex() int {
	if !d.hasValue {
		panic("TableIndex() called out of sequence")
	}

	return d.tableIndex
}

func fieldByIndex(v reflect.Value, index []int, initPtr bool) (reflect.Value, bool) {
	for _, fieldIndex := range index {
		v = v.Field(fieldIndex)

		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				if initPtr {
					v.Set(reflect.New(v.Type().Elem()))
				} else {
					return reflect.Value{}, false
				}
			}

			v = v.Elem()
		}
	}

	return v, true
}

func (d *Decoder) decodeStruct(ops []fieldOp, value any) error {
	v := reflect.ValueOf(value).Elem()
	v.Set(reflect.New(v.Type()).Elem())

	for _, op := range ops {
		if op.optional {
			if d.r.readUint8() == 0 {
				continue
			}
		}

		if !op.unused {
			f, _ := fieldByIndex(v, op.index, true)
			switch op.wt {
			case TypeBoolean:
				f.SetBool(d.r.readUint8() != 0)
			case TypeInt64:
				f.SetInt(d.r.readInt64())
			case TypeUint64:
				f.SetUint(d.r.readUint64())
			case TypeDouble:
				f.SetFloat(d.r.readDouble())
			case TypeString32:
				b := d.r.readBytes()
				if d.r.err != nil {
					return d.r.err
				}

				if f.Kind() == reflect.String {
					f.SetString(string(b))
				} else {
					c := make([]byte, len(b))
					copy(c, b)
					f.SetBytes(c)
				}
			case TypeYSON32:
				b := d.r.readBytes()
				if d.r.err != nil {
					return d.r.err
				}

				if err := yson.Unmarshal(b, f.Addr().Interface()); err != nil {
					return err
				}
			default:
				panic(fmt.Sprintf("unexpected wire type %s", op.wt))
			}
		} else {
			switch op.wt {
			case TypeBoolean:
				d.r.readUint8()
			case TypeInt64:
				d.r.readInt64()
			case TypeUint64:
				d.r.readUint64()
			case TypeDouble:
				d.r.readDouble()
			case TypeString32, TypeYSON32:
				d.r.readBytes()
			default:
				panic(fmt.Sprintf("unexpected wire type %s", op.wt))
			}
		}
	}

	return d.r.err
}

func (d *Decoder) decodeMap(ops []fieldOp, value any) error {
	v := reflect.ValueOf(value).Elem()

	if v.IsNil() || v.Len() != 0 {
		v.Set(reflect.MakeMap(v.Type()))
	}

	mapValueType := v.Type().Elem()
	generic := mapValueType.Kind() == reflect.Interface

	for _, op := range ops {
		if op.optional {
			if d.r.readUint8() == 0 {
				continue
			}
		}

		if generic {
			var field any
			switch op.wt {
			case TypeBoolean:
				field = d.r.readUint8() != 0
			case TypeInt64:
				field = d.r.readInt64()
			case TypeUint64:
				field = d.r.readUint64()
			case TypeDouble:
				field = d.r.readDouble()
			case TypeString32:
				b := d.r.readBytes()

				c := make([]byte, len(b))
				copy(c, b)
				field = c

			case TypeYSON32:
				b := d.r.readBytes()
				if d.r.err != nil {
					return d.r.err
				}

				if err := yson.Unmarshal(b, &field); err != nil {
					return err
				}

			default:
				panic(fmt.Sprintf("unexpected wire type %s", op.wt))
			}

			v.SetMapIndex(reflect.ValueOf(op.schemaName), reflect.ValueOf(field))
		} else {
			field := reflect.New(mapValueType).Elem()
			if err := checkTypes(mapValueType, op.wt); err != nil {
				return xerrors.Errorf("skiff: can't decode field %q: %w", op.schemaName, err)
			}

			switch op.wt {
			case TypeBoolean:
				field.SetBool(d.r.readUint8() != 0)
			case TypeInt64:
				field.SetInt(d.r.readInt64())
			case TypeUint64:
				field.SetUint(d.r.readUint64())
			case TypeDouble:
				field.SetFloat(d.r.readDouble())
			case TypeString32:
				b := d.r.readBytes()
				if field.Kind() == reflect.String {
					field.SetString(string(b))
				} else {
					c := make([]byte, len(b))
					copy(c, b)
					field.SetBytes(c)
				}

			case TypeYSON32:
				b := d.r.readBytes()
				if d.r.err != nil {
					return d.r.err
				}

				if err := yson.Unmarshal(b, field.Addr().Interface()); err != nil {
					return err
				}

			default:
				panic(fmt.Sprintf("unexpected wire type %s", op.wt))
			}

			v.SetMapIndex(reflect.ValueOf(op.schemaName), field)
		}

		if d.r.err != nil {
			return d.r.err
		}
	}

	return nil
}

func (d *Decoder) getTranscoder(typ reflect.Type) (ops []fieldOp, err error) {
	cache := d.opCaches[d.tableIndex]
	ops, ok := cache[typ]
	if !ok {
		ops, err = newTranscoder(d.schemas[d.tableIndex], typ)
		if err != nil {
			return
		}
		cache[typ] = ops
	}

	return
}

// Scan unmarshals current record into the value.
func (d *Decoder) Scan(value any) (err error) {
	if !d.hasValue {
		panic("Scan() called out of sequence")
	}

	if err := d.Err(); err != nil {
		return err
	}

	if d.valueRead {
		d.r.backup()
		d.valueRead = false
	}

	typ := reflect.TypeOf(value)
	if typ.Kind() != reflect.Ptr {
		return xerrors.Errorf("skiff: type %v is not a pointer", typ)
	}
	typ = typ.Elem()

	if out, ok := value.(*any); ok {
		typ = genericMapType

		genericMap := map[string]any{}
		*out = genericMap
		value = &genericMap
	}

	switch typ.Kind() {
	case reflect.Struct:
		ops, err := d.getTranscoder(typ)
		if err != nil {
			return err
		}

		if err := d.decodeStruct(ops, value); err != nil {
			d.r.backup()
			return err
		}

	case reflect.Map:
		ops, err := d.getTranscoder(emptyStructType)
		if err != nil {
			return err
		}

		if err := d.decodeMap(ops, value); err != nil {
			d.r.backup()
			return err
		}

	default:
		return xerrors.Errorf("skiff: type %v is not a struct", typ)
	}

	d.valueRead = true
	return nil
}
