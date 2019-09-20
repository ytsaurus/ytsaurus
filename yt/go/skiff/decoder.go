package skiff

import (
	"fmt"
	"io"
	"reflect"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/yson"
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
}

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

	d.keySwitch = d.r.readUint8() != 0

	if d.r.readUint8() == 1 {
		d.rowIndex = d.r.readInt64()
	} else {
		d.rowIndex++
	}

	if d.r.readUint8() == 1 {
		d.rangeIndex = int(d.r.readInt64())
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

func (d *Decoder) decode(ops []fieldOp, value interface{}) error {
	v := reflect.ValueOf(value).Elem()
	v.Set(reflect.New(v.Type()).Elem())

	for _, op := range ops {
		if op.optional {
			if d.r.readUint8() == 0 {
				continue
			}
		}

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
				f.Set(reflect.ValueOf(c))
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
			panic(fmt.Sprintf("unexpected wire type %d", op.wt))
		}
	}

	return d.r.err
}

// Scan unmarshals current record into the value.
func (d *Decoder) Scan(value interface{}) (err error) {
	if !d.hasValue {
		panic("Scan() called out of sequence")
	}

	if err := d.Err(); err != nil {
		return err
	}

	if d.valueRead {
		d.r.backup()
	}

	cache := d.opCaches[d.tableIndex]
	typ := reflect.TypeOf(value)
	if typ.Kind() != reflect.Ptr {
		return xerrors.Errorf("skiff: type %v is not a pointer", typ)
	}
	typ = typ.Elem()

	ops, ok := cache[typ]
	if !ok {
		ops, err = newTranscoder(d.schemas[0], typ)
		if err != nil {
			return
		}
		cache[typ] = ops
	}

	if err := d.decode(ops, value); err != nil {
		d.r.backup()
		return err
	}

	d.valueRead = true
	return nil
}
