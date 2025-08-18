// Package skiff implements YT skiff format.
//
// Skiff provides very efficient encoding and decoding, but requires schema negotiation
// and does not support schema evolution.
package skiff

import (
	"fmt"
	"io"
	"reflect"
	"time"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

type opCache map[reflect.Type][]fieldOp

type decoderOption func(s *decoderOptions)

type decoderOptions struct {
	tableSchemas []*schema.Schema
}

// WithDecoderTableSchemas sets the table schemas for the Decoder.
func WithDecoderTableSchemas(schemas ...*schema.Schema) decoderOption {
	return func(s *decoderOptions) {
		s.tableSchemas = schemas
	}
}

// Decoder reads stream of skiff tuples.
//
// Streaming format is specific for YT job input.
type Decoder struct {
	tables []tableInfo
	r      *reader

	hasValue, valueRead bool
	tableIndex          int
}

type tableInfo struct {
	opCache       opCache
	schema        *Schema
	systemColumns []Schema
	tableSchema   *schema.Schema

	rangeIndex int
	rowIndex   int64
	keySwitch  bool
}

// NewDecoder creates decoder for reading rows from input stream formatted by format.
//
// Each table schema in format must start with three system columns.
func NewDecoder(r io.Reader, format Format, opts ...decoderOption) (*Decoder, error) {
	d := &Decoder{
		tables: make([]tableInfo, len(format.TableSchemas)),
		r:      newReader(r),
	}
	var decoderOpts decoderOptions
	for _, opt := range opts {
		opt(&decoderOpts)
	}

	if len(decoderOpts.tableSchemas) > 0 && len(decoderOpts.tableSchemas) != len(d.tables) {
		return nil, xerrors.Errorf(
			"skiff: number of table schemas (%d) does not match number of tables (%d)", len(decoderOpts.tableSchemas), len(d.tables))
	}

	schemaCaches := map[string]map[reflect.Type][]fieldOp{}

	for i, s := range format.TableSchemas {
		switch v := s.(type) {
		case string:
			if len(v) == 0 || v[0] != '$' {
				return nil, xerrors.Errorf("skiff: invalid schema key %q", v)
			}

			d.tables[i].schema = format.SchemaRegistry[v[1:]]
			if schemaCaches[v] == nil {
				schemaCaches[v] = make(opCache)
			}
			d.tables[i].opCache = schemaCaches[v]

		case *Schema:
			d.tables[i].schema = v
			d.tables[i].opCache = make(opCache)
		}

		// System columns are decoded by hand.
		s := *d.tables[i].schema
		var systemColumns []Schema
		for i, col := range s.Children {
			if col.IsSystem() {
				if len(systemColumns) != i {
					return nil, xerrors.Errorf(
						"skiff: system column %q goes after nonsystem (index: %d)", col.Name, i,
					)
				}
				systemColumns = append(systemColumns, col)
			}
		}
		s.Children = s.Children[len(systemColumns):]
		d.tables[i].schema = &s
		d.tables[i].systemColumns = systemColumns
		if len(decoderOpts.tableSchemas) > 0 {
			d.tables[i].tableSchema = decoderOpts.tableSchemas[i]
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

	if d.tableIndex >= len(d.tables) {
		d.r.err = xerrors.Errorf("skiff: table index %d >= %d", d.tableIndex, len(d.tables))
		return false
	}

	systemColumns := d.tables[d.tableIndex].systemColumns
	for _, col := range systemColumns {
		switch col.Name {
		case "$key_switch":
			d.tables[d.tableIndex].keySwitch = d.r.readUint8() != 0
		case "$row_index":
			if d.r.readUint8() == 1 {
				d.tables[d.tableIndex].rowIndex = d.r.readInt64()
			} else {
				d.tables[d.tableIndex].rowIndex++
			}
		case "$range_index":
			if d.r.readUint8() == 1 {
				d.tables[d.tableIndex].rangeIndex = int(d.r.readInt64())
			}
		}
	}
	if len(systemColumns) == 0 {
		d.tables[d.tableIndex].rowIndex++
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

	return d.tables[d.tableIndex].keySwitch
}

func (d *Decoder) RowIndex() int64 {
	if !d.hasValue {
		panic("RowIndex() called out of sequence")
	}

	return d.tables[d.tableIndex].rowIndex
}

func (d *Decoder) RangeIndex() int {
	if !d.hasValue {
		panic("RangeIndex() called out of sequence")
	}

	return d.tables[d.tableIndex].rangeIndex
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

func (d *Decoder) decodeInt(v reflect.Value, wt WireType) (int64, error) {
	var i int64

	switch wt {
	case TypeInt8:
		i = int64(d.r.readInt8())
	case TypeInt16:
		i = int64(d.r.readInt16())
	case TypeInt32:
		i = int64(d.r.readInt32())
	case TypeInt64:
		i = d.r.readInt64()
		if v.Type() == ysonDurationType {
			i = int64(time.Millisecond * time.Duration(i))
		}
	}

	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if v.OverflowInt(i) {
			return 0, xerrors.Errorf("value %d overflows type %s", i, v.Kind().String())
		}
	}
	return i, nil
}

func (d *Decoder) decodeUint(v reflect.Value, wt WireType) (uint64, error) {
	var i uint64

	switch wt {
	case TypeUint8:
		i = uint64(d.r.readUint8())
	case TypeUint16:
		i = uint64(d.r.readUint16())
	case TypeUint32:
		i = uint64(d.r.readUint32())
	case TypeUint64:
		i = d.r.readUint64()
	}

	switch v.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if v.OverflowUint(i) {
			return 0, xerrors.Errorf("value %d overflows type %s", i, v.Kind().String())
		}
	}
	return i, nil
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
			switch op.schema.Type {
			case TypeBoolean:
				f.SetBool(d.r.readUint8() != 0)
			case TypeInt8, TypeInt16, TypeInt32, TypeInt64:
				i, err := d.decodeInt(f, op.schema.Type)
				if err != nil {
					return xerrors.Errorf("skiff: failed to decode field %q: %w", op.schemaName, err)
				}
				f.SetInt(i)
			case TypeUint8, TypeUint16, TypeUint32, TypeUint64:
				i, err := d.decodeUint(f, op.schema.Type)
				if err != nil {
					return xerrors.Errorf("skiff: failed to decode field %q: %w", op.schemaName, err)
				}
				f.SetUint(i)
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
					return xerrors.Errorf("skiff: failed to unmarshal yson (field %q): %w", op.schemaName, err)
				}
			default:
				return xerrors.Errorf("unexpected wire type %s", op.schema.Type)
			}
		} else {
			switch op.schema.Type {
			case TypeBoolean:
				d.r.readUint8()
			case TypeInt8:
				d.r.readInt8()
			case TypeInt16:
				d.r.readInt16()
			case TypeInt32:
				d.r.readInt32()
			case TypeInt64:
				d.r.readInt64()
			case TypeUint8:
				d.r.readUint8()
			case TypeUint16:
				d.r.readUint16()
			case TypeUint32:
				d.r.readUint32()
			case TypeUint64:
				d.r.readUint64()
			case TypeDouble:
				d.r.readDouble()
			case TypeString32, TypeYSON32:
				d.r.readBytes()
			default:
				return xerrors.Errorf("unexpected wire type %s", op.schema.Type)
			}
		}
	}

	return d.r.err
}

func (d *Decoder) decodeSimpleTypeGeneric(skiffSchema *Schema) (any, error) {
	switch skiffSchema.Type {
	case TypeNothing:
		return nil, nil
	case TypeBoolean:
		return d.r.readUint8() != 0, nil
	case TypeInt8:
		return int64(d.r.readInt8()), nil
	case TypeInt16:
		return int64(d.r.readInt16()), nil
	case TypeInt32:
		return int64(d.r.readInt32()), nil
	case TypeInt64:
		return d.r.readInt64(), nil
	case TypeUint8:
		return uint64(d.r.readUint8()), nil
	case TypeUint16:
		return uint64(d.r.readUint16()), nil
	case TypeUint32:
		return uint64(d.r.readUint32()), nil
	case TypeUint64:
		return d.r.readUint64(), nil
	case TypeDouble:
		return d.r.readDouble(), nil
	case TypeString32:
		return string(d.r.readBytes()), nil
	case TypeYSON32:
		b := d.r.readBytes()
		if d.r.err != nil {
			return nil, d.r.err
		}
		var field any
		if err := yson.Unmarshal(b, &field); err != nil {
			return nil, err
		}
		return field, nil
	default:
		return nil, xerrors.Errorf(
			"unexpected simple wire type %q: complex wire types are only supported for tables with strong schema", skiffSchema.Type,
		)
	}
}

func (d *Decoder) decodeGenericValue(skiffSchema *Schema, tableSchemaType schema.ComplexType) (any, error) {
	if tableSchemaType == nil { // Weak schema mode.
		return d.decodeSimpleTypeGeneric(skiffSchema)
	}

	switch t := tableSchemaType.(type) {
	case schema.Type:
		return d.decodeSimpleTypeGeneric(skiffSchema)
	case schema.Decimal:
		decimal, err := d.decodeGenericDecimal(t)
		if err != nil {
			return nil, err
		}
		// Conversion to string is required for compatibility with YSON.
		return string(decimal), nil
	case schema.Optional:
		return d.decodeGenericValue(&skiffSchema.Children[d.r.readUint8()], t.Item)
	case schema.List:
		return d.decodeGenericList(t, skiffSchema)
	case schema.Struct:
		return d.decodeGenericStruct(t, skiffSchema)
	case schema.Tuple:
		return d.decodeGenericTuple(t, skiffSchema)
	case schema.Variant:
		return d.decodeGenericVariant(t, skiffSchema)
	case schema.Dict:
		return d.decodeGenericDict(t, skiffSchema)
	case schema.Tagged:
		return d.decodeGenericValue(skiffSchema, t.Item)
	default:
		return nil, fmt.Errorf("unsupported YT complex type in table schema: %T", t)
	}
}

func (d *Decoder) decodeGenericDecimal(t schema.Decimal) ([]byte, error) {
	var binaryDecimalLength int
	if t.Precision <= 9 {
		binaryDecimalLength = 4
	} else if t.Precision <= 18 {
		binaryDecimalLength = 8
	} else if t.Precision <= 38 {
		binaryDecimalLength = 16
	} else if t.Precision <= 76 {
		binaryDecimalLength = 32
	} else {
		return nil, xerrors.Errorf("skiff: decimal precision %d exceeds maximum supported value 76", t.Precision)
	}
	b := d.r.pull(binaryDecimalLength)
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	b[0] ^= 0x80
	return b, nil
}

func (d *Decoder) decodeGenericList(t schema.List, skiffSchema *Schema) ([]any, error) {
	elems := []any{}
	for {
		if tag := d.r.readUint8(); tag == repeatedVariant8End {
			break
		}
		e, err := d.decodeGenericValue(&skiffSchema.Children[0], t.Item)
		if err != nil {
			return nil, err
		}
		elems = append(elems, e)
	}
	return elems, nil
}

func (d *Decoder) decodeGenericStruct(t schema.Struct, skiffSchema *Schema) (map[string]any, error) {
	result := make(map[string]any, len(t.Members))
	for i, m := range t.Members {
		v, err := d.decodeGenericValue(&skiffSchema.Children[i], m.Type)
		if err != nil {
			return nil, err
		}
		result[m.Name] = v
	}
	return result, nil
}

func (d *Decoder) decodeGenericTuple(t schema.Tuple, skiffSchema *Schema) ([]any, error) {
	vals := make([]any, len(t.Elements))
	for i, elem := range t.Elements {
		v, err := d.decodeGenericValue(&skiffSchema.Children[i], elem.Type)
		if err != nil {
			return nil, err
		}
		vals[i] = v
	}
	return vals, nil
}

func (d *Decoder) decodeGenericVariant(t schema.Variant, skiffSchema *Schema) ([]any, error) {
	var tag int64
	if skiffSchema.Type == TypeVariant8 {
		tag = int64(d.r.readUint8())
	} else {
		tag = int64(d.r.readUint16())
	}
	if tag < 0 || int(tag) >= len(skiffSchema.Children) {
		return nil, xerrors.Errorf("skiff: tag value %d out of bounds for variant children", tag)
	}
	elemSchema := &skiffSchema.Children[tag]
	var itemType schema.ComplexType
	var name any
	if t.Elements != nil {
		if int(tag) >= len(t.Elements) {
			return nil, xerrors.Errorf("skiff: variant index %d out of bounds for elements", tag)
		}
		itemType = t.Elements[tag].Type
		name = tag
	} else {
		if int(tag) >= len(t.Members) {
			return nil, xerrors.Errorf("skiff: variant index %d out of bounds for members", tag)
		}
		itemType = t.Members[tag].Type
		name = t.Members[tag].Name
	}
	v, err := d.decodeGenericValue(elemSchema, itemType)
	if err != nil {
		return nil, err
	}
	return []any{name, v}, nil
}

func (d *Decoder) decodeGenericDict(t schema.Dict, skiffSchema *Schema) ([]any, error) {
	itemSchema := &skiffSchema.Children[0]
	entries := []any{}
	for {
		if tag := d.r.readUint8(); tag == repeatedVariant8End {
			break
		}
		key, err := d.decodeGenericValue(&itemSchema.Children[0], t.Key)
		if err != nil {
			return nil, err
		}
		val, err := d.decodeGenericValue(&itemSchema.Children[1], t.Value)
		if err != nil {
			return nil, err
		}
		entries = append(entries, []any{key, val})
	}
	return entries, nil
}

func (d *Decoder) decodeMap(ops []fieldOp, value any) error {
	v := reflect.ValueOf(value).Elem()

	if v.IsNil() {
		v.Set(reflect.MakeMap(v.Type()))
	} else if v.Len() != 0 {
		for _, k := range v.MapKeys() {
			v.SetMapIndex(k, reflect.Value{})
		}
	}

	mapValueType := v.Type().Elem()
	generic := mapValueType.Kind() == reflect.Interface

	for _, op := range ops {
		if op.optional {
			if d.r.readUint8() == 0 {
				v.SetMapIndex(reflect.ValueOf(op.schemaName), reflect.New(mapValueType).Elem())
				continue
			}
		}

		if generic {
			rawVal, err := d.decodeGenericValue(op.schema, op.tableSchemaType)
			if err != nil {
				return err
			}
			v.SetMapIndex(reflect.ValueOf(op.schemaName), reflect.ValueOf(rawVal))
		} else {
			field := reflect.New(mapValueType).Elem()
			if err := checkTypes(mapValueType, op.schema.Type); err != nil {
				return xerrors.Errorf("skiff: can't decode field %q: %w", op.schemaName, err)
			}

			switch op.schema.Type {
			case TypeBoolean:
				field.SetBool(d.r.readUint8() != 0)
			case TypeInt8:
				field.SetInt(int64(d.r.readInt8()))
			case TypeInt16:
				field.SetInt(int64(d.r.readInt16()))
			case TypeInt32:
				field.SetInt(int64(d.r.readInt32()))
			case TypeInt64:
				field.SetInt(d.r.readInt64())
			case TypeUint8:
				field.SetUint(uint64(d.r.readUint8()))
			case TypeUint16:
				field.SetUint(uint64(d.r.readUint16()))
			case TypeUint32:
				field.SetUint(uint64(d.r.readUint32()))
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
				return xerrors.Errorf("unexpected wire type %s", op.schema.Type)
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
	cache := d.tables[d.tableIndex].opCache
	ops, ok := cache[typ]
	if !ok {
		ops, err = newTranscoder(d.tables[d.tableIndex].schema, d.tables[d.tableIndex].tableSchema, typ)
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
		return xerrors.New("Scan() called out of sequence")
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
