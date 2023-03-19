package wire

import (
	"encoding"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

type field struct {
	name  string
	index []int

	omitempty bool
	attribute bool
	value     bool
	attrs     bool

	required bool // todo needed?
}

type structType struct {
	fields       []*field
	fieldsByName map[string]*field

	value *field // field decoded directly from the whole value
}

func newStructType(t reflect.Type) *structType {
	structType := &structType{
		fieldsByName: make(map[string]*field),
	}

	var nameConflict field

	var fieldOrder []string

	var visitFields func(fieldStack []int, t reflect.Type)
	visitFields = func(fieldStack []int, t reflect.Type) {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)

			tag, skip := yson.ParseTag(f.Name, f.Tag)
			if skip {
				continue
			}

			var index []int
			index = append(index, fieldStack...)
			index = append(index, i)

			isUnexported := f.PkgPath != ""
			if f.Anonymous {
				ft := f.Type
				if ft.Kind() == reflect.Ptr {
					ft = ft.Elem()
				}

				if isUnexported && ft.Kind() != reflect.Struct {
					continue
				}

				_, tagged := f.Tag.Lookup("yson")
				if !tagged {
					if ft.Kind() == reflect.Struct {
						visitFields(index, ft)
						continue
					}
				}
			} else if isUnexported {
				continue
			}

			structField := field{
				name:      tag.Name,
				index:     index,
				attribute: tag.Attr,
				omitempty: tag.Omitempty,
				value:     tag.Value,
				attrs:     tag.Attrs,
			}

			if structField.value {
				if structType.value == nil {
					structType.value = &structField
				} else {
					structType.value = &nameConflict
				}
			} else {
				addField(&fieldOrder, structType.fieldsByName, &structField)
			}
		}
	}

	visitFields(nil, t)

	if structType.value == &nameConflict {
		structType.value = nil
	}

	structType.fields = filterConflicts(fieldOrder, structType.fieldsByName)

	return structType
}

// addField adds field to field map and order slice, resolving name conflict according to go embedding rules.
func addField(order *[]string, fieldMap map[string]*field, f *field) {
	*order = append(*order, f.name)

	otherField := fieldMap[f.name]
	if otherField == nil {
		fieldMap[f.name] = f
	} else if len(otherField.index) > len(f.index) {
		fieldMap[f.name] = f
	} else if len(otherField.index) == len(f.index) {
		otherField.name = ""
	}
}

// filterConflicts extracts fields from field map in specified order and removes fields without name.
func filterConflicts(order []string, fieldMap map[string]*field) (fields []*field) {
	for _, name := range order {
		field, ok := fieldMap[name]
		if !ok {
			continue
		}

		if field.name == "" {
			delete(fieldMap, name)
		} else {
			fields = append(fields, field)
		}
	}
	return
}

var typeCache sync.Map

func getStructType(v reflect.Value) *structType {
	t := v.Type()

	var info *structType
	cachedInfo, ok := typeCache.Load(t)
	if !ok {
		info = newStructType(t)
		typeCache.Store(t, info)
	} else {
		info = cachedInfo.(*structType)
	}

	return info
}

type UnsupportedTypeError struct {
	UserType reflect.Type
}

func (e *UnsupportedTypeError) Error() string {
	return "wire: value of type " + e.UserType.String() + " is not supported"
}

type ReflectTypeError struct {
	UserType reflect.Type
	WireType ValueType

	Struct string
	Field  string
}

func (e *ReflectTypeError) Error() string {
	msg := "wire: cannot unmarshal " + e.WireType.String() + " into value of type " + e.UserType.String()
	if e.Struct != "" {
		msg += " at " + e.Struct + "." + e.Field
	}
	return msg
}

type WireDecoder struct {
	NameTable NameTable
}

func NewDecoder(table NameTable) *WireDecoder {
	return &WireDecoder{NameTable: table}
}

func (d *WireDecoder) getNameTableEntry(value Value) (*NameTableEntry, error) {
	if int(value.ID) >= len(d.NameTable) {
		return nil, xerrors.Errorf("unexpected field id %d; name table has %d entries",
			value.ID, len(d.NameTable))
	}

	return &d.NameTable[int(value.ID)], nil
}

func (d *WireDecoder) UnmarshalRow(row Row, v interface{}) error {
	return d.decodeAny(row, v)
}

func (d *WireDecoder) decodeAny(row Row, v interface{}) (err error) {
	if v == nil {
		return &UnsupportedTypeError{}
	}

	if row == nil {
		return nil
	}

	switch vv := v.(type) {
	case *interface{}:
		err = d.decodeRowGeneric(row, vv)
	default:
		err = d.decodeRowReflect(row, reflect.ValueOf(v))
	}

	return
}

func (d *WireDecoder) decodeRowGeneric(row Row, v *interface{}) error {
	m := make(map[string]interface{})
	*v = m

	for _, value := range row {
		entry, err := d.getNameTableEntry(value)
		if err != nil {
			return err
		}

		var mapValue interface{}
		if err := d.decodeValueGeneric(value, &mapValue); err != nil {
			return err
		}

		m[entry.Name] = mapValue
	}

	return nil
}

type TypesMismatchError struct {
	WireType   ValueType
	SchemaType schema.Type
}

func (e *TypesMismatchError) Error() string {
	return fmt.Sprintf("wire: unable to decode wire type %q into YT type %q", e.WireType, e.SchemaType)
}

func (d *WireDecoder) decodeValueGeneric(value Value, v *interface{}) (err error) {
	switch value.Type {
	case TypeNull:
		*v = nil
	case TypeInt64:
		*v = value.Int64()
	case TypeUint64:
		*v = value.Uint64()
	case TypeFloat64:
		*v = value.Float64()
	case TypeBool:
		*v = value.Bool()
	case TypeBytes:
		*v = value.Bytes()
	case TypeAny:
		return yson.Unmarshal(value.Any(), v)
	}

	return
}

func (d *WireDecoder) decodeRowReflect(row Row, v reflect.Value) error {
	if v.Kind() != reflect.Ptr {
		return &UnsupportedTypeError{v.Type()}
	}

	switch v.Elem().Type().Kind() {
	case reflect.Struct:
		return d.decodeRowReflectStruct(row, v.Elem())
	case reflect.Map:
		return d.decodeRowReflectMap(row, v)
	default:
		return &UnsupportedTypeError{v.Type()}
	}
}

func (d *WireDecoder) decodeRowReflectStruct(row Row, v reflect.Value) error {
	structType := getStructType(v)

	for _, value := range row {
		entry, err := d.getNameTableEntry(value)
		if err != nil {
			return err
		}

		structField, ok := structType.fieldsByName[entry.Name]
		if !ok {
			continue
		}

		field, _, err := fieldByIndex(v, structField.index, true)
		if err != nil {
			return err
		}

		if err = d.decodeValueAny(value, field.Addr().Interface()); err != nil {
			if typeError, ok := err.(*ReflectTypeError); ok {
				return &ReflectTypeError{
					UserType: typeError.UserType,
					WireType: typeError.WireType,
					Struct:   v.Type().String(),
					Field:    entry.Name,
				}
			}

			return err
		}
	}

	return nil
}

func fieldByIndex(v reflect.Value, index []int, initPtr bool) (reflect.Value, bool, error) {
	for i, fieldIndex := range index {
		if i != 0 {
			if v.Kind() == reflect.Ptr {
				if v.IsNil() {
					if initPtr {
						if !v.CanSet() {
							err := xerrors.Errorf("wire: cannot set embedded pointer to unexported field: %v", v.Type())
							return reflect.Value{}, false, err
						}
						v.Set(reflect.New(v.Type().Elem()))
					} else {
						return reflect.Value{}, false, nil
					}
				}

				v = v.Elem()
			}
		}

		v = v.Field(fieldIndex)
	}

	return v, true, nil
}

func (d *WireDecoder) decodeRowReflectMap(row Row, v reflect.Value) error {
	kt := v.Type().Elem().Key()

	switch kt.Kind() {
	case reflect.String:
	default:
		return &UnsupportedTypeError{v.Type().Elem()}
	}

	m := reflect.MakeMap(v.Elem().Type())
	v.Elem().Set(m)
	elementType := m.Type().Elem()

	for _, value := range row {
		entry, err := d.getNameTableEntry(value)
		if err != nil {
			return err
		}

		var kv reflect.Value
		switch {
		case kt.Kind() == reflect.String:
			kv = reflect.ValueOf(entry.Name).Convert(kt)
		default:
			return xerrors.Errorf("unexpected key type %v", kt.Kind())
		}

		elem := reflect.New(elementType)
		if err := d.decodeValueAny(value, elem.Interface()); err != nil {
			return err
		}

		m.SetMapIndex(kv, elem.Elem())
	}

	return nil
}

func (d *WireDecoder) decodeValueAny(value Value, v interface{}) (err error) {
	if v == nil {
		return &UnsupportedTypeError{}
	}

	if value.Type == TypeNull {
		return
	}

	switch vv := v.(type) {
	case *int:
		if err := validateWireType(TypeInt64, value.Type); err != nil {
			return err
		}
		*vv = int(value.Int64())
	case *int8:
		*vv, err = convertInt8(value)
	case *int16:
		*vv, err = convertInt16(value)
	case *int32:
		*vv, err = convertInt32(value)
	case *int64:
		*vv, err = convertInt64(value)
	case *uint:
		if err := validateWireType(TypeUint64, value.Type); err != nil {
			return err
		}
		*vv = uint(value.Uint64())
	case *uint8:
		*vv, err = convertUint8(value)
	case *uint16:
		*vv, err = convertUint16(value)
	case *uint32:
		*vv, err = convertUint32(value)
	case *uint64:
		*vv, err = convertUint64(value)
	case *bool:
		if err := validateWireType(TypeBool, value.Type); err != nil {
			return err
		}
		*vv = value.Bool()
	case *float32:
		if err := validateWireType(TypeFloat64, value.Type); err != nil {
			return err
		}
		f := value.Float64()
		if err := checkFloat32Overflow(f); err != nil {
			return err
		}
		*vv = float32(f)
	case *float64:
		if err := validateWireType(TypeFloat64, value.Type); err != nil {
			return err
		}
		*vv = value.Float64()
	case *string:
		if err := validateWireType(TypeBytes, value.Type); err != nil {
			return err
		}
		*vv = string(value.Bytes())
	case *[]byte:
		var b []byte
		b = value.Bytes()

		if b == nil {
			*vv = nil
		} else {
			*vv = make([]byte, len(b))
			copy(*vv, b)
		}
	case *yson.RawValue:
		raw := value.Bytes()

		*vv = make([]byte, len(raw))
		copy(*vv, raw)

	case *yson.Duration:
		if err := checkWireTypeScalar(value.Type); err != nil {
			return err
		}

		i := value.Int64()
		*vv = yson.Duration(time.Microsecond * time.Duration(i))

	case yson.Unmarshaler:
		err = vv.UnmarshalYSON(value.Bytes())

	case encoding.TextUnmarshaler:
		err = vv.UnmarshalText(value.Bytes())

	case encoding.BinaryUnmarshaler:
		err = vv.UnmarshalBinary(value.Bytes())

	case *interface{}:
		err = d.decodeValueGeneric(value, vv)

	default:
		if err := decodeReflect(value, reflect.ValueOf(v)); err != nil {
			return err
		}
	}

	return
}

func decodeReflect(value Value, v reflect.Value) error {
	if v.Kind() != reflect.Ptr {
		return &UnsupportedTypeError{v.Type()}
	}

	if value.Type == TypeNull {
		return nil
	}

	switch v.Elem().Type().Kind() {
	case reflect.Int, reflect.Int64:
		i, err := convertInt64(value)
		v.Elem().SetInt(i)
		return err
	case reflect.Int8:
		i, err := convertInt8(value)
		v.Elem().SetInt(int64(i))
		return err
	case reflect.Int16:
		i, err := convertInt16(value)
		v.Elem().SetInt(int64(i))
		return err
	case reflect.Int32:
		i, err := convertInt32(value)
		v.Elem().SetInt(int64(i))
		return err

	case reflect.Uint, reflect.Uint64:
		i, err := convertUint64(value)
		v.Elem().SetUint(i)
		return err
	case reflect.Uint8:
		i, err := convertUint8(value)
		v.Elem().SetUint(uint64(i))
		return err
	case reflect.Uint16:
		i, err := convertUint16(value)
		v.Elem().SetUint(uint64(i))
		return err
	case reflect.Uint32:
		i, err := convertUint32(value)
		v.Elem().SetUint(uint64(i))
		return err

	case reflect.String:
		s := string(value.Bytes())
		v.Elem().SetString(string(s))
		return nil

	default:
		if err := validateWireType(TypeAny, value.Type); err != nil {
			return err
		}

		return yson.Unmarshal(value.Any(), v.Interface())
	}
}

func convertInt8(value Value) (int8, error) {
	if err := validateWireType(TypeInt64, value.Type); err != nil {
		return 0, err
	}
	i := value.Int64()
	if err := checkIntOverflow(i, 8); err != nil {
		return 0, err
	}
	return int8(i), nil
}

func convertInt16(value Value) (int16, error) {
	if err := validateWireType(TypeInt64, value.Type); err != nil {
		return 0, err
	}
	i := value.Int64()
	if err := checkIntOverflow(i, 16); err != nil {
		return 0, err
	}
	return int16(i), nil
}

func convertInt32(value Value) (int32, error) {
	if err := validateWireType(TypeInt64, value.Type); err != nil {
		return 0, err
	}
	i := value.Int64()
	if err := checkIntOverflow(i, 32); err != nil {
		return 0, err
	}
	return int32(i), nil
}

func convertInt64(value Value) (int64, error) {
	if err := validateWireType(TypeInt64, value.Type); err != nil {
		return 0, err
	}
	return value.Int64(), nil
}

func convertUint8(value Value) (uint8, error) {
	if err := validateWireType(TypeUint64, value.Type); err != nil {
		return 0, err
	}
	i := value.Uint64()
	if err := checkUintOverflow(i, 8); err != nil {
		return 0, err
	}
	return uint8(i), nil
}

func convertUint16(value Value) (uint16, error) {
	if err := validateWireType(TypeUint64, value.Type); err != nil {
		return 0, err
	}
	i := value.Uint64()
	if err := checkUintOverflow(i, 16); err != nil {
		return 0, err
	}
	return uint16(i), nil
}

func convertUint32(value Value) (uint32, error) {
	if err := validateWireType(TypeUint64, value.Type); err != nil {
		return 0, err
	}
	i := value.Uint64()
	if err := checkUintOverflow(i, 32); err != nil {
		return 0, err
	}
	return uint32(i), nil
}

func convertUint64(value Value) (uint64, error) {
	if err := validateWireType(TypeUint64, value.Type); err != nil {
		return 0, err
	}
	return value.Uint64(), nil
}

var (
	ErrIntegerOverflow = xerrors.New("wire: integer overflow")
	ErrFloat32Overflow = xerrors.New("wire: float32 overflow")
)

func checkIntOverflow(value int64, bits int) error {
	switch bits {
	case 8:
		if value > math.MaxInt8 || value < math.MinInt8 {
			return ErrIntegerOverflow
		}
	case 16:
		if value > math.MaxInt16 || value < math.MinInt16 {
			return ErrIntegerOverflow
		}
	case 32:
		if value > math.MaxInt32 || value < math.MinInt32 {
			return ErrIntegerOverflow
		}
	}
	return nil
}

func checkUintOverflow(value uint64, bits int) error {
	switch bits {
	case 8:
		if value > math.MaxUint8 {
			return ErrIntegerOverflow
		}
	case 16:
		if value > math.MaxUint16 {
			return ErrIntegerOverflow
		}
	case 32:
		if value > math.MaxUint32 {
			return ErrIntegerOverflow
		}
	}
	return nil
}

func checkFloat32Overflow(value float64) error {
	if value > math.MaxFloat32 {
		return ErrFloat32Overflow
	}
	return nil
}

func validateWireType(expected, actual ValueType) error {
	if expected != actual {
		return xerrors.Errorf("wire: unable to deserialize value of type %v into %v", actual, expected)
	}
	return nil
}

func checkWireTypeScalar(typ ValueType) error {
	switch typ {
	case TypeInt64, TypeUint64, TypeFloat64:
	default:
		return xerrors.Errorf("wire: expected scalar type, got %v", typ)
	}
	return nil
}
