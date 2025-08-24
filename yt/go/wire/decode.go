package wire

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
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
	Schema    *rpc_proxy.TTableSchema
}

func NewDecoder(table NameTable, schema *rpc_proxy.TTableSchema) *WireDecoder {
	return &WireDecoder{NameTable: table, Schema: schema}
}

func (d *WireDecoder) getNameTableEntry(value Value) (*NameTableEntry, error) {
	if int(value.ID) >= len(d.NameTable) {
		return nil, xerrors.Errorf("unexpected field id %d; name table has %d entries",
			value.ID, len(d.NameTable))
	}

	return &d.NameTable[int(value.ID)], nil
}

func (d *WireDecoder) UnmarshalRow(row Row, v any) error {
	return d.decodeAny(row, v)
}

func (d *WireDecoder) decodeAny(row Row, v any) (err error) {
	if v == nil {
		return &UnsupportedTypeError{}
	}

	if row == nil {
		return nil
	}

	switch vv := v.(type) {
	case *any:
		err = d.decodeRowGeneric(row, vv)
	default:
		err = d.decodeRowReflect(row, reflect.ValueOf(v))
	}

	return
}

func (d *WireDecoder) decodeRowGeneric(row Row, v *any) error {
	m := make(map[string]any)
	*v = m

	for _, value := range row {
		entry, err := d.getNameTableEntry(value)
		if err != nil {
			return err
		}

		var mapValue any
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

func (d *WireDecoder) decodeValueGeneric(value Value, v *any) (err error) {
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
	case TypeComposite:
		return d.decodeValueComposite(value, v)
	case TypeAny:
		return yson.Unmarshal(value.Any(), v)
	}

	return
}

func (d *WireDecoder) decodeValueComposite(value Value, v *any) error {
	entry, err := d.getNameTableEntry(value)
	if err != nil {
		return err
	}

	var columnSchema *rpc_proxy.TColumnSchema
	if d.Schema != nil {
		for _, col := range d.Schema.GetColumns() {
			if col.GetName() == entry.Name {
				columnSchema = col
				break
			}
		}
	}

	if columnSchema == nil || len(columnSchema.GetTypeV3()) == 0 {
		return yson.Unmarshal(value.Any(), v)
	}

	var complexType schema.ComplexType
	if err := yson.Unmarshal(columnSchema.GetTypeV3(), &complexType); err != nil {
		return err
	}

	var ysonData any
	if err := yson.Unmarshal(value.Any(), &ysonData); err != nil {
		return err
	}

	return d.decodeSchemaType(ysonData, complexType, v)
}

func (d *WireDecoder) decodeSchemaType(data any, complexType schema.ComplexType, v *any) error {
	switch t := complexType.(type) {
	case schema.Type:
		return d.decodePrimitiveType(data, t, v)
	case schema.Decimal:
		return d.decodeDecimal(data, t, v)
	case schema.Tagged:
		return d.decodeTagged(data, t, v)
	case schema.Optional:
		return d.decodeOptional(data, t, v)
	case schema.List:
		return d.decodeList(data, t, v)
	case schema.Struct:
		return d.decodeStruct(data, t, v)
	case schema.Tuple:
		return d.decodeTuple(data, t, v)
	case schema.Dict:
		return d.decodeDict(data, t, v)
	case schema.Variant:
		return d.decodeVariant(data, t, v)
	default:
		return fmt.Errorf("unknown type %T", complexType)
	}
}

func (d *WireDecoder) decodePrimitiveType(data any, t schema.Type, v *any) error {
	switch t {
	case schema.TypeDate:
		return d.decodeDate(data, v)
	case schema.TypeDatetime:
		return d.decodeDatetime(data, v)
	case schema.TypeTimestamp:
		return d.decodeTimestamp(data, v)
	default:
		*v = data
		return nil
	}
}

func (d *WireDecoder) decodeDate(data any, v *any) error {
	if unixTimeDays, ok := data.(uint64); ok {
		*v = unixTimeDays
		return nil
	}
	if dateStr, ok := data.(string); ok {
		t, err := time.Parse(time.DateOnly, dateStr)
		if err != nil {
			return err
		}
		*v = t.Unix() / (24 * 60 * 60)
		return nil
	}
	return fmt.Errorf("expected uint64 or string for date, got %T", data)
}

func (d *WireDecoder) decodeDatetime(data any, v *any) error {
	if unixTimeSeconds, ok := data.(uint64); ok {
		*v = unixTimeSeconds
		return nil
	}
	if dateStr, ok := data.(string); ok {
		t, err := time.Parse(time.RFC3339, dateStr)
		if err != nil {
			return err
		}

		*v = t.Unix()
		return nil
	}
	return fmt.Errorf("expected uint64 or string for datetime, got %T", data)
}

func (d *WireDecoder) decodeTimestamp(data any, v *any) error {
	if unixTimeMilliseconds, ok := data.(uint64); ok {
		*v = unixTimeMilliseconds
		return nil
	}
	if dateStr, ok := data.(string); ok {
		t, err := time.Parse(time.RFC3339Nano, dateStr)
		if err != nil {
			return err
		}

		*v = t.UnixMilli()
		return nil
	}
	return fmt.Errorf("expected uint64 or string for timestamp, got %T", data)
}

func (d *WireDecoder) decodeDecimal(data any, t schema.Decimal, v *any) error {
	if decimalStr, ok := data.(string); ok {
		*v = decimalStr
		return nil
	}

	if binaryData, ok := data.([]byte); ok {
		decimalStr, err := decodeDecimalFromBinary(binaryData, t.Precision, t.Scale)
		if err != nil {
			return fmt.Errorf("failed to decode decimal from binary: %w", err)
		}
		*v = decimalStr
		return nil
	}

	return fmt.Errorf("expected string or []byte for decimal, got %T", data)
}

func (d *WireDecoder) decodeTagged(data any, t schema.Tagged, v *any) error {
	return d.decodeSchemaType(data, t.Item, v)
}

func (d *WireDecoder) decodeOptional(data any, t schema.Optional, v *any) error {
	if data == nil {
		*v = nil
		return nil
	}
	return d.decodeSchemaType(data, t.Item, v)
}

func (d *WireDecoder) decodeList(data any, t schema.List, v *any) error {
	ysonList, ok := data.([]any)
	if !ok {
		return fmt.Errorf("expected list, got %T", data)
	}

	result := make([]any, len(ysonList))
	for i, item := range ysonList {
		if err := d.decodeSchemaType(item, t.Item, &result[i]); err != nil {
			return err
		}
	}
	*v = result
	return nil
}

func (d *WireDecoder) decodeStruct(data any, t schema.Struct, v *any) error {
	// complex_type_mode = named
	if ysonMap, ok := data.(map[any]any); ok {
		result := make(map[string]any)
		for key, val := range ysonMap {
			keyStr, ok := key.(string)
			if !ok {
				return fmt.Errorf("expected string key in struct map, got %T", key)
			}

			var memberType schema.ComplexType
			for _, member := range t.Members {
				if member.Name == keyStr {
					memberType = member.Type
					break
				}
			}

			if memberType == nil {
				continue
			}

			var decodedVal any
			if err := d.decodeSchemaType(val, memberType, &decodedVal); err != nil {
				return err
			}
			result[keyStr] = decodedVal
		}
		*v = result
		return nil
	}

	// complex_type_mode = positional
	if ysonList, ok := data.([]any); ok {
		// Positional representation: struct is encoded as a YSON list
		// The i-th position contains the i-th field's value
		result := make(map[string]any)
		for i, member := range t.Members {
			if i < len(ysonList) {
				var value any
				if err := d.decodeSchemaType(ysonList[i], member.Type, &value); err != nil {
					return err
				}
				result[member.Name] = value
			} else {
				if _, isOptional := member.Type.(schema.Optional); isOptional {
					result[member.Name] = nil
				}
			}
		}
		*v = result
		return nil
	}

	return fmt.Errorf("expected map or list for struct, got %T", data)
}

func (d *WireDecoder) decodeTuple(data any, t schema.Tuple, v *any) error {
	ysonList, ok := data.([]any)
	if !ok {
		return fmt.Errorf("expected list for tuple, got %T", data)
	}

	result := make([]any, len(t.Elements))
	for i, elem := range t.Elements {
		if i >= len(ysonList) {
			break
		}
		if err := d.decodeSchemaType(ysonList[i], elem.Type, &result[i]); err != nil {
			return err
		}
	}
	*v = result
	return nil
}

func (d *WireDecoder) decodeDict(data any, t schema.Dict, v *any) error {
	// Dict is represented by default as a YSON list, where each element is a YSON list of two elements: key and value
	if ysonList, ok := data.([]any); ok {
		result := make(map[any]any)
		for _, item := range ysonList {
			kvList, ok := item.([]any)
			if !ok || len(kvList) != 2 {
				return fmt.Errorf("expected key-value pair list for dict item, got %T", item)
			}
			var decodedKey, decodedVal any
			if err := d.decodeSchemaType(kvList[0], t.Key, &decodedKey); err != nil {
				return err
			}
			if err := d.decodeSchemaType(kvList[1], t.Value, &decodedVal); err != nil {
				return err
			}
			result[decodedKey] = decodedVal
		}
		*v = result
		return nil
	}

	// When string_keyed_dict_mode is enabled
	if ysonMap, ok := data.(map[any]any); ok {
		result := make(map[any]any)
		for key, val := range ysonMap {
			var decodedKey, decodedVal any
			if err := d.decodeSchemaType(key, t.Key, &decodedKey); err != nil {
				return err
			}
			if err := d.decodeSchemaType(val, t.Value, &decodedVal); err != nil {
				return err
			}

			result[decodedKey] = decodedVal
		}
		*v = result
		return nil
	}

	return fmt.Errorf("expected list or map for dict, got %T", data)
}

func (d *WireDecoder) decodeVariant(data any, t schema.Variant, v *any) error {
	// Variant is represented as a YSON list of length 2: [alternative, value]
	ysonList, ok := data.([]any)
	if !ok || len(ysonList) != 2 {
		return fmt.Errorf("expected list of length 2 for variant, got %T", data)
	}

	alternative := ysonList[0]
	value := ysonList[1]

	// named variants
	if altName, ok := alternative.(string); ok {
		for _, member := range t.Members {
			if member.Name == altName {
				var decodedValue any
				if err := d.decodeSchemaType(value, member.Type, &decodedValue); err != nil {
					return err
				}
				*v = decodedValue
				return nil
			}
		}
	}

	// unnamed variants (index)
	if altIndex, ok := alternative.(int64); ok {
		if int(altIndex) < len(t.Elements) {
			var decodedValue any
			if err := d.decodeSchemaType(value, t.Elements[altIndex].Type, &decodedValue); err != nil {
				return err
			}
			*v = decodedValue
			return nil
		}
	}

	return fmt.Errorf("expected alternative to be either string or int64, got %T", alternative)
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

func (d *WireDecoder) decodeValueAny(value Value, v any) (err error) {
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

	case *any:
		err = d.decodeValueGeneric(value, vv)

	default:
		if err := d.decodeReflect(value, reflect.ValueOf(v)); err != nil {
			return err
		}
	}

	return
}

func (d *WireDecoder) decodeReflect(value Value, v reflect.Value) error {
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
		if err := validateWireTypes([]ValueType{TypeAny, TypeComposite}, value.Type); err != nil {
			return err
		}

		if value.Type == TypeAny {
			return yson.Unmarshal(value.Any(), v.Interface())
		}

		res := v.Interface()
		if err := d.decodeValueComposite(value, &res); err != nil {
			return err
		}
		v.Elem().Set(reflect.ValueOf(&res))
		return nil
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

func validateWireTypes(expected []ValueType, actual ValueType) error {
	if !slices.Contains(expected, actual) {
		return xerrors.Errorf("wire: unable to deserialize value of type %v into any of %v", actual, expected)
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

// decodeDecimalFromBinary decodes a decimal value from its binary representation
// according to the YT decimal format specification.
func decodeDecimalFromBinary(data []byte, precision, scale int) (string, error) {
	expectedSize := getDecimalByteSize(precision)
	if len(data) != expectedSize {
		return "", xerrors.Errorf("wire: binary value of Decimal<%d,%d> has invalid length; expected length: %d actual length: %d",
			precision, scale, expectedSize, len(data))
	}

	var bigInt *big.Int
	switch len(data) {
	case 4:
		uintVal := binary.BigEndian.Uint32(data)
		intVal := int64(uintVal)
		intVal -= 1 << 31
		bigInt = big.NewInt(intVal)
	case 8:
		uintVal := binary.BigEndian.Uint64(data)
		bigInt = big.NewInt(0).SetUint64(uintVal)
		bitInverter := big.NewInt(1)
		bitInverter.Lsh(bitInverter, 63)
		bigInt.Sub(bigInt, bitInverter)
	case 16:
		hi := binary.BigEndian.Uint64(data[:8])
		lo := binary.BigEndian.Uint64(data[8:])
		bigHi := big.NewInt(0).SetUint64(hi)
		bigLo := big.NewInt(0).SetUint64(lo)
		bigHi.Lsh(bigHi, 64)
		bigHi.Add(bigHi, bigLo)

		bitInverter := big.NewInt(1)
		bitInverter.Lsh(bitInverter, 127)
		bigHi.Sub(bigHi, bitInverter)

		bigInt = bigHi
	case 32:
		p3 := binary.BigEndian.Uint64(data[:8])
		p2 := binary.BigEndian.Uint64(data[8:16])
		p1 := binary.BigEndian.Uint64(data[16:24])
		p0 := binary.BigEndian.Uint64(data[24:32])

		bigInt = big.NewInt(0)
		bigP3 := big.NewInt(0).SetUint64(p3)
		bigP2 := big.NewInt(0).SetUint64(p2)
		bigP1 := big.NewInt(0).SetUint64(p1)
		bigP0 := big.NewInt(0).SetUint64(p0)

		// Combine the parts: p3 << 192 + p2 << 128 + p1 << 64 + p0
		bigP3.Lsh(bigP3, 192)
		bigP2.Lsh(bigP2, 128)
		bigP1.Lsh(bigP1, 64)

		bigInt.Add(bigInt, bigP3)
		bigInt.Add(bigInt, bigP2)
		bigInt.Add(bigInt, bigP1)
		bigInt.Add(bigInt, bigP0)

		bitInverter := big.NewInt(1)
		bitInverter.Lsh(bitInverter, 255)
		bigInt.Sub(bigInt, bitInverter)
	default:
		return "", xerrors.Errorf("wire: unexpected decimal binary length: %d", len(data))
	}

	if special := checkSpecialValue(bigInt, len(data)); special != "" {
		return special, nil
	}

	// Apply scale by dividing by 10^scale
	if scale != 0 {
		scaleFactor := big.NewInt(10)
		scaleFactor.Exp(scaleFactor, big.NewInt(int64(-scale)), nil)
		bigInt.Mul(bigInt, scaleFactor)
	}

	result := bigInt.String()

	if scale > 0 {
		isNegative := false
		if len(result) > 0 && result[0] == '-' {
			isNegative = true
			result = result[1:] // Remove the minus sign temporarily
		}

		if len(result) <= scale {
			result = "0." + strings.Repeat("0", scale-len(result)) + result
		} else {
			result = result[:len(result)-scale] + "." + result[len(result)-scale:]
		}

		if isNegative {
			result = "-" + result
		}
	}

	return result, nil
}

func getDecimalByteSize(precision int) int {
	if precision <= 0 || precision > 76 {
		return 0 // Invalid precision
	} else if precision <= 9 {
		return 4
	} else if precision <= 18 {
		return 8
	} else if precision <= 38 {
		return 16
	} else {
		return 32
	}
}

func checkSpecialValue(bigInt *big.Int, byteSize int) string {
	shift := byteSize*8 - 1
	nanValue := big.NewInt(1)
	nanValue.Lsh(nanValue, uint(shift))
	nanValue.Sub(nanValue, big.NewInt(1))

	plusInf := big.NewInt(0).Sub(nanValue, big.NewInt(1))
	minusInf := big.NewInt(0).Add(big.NewInt(0).Neg(nanValue), big.NewInt(1))

	if bigInt.Cmp(nanValue) == 0 {
		return "nan"
	} else if bigInt.Cmp(plusInf) == 0 {
		return "+inf"
	} else if bigInt.Cmp(minusInf) == 0 {
		return "-inf"
	}
	return ""
}
