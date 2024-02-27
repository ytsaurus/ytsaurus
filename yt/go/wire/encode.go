package wire

import (
	"encoding"
	"reflect"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

type NameTable []NameTableEntry

type NameTableEntry struct {
	Name string
}

func Encode(items []any) (NameTable, []Row, error) {
	rows := make([]Row, 0, len(items))

	indexMap := make(map[NameTableEntry]uint16)

	for _, i := range items {
		row, err := encode(i, indexMap)
		if err != nil {
			return nil, nil, err
		}

		rows = append(rows, row)
	}

	reverseIndexMap := make(map[uint16]NameTableEntry)
	for k, id := range indexMap {
		reverseIndexMap[id] = k
	}

	nameTable := make([]NameTableEntry, 0, len(reverseIndexMap))
	for i := 0; i < len(reverseIndexMap); i++ {
		nameTable = append(nameTable, reverseIndexMap[uint16(i)])
	}

	return nameTable, rows, nil
}

func encode(item any, indexMap map[NameTableEntry]uint16) (Row, error) {
	vv := reflect.ValueOf(item)
	if item == nil || vv.Kind() == reflect.Ptr && vv.IsNil() {
		return nil, xerrors.Errorf("unsupported nil item")
	}

	return encodeReflect(vv, indexMap)
}

func encodeReflect(item reflect.Value, indexMap map[NameTableEntry]uint16) (Row, error) {
	switch item.Type().Kind() {
	case reflect.Ptr:
		return encodeReflect(item.Elem(), indexMap)
	case reflect.Struct:
		return encodeReflectStruct(item, indexMap)
	case reflect.Map:
		return encodeReflectMap(item, indexMap)
	}

	return nil, xerrors.Errorf("wire: type %T not supported", item.Interface())
}

var (
	typeOfBytes           = reflect.TypeOf([]byte(nil))
	typeOfBinaryMarshaler = reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem()
	typeOfTextMarshaler   = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
)

func ytTypeFor(typ reflect.Type) (ytTyp schema.Type, err error) {
	if typ == typeOfBytes {
		return schema.TypeBytes, nil
	}

	if typ.Implements(typeOfTextMarshaler) || (typ.Kind() != reflect.Ptr && reflect.PtrTo(typ).Implements(typeOfTextMarshaler)) {
		return schema.TypeString, nil
	}

	if typ.Implements(typeOfBinaryMarshaler) || (typ.Kind() != reflect.Ptr && reflect.PtrTo(typ).Implements(typeOfBinaryMarshaler)) {
		return schema.TypeBytes, nil
	}

	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	switch {
	case typ == reflect.TypeOf(schema.Date(0)):
		return schema.TypeDate, nil
	case typ == reflect.TypeOf(schema.Datetime(0)):
		return schema.TypeDatetime, nil
	case typ == reflect.TypeOf(schema.Timestamp(0)):
		return schema.TypeTimestamp, nil
	case typ == reflect.TypeOf(schema.Interval(0)):
		return schema.TypeInterval, nil
	}

	switch typ.Kind() {
	case reflect.Int, reflect.Int64:
		return schema.TypeInt64, nil
	case reflect.Int32:
		return schema.TypeInt32, nil
	case reflect.Int16:
		return schema.TypeInt16, nil
	case reflect.Int8:
		return schema.TypeInt8, nil

	case reflect.Uint, reflect.Uint64:
		return schema.TypeUint64, nil
	case reflect.Uint32:
		return schema.TypeUint32, nil
	case reflect.Uint16:
		return schema.TypeUint16, nil
	case reflect.Uint8:
		return schema.TypeUint8, nil

	case reflect.String:
		return schema.TypeString, nil
	case reflect.Bool:
		return schema.TypeBoolean, nil
	case reflect.Float32:
		return schema.TypeFloat32, nil
	case reflect.Float64:
		return schema.TypeFloat64, nil

	case reflect.Struct, reflect.Slice, reflect.Map, reflect.Interface, reflect.Array:
		return schema.TypeAny, nil
	}

	return "", xerrors.Errorf("type %v has no associated YT type", typ)
}

func parseTag(fieldName string, typ reflect.Type, tag reflect.StructTag) (f *field, err error) {
	f = &field{required: true}

	decodedTag, skip := yson.ParseTag(fieldName, tag)
	if skip {
		return nil, nil
	}

	f.name = decodedTag.Name

	if decodedTag.Omitempty {
		f.required = false
	}

	ytTyp, err := ytTypeFor(typ)
	if err != nil {
		return nil, err
	}

	if typ.Kind() == reflect.Ptr || ytTyp == schema.TypeAny {
		f.required = false
	}

	return
}

func encodeReflectStruct(value reflect.Value, indexMap map[NameTableEntry]uint16) (row Row, err error) {
	structType := getStructType(value)

	if structType.value != nil {
		return nil, xerrors.Errorf("value attr not supported")
	}

	row = make(Row, 0, len(structType.fields))

	for _, f := range structType.fields {
		v, ok, err := fieldByIndex(value, f.index, false)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		if f.omitempty && isZeroValue(v) {
			continue
		}

		k := NameTableEntry{Name: f.name}
		if _, ok := indexMap[k]; !ok {
			id := uint16(len(indexMap))
			indexMap[k] = id
		}

		value, err := convertValue(indexMap[k], v)
		if err != nil {
			return nil, err
		}

		row = append(row, value)
	}

	return row, nil
}

func encodeReflectMap(v reflect.Value, indexMap map[NameTableEntry]uint16) (row Row, err error) {
	iter := v.MapRange()

	for iter.Next() {
		k := iter.Key()
		if k.Kind() != reflect.String {
			err = xerrors.Errorf("can't encode map with key of type %v, only string is supported", k.Type())
			return
		}

		name := k.Interface().(string)
		v := iter.Value()

		var typ reflect.Type
		if v.IsNil() {
			// In general nil value is an empty interface.
			typ = v.Type()
		} else {
			typ = v.Elem().Type()
		}

		f, err := parseTag(name, typ, "")
		if err != nil {
			return nil, err
		}

		if f == nil {
			continue
		}

		key := NameTableEntry{Name: f.name}
		if _, ok := indexMap[key]; !ok {
			id := uint16(len(indexMap))
			indexMap[key] = id
		}

		vv := v
		if !vv.IsNil() {
			vv = vv.Elem()
		}

		value, err := convertValue(indexMap[key], vv)
		if err != nil {
			return nil, err
		}

		row = append(row, value)
	}

	return
}

func convertValue(id uint16, value reflect.Value) (Value, error) {
	typ := value.Type()

	if typ == typeOfBytes {
		return NewBytes(id, value.Bytes()), nil
	}

	if m, ok := value.Interface().(encoding.BinaryMarshaler); ok {
		buf, err := m.MarshalBinary()
		if err != nil {
			return Value{}, err
		}
		return NewBytes(id, buf), nil
	}

	if m, ok := value.Interface().(encoding.TextMarshaler); ok {
		buf, err := m.MarshalText()
		if err != nil {
			return Value{}, err
		}
		return NewBytes(id, buf), nil
	}

	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	switch {
	case typ == reflect.TypeOf(schema.Date(0)) ||
		typ == reflect.TypeOf(schema.Datetime(0)) ||
		typ == reflect.TypeOf(schema.Timestamp(0)) ||
		typ == reflect.TypeOf(schema.Interval(0)):
		return Value{}, xerrors.Errorf("unsupported schema type %T when converting to wire", typ)
	}

	var v Value

	switch typ.Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		v = NewInt64(id, value.Int())
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		v = NewUint64(id, value.Uint())
	case reflect.String:
		v = NewBytes(id, []byte(value.String()))
	case reflect.Bool:
		v = NewBool(id, value.Bool())
	case reflect.Float64, reflect.Float32:
		v = NewFloat64(id, value.Float())
	case reflect.Slice, reflect.Map, reflect.Interface:
		if value.IsNil() {
			return NewNull(id), nil
		}

		blob, err := yson.Marshal(value.Interface())
		if err != nil {
			return Value{}, err
		}
		v = NewAny(id, blob)
	case reflect.Struct, reflect.Array:
		blob, err := yson.Marshal(value.Interface())
		if err != nil {
			return Value{}, err
		}
		v = NewAny(id, blob)
	default:
		return Value{}, xerrors.Errorf("unable to convert type %T to wire", typ)
	}

	return v, nil
}

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

func EncodePivotKeys(keys []any) ([]Row, error) {
	rows := make([]Row, 0, len(keys))

	for _, key := range keys {
		row, err := encodeReflectPivotKey(key)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func encodeReflectPivotKey(key any) (row Row, err error) {
	vv := reflect.ValueOf(key)
	if vv.Kind() != reflect.Slice {
		return nil, xerrors.Errorf("unsupported pivot key type: %v", vv.Kind())
	}

	if vv.Len() == 0 {
		row = make(Row, 0)
		return
	}

	for i := 0; i < vv.Len(); i++ {
		val := vv.Index(i).Interface()

		value, err := convertValue(0, reflect.ValueOf(val))
		if err != nil {
			return nil, err
		}

		row = append(row, value)
	}

	return
}
