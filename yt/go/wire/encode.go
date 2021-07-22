package wire

import (
	"encoding"
	"reflect"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/yson"
)

type NameTable []NameTableEntry

type NameTableEntry struct {
	Name string
}

func Encode(items []interface{}) (NameTable, []Row, error) {
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

func encode(item interface{}, indexMap map[NameTableEntry]uint16) (Row, error) {
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

		ytTyp, err := ytTypeFor(v.Type())
		if err != nil {
			return nil, err
		}

		k := NameTableEntry{Name: f.name}
		if _, ok := indexMap[k]; !ok {
			id := uint16(len(indexMap))
			indexMap[k] = id
		}

		value, err := convertValue(ytTyp, indexMap[k], v.Interface())
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

		ytTyp, err := ytTypeFor(typ)
		if err != nil {
			return nil, err
		}

		key := NameTableEntry{Name: f.name}
		if _, ok := indexMap[key]; !ok {
			id := uint16(len(indexMap))
			indexMap[key] = id
		}

		value, err := convertValue(ytTyp, indexMap[key], v.Interface())
		if err != nil {
			return nil, err
		}

		row = append(row, value)
	}

	return
}

func convertValue(typ schema.Type, id uint16, value interface{}) (Value, error) {
	var v Value

	switch typ {
	case schema.TypeInt64:
		if i, ok := value.(int); ok {
			v = NewInt64(id, int64(i))
		} else {
			v = NewInt64(id, value.(int64))
		}
	case schema.TypeInt32:
		v = NewInt64(id, int64(value.(int32)))
	case schema.TypeInt16:
		v = NewInt64(id, int64(value.(int16)))
	case schema.TypeInt8:
		v = NewInt64(id, int64(value.(int8)))
	case schema.TypeUint64:
		if u, ok := value.(uint); ok {
			v = NewUint64(id, uint64(u))
		} else {
			v = NewUint64(id, value.(uint64))
		}
	case schema.TypeUint32:
		v = NewUint64(id, uint64(value.(uint32)))
	case schema.TypeUint16:
		v = NewUint64(id, uint64(value.(uint16)))
	case schema.TypeUint8:
		v = NewUint64(id, uint64(value.(uint8)))
	case schema.TypeFloat32:
		v = NewFloat64(id, float64(value.(float32)))
	case schema.TypeFloat64:
		v = NewFloat64(id, value.(float64))
	case schema.TypeBytes:
		v = NewBytes(id, value.([]byte))
	case schema.TypeString:
		v = NewBytes(id, []byte(value.(string)))
	case schema.TypeBoolean:
		v = NewBool(id, value.(bool))
	case schema.TypeAny:
		blob, err := yson.Marshal(value)
		if err != nil {
			return Value{}, err
		}
		v = NewAny(id, blob)
	case schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp, schema.TypeInterval:
		return Value{}, xerrors.Errorf("unsupported schema type %T when converting to wire", typ)
	default:
		return Value{}, xerrors.Errorf("unable to convert schema type %T to wire", typ)
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
