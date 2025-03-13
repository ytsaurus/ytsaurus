package schema

import (
	"reflect"
	"slices"
	"sort"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/yson"
)

type inferer struct {
	typeStack []reflect.Type
}

func (in *inferer) isLoop(typ reflect.Type) bool {
	return slices.Contains(in.typeStack, typ)
}

func (in *inferer) ytComplexTypeFor(typ reflect.Type) (ytTyp ComplexType, err error) {
	if typ == typeOfBytes {
		return TypeBytes, nil
	}

	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
		nested, err := in.ytComplexTypeFor(typ)
		if err != nil {
			return nested, err
		}

		return Optional{Item: nested}, nil
	}

	if typ.Implements(typeOfTextMarshaler) || (reflect.PointerTo(typ).Implements(typeOfTextMarshaler)) {
		return TypeString, nil
	}

	if typ.Implements(typeOfBinaryMarshaler) || (reflect.PointerTo(typ).Implements(typeOfBinaryMarshaler)) {
		return TypeBytes, nil
	}

	switch {
	case typ == reflect.TypeOf(Date(0)):
		return TypeDate, nil
	case typ == reflect.TypeOf(Datetime(0)):
		return TypeDatetime, nil
	case typ == reflect.TypeOf(Timestamp(0)):
		return TypeTimestamp, nil
	case typ == reflect.TypeOf(Interval(0)):
		return TypeInterval, nil
	}

	switch typ.Kind() {
	case reflect.Int, reflect.Int64:
		return TypeInt64, nil
	case reflect.Int32:
		return TypeInt32, nil
	case reflect.Int16:
		return TypeInt16, nil

	case reflect.Uint, reflect.Uint64:
		return TypeUint64, nil
	case reflect.Uint32:
		return TypeUint32, nil
	case reflect.Uint16:
		return TypeUint16, nil

	case reflect.String:
		return TypeString, nil
	case reflect.Bool:
		return typeBool, nil
	case reflect.Float32:
		return TypeFloat32, nil
	case reflect.Float64:
		return TypeFloat64, nil
	case reflect.Struct:
		if in.isLoop(typ) {
			// Have no idea how to fix this.
			return typeYSON, nil
		}
		in.typeStack = append(in.typeStack, typ)

		members, err := traverseStruct(
			typ,
			func(field reflect.StructField, forceOptional bool) (*StructMember, error) {
				memberType, err := in.ytComplexTypeFor(field.Type)
				if err != nil || memberType == nil {
					return nil, err
				}

				if forceOptional {
					memberType = Optional{Item: memberType}
				}

				return &StructMember{Name: field.Name, Type: memberType}, nil
			})

		if err != nil {
			return nil, err
		}

		in.typeStack = in.typeStack[:len(in.typeStack)-1]
		return Struct{Members: members}, nil
	case reflect.Slice, reflect.Array:
		nested, err := in.ytComplexTypeFor(typ.Elem())
		if err != nil {
			return nil, err
		}
		return List{Item: nested}, nil
	case reflect.Map:
		keyType, err := in.ytComplexTypeFor(typ.Key())
		if err != nil {
			return nil, err
		}
		if keyType == TypeString {
			keyType = TypeBytes
		}
		valueType, err := in.ytComplexTypeFor(typ.Elem())
		if err != nil {
			return nil, err
		}
		if valueType == TypeString {
			valueType = TypeBytes
		}
		return Dict{Key: keyType, Value: valueType}, nil

	case reflect.Interface:
		return typeYSON, nil
	}

	return nil, xerrors.Errorf("type %q has no associated YT type", typ)
}

func (in *inferer) ParseTag(fieldName string, typ reflect.Type, tag reflect.StructTag, forceOptional bool) (c *Column, err error) {
	c = &Column{}

	decodedTag, skip := yson.ParseTag(fieldName, tag)
	if skip {
		return nil, nil
	}

	if decodedTag.Name == tabletIndexTag || decodedTag.Name == rowIndexTag {
		return nil, nil
	}

	c.Name = decodedTag.Name
	if decodedTag.Key {
		c.SortOrder = SortAscending
	}

	c.ComplexType, err = in.ytComplexTypeFor(typ)

	if decodedTag.Omitempty || forceOptional {
		c.ComplexType = Optional{c.ComplexType}
	}

	if ytGroup, ok := tag.Lookup("ytgroup"); ok {
		c.Group = ytGroup
	}

	return
}

func infer(value any) (Schema, error) {
	v, err := reflectValueOfType(value, reflect.Struct)
	if err != nil {
		return Schema{}, err
	}

	in := inferer{}

	columns, err := traverseStruct(
		v.Type(),
		func(field reflect.StructField, forceOptional bool) (*Column, error) {
			return in.ParseTag(field.Name, field.Type, field.Tag, forceOptional)
		})
	if err != nil {
		return Schema{}, err
	}

	return Schema{Columns: columns}, nil
}

// InferV3 infers Schema from go struct (using ComplexType[type_v3]).
//
// By default InferV3 creates column for each struct field.
//
// Column name is inferred from the value of yson tag,
// by the usual rules of the yson.Marshal.
//
// `yson:",key"` tag corresponds to sort_order=ascending scheme attribute.
//
// `ytgroup:""` tag is used to fill "group" scheme attribute.
//
// Type of the column corresponds to go type of the field.
//
// Mapping between go and YT types is defined as follows:
//
// Go types int, int16, int32, int64, uint, uint16, uint32, uint64, float32, float64 and bool are mapped to equivalent YT types.
// NOTE: int8 and uint8 types are not supported.
//
// Go type []byte is mapped to YT type string.
//
// Go types implementing encoding.BinaryMarshaler interface are mapper to YT type string.
//
// Go type string is mapped to YT type utf8 (except Dict key or value: they mapped to YT type string).
//
// Go types implementing encoding.TextMarshaler interface are mapped to YT type utf8.
//
// Go types list and slice are mapped to YT type List.
//
// Go type map is mapped to YT type Dict (string key or value are mapped to YT type string).
//
// Go type struct is mapped to YT type Struct.
func InferV3(value any) (Schema, error) {
	return infer(value)
}

func inferMap(value any) (s Schema, err error) {
	v, err := reflectValueOfType(value, reflect.Map)
	if err != nil {
		return
	}

	in := inferer{}

	iter := v.MapRange()
	for iter.Next() {
		k := iter.Key()
		if k.Kind() != reflect.String {
			err = xerrors.Errorf("can't infer schema from map with key of type %v, only string is supported", k.Type())
			return
		}

		name := k.Interface().(string)
		v := iter.Value()

		var typ reflect.Type
		if v.IsNil() {
			// In general nil value is an empty interface
			typ = v.Type()
		} else {
			typ = v.Elem().Type()
		}

		var column *Column
		column, err = in.ParseTag(name, typ, "", false)
		if err != nil {
			return
		}

		if column != nil {
			s.Columns = append(s.Columns, *column)
		}
	}

	sort.Slice(s.Columns, func(i, j int) bool {
		return s.Columns[i].Name < s.Columns[j].Name
	})

	return
}

// InferV3Map infers Schema from go map[string]any (using ComplexType[type_v3]).
//
// InferV3Map creates column for each key value pair.
// Column name inferred from key itself, and column type inferred from the type of value.
//
// To avoid ambiguity key type should always be string, while value type doesn't matter.
func InferV3Map(value any) (s Schema, err error) {
	return inferMap(value)
}

// MustInferV3Map infers Schema from go map[string]any (using ComplexType[type_v3]).
//
// MustInferV3Map panics on errors.
func MustInferV3Map(value any) (s Schema) {
	s, err := InferV3Map(value)
	if err != nil {
		panic(err)
	}
	return s
}

// MustInferV3 infers Schema from go struct (using ComplexType[type_v3]).
//
// MustInferV3 panics on errors.
func MustInferV3(value any) Schema {
	s, err := InferV3(value)
	if err != nil {
		panic(err)
	}
	return s
}
