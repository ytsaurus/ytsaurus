package schema

import (
	"encoding"
	"reflect"

	"golang.org/x/xerrors"
)

var (
	typeOfBytes           = reflect.TypeOf([]byte(nil))
	typeOfBinaryMarshaler = reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem()
	typeOfTextMarshaler   = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
)

func ytTypeFromV3(ytType ComplexType) (ComplexType, error) {
	switch t := ytType.(type) {
	case Type:
		switch t {
		case typeBool:
			return TypeBoolean, nil
		case typeYSON:
			return TypeAny, nil
		default:
			return t, nil
		}
	case Optional:
		return ytTypeFromV3(t.Item)
	case Struct, List, Dict:
		return TypeAny, nil
	default:
		return ytType, nil
	}
}

const (
	tabletIndexTag = "$tablet_index"
	rowIndexTag    = "$row_index"
)

func convertColumnFromV3(c Column) (Column, error) {
	if c.ComplexType == nil {
		return c, xerrors.Errorf("ComplexType is not set in column \"%+v\"", c)
	}
	switch t := c.ComplexType.(type) {
	case Optional, Struct, List, Dict:
		c.Required = false
	case Type:
		if t == typeYSON {
			c.Required = false
		} else {
			c.Required = true
		}
	default:
		c.Required = true
	}

	ytTyp, err := ytTypeFromV3(c.ComplexType)
	if err != nil {
		return c, err
	}

	c.Type = ytTyp.(Type)
	c.ComplexType = nil

	return c, nil
}

func convertSchemaFromV3(s Schema) (Schema, error) {
	columns := make([]Column, 0, len(s.Columns))
	for _, c := range s.Columns {
		converted, err := convertColumnFromV3(c)
		if err != nil {
			return Schema{}, err
		}

		columns = append(columns, converted)
	}
	return Schema{Columns: columns, UniqueKeys: s.UniqueKeys, Strict: s.Strict}, nil
}

// Infer infers Schema from go struct.
//
// By default Infer creates column for each struct field.
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
// Go type string is mapped to YT type utf8.
//
// Go types implementing encoding.TextMarshaler interface are mapped to YT type utf8.
func Infer(value any) (Schema, error) {
	s, err := infer(value)
	if err != nil {
		return Schema{}, err
	}

	return convertSchemaFromV3(s)
}

// InferMap infers Schema from go map[string]any.
//
// InferMap creates column for each key value pair.
// Column name inferred from key itself, and column type inferred from the type of value.
//
// To avoid ambiguity key type should always be string, while value type doesn't matter.
func InferMap(value any) (Schema, error) {
	s, err := inferMap(value)
	if err != nil {
		return Schema{}, err
	}

	return convertSchemaFromV3(s)
}

// MustInferMap infers Schema from go map[string]any.
//
// MustInferMap panics on errors.
func MustInferMap(value any) (s Schema) {
	s, err := InferMap(value)
	if err != nil {
		panic(err)
	}
	return s
}

// reflectValueOfType creates value reflection of requested type for schema inferring.
func reflectValueOfType(value any, k reflect.Kind) (v reflect.Value, err error) {
	// Check for nil, reflect of nil value causes panic.
	if value == nil {
		err = xerrors.New("can't infer schema from nil value")
		return
	}

	v = reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != k {
		err = xerrors.Errorf("can't infer schema from value of type %v", v.Type())
	}
	return
}

// MustInfer infers Schema from go struct.
//
// MustInfer panics on errors.
func MustInfer(value any) Schema {
	s, err := Infer(value)
	if err != nil {
		panic(err)
	}
	return s
}

type fieldParser[T any] func(field reflect.StructField, forceOptional bool) (T, error)

func traverseStruct[T any](typ reflect.Type, p fieldParser[*T]) ([]T, error) {
	return traverseNestedStruct(typ, false, p)
}

func traverseNestedStruct[T any](typ reflect.Type, forceOptional bool, p fieldParser[*T]) ([]T, error) {
	if typ.Kind() == reflect.Ptr {
		forceOptional = true
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct {
		return nil, xerrors.Errorf("can't infer schema from type %q", typ)
	}

	results := make([]T, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		if field.Anonymous {
			if field.Type.Kind() == reflect.Interface {
				res, err := p(field, forceOptional)
				if err != nil {
					return nil, err
				}

				results = append(results, *res)
			} else {
				embedded, err := traverseNestedStruct(field.Type, forceOptional, p)
				if err != nil {
					return nil, err
				}

				results = append(results, embedded...)
			}

			continue
		} else if field.PkgPath != "" {
			continue
		}

		res, err := p(field, forceOptional)
		if err != nil {
			return nil, err
		}

		if res != nil {
			results = append(results, *res)
		}
	}

	return results, nil
}
