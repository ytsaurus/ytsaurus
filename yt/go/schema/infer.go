package schema

import (
	"encoding"
	"reflect"
	"strings"

	"golang.org/x/xerrors"
)

var (
	typeOfBytes           = reflect.TypeOf([]byte(nil))
	typeOfBinaryMarshaler = reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem()
	typeOfTextMarshaler   = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
)

func ytTypeFor(typ reflect.Type) (ytTyp Type, err error) {
	if typ == typeOfBytes {
		return TypeBytes, nil
	}

	if typ.Implements(typeOfBinaryMarshaler) || (typ.Kind() != reflect.Ptr && reflect.PtrTo(typ).Implements(typeOfBinaryMarshaler)) {
		return TypeBytes, nil
	}

	if typ.Implements(typeOfTextMarshaler) || (typ.Kind() != reflect.Ptr && reflect.PtrTo(typ).Implements(typeOfTextMarshaler)) {
		return TypeString, nil
	}

	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
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
		return TypeBoolean, nil
	case reflect.Float64:
		return TypeFloat64, nil

	case reflect.Struct, reflect.Slice, reflect.Map, reflect.Interface:
		return TypeAny, nil
	}

	return "", xerrors.Errorf("type %v has no associated YT type", typ)
}

func parseTag(fieldName string, typ reflect.Type, tag string) (c *Column, err error) {
	c = &Column{Required: true}

	c.Name = fieldName
	if tag != "" {
		parts := strings.Split(tag, ",")
		if parts[0] == "-" {
			return nil, nil
		}

		if parts[0] != "" {
			c.Name = parts[0]
		}

		for _, option := range parts[1:] {
			switch option {
			case "key":
				c.SortOrder = SortAscending
			case "omitempty":
				c.Required = false
			}
		}
	}

	c.Type, err = ytTypeFor(typ)
	if typ.Kind() == reflect.Ptr || c.Type == TypeAny {
		c.Required = false
	}

	return
}

// Infer infers Schema from go struct.
//
// By default Infer creates column for each struct field.
// Type of the column corresponds to go type of the field.
//
// Mapping between go and YT types is defined as follows:
//
// Go types int, int16, int32, int64, uint, uint16, uint32, uint64, bool and float64 are mapped to equivalent YT types.
// NOTE: int8 and uint8 types are not supported.
//
// Go type []byte is mapped to YT type string.
//
// Go types implementing encoding.BinaryMarshaler interface are mapper to YT type string.
//
// Go type string is mapped to YT type utf8.
//
// Go types implementing encoding.TextMarshaler interface are mapped to YT type utf8.
//
func Infer(value interface{}) (s Schema, err error) {
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		err = xerrors.Errorf("can't infer schema from value of type %v", v.Type())
		return
	}

	typ := v.Type()
	s = Schema{}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		var column *Column
		column, err = parseTag(field.Name, field.Type, field.Tag.Get("yson"))
		if err != nil {
			return
		}

		if column != nil {
			s.Columns = append(s.Columns, *column)
		}
	}

	return
}

// MustInfer infers Schema from go struct.
//
// MustInfer panics on errors.
func MustInfer(value interface{}) Schema {
	s, err := Infer(value)
	if err != nil {
		panic(err)
	}
	return s
}
