package yson

import (
	"reflect"

	"golang.org/x/xerrors"
)

// InferAttrs infers attribute names from struct.
//
// Function returns attribute names of struct fields
// including exported fields of yson-untagged anonymous fields
func InferAttrs(v any) ([]string, error) {
	if v == nil {
		return nil, xerrors.New("can't infer attrs from nil value")
	}

	uv := reflect.ValueOf(v)
	if uv.Kind() == reflect.Ptr {
		uv = uv.Elem()
	}

	if uv.Kind() != reflect.Struct {
		return nil, xerrors.Errorf("can't infer attrs from value of type %v", uv.Type())
	}

	var attrs []string

	var inferFields func(typ reflect.Type)
	inferFields = func(typ reflect.Type) {
		if typ.Kind() == reflect.Ptr {
			typ = typ.Elem()
		}

		if typ.Kind() != reflect.Struct {
			return
		}

		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)

			_, tagged := field.Tag.Lookup("yson")

			decodedTag, skip := ParseTag(field.Name, field.Tag)
			if skip {
				continue
			}
			attr := decodedTag.Name

			if field.Anonymous && !tagged {
				inferFields(field.Type)
				continue
			}

			if decodedTag.Attr {
				attrs = append(attrs, attr)
			}
		}
	}

	inferFields(uv.Type())

	return attrs, nil
}

// MustInferAttrs is like InferAttrs but panics in case of an error.
func MustInferAttrs(v any) []string {
	attrs, err := InferAttrs(v)
	if err != nil {
		panic(err)
	}
	return attrs
}
