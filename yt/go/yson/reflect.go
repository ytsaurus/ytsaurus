package yson

import (
	"reflect"
	"strings"
	"sync"
)

type field struct {
	name  string
	index int
	field reflect.StructField

	omitempty bool
	attribute bool
	value     bool
}

func (f *field) parseTag(tag string) (skip bool) {
	if tag == "" {
		return false
	}

	tokens := strings.Split(tag, ",")
	if tokens[0] == "-" && len(tokens) == 1 {
		return true
	}

	i := 0
	if tag[0] != ',' {
		f.name = tokens[0]
		i = 1
	}

	for _, option := range tokens[i:] {
		switch option {
		case "attr":
			f.attribute = true
		case "omitempty":
			f.omitempty = true
		case "value":
			f.value = true
		}
	}

	return false
}

type structType struct {
	// fields decoded from attributes
	attributes       []field
	attributesByName map[string]field

	// fields decoded from map keys
	fields       []field
	fieldsByName map[string]field

	value *field // field decoded directly from the whole value
}

var typeCache sync.Map

func newStructType(t reflect.Type) *structType {
	structType := &structType{
		attributesByName: make(map[string]field),
		fieldsByName:     make(map[string]field),
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		structField := field{
			name:  f.Name,
			index: i,
			field: f,
		}

		tag, ok := f.Tag.Lookup("yson")
		if ok {
			skip := structField.parseTag(tag)
			if skip {
				continue
			}
		}

		switch {
		case structField.value:
			if structType.value == nil {
				structType.value = &structField
			}

		case structField.attribute:
			structType.attributes = append(structType.attributes, structField)
			structType.attributesByName[structField.name] = structField

		default:
			structType.fields = append(structType.fields, structField)
			structType.fieldsByName[structField.name] = structField
		}
	}

	return structType
}

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

func decodeReflect(d *Reader, v reflect.Value) error {
	if v.Kind() != reflect.Ptr {
		return &UnsupportedTypeError{v.Type()}
	}

	switch v.Elem().Type().Kind() {
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := decodeInt(d, v.Elem().Type().Bits())

		// TODO(prime@): check for overflow
		v.Elem().SetInt(i)
		return err

	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u, err := decodeUint(d, v.Elem().Type().Bits())

		// TODO(prime@): check for overflow
		v.Elem().SetUint(u)
		return err

	case reflect.String:
		s, err := decodeString(d)
		v.Elem().SetString(string(s))
		return err

	case reflect.Struct:
		return decodeReflectStruct(d, v.Elem())
	case reflect.Slice:
		return decodeReflectSlice(d, v)
	case reflect.Ptr:
		return decodeReflectPtr(d, v.Elem())
	case reflect.Map:
		return decodeReflectMap(d, v)
	default:
		return &UnsupportedTypeError{v.Type()}
	}
}

func decodeReflectSlice(d *Reader, v reflect.Value) error {
	e, err := d.Next(true)
	if err != nil {
		return err
	}

	if e == EventLiteral && d.currentType == TypeEntity {
		return nil
	}

	if e != EventBeginList {
		return &TypeError{UserType: v.Type(), YSONType: d.currentType}
	}

	slice := v.Elem()
	elementType := slice.Type().Elem()

	for i := 0; true; i++ {
		if ok, err := d.NextListItem(); err != nil {
			return err
		} else if !ok {
			break
		}

		slice = reflect.Append(slice, reflect.New(elementType).Elem())
		err = decodeAny(d, slice.Index(i).Addr().Interface())
		if err != nil {
			return err
		}
	}

	if e, err = d.Next(false); err != nil {
		return err
	}
	if e != EventEndList {
		panic("invalid decoder state")
	}

	v.Elem().Set(slice)
	return nil
}

func decodeReflectPtr(r *Reader, v reflect.Value) error {
	e, err := r.Next(false)
	if err != nil {
		return err
	}

	if e == EventLiteral && r.Type() == TypeEntity {
		return nil
	}

	r.Undo(e)
	elem := v.Type().Elem()
	v.Set(reflect.New(elem))
	return decodeAny(r, v.Interface())
}

func decodeReflectMap(r *Reader, v reflect.Value) error {
	if v.Type().Elem().Key().Kind() != reflect.String {
		return &UnsupportedTypeError{v.Type().Elem()}
	}

	e, err := r.Next(true)
	if err != nil {
		return err
	}

	if e == EventLiteral && r.currentType == TypeEntity {
		return nil
	}

	if e != EventBeginMap {
		return &TypeError{UserType: v.Type(), YSONType: r.currentType}
	}

	m := reflect.MakeMap(v.Elem().Type())
	v.Elem().Set(m)
	elementType := m.Type().Elem()

	for {
		ok, err := r.NextKey()
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		keyName := r.String()
		elem := reflect.New(elementType)
		if err = decodeAny(r, elem.Interface()); err != nil {
			return err
		}

		m.SetMapIndex(reflect.ValueOf(keyName), elem.Elem())
	}

	if e, err = r.Next(false); err != nil {
		return err
	}
	if e != EventEndMap {
		panic("invalid decoder state")
	}

	return nil
}

func decodeMapFragment(r *Reader, v reflect.Value, fields map[string]field) error {
	for {
		ok, err := r.NextKey()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}

		fieldName := r.String()
		structField, ok := fields[fieldName]
		if !ok {
			_, err = r.NextRawValue()
			if err != nil {
				return err
			}

			continue
		}

		field := v.Field(structField.index)
		if err = decodeAny(r, field.Addr().Interface()); err != nil {
			if typeError, ok := err.(*TypeError); ok {
				return &TypeError{
					UserType: typeError.UserType,
					YSONType: typeError.YSONType,
					Struct:   v.Type().String(),
					Field:    fieldName,
				}
			}

			return err
		}
	}
}

func decodeReflectStruct(r *Reader, v reflect.Value) error {
	structType := getStructType(v)

	var e Event
	var err error
	if structType.attributes != nil {
		e, err = r.Next(false)
		if err != nil {
			return err
		}

		if e == EventBeginAttrs {
			if err = decodeMapFragment(r, v, structType.attributesByName); err != nil {
				return err
			}

			e, err = r.Next(false)
			if err != nil {
				return err
			}

			if e != EventEndAttrs {
				panic("invalid decoder state")
			}
		} else {
			r.Undo(e)
		}
	} else {
		e, err = r.Next(true)
		if err != nil {
			return err
		}

		r.Undo(e)
	}

	if structType.value != nil {
		return decodeAny(r, v.Field(structType.value.index).Addr().Interface())
	}

	e, err = r.Next(false)
	if err != nil {
		return err
	}

	if e != EventBeginMap {
		return &TypeError{UserType: v.Type(), YSONType: r.currentType}
	}

	if err = decodeMapFragment(r, v, structType.fieldsByName); err != nil {
		return err
	}

	if e, err = r.Next(false); err != nil {
		return err
	}
	if e != EventEndMap {
		panic("invalid decoder state")
	}

	return nil
}
