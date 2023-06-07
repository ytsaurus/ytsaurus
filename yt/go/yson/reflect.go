package yson

import (
	"encoding"
	"fmt"
	"reflect"
	"sync"

	"go.ytsaurus.tech/library/go/core/xerrors"
)

type field struct {
	name  string
	index []int

	omitempty bool
	attribute bool
	value     bool
	attrs     bool
}

type structType struct {
	// fields decoded from attributes
	attributes       []*field
	attributesByName map[string]*field

	// fields decoded from map keys
	fields       []*field
	fieldsByName map[string]*field

	value *field // field decoded directly from the whole value
	attrs *field // field decoded directly from attributes map
}

var typeCache sync.Map

func newStructType(t reflect.Type) *structType {
	structType := &structType{
		attributesByName: make(map[string]*field),
		fieldsByName:     make(map[string]*field),
	}

	var nameConflict field

	var attributeOrder, fieldOrder []string

	var visitFields func(fieldStack []int, t reflect.Type)
	visitFields = func(fieldStack []int, t reflect.Type) {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)

			tag, skip := ParseTag(f.Name, f.Tag)
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

			// Add field, resolving name conflict according to go embedding rules.
			addField := func(order *[]string, fieldMap map[string]*field, f *field) {
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

			switch {
			case structField.value:
				if structType.value == nil {
					structType.value = &structField
				} else {
					structType.value = &nameConflict
				}

			case structField.attrs:
				// ",attrs" field must have type map[K]V
				if f.Type.Kind() != reflect.Map {
					break
				}

				if structType.attrs == nil {
					structType.attrs = &structField
				} else {
					structType.attrs = &nameConflict
				}

			case structField.attribute:
				addField(&attributeOrder, structType.attributesByName, &structField)

			default:
				addField(&fieldOrder, structType.fieldsByName, &structField)
			}
		}
	}

	visitFields(nil, t)

	if structType.value == &nameConflict {
		structType.value = nil
	}
	if structType.attrs == &nameConflict {
		structType.attrs = nil
	}

	filterConflicts := func(order []string, fieldMap map[string]*field) (fields []*field) {
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

	structType.fields = filterConflicts(fieldOrder, structType.fieldsByName)
	structType.attributes = filterConflicts(attributeOrder, structType.attributesByName)

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

var (
	ifaceDecoders sync.Map
)

type DecoderFn func(*Reader, any) error

func RegisterInterfaceDecoder(iface any, decoder DecoderFn) {
	_, loaded := ifaceDecoders.LoadOrStore(reflect.TypeOf(iface), decoder)
	if loaded {
		panic(fmt.Sprintf("decode for type %s is already registered", iface))
	}
}

func decodeReflectInterface(d *Reader, v reflect.Value) error {
	decoder, ok := ifaceDecoders.Load(v.Type())
	if !ok {
		return &UnsupportedTypeError{v.Type()}
	}

	return decoder.(DecoderFn)(d, v.Interface())
}

func decodeReflect(d *Reader, v reflect.Value, opts *DecoderOptions) error {
	if v.Kind() != reflect.Ptr {
		return &UnsupportedTypeError{v.Type()}
	}

	switch v.Elem().Type().Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := decodeInt(d, v.Elem().Type().Bits())
		v.Elem().SetInt(i)
		return err

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u, err := decodeUint(d, v.Elem().Type().Bits())
		v.Elem().SetUint(u)
		return err

	case reflect.String:
		s, err := decodeString(d)
		v.Elem().SetString(string(s))
		return err

	case reflect.Struct:
		return decodeReflectStruct(d, v.Elem(), opts)
	case reflect.Slice:
		return decodeReflectSlice(d, v, opts)
	case reflect.Array:
		return decodeReflectArray(d, v, opts)
	case reflect.Ptr:
		return decodeReflectPtr(d, v.Elem(), opts)
	case reflect.Map:
		return decodeReflectMap(d, v, false, opts)
	case reflect.Interface:
		return decodeReflectInterface(d, v)

	default:
		return &UnsupportedTypeError{v.Type()}
	}
}

func decodeReflectSlice(d *Reader, v reflect.Value, opts *DecoderOptions) error {
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

	slice.Set(reflect.Zero(slice.Type()))
	for i := 0; true; i++ {
		if ok, err := d.NextListItem(); err != nil {
			return err
		} else if !ok {
			break
		}

		slice = reflect.Append(slice, reflect.New(elementType).Elem())
		err = decodeAny(d, slice.Index(i).Addr().Interface(), opts)
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

func decodeReflectArray(d *Reader, v reflect.Value, opts *DecoderOptions) error {
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

	array := v.Elem()
	for i := 0; true; i++ {
		if ok, err := d.NextListItem(); err != nil {
			return err
		} else if !ok {
			break
		}

		if i < array.Len() {
			err = decodeAny(d, array.Index(i).Addr().Interface(), opts)
			if err != nil {
				return err
			}
		} else {
			_, err = d.NextRawValue()
			if err != nil {
				return err
			}
		}
	}

	if e, err = d.Next(false); err != nil {
		return err
	}
	if e != EventEndList {
		panic("invalid decoder state")
	}

	return nil
}

func decodeReflectPtr(r *Reader, v reflect.Value, opts *DecoderOptions) error {
	e, err := r.Next(false)
	if err != nil {
		return err
	}

	if e == EventLiteral && r.Type() == TypeEntity {
		v.Set(reflect.Zero(v.Type()))
		return nil
	}

	r.Undo(e)
	elem := v.Type().Elem()
	v.Set(reflect.New(elem))
	return decodeAny(r, v.Interface(), opts)
}

var (
	textUnmarshalerType   = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
	binaryUnmarshalerType = reflect.TypeOf((*encoding.BinaryUnmarshaler)(nil)).Elem()
)

func decodeReflectMap(r *Reader, v reflect.Value, attrs bool, opts *DecoderOptions) error {
	kt := v.Type().Elem().Key()

	switch kt.Kind() {
	case reflect.String:
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if opts == nil || !opts.SupportYPAPIMaps {
			return &UnsupportedTypeError{v.Type().Elem()}
		}
		return decodeReflectYPAPIMap(r, v, opts)
	default:
		switch {
		case reflect.PtrTo(kt).Implements(textUnmarshalerType),
			reflect.PtrTo(kt).Implements(binaryUnmarshalerType):
		default:
			return &UnsupportedTypeError{v.Type().Elem()}
		}
	}

	e, err := r.Next(!attrs)
	if err != nil {
		return err
	}

	if !attrs {
		if e == EventLiteral && r.currentType == TypeEntity {
			return nil
		}

		if e != EventBeginMap {
			return &TypeError{UserType: v.Type(), YSONType: r.currentType}
		}
	} else {
		if e != EventBeginAttrs {
			return &TypeError{UserType: v.Type(), YSONType: r.currentType}
		}
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

		var kv reflect.Value
		switch {
		case kt.Kind() == reflect.String:
			kv = reflect.ValueOf(r.String()).Convert(kt)
		case reflect.PtrTo(kt).Implements(textUnmarshalerType):
			kv = reflect.New(kt)
			err := kv.Interface().(encoding.TextUnmarshaler).UnmarshalText(r.currentString)
			if err != nil {
				return err
			}
			kv = kv.Elem()
		case reflect.PtrTo(kt).Implements(binaryUnmarshalerType):
			kv = reflect.New(kt)
			err := kv.Interface().(encoding.BinaryUnmarshaler).UnmarshalBinary(r.currentString)
			if err != nil {
				return err
			}
			kv = kv.Elem()
		default:
			panic("yson: Unexpected key type")
		}

		elem := reflect.New(elementType)
		if err = decodeAny(r, elem.Interface(), opts); err != nil {
			return err
		}

		m.SetMapIndex(kv, elem.Elem())
	}

	if e, err = r.Next(false); err != nil {
		return err
	}

	if !attrs {
		if e != EventEndMap {
			panic("invalid decoder state")
		}
	} else {
		if e != EventEndAttrs {
			panic("invalid decoder state")
		}
	}

	return nil
}

var ypAPIMapTypes sync.Map

func getYPAPIMapType(v reflect.Value) reflect.Type {
	t := v.Type()

	cachedType, ok := ypAPIMapTypes.Load(t)
	if ok {
		return cachedType.(reflect.Type)
	}

	kt := t.Elem().Key()
	vt := t.Elem().Elem()

	typ := reflect.StructOf([]reflect.StructField{
		{
			Name: "Key",
			Type: kt,
			Tag:  `yson:"key"`,
		},
		{
			Name: "Value",
			Type: vt,
			Tag:  `yson:"value"`,
		},
	})

	ypAPIMapTypes.Store(t, typ)
	return typ
}

func decodeReflectYPAPIMap(r *Reader, v reflect.Value, opts *DecoderOptions) error {
	m := reflect.MakeMap(v.Elem().Type())
	v.Elem().Set(m)

	kvType := getYPAPIMapType(v)

	slice := reflect.New(reflect.SliceOf(kvType))
	if err := decodeReflectSlice(r, slice, opts); err != nil {
		return err
	}

	for i := 0; i < slice.Elem().Len(); i++ {
		elem := slice.Elem().Index(i)
		m.SetMapIndex(elem.FieldByIndex([]int{0}), elem.FieldByIndex([]int{1}))
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
							err := xerrors.Errorf("yson: cannot set embedded pointer to unexported field: %v", v.Type())
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

func decodeMapFragment(r *Reader, v reflect.Value, fields map[string]*field, opts *DecoderOptions) error {
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

		field, _, err := fieldByIndex(v, structField.index, true)
		if err != nil {
			return err
		}
		if err = decodeAny(r, field.Addr().Interface(), opts); err != nil {
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

func decodeReflectStruct(r *Reader, v reflect.Value, opts *DecoderOptions) error {
	structType := getStructType(v)

	var e Event
	var err error
	if structType.attributes != nil {
		e, err = r.Next(false)
		if err != nil {
			return err
		}

		if e == EventBeginAttrs {
			if err = decodeMapFragment(r, v, structType.attributesByName, opts); err != nil {
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
	} else if structType.attrs != nil {
		e, err = r.Next(false)
		if err != nil {
			return err
		}
		r.Undo(e)

		if e == EventBeginAttrs {
			if err := decodeReflectMap(r, v.FieldByIndex(structType.attrs.index).Addr(), true, opts); err != nil {
				return err
			}
		}
	} else {
		e, err = r.Next(true)
		if err != nil {
			return err
		}

		r.Undo(e)
	}

	if structType.value != nil {
		return decodeAny(r, v.FieldByIndex(structType.value.index).Addr().Interface(), opts)
	}

	e, err = r.Next(false)
	if err != nil {
		return err
	}

	if e == EventLiteral && r.Type() == TypeEntity {
		return nil
	}

	if e != EventBeginMap {
		return &TypeError{UserType: v.Type(), YSONType: r.currentType}
	}

	if err = decodeMapFragment(r, v, structType.fieldsByName, opts); err != nil {
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
