package skiff

import (
	"reflect"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/yson"
)

var (
	emptyStructType = reflect.TypeOf(struct{}{})
	genericMapType  = reflect.TypeOf(map[string]any{})
)

type fieldOp struct {
	wt WireType

	optional  bool // encoded as variant<nothing;T>
	unused    bool // not present in type
	omitempty bool // tagged with omitempty

	schemaName string

	index []int
}

func unpackSimpleVariant(schema *Schema) (wt WireType, optional bool, err error) {
	if schema.Type == TypeVariant8 {
		if len(schema.Children) != 2 || schema.Children[0].Type != TypeNothing || !schema.Children[1].Type.IsSimple() {
			err = xerrors.Errorf("unsupported skiff type %v", schema)
			return
		}

		wt = schema.Children[1].Type
		optional = true
		return
	} else {
		if len(schema.Children) != 0 || !schema.Type.IsSimple() {
			err = xerrors.Errorf("unsupported skiff type %v", schema)
			return
		}

		wt = schema.Type
		optional = false
		return
	}
}

func checkTypes(typ reflect.Type, wt WireType) error {
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	switch wt {
	case TypeYSON32:
		return nil

	case TypeInt64:
		switch typ.Kind() {
		case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
			return nil
		}

	case TypeUint64:
		switch typ.Kind() {
		case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return nil
		}

	case TypeDouble:
		switch typ.Kind() {
		case reflect.Float32, reflect.Float64:
			return nil
		}

	case TypeBoolean:
		if typ.Kind() == reflect.Bool {
			return nil
		}

	case TypeString32:
		if typ.Kind() == reflect.String {
			return nil
		}

		if typ == reflect.TypeOf([]byte{}) {
			return nil
		}
	}

	return xerrors.Errorf("type %v is not compatible with wire type %s", typ, wt)
}

// skiff encoding and decoding is always driven by the schema. newTranscoder generates
// table describing necessary steps, that must be taken in order to decodeStruct or encode given struct type.
func newTranscoder(schema *Schema, typ reflect.Type) ([]fieldOp, error) {
	var fields []fieldOp

	if schema.Type != TypeTuple {
		return nil, xerrors.Errorf("skiff: invalid root type %v", schema.Type)
	}

	if typ.Kind() != reflect.Struct {
		return nil, xerrors.Errorf("skiff: type %v is not supported at root", typ)
	}

	fieldByName := map[string]reflect.StructField{}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		tag, skip := yson.ParseTag(field.Name, field.Tag)
		if skip {
			continue
		}

		fieldByName[tag.Name] = field
	}

	var err error
	for _, topValue := range schema.Children {
		var op fieldOp
		op.schemaName = topValue.Name
		op.wt, op.optional, err = unpackSimpleVariant(&topValue)
		if err != nil {
			return nil, xerrors.Errorf("skiff: invalid schema for column %q: %w", topValue.Name, err)
		}

		field, ok := fieldByName[topValue.Name]
		if ok {
			tag, _ := yson.ParseTag(field.Name, field.Tag)
			if err := checkTypes(field.Type, op.wt); err != nil {
				return nil, xerrors.Errorf("skiff: invalid schema for column %q: %w", topValue.Name, err)
			}

			op.index = field.Index
			op.omitempty = tag.Omitempty
		} else {
			op.unused = true
		}

		fields = append(fields, op)
	}

	return fields, nil
}
