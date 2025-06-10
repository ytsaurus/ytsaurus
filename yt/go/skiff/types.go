package skiff

import (
	"fmt"
	"math"
	"reflect"
	"slices"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

const (
	repeatedVariant8End = math.MaxUint8
)

var (
	emptyStructType  = reflect.TypeOf(struct{}{})
	genericMapType   = reflect.TypeOf(map[string]any{})
	ysonDurationType = reflect.TypeOf(yson.Duration(0))
)

type fieldOp struct {
	schema          *Schema
	tableSchemaType schema.ComplexType

	optional  bool // encoded as variant<nothing;T>
	unused    bool // not present in type
	omitempty bool // tagged with omitempty

	schemaName string

	index []int
}

func unpackOptional(schema *Schema) (s *Schema, optional bool) {
	if isOptionalSchema(schema) {
		return &schema.Children[1], true
	}
	return schema, false
}

func isOptionalSchema(schema *Schema) bool {
	return schema.Type == TypeVariant8 && len(schema.Children) == 2 &&
		slices.ContainsFunc(schema.Children, func(s Schema) bool { return s.Type == TypeNothing })
}

func checkTypes(typ reflect.Type, wt WireType) error {
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	switch wt {
	case TypeInt128, TypeInt256, TypeYSON32, TypeVariant8, TypeVariant16, TypeRepeatedVariant8, TypeRepeatedVariant16, TypeTuple:
		return nil

	case TypeInt8, TypeInt16, TypeInt32, TypeInt64:
		switch typ.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return nil
		}

	case TypeUint8, TypeUint16, TypeUint32, TypeUint64:
		switch typ.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
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
func newTranscoder(skiffSchema *Schema, tableSchema *schema.Schema, typ reflect.Type) ([]fieldOp, error) {
	var fields []fieldOp

	if skiffSchema.Type != TypeTuple {
		return nil, xerrors.Errorf("skiff: invalid root type %v", skiffSchema.Type)
	}

	if typ.Kind() != reflect.Struct {
		return nil, xerrors.Errorf("skiff: type %v is not supported at root", typ)
	}

	if tableSchema != nil && len(skiffSchema.Children) > len(tableSchema.Columns) {
		return nil, xerrors.Errorf(
			"skiff: skiff schema has more fields (%d) than table schema columns (%d)",
			len(skiffSchema.Children), len(tableSchema.Columns),
		)
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

	tableSchemaTypeByName := make(map[string]schema.ComplexType)
	if tableSchema != nil {
		for _, c := range tableSchema.Columns {
			tableSchemaTypeByName[c.Name] = c.ComplexType
		}
	}

	for _, topValue := range skiffSchema.Children {
		var op fieldOp
		op.schemaName = topValue.Name
		op.schema, op.optional = unpackOptional(&topValue)
		if tableSchema != nil {
			tableSchemaType, ok := tableSchemaTypeByName[topValue.Name]
			if !ok {
				return nil, xerrors.Errorf("skiff: field %q not found in table schema columns", topValue.Name)
			}
			if err := validateColumnSchema(&topValue, tableSchemaType); err != nil {
				return nil, xerrors.Errorf("skiff: column %q schema validation error: %w", topValue.Name, err)
			}
			if op.optional {
				optionalType, ok := tableSchemaType.(schema.Optional)
				if !ok {
					return nil, xerrors.Errorf(
						"skiff: field %q is optional in skiff schema, but not optional in table schema",
						topValue.Name,
					)
				}
				tableSchemaType = optionalType.Item
			}
			op.tableSchemaType = tableSchemaType
		}

		field, ok := fieldByName[topValue.Name]
		if ok {
			tag, _ := yson.ParseTag(field.Name, field.Tag)
			if err := checkTypes(field.Type, op.schema.Type); err != nil {
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

func validateColumnSchema(skiffSchema *Schema, tableSchemaType schema.ComplexType) error {
	if tableSchemaType == nil { // Weak schema mode.
		return nil
	}

	switch t := tableSchemaType.(type) {
	case schema.Type:
	case schema.Decimal:
		if t.Precision > 76 {
			return xerrors.Errorf("skiff: decimal precision %d exceeds maximum supported value 76", t.Precision)
		}
	case schema.Optional:
		if skiffSchema.Type != TypeVariant8 {
			return xerrors.Errorf("skiff: expected variant8 for optional type, got %q", skiffSchema.Type)
		}
		if len(skiffSchema.Children) != 2 {
			return xerrors.Errorf("skiff: expected 2 children in skiff schema for optional type, got %d", len(skiffSchema.Children))
		}
	case schema.List:
		if skiffSchema.Type != TypeRepeatedVariant8 {
			return xerrors.Errorf("skiff: expected repeated_variant8 for list type, got %q", skiffSchema.Type)
		}
		if len(skiffSchema.Children) != 1 {
			return xerrors.Errorf("skiff: expected 1 child in skiff schema for list type, got %d", len(skiffSchema.Children))
		}
	case schema.Struct:
		if skiffSchema.Type != TypeTuple {
			return xerrors.Errorf("skiff: expected tuple for struct type, got %q", skiffSchema.Type)
		}
		if len(skiffSchema.Children) != len(t.Members) {
			return xerrors.Errorf("skiff: struct field count mismatch: schema has %d, Skiff has %d", len(t.Members), len(skiffSchema.Children))
		}
	case schema.Tuple:
		if skiffSchema.Type != TypeTuple {
			return xerrors.Errorf("skiff: expected tuple for tuple type, got %q", skiffSchema.Type)
		}
		if len(skiffSchema.Children) != len(t.Elements) {
			return xerrors.Errorf("skiff: tuple element count mismatch: table schema has %d, skiff schema has %d", len(t.Elements), len(skiffSchema.Children))
		}
	case schema.Variant:
		if skiffSchema.Type != TypeVariant8 && skiffSchema.Type != TypeVariant16 {
			return xerrors.Errorf("skiff: expected variant8 or variant16 for variant type, got %q", skiffSchema.Type)
		}
	case schema.Dict:
		if skiffSchema.Type != TypeRepeatedVariant8 {
			return xerrors.Errorf("skiff: expected repeated_variant8 for dict type, got %q", skiffSchema.Type)
		}
		if len(skiffSchema.Children) != 1 {
			return xerrors.Errorf("skiff: expected 1 child in skiff schema for dict type, got %d", len(skiffSchema.Children))
		}
		itemSchema := &skiffSchema.Children[0]
		if itemSchema.Type != TypeTuple || len(itemSchema.Children) != 2 {
			return xerrors.Errorf("skiff: expected tuple of key/value in dict item, got type %q with %d children", itemSchema.Type, len(itemSchema.Children))
		}
	case schema.Tagged:
	default:
		return fmt.Errorf("unsupported YT complex type in table schema: %T", t)
	}

	return nil
}
