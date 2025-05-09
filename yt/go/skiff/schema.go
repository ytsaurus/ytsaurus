// Package skiff implements efficient serialization format, optimized for YT.
package skiff

import (
	"encoding/gob"
	"fmt"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

type WireType int

const (
	TypeNothing WireType = iota
	TypeBoolean
	TypeInt8
	TypeInt16
	TypeInt32
	TypeInt64
	TypeUint8
	TypeUint16
	TypeUint32
	TypeUint64
	TypeDouble
	TypeString32
	TypeYSON32

	TypeVariant8
	TypeVariant16
	TypeRepeatedVariant16
	TypeTuple
)

// FromYTType returns skiff wire type used for transferring YT type.
func FromYTType(typ schema.Type) WireType {
	switch typ {
	case schema.TypeBoolean:
		return TypeBoolean
	case schema.TypeInt8:
		return TypeInt8
	case schema.TypeInt16:
		return TypeInt16
	case schema.TypeInt32:
		return TypeInt32
	case schema.TypeInt64:
		return TypeInt64
	case schema.TypeUint8:
		return TypeUint8
	case schema.TypeUint16:
		return TypeUint16
	case schema.TypeUint32:
		return TypeUint32
	case schema.TypeUint64:
		return TypeUint64
	case schema.TypeFloat32, schema.TypeFloat64:
		return TypeDouble
	case schema.TypeBytes, schema.TypeString:
		return TypeString32
	case schema.TypeAny:
		return TypeYSON32
	case schema.TypeDate:
		return TypeUint64
	case schema.TypeDatetime:
		return TypeUint64
	case schema.TypeTimestamp:
		return TypeUint64
	case schema.TypeInterval:
		return TypeInt64
	default:
		panic(fmt.Sprintf("invalid YT type %s", typ))
	}
}

func (t WireType) IsSimple() bool {
	switch t {
	case TypeBoolean, TypeInt8, TypeInt16, TypeInt32, TypeInt64,
		TypeUint8, TypeUint16, TypeUint32, TypeUint64,
		TypeDouble, TypeString32, TypeYSON32:
		return true
	default:
		return false
	}
}

func (t *WireType) UnmarshalYSON(data []byte) error {
	var s string
	if err := yson.Unmarshal(data, &s); err != nil {
		return err
	}

	switch s {
	case "nothing":
		*t = TypeNothing
	case "boolean":
		*t = TypeBoolean
	case "int8":
		*t = TypeInt8
	case "int16":
		*t = TypeInt16
	case "int32":
		*t = TypeInt32
	case "int64":
		*t = TypeInt64
	case "uint8":
		*t = TypeUint8
	case "uint16":
		*t = TypeUint16
	case "uint32":
		*t = TypeUint32
	case "uint64":
		*t = TypeUint64
	case "double":
		*t = TypeDouble
	case "string32":
		*t = TypeString32
	case "yson32":
		*t = TypeYSON32
	case "variant8":
		*t = TypeVariant8
	case "variant16":
		*t = TypeVariant16
	case "repeated_variant16":
		*t = TypeRepeatedVariant16
	case "tuple":
		*t = TypeTuple
	default:
		return fmt.Errorf("invalid skiff type %q", s)
	}

	return nil
}

func (t WireType) String() string {
	switch t {
	case TypeNothing:
		return "nothing"
	case TypeBoolean:
		return "boolean"
	case TypeInt8:
		return "int8"
	case TypeInt16:
		return "int16"
	case TypeInt32:
		return "int32"
	case TypeInt64:
		return "int64"
	case TypeUint8:
		return "uint8"
	case TypeUint16:
		return "uint16"
	case TypeUint32:
		return "uint32"
	case TypeUint64:
		return "uint64"
	case TypeDouble:
		return "double"
	case TypeString32:
		return "string32"
	case TypeYSON32:
		return "yson32"
	case TypeVariant8:
		return "variant8"
	case TypeVariant16:
		return "variant16"
	case TypeRepeatedVariant16:
		return "repeated_variant16"
	case TypeTuple:
		return "tuple"
	default:
		return "invalid"
	}
}

func (t WireType) MarshalYSON(w *yson.Writer) error {
	w.String(t.String())
	return nil
}

// Schema describes wire format for the single value.
type Schema struct {
	Type     WireType `yson:"wire_type"`
	Name     string   `yson:"name,omitempty"`
	Children []Schema `yson:"children,omitempty"`
}

func (c Schema) IsSystem() bool {
	for _, col := range systemColumns {
		if col.Name == c.Name && col.Type == c.Type {
			return true
		}
	}
	return false
}

func OptionalColumn(name string, typ WireType) Schema {
	return Schema{Type: TypeVariant8, Name: name, Children: []Schema{{Type: TypeNothing}, {Type: typ}}}
}

func init() {
	gob.Register(&Schema{})
}

type schemaOptions struct {
	enableKeySwitch  bool
	enableRowIndex   bool
	enableRangeIndex bool
}

type schemaOption func(s *schemaOptions)

func withKeySwitch() schemaOption {
	return func(s *schemaOptions) {
		s.enableKeySwitch = true
	}
}

func withRowIndex() schemaOption {
	return func(s *schemaOptions) {
		s.enableRowIndex = true
	}
}

func withRangeIndex() schemaOption {
	return func(s *schemaOptions) {
		s.enableRangeIndex = true
	}
}

// FromTableSchema creates skiff schema from table schema.
func FromTableSchema(schema schema.Schema, opts ...schemaOption) Schema {
	var schemaOptions schemaOptions
	for _, opt := range opts {
		opt(&schemaOptions)
	}

	var columns []Schema
	if schemaOptions.enableKeySwitch {
		columns = append(columns, systemColumns["$key_switch"])
	}
	if schemaOptions.enableRowIndex {
		columns = append(columns, systemColumns["$row_index"])
	}
	if schemaOptions.enableRangeIndex {
		columns = append(columns, systemColumns["$range_index"])
	}

	for _, row := range schema.Columns {
		if row.Required {
			columns = append(columns, Schema{Name: row.Name, Type: FromYTType(row.Type)})
		} else {
			columns = append(columns, OptionalColumn(row.Name, FromYTType(row.Type)))
		}
	}

	return Schema{
		Type:     TypeTuple,
		Children: columns,
	}
}

var systemColumns = map[string]Schema{
	"$key_switch":  {Type: TypeBoolean, Name: "$key_switch"},
	"$row_index":   OptionalColumn("$row_index", TypeInt64),
	"$range_index": OptionalColumn("$range_index", TypeInt64),
}

// Format describes skiff schemas for the stream.
type Format struct {
	// name is always equal to string "skiff"
	Name string `yson:",value"`

	// either skiff.Schema of reference into registry
	TableSchemas []any `yson:"table_skiff_schemas,attr"`

	// schemas shared between multiple tables
	SchemaRegistry map[string]*Schema `yson:"skiff_schema_registry,attr"`
}

// InferFormat infers skiff.Format from go struct.
//
// This function is a combination of schema.Infer(value) and skiff.FromTableSchema(inferredSchema).
func InferFormat(value any) (f Format, err error) {
	tableSchema, err := schema.Infer(value)
	if err != nil {
		return
	}
	skiffSchema := FromTableSchema(tableSchema)
	f = Format{
		Name:         "skiff",
		TableSchemas: []any{&skiffSchema},
	}
	return
}

// MustInferFormat infers skiff.Format from go struct.
//
// MustInferFormat panics on errors.
func MustInferFormat(value any) Format {
	s, err := InferFormat(value)
	if err != nil {
		panic(err)
	}
	return s
}
