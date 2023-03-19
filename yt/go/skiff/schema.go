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
	TypeInt64
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
	case schema.TypeInt8, schema.TypeInt16, schema.TypeInt32, schema.TypeInt64:
		return TypeInt64
	case schema.TypeUint8, schema.TypeUint16, schema.TypeUint32, schema.TypeUint64:
		return TypeUint64
	case schema.TypeFloat64:
		return TypeDouble
	case schema.TypeBytes, schema.TypeString:
		return TypeString32
	case schema.TypeAny:
		return TypeYSON32
	default:
		panic(fmt.Sprintf("invalid YT type %s", typ))
	}
}

func (t WireType) IsSimple() bool {
	switch t {
	case TypeBoolean, TypeInt64, TypeUint64, TypeDouble, TypeString32, TypeYSON32:
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
	case "int64":
		*t = TypeInt64
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
	case TypeInt64:
		return "int64"
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
	for _, col := range systemPrefix {
		if col.Name == c.Name && col.Type == c.Type {
			return true
		}
	}
	return false
}

func init() {
	gob.Register(&Schema{})
}

// FromTableSchema creates skiff schema from table schema.
func FromTableSchema(schema schema.Schema) Schema {
	var columns []Schema
	columns = append(columns, systemPrefix...)

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

func OptionalColumn(name string, typ WireType) Schema {
	return Schema{Type: TypeVariant8, Name: name, Children: []Schema{{Type: TypeNothing}, {Type: typ}}}
}

var systemPrefix = []Schema{
	{Type: TypeBoolean, Name: "$key_switch"},
	OptionalColumn("$row_index", TypeInt64),
	OptionalColumn("$range_index", TypeInt64),
}

// Format describes skiff schemas for the stream.
type Format struct {
	// name is always equal to string "skiff"
	Name string `yson:",value"`

	// either skiff.Schema of reference into registry
	TableSchemas []interface{} `yson:"table_skiff_schemas,attr"`

	// schemas shared between multiple tables
	SchemaRegistry map[string]*Schema `yson:"skiff_schema_registry,attr"`
}
