// Package skiff implements efficient serialization format, optimized for YT.
package skiff

import (
	"fmt"

	"a.yandex-team.ru/yt/go/yson"
	"golang.org/x/xerrors"
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

func (t WireType) MarshalYSON(w *yson.Writer) error {
	switch t {
	case TypeNothing:
		w.String("nothing")
	case TypeBoolean:
		w.String("boolean")
	case TypeInt64:
		w.String("int64")
	case TypeUint64:
		w.String("uint64")
	case TypeDouble:
		w.String("double")
	case TypeString32:
		w.String("string32")
	case TypeYSON32:
		w.String("yson32")
	case TypeVariant8:
		w.String("variant8")
	case TypeVariant16:
		w.String("variant16")
	case TypeRepeatedVariant16:
		w.String("repeated_variant16")
	case TypeTuple:
		w.String("tuple")
	default:
		return xerrors.Errorf("invalid skiff type %d", t)
	}

	return nil
}

// Schema describes wire format for the single value.
type Schema struct {
	Type     WireType `yson:"type"`
	Name     string   `yson:"name,omitempty"`
	Children []Schema `yson:"children,omitempty"`
}

// Format describes skiff schemas for the stream.
type Format struct {
	// name is always equal to string "skiff"
	Name string `yson:",value"`

	// either skiff.Schema of reference into registry
	TableSchemas interface{} `yson:"table_skiff_schemas,attr"`

	// schemas shared between multiple tables
	SchemaRegistry map[string]Schema `yson:"skiff_schema_registry,attr"`
}
