// package schema defines schema of YT tables.
package schema

type Type string

func (t Type) String() string {
	return string(t)
}

const (
	TypeInt64  Type = "int64"
	TypeInt32  Type = "int32"
	TypeInt16  Type = "int16"
	TypeInt8   Type = "int8"
	TypeUint64 Type = "uint64"
	TypeUint32 Type = "uint32"
	TypeUint16 Type = "uint16"
	TypeUint8  Type = "uint8"
	// No float32 type :(
	TypeFloat64 Type = "double"
	TypeBoolean Type = "boolean"
	TypeBytes   Type = "string"
	TypeString  Type = "utf8"
	TypeAny     Type = "any"
)

type SortOrder string

const (
	SortNode      SortOrder = ""
	SortAscending SortOrder = "ascending"
)

type AggregateFunction string

const (
	AggregateSum AggregateFunction = "sum"
	AggregateMin AggregateFunction = "min"
	AggregateMax AggregateFunction = "max"
)

// Field specifies schema of a single column.
//
// See https://wiki.yandex-team.ru/yt/userdoc/tables/#sxema
type Column struct {
	Name       string            `yson:"name"`
	Type       Type              `yson:"type"`
	Required   bool              `yson:"required"`
	SortOrder  SortOrder         `yson:"sort_order,omitempty"`
	Lock       string            `yson:"lock,omitempty"`
	Expression string            `yson:"expression,omitempty"`
	Aggregate  AggregateFunction `yson:"aggregate,omitempty"`
	Group      string            `yson:"group,omitempty"`
}

// Schema describe schema of YT table.
//
// See https://wiki.yandex-team.ru/yt/userdoc/tables/#sxema
type Schema struct {
	// Schema is strict by default. Change this option only if you know that you are doing.
	Strict     *bool `yson:"strict,attr,omitempty"`
	UniqueKeys bool  `yson:"unique_keys,attr"`

	Columns []Column `yson:",value"`
}

// WithUniqueKeys returns copy of schema with UniqueKeys attribute set.
func (s Schema) WithUniqueKeys() Schema {
	out := s
	out.UniqueKeys = true
	return out
}
