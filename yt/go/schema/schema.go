// package schema defines schema of YT tables.
package schema

type Type string

func (t Type) MarshalText() (text []byte, err error) {
	return []byte(t), nil
}

func (t *Type) UnmarshalText(text []byte) error {
	*t = Type(text)
	// TODO(prime@): validate
	return nil
}

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

func (o SortOrder) MarshalText() (text []byte, err error) {
	return []byte(o), nil
}

func (o *SortOrder) UnmarshalText(text []byte) error {
	*o = SortOrder(text)
	// TODO(prime@): validate
	return nil
}

const (
	SortNode      SortOrder = ""
	SortAscending SortOrder = "ascending"
)

type AggregateFunction string

func (a AggregateFunction) MarshalText() (text []byte, err error) {
	return []byte(a), nil
}

func (a *AggregateFunction) UnmarshalText(text []byte) error {
	*a = AggregateFunction(text)
	// TODO(prime@): validate
	return nil
}

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

func (s Schema) Copy() Schema {
	if s.Strict != nil {
		strict := *s.Strict
		s.Strict = &strict
	}

	columns := s.Columns
	s.Columns = make([]Column, len(columns))
	copy(s.Columns, columns)

	return s
}

func (s Schema) SortedBy(keyColumns ...string) Schema {
	s = s.Copy()

	for _, keyName := range keyColumns {
		for i := range s.Columns {
			if s.Columns[i].Name == keyName {
				s.Columns[i].SortOrder = SortAscending
			}
		}
	}

	return s
}

// WithUniqueKeys returns copy of schema with UniqueKeys attribute set.
func (s Schema) WithUniqueKeys() Schema {
	out := s
	out.UniqueKeys = true
	return out
}
