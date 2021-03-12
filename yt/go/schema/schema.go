// Package schema defines schema of YT tables.
package schema

import "reflect"

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

	TypeDate      Type = "date"
	TypeDatetime  Type = "datetime"
	TypeTimestamp Type = "timestamp"
	TypeInterval  Type = "interval"
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

// Column specifies schema of a single column.
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
// Schema is strict by default.
//
// See https://wiki.yandex-team.ru/yt/userdoc/tables/#sxema
type Schema struct {
	// Schema is strict by default. Change this option only if you know that you are doing.
	Strict     *bool `yson:"strict,attr,omitempty"`
	UniqueKeys bool  `yson:"unique_keys,attr"`

	Columns []Column `yson:",value"`
}

func (s Schema) Append(column ...Column) Schema {
	s = s.Copy()
	s.Columns = append(s.Columns, column...)
	return s
}

func (s Schema) Prepend(column ...Column) Schema {
	s = s.Copy()
	s.Columns = append(column, s.Columns...)
	return s
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

// KeyColumns returns list of columns names marked as sorted.
func (s Schema) KeyColumns() []string {
	var keys []string
	for _, c := range s.Columns {
		if c.SortOrder != SortNode {
			keys = append(keys, c.Name)
		}
	}
	return keys
}

// SortedBy returns copy of the schema, with keyColumns (and only them) marked as sorted.
func (s Schema) SortedBy(keyColumns ...string) Schema {
	s = s.Copy()

	allKeys := map[string]struct{}{}
	for _, k := range keyColumns {
		allKeys[k] = struct{}{}
	}

	for i := range s.Columns {
		if _, ok := allKeys[s.Columns[i].Name]; ok {
			s.Columns[i].SortOrder = SortAscending
		} else {
			s.Columns[i].SortOrder = SortNode
		}
	}

	return s
}

func (s Schema) Equal(other Schema) bool {
	explicitTrue := true
	if s.Strict == nil {
		s.Strict = &explicitTrue
	}
	if other.Strict == nil {
		other.Strict = &explicitTrue
	}
	return reflect.DeepEqual(&s, &other)
}

// WithUniqueKeys returns copy of schema with UniqueKeys attribute set.
func (s Schema) WithUniqueKeys() Schema {
	out := s
	out.UniqueKeys = true
	return out
}

func (s *Schema) IsStrict() bool {
	return s.Strict != nil && *s.Strict
}

// MergeSchemas merges two schemas inferred from rows
//
// If column have different types, then result column will be of type Any.
// Result schema does not have key columns, and columns have following order:
// columns from `lhs` schema, then columns from `rhs` schema that does not
// exist in `lhs` schema.
func MergeSchemas(lhs, rhs Schema) Schema {
	rhsColumns := make(map[string]Column)
	for _, c := range rhs.Columns {
		rhsColumns[c.Name] = c
	}
	var schema Schema
	lhsColumns := make(map[string]Column)
	for _, lCol := range lhs.Columns {
		if rCol, ok := rhsColumns[lCol.Name]; ok {
			schema.Columns = append(schema.Columns, mergeColumns(lCol, rCol))
		} else {
			// If one row does not have such column we need to mark this
			// column as not required due to `null` values.
			lCol.Required = false
			lCol.SortOrder = SortNode
			schema.Columns = append(schema.Columns, lCol)
		}
		lhsColumns[lCol.Name] = lCol
	}
	for _, c := range rhs.Columns {
		if _, ok := lhsColumns[c.Name]; !ok {
			c.Required = false
			c.SortOrder = SortNode
			schema.Columns = append(schema.Columns, c)
		}
	}
	return schema
}

func mergeColumns(lhs, rhs Column) Column {
	if lhs.Required != rhs.Required {
		lhs.Required = false
	}
	if lhs.Type != rhs.Type {
		lhs.Required = false
		lhs.Type = TypeAny
	}
	lhs.SortOrder = SortNode
	return lhs
}
