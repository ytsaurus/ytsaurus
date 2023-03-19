// Package schema defines schema of YT tables.
package schema

import (
	"reflect"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/yson"
)

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
	TypeInt64   Type = "int64"
	TypeInt32   Type = "int32"
	TypeInt16   Type = "int16"
	TypeInt8    Type = "int8"
	TypeUint64  Type = "uint64"
	TypeUint32  Type = "uint32"
	TypeUint16  Type = "uint16"
	TypeUint8   Type = "uint8"
	TypeFloat32 Type = "float"
	TypeFloat64 Type = "double"

	TypeBytes  Type = "string"
	TypeString Type = "utf8"

	// NOTE: "boolean" was renamed into "bool" in type_v3, and "any" was renamed into "yson"
	//
	// We keep old in-memory representation for this types for compatibility reasons.
	// Code that only operates with constants provided by this package should not care.
	TypeBoolean Type = "boolean"
	typeBool    Type = "bool"

	TypeAny  Type = "any"
	typeYSON Type = "yson"

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
	SortNone       SortOrder = ""
	SortAscending  SortOrder = "ascending"
	SortDescending SortOrder = "descending"
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
// See https://yt.yandex-team.ru/docs/description/storage/static_schema
type Column struct {
	Name string `yson:"name"`

	// Type and Required represent "legacy" row schema.
	//
	// When creating Column fill either Type and Required, or ComplexType but not both.
	//
	// When reading schema from YT, both "legacy" and "new" fields are available.
	Type     Type `yson:"type,omitempty"`
	Required bool `yson:"required,omitempty"`

	// ComplexType is "new" row schema.
	//
	//   Type == TypeInt64 && Required == true is equivalent to ComplexType == TypeInt64
	//   Type == TypeInt64 && Required == false is equivalent to ComplexType == Optional{Item: TypeInt64}
	ComplexType ComplexType `yson:"type_v3,omitempty"`

	SortOrder SortOrder `yson:"sort_order,omitempty"`

	// Lock specifies lock group.
	//
	// Used by the MVCC mechanism of dynamic tables. Columns from different lock groups might be modified concurrently.
	Lock       string            `yson:"lock,omitempty"`
	Expression string            `yson:"expression,omitempty"`
	Aggregate  AggregateFunction `yson:"aggregate,omitempty"`

	// Group specifies column group. When using columnar format (optimize_for=scan), columns groups are stored together.
	Group string `yson:"group,omitempty"`
}

var (
	_ yson.StreamMarshaler = Column{}
)

func (c Column) MarshalYSON(w *yson.Writer) error {
	type column Column

	cc := column(c)
	cc.ComplexType = fixV3type(cc.ComplexType)
	w.Any(cc)

	return w.Err()
}

func (c Column) NormalizeType() Column {
	if c.ComplexType == nil {
		if c.Required {
			c.ComplexType = c.Type
		} else {
			c.ComplexType = Optional{Item: c.Type}
		}
	}

	c.Type = ""
	c.Required = false
	return c
}

// Schema describe schema of YT table.
//
// Schema is strict by default.
//
// See https://yt.yandex-team.ru/docs/description/storage/static_schema
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
		if c.SortOrder != SortNone {
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
			s.Columns[i].SortOrder = SortNone
		}
	}

	return s
}

// Normalize converts in-memory Schema to canonical representation.
//   - Strict flag is explicitly set to %true.
//   - Old types are converted to new types.
func (s Schema) Normalize() Schema {
	if s.Strict == nil {
		s.Strict = ptr.Bool(true)
	}

	for i := range s.Columns {
		s.Columns[i] = s.Columns[i].NormalizeType()
	}

	return s
}

func (s Schema) Equal(other Schema) bool {
	return reflect.DeepEqual(s.Normalize(), other.Normalize())
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
			lCol.SortOrder = SortNone
			schema.Columns = append(schema.Columns, lCol)
		}
		lhsColumns[lCol.Name] = lCol
	}
	for _, c := range rhs.Columns {
		if _, ok := lhsColumns[c.Name]; !ok {
			c.Required = false
			c.SortOrder = SortNone
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
	lhs.SortOrder = SortNone
	return lhs
}
