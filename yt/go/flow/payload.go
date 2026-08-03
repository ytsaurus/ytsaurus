// Package flow implements the Go SDK for YT Flow companions.
package flow

import (
	"bytes"
	"math"
	"reflect"
	"sync"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/wire"
	"go.ytsaurus.tech/yt/go/yson"
)

// NoStreamSpecID identifies inputs without a stream spec.
const NoStreamSpecID int64 = -1

var (
	// ErrColumnNotFound reports a column name absent from the payload schema.
	ErrColumnNotFound = xerrors.NewSentinel("column not found")

	// ErrNullValue reports a null or missing cell.
	ErrNullValue = xerrors.NewSentinel("null value")

	// ErrTypeMismatch reports a Go value that does not fit the column's wire type.
	ErrTypeMismatch = xerrors.NewSentinel("type mismatch")

	// ErrUnsupportedSchemaType reports a column type the companion cannot encode.
	ErrUnsupportedSchemaType = xerrors.NewSentinel("unsupported schema type")
)

// Schema is a table schema prepared for row access.
type Schema struct {
	table  schema.Schema
	byName map[string]int
	types  []wire.ValueType
	valid  []bool
	err    error
}

// NewSchema prepares a table schema for row access.
func NewSchema(table schema.Schema) Schema {
	table = table.Copy()
	s := Schema{
		table:  table,
		byName: make(map[string]int, len(table.Columns)),
		types:  make([]wire.ValueType, len(table.Columns)),
		valid:  make([]bool, len(table.Columns)),
	}
	for i, column := range table.Columns {
		if _, ok := s.byName[column.Name]; !ok {
			s.byName[column.Name] = i
		}
		valueType, ok := columnValueType(column)
		if !ok {
			if s.err == nil {
				columnType := any(column.ComplexType)
				if column.Type != "" {
					columnType = column.Type
				}
				s.err = xerrors.Errorf(
					"flow: %w: column %q has type %v",
					ErrUnsupportedSchemaType,
					column.Name,
					columnType,
				)
			}
			continue
		}
		s.types[i] = valueType
		s.valid[i] = true
	}
	return s
}

// Table returns the YT table schema.
func (s Schema) Table() schema.Schema {
	return s.table.Copy()
}

// Columns returns the schema columns in id order.
func (s Schema) Columns() []schema.Column {
	return s.table.Copy().Columns
}

// Len returns the number of columns.
func (s Schema) Len() int {
	return len(s.table.Columns)
}

// FindColumn returns the id of the named column.
func (s Schema) FindColumn(name string) (int, bool) {
	id, ok := s.byName[name]
	return id, ok
}

// ColumnName returns the name of the column with the given id.
func (s Schema) ColumnName(id int) (string, bool) {
	if id < 0 || id >= len(s.table.Columns) {
		return "", false
	}
	return s.table.Columns[id].Name, true
}

// ColumnType returns the wire type values of the column with the given id are written as.
func (s Schema) ColumnType(id int) (wire.ValueType, bool) {
	if id < 0 || id >= len(s.types) || !s.valid[id] {
		return wire.TypeNull, false
	}
	return s.types[id], true
}

func (s Schema) validate() error {
	return s.err
}

func columnValueType(column schema.Column) (wire.ValueType, bool) {
	if column.Type != "" {
		return primitiveValueType(column.Type)
	}
	return complexValueType(column.ComplexType)
}

func complexValueType(t schema.ComplexType) (wire.ValueType, bool) {
	switch t := t.(type) {
	case nil:
		return wire.TypeNull, false
	case schema.Type:
		return primitiveValueType(t)
	case schema.Optional:
		return complexValueType(t.Item)
	case schema.Tagged:
		return complexValueType(t.Item)
	case schema.Decimal:
		return wire.TypeBytes, true
	case schema.List, schema.Struct, schema.Tuple, schema.Variant, schema.Dict:
		return wire.TypeComposite, true
	default:
		return wire.TypeNull, false
	}
}

func primitiveValueType(t schema.Type) (wire.ValueType, bool) {
	switch t {
	case schema.TypeInt8, schema.TypeInt16, schema.TypeInt32, schema.TypeInt64, schema.TypeInterval:
		return wire.TypeInt64, true
	case schema.TypeUint8, schema.TypeUint16, schema.TypeUint32, schema.TypeUint64,
		schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp:
		return wire.TypeUint64, true
	case schema.TypeFloat32, schema.TypeFloat64:
		return wire.TypeFloat64, true
	case schema.TypeBoolean, "bool":
		return wire.TypeBool, true
	case schema.TypeBytes, schema.TypeString, "json", "uuid":
		return wire.TypeBytes, true
	case schema.TypeNull, "void":
		return wire.TypeNull, true
	case schema.TypeAny, "yson":
		return wire.TypeAny, true
	default:
		return wire.TypeNull, false
	}
}

// Payload is an unversioned row and its schema.
type Payload struct {
	row    wire.Row
	schema Schema
}

// NewPayload copies a row and binds its schema.
func NewPayload(row wire.Row, s Schema) Payload {
	return Payload{row: cloneRow(row), schema: s}
}

// Row returns a copy of the unversioned row.
func (p Payload) Row() wire.Row {
	return cloneRow(p.row)
}

// Schema returns the schema describing the row.
func (p Payload) Schema() Schema {
	return p.schema
}

// Value returns the named cell, copying byte data.
func (p Payload) Value(column string) (wire.Value, bool) {
	id, ok := p.schema.FindColumn(column)
	if !ok {
		return wire.Value{}, false
	}
	value, ok := p.valueByID(id)
	return cloneValue(value), ok
}

func (p Payload) valueByID(id int) (wire.Value, bool) {
	if id < 0 || id > math.MaxUint16 {
		return wire.Value{}, false
	}
	if id < len(p.row) && int(p.row[id].ID) == id {
		return p.row[id], true
	}
	for i := range p.row {
		if int(p.row[i].ID) == id {
			return p.row[i], true
		}
	}
	return wire.Value{}, false
}

func cloneRow(row wire.Row) wire.Row {
	if row == nil {
		return nil
	}
	cloned := make(wire.Row, len(row))
	for i := range row {
		cloned[i] = cloneValue(row[i])
	}
	return cloned
}

func cloneValue(value wire.Value) wire.Value {
	var cloned wire.Value
	switch value.Type {
	case wire.TypeNull:
		cloned = wire.NewNull(value.ID)
	case wire.TypeInt64:
		cloned = wire.NewInt64(value.ID, value.Int64())
	case wire.TypeUint64:
		cloned = wire.NewUint64(value.ID, value.Uint64())
	case wire.TypeFloat64:
		cloned = wire.NewFloat64(value.ID, value.Float64())
	case wire.TypeBool:
		cloned = wire.NewBool(value.ID, value.Bool())
	case wire.TypeBytes:
		cloned = wire.NewBytes(value.ID, bytes.Clone(value.Bytes()))
	case wire.TypeAny:
		cloned = wire.NewAny(value.ID, bytes.Clone(value.Bytes()))
	case wire.TypeComposite:
		cloned = wire.NewComposite(value.ID, bytes.Clone(value.Bytes()))
	default:
		cloned = value
	}
	cloned.Aggregate = value.Aggregate
	return cloned
}

// Has reports whether the named column carries a non-null value.
func (p Payload) Has(column string) bool {
	v, ok := p.Value(column)
	return ok && v.Type != wire.TypeNull
}

// Columns returns the names of non-null cells in row order.
func (p Payload) Columns() []string {
	var names []string
	for i := range p.row {
		if p.row[i].Type == wire.TypeNull {
			continue
		}
		if name, ok := p.schema.ColumnName(int(p.row[i].ID)); ok {
			names = append(names, name)
		}
	}
	return names
}

func (p Payload) toMap() map[string]any {
	result := make(map[string]any)
	for i := range p.row {
		v := p.row[i]
		if v.Type == wire.TypeNull {
			continue
		}
		name, ok := p.schema.ColumnName(int(v.ID))
		if !ok {
			continue
		}
		value, err := goValue(v)
		if err != nil {
			continue
		}
		result[name] = value
	}
	return result
}

// ToBuilder returns a builder prefilled with this payload's cells.
func (p Payload) ToBuilder() *PayloadBuilder {
	b := NewPayloadBuilder(p.schema)
	for i := range p.row {
		id := int(p.row[i].ID)
		if id < len(b.values) {
			b.values[id] = cloneValue(p.row[i])
		}
	}
	return b
}

// Int64 returns the value of an int64-typed column.
func (p Payload) Int64(column string) (int64, error) {
	v, err := p.typedValue(column, wire.TypeInt64)
	if err != nil {
		return 0, err
	}
	return v.Int64(), nil
}

// Uint64 returns the value of a uint64-typed column.
func (p Payload) Uint64(column string) (uint64, error) {
	v, err := p.typedValue(column, wire.TypeUint64)
	if err != nil {
		return 0, err
	}
	return v.Uint64(), nil
}

// Float64 returns the value of a double-typed column.
func (p Payload) Float64(column string) (float64, error) {
	v, err := p.typedValue(column, wire.TypeFloat64)
	if err != nil {
		return 0, err
	}
	return v.Float64(), nil
}

// Bool returns the value of a boolean-typed column.
func (p Payload) Bool(column string) (bool, error) {
	v, err := p.typedValue(column, wire.TypeBool)
	if err != nil {
		return false, err
	}
	return v.Bool(), nil
}

// Bytes returns a copy of a string-typed column.
func (p Payload) Bytes(column string) ([]byte, error) {
	v, err := p.typedValue(column, wire.TypeBytes)
	if err != nil {
		return nil, err
	}
	return bytes.Clone(v.Bytes()), nil
}

// String returns the value of a string-typed column decoded as text.
func (p Payload) String(column string) (string, error) {
	v, err := p.typedValue(column, wire.TypeBytes)
	if err != nil {
		return "", err
	}
	return string(v.Bytes()), nil
}

// ConvertTo fills fields selected by their yson tags. Missing cells leave fields unchanged.
func (p Payload) ConvertTo(dst any) error {
	if reflect.ValueOf(dst).Kind() != reflect.Pointer {
		return xerrors.Errorf("flow: convert payload: %T is not a pointer to a struct", dst)
	}
	row, err := structValue(dst)
	if err != nil {
		return xerrors.Errorf("flow: convert payload: %w", err)
	}

	for _, f := range structFields(row.Type()) {
		id, ok := p.schema.FindColumn(f.name)
		if !ok {
			continue
		}
		v, ok := p.valueByID(id)
		if !ok || v.Type == wire.TypeNull {
			continue
		}
		field, _ := fieldByIndex(row, f.index, true)
		if err := scanValue(v, field); err != nil {
			return xerrors.Errorf("flow: convert payload: column %q: %w", f.name, err)
		}
	}
	return nil
}

// Any deserializes an any- or composite-typed column into dst.
func (p Payload) Any(column string, dst any) error {
	id, ok := p.schema.FindColumn(column)
	if !ok {
		return xerrors.Errorf("flow: %w: %q", ErrColumnNotFound, column)
	}
	v, ok := p.valueByID(id)
	if !ok || v.Type == wire.TypeNull {
		return xerrors.Errorf("flow: %w: column %q", ErrNullValue, column)
	}
	if v.Type != wire.TypeAny && v.Type != wire.TypeComposite {
		return xerrors.Errorf("flow: %w: column %q holds %v, want any", ErrTypeMismatch, column, v.Type)
	}
	return yson.Unmarshal(v.Bytes(), dst)
}

func (p Payload) typedValue(column string, want wire.ValueType) (wire.Value, error) {
	id, ok := p.schema.FindColumn(column)
	if !ok {
		return wire.Value{}, xerrors.Errorf("flow: %w: %q", ErrColumnNotFound, column)
	}
	v, ok := p.valueByID(id)
	if !ok || v.Type == wire.TypeNull {
		return wire.Value{}, xerrors.Errorf("flow: %w: column %q", ErrNullValue, column)
	}
	if v.Type != want {
		return wire.Value{}, xerrors.Errorf("flow: %w: column %q holds %v, want %v", ErrTypeMismatch, column, v.Type, want)
	}
	return v, nil
}

func goValue(v wire.Value) (any, error) {
	switch v.Type {
	case wire.TypeNull:
		return nil, nil
	case wire.TypeInt64:
		return v.Int64(), nil
	case wire.TypeUint64:
		return v.Uint64(), nil
	case wire.TypeFloat64:
		return v.Float64(), nil
	case wire.TypeBool:
		return v.Bool(), nil
	case wire.TypeBytes, wire.TypeAny, wire.TypeComposite:
		return bytes.Clone(v.Bytes()), nil
	default:
		return nil, xerrors.Errorf("flow: unsupported value type %v in column %d", v.Type, v.ID)
	}
}

// PayloadBuilder builds one payload and reports setter errors from Finish.
type PayloadBuilder struct {
	schema Schema
	values wire.Row
	err    error
}

// NewPayloadBuilder returns a builder for rows of the given schema, with every cell null.
func NewPayloadBuilder(s Schema) *PayloadBuilder {
	b := &PayloadBuilder{schema: s, values: make(wire.Row, s.Len()), err: s.validate()}
	for i := range b.values {
		b.values[i] = wire.NewNull(uint16(i))
	}
	return b
}

// Schema returns the schema of the rows being built.
func (b *PayloadBuilder) Schema() Schema {
	return b.schema
}

// Set stores a value in the named column. Nil stores a null cell.
func (b *PayloadBuilder) Set(column string, value any) *PayloadBuilder {
	if b.err != nil {
		return b
	}

	id, ok := b.schema.FindColumn(column)
	if !ok {
		b.err = xerrors.Errorf("flow: %w: %q", ErrColumnNotFound, column)
		return b
	}
	valueType, _ := b.schema.ColumnType(id)
	v, err := convertValue(uint16(id), valueType, value)
	if err != nil {
		b.err = xerrors.Errorf("flow: column %q: %w", column, err)
		return b
	}
	b.values[id] = v
	return b
}

// SetStruct stores fields selected by their yson tags.
func (b *PayloadBuilder) SetStruct(v any) *PayloadBuilder {
	if b.err != nil {
		return b
	}

	row, err := structValue(v)
	if err != nil {
		b.err = xerrors.Errorf("flow: SetStruct value: %w", err)
		return b
	}

	for _, f := range structFields(row.Type()) {
		field, ok := fieldByIndex(row, f.index, false)
		if !ok {
			continue
		}
		b.Set(f.name, fieldValue(field))
	}
	return b
}

// Finish returns the payload.
func (b *PayloadBuilder) Finish() (Payload, error) {
	if b.err != nil {
		return Payload{}, b.err
	}

	return NewPayload(b.values, b.schema), nil
}

func convertValue(id uint16, valueType wire.ValueType, value any) (wire.Value, error) {
	if value == nil {
		return wire.NewNull(id), nil
	}

	switch valueType {
	case wire.TypeInt64:
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return wire.NewInt64(id, rv.Int()), nil
		}

	case wire.TypeUint64:
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return wire.NewUint64(id, rv.Uint()), nil
		}

	case wire.TypeFloat64:
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Float32, reflect.Float64:
			return wire.NewFloat64(id, rv.Float()), nil
		}

	case wire.TypeBool:
		if v, ok := value.(bool); ok {
			return wire.NewBool(id, v), nil
		}

	case wire.TypeBytes:
		switch v := value.(type) {
		case []byte:
			return wire.NewBytes(id, bytes.Clone(v)), nil
		case string:
			return wire.NewBytes(id, []byte(v)), nil
		}

	case wire.TypeAny, wire.TypeComposite:
		blob, ok := value.([]byte)
		if !ok {
			var err error
			if blob, err = yson.MarshalFormat(value, yson.FormatBinary); err != nil {
				return wire.Value{}, err
			}
		} else {
			blob = bytes.Clone(blob)
		}
		if valueType == wire.TypeComposite {
			return wire.NewComposite(id, blob), nil
		}
		return wire.NewAny(id, blob), nil

	case wire.TypeNull:
		return wire.Value{}, xerrors.Errorf("%w: column is null-typed, only nil can be stored", ErrTypeMismatch)
	}

	return wire.Value{}, xerrors.Errorf("%w: cannot store %T in a %v column", ErrTypeMismatch, value, valueType)
}

func scanValue(v wire.Value, dst reflect.Value) error {
	if dst.Kind() == reflect.Pointer {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		return scanValue(v, dst.Elem())
	}

	switch v.Type {
	case wire.TypeInt64:
		switch dst.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if dst.OverflowInt(v.Int64()) {
				return xerrors.Errorf("%w: %d does not fit a %s", ErrTypeMismatch, v.Int64(), dst.Type())
			}
			dst.SetInt(v.Int64())
			return nil
		}

	case wire.TypeUint64:
		switch dst.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if dst.OverflowUint(v.Uint64()) {
				return xerrors.Errorf("%w: %d does not fit a %s", ErrTypeMismatch, v.Uint64(), dst.Type())
			}
			dst.SetUint(v.Uint64())
			return nil
		}

	case wire.TypeFloat64:
		switch dst.Kind() {
		case reflect.Float32, reflect.Float64:
			if dst.OverflowFloat(v.Float64()) {
				return xerrors.Errorf("%w: %g does not fit a %s", ErrTypeMismatch, v.Float64(), dst.Type())
			}
			dst.SetFloat(v.Float64())
			return nil
		}

	case wire.TypeBool:
		if dst.Kind() == reflect.Bool {
			dst.SetBool(v.Bool())
			return nil
		}

	case wire.TypeBytes:
		switch {
		case dst.Kind() == reflect.String:
			dst.SetString(string(v.Bytes()))
			return nil
		case dst.Kind() == reflect.Slice && dst.Type().Elem().Kind() == reflect.Uint8:
			dst.SetBytes(bytes.Clone(v.Bytes()))
			return nil
		}

	case wire.TypeAny, wire.TypeComposite:
		return yson.Unmarshal(v.Bytes(), dst.Addr().Interface())
	}

	return xerrors.Errorf("%w: cannot read a %v cell into a %s", ErrTypeMismatch, v.Type, dst.Type())
}

func structValue(v any) (reflect.Value, error) {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return reflect.Value{}, xerrors.Errorf("%T is nil", v)
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return reflect.Value{}, xerrors.Errorf("%T is not a struct", v)
	}
	return rv, nil
}

func fieldValue(field reflect.Value) any {
	for field.Kind() == reflect.Pointer {
		if field.IsNil() {
			return nil
		}
		field = field.Elem()
	}
	return field.Interface()
}

type structField struct {
	name  string
	index []int
}

var structFieldCache sync.Map

func structFields(t reflect.Type) []structField {
	if cached, ok := structFieldCache.Load(t); ok {
		return cached.([]structField)
	}
	fields := collectStructFields(t, nil)
	structFieldCache.Store(t, fields)
	return fields
}

func collectStructFields(t reflect.Type, prefix []int) []structField {
	var fields []structField

	for i := range t.NumField() {
		f := t.Field(i)
		tag, skip := yson.ParseTag(f.Name, f.Tag)
		if skip {
			continue
		}

		index := append(append([]int{}, prefix...), i)
		embedded := f.Type
		for embedded.Kind() == reflect.Pointer {
			embedded = embedded.Elem()
		}
		if _, tagged := f.Tag.Lookup("yson"); f.Anonymous && !tagged && embedded.Kind() == reflect.Struct {
			fields = append(fields, collectStructFields(embedded, index)...)
			continue
		}

		if f.PkgPath != "" {
			continue
		}
		fields = append(fields, structField{name: tag.Name, index: index})
	}

	return fields
}

func fieldByIndex(row reflect.Value, index []int, alloc bool) (reflect.Value, bool) {
	for i, at := range index {
		if i > 0 {
			for row.Kind() == reflect.Pointer {
				if row.IsNil() {
					if !alloc {
						return reflect.Value{}, false
					}
					row.Set(reflect.New(row.Type().Elem()))
				}
				row = row.Elem()
			}
		}
		row = row.Field(at)
	}
	return row, true
}
