package integration

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/skiff"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

var (
	members = []schema.StructMember{
		{Name: "fieldInt16", Type: schema.TypeInt16},
		{Name: "fieldString", Type: schema.TypeString},
		{Name: "fieldOptionalString", Type: schema.Optional{Item: schema.TypeString}},
	}
	elements = []schema.TupleElement{
		{Type: schema.TypeInt16},
		{Type: schema.TypeString},
		{Type: schema.Optional{Item: schema.TypeString}},
	}
	secondsPerDay          = int64(24 * 60 * 60)
	schemaWithComplexTypes = schema.Schema{
		Strict:     nil,
		UniqueKeys: false,
		Columns: []schema.Column{
			{Name: "t_int8", ComplexType: schema.TypeInt8, SortOrder: schema.SortAscending},
			{Name: "t_int16", ComplexType: schema.TypeInt16},
			{Name: "t_int32", ComplexType: schema.TypeInt32},
			{Name: "t_int64", ComplexType: schema.TypeInt64},
			{Name: "t_uint8", ComplexType: schema.TypeUint8},
			{Name: "t_uint16", ComplexType: schema.TypeUint16},
			{Name: "t_uint32", ComplexType: schema.TypeUint32},
			{Name: "t_uint64", ComplexType: schema.TypeUint64},
			{Name: "t_float", ComplexType: schema.TypeFloat32},
			{Name: "t_double", ComplexType: schema.TypeFloat64},
			{Name: "t_bool", ComplexType: schema.TypeBoolean},
			{Name: "t_string", ComplexType: schema.TypeBytes},
			{Name: "t_utf8", ComplexType: schema.TypeString},
			{Name: "t_date", ComplexType: schema.TypeDate},
			{Name: "t_datetime", ComplexType: schema.TypeDatetime},
			{Name: "t_timestamp", ComplexType: schema.TypeTimestamp},
			{Name: "t_interval", ComplexType: schema.TypeInterval},
			{Name: "t_decimal", ComplexType: schema.Decimal{Precision: 5, Scale: 4}},
			{Name: "t_yson", ComplexType: schema.Optional{Item: schema.TypeAny}},
			{Name: "t_opt_int64", ComplexType: schema.Optional{Item: schema.TypeInt64}},
			{Name: "t_list", ComplexType: schema.List{Item: schema.TypeString}},
			{Name: "t_struct", ComplexType: schema.Struct{Members: members}},
			{Name: "t_tuple", ComplexType: schema.Tuple{Elements: elements}},
			{Name: "t_variant_named", ComplexType: schema.Variant{Members: members}},
			{Name: "t_variant_unnamed", ComplexType: schema.Variant{Elements: elements}},
			{Name: "t_dict", ComplexType: schema.Dict{Key: schema.TypeString, Value: schema.TypeInt64}},
			{Name: "t_tagged", ComplexType: schema.Tagged{Tag: "mytag", Item: schema.Variant{Members: members}}},
		},
	}
	rowsWithComplexTypes = []map[string]any{
		{
			"t_int8":      math.MinInt8,
			"t_int16":     math.MinInt16,
			"t_int32":     math.MinInt32,
			"t_int64":     math.MinInt64,
			"t_uint8":     0,
			"t_uint16":    0,
			"t_uint32":    0,
			"t_uint64":    0,
			"t_float":     float32(0.0),
			"t_double":    0.0,
			"t_bool":      false,
			"t_string":    "",
			"t_utf8":      "",
			"t_date":      0,                                      // Min allowed by YT Date.
			"t_datetime":  0,                                      // Min allowed by YT Datetime.
			"t_timestamp": 0,                                      // Min allowed by YT Timestamp.
			"t_interval":  ytInterval(-49673*24*time.Hour + 1000), // Min allowed by YT Duration.
			"t_decimal":   []byte{0x80, 0x00, 0x7A, 0xB7},         // 3.1415 in binary representation.
			// "t_yson":       It is optional field and not enabled here.
			// "t_opt_int64":  It is optional field and not enabled here.
			"t_list":            []string{},
			"t_struct":          map[string]any{"fieldInt16": 100, "fieldString": "abc"},
			"t_tuple":           []any{-5, "my data", nil},
			"t_variant_named":   []any{"fieldInt16", 100},
			"t_variant_unnamed": []any{0, 100},
			"t_dict":            []any{},
			"t_tagged":          []any{"fieldInt16", 100},
		},
		{
			"t_int8":            10,
			"t_int16":           -2000,
			"t_int32":           -200000,
			"t_int64":           -20000000000,
			"t_uint8":           20,
			"t_uint16":          2000,
			"t_uint32":          2000000,
			"t_uint64":          20000000000,
			"t_float":           float32(2.2),
			"t_double":          2.2,
			"t_bool":            true,
			"t_string":          "Test byte string 2",
			"t_utf8":            "Test utf8 string 2",
			"t_date":            1640604030 / secondsPerDay,
			"t_datetime":        1640604030,
			"t_timestamp":       1640604030502383,
			"t_interval":        ytInterval(time.Minute),
			"t_decimal":         []byte{0x7F, 0xFF, 0x95, 0xD2}, // -2.7182 in binary representation.
			"t_yson":            []uint64{100, 200, 300},
			"t_opt_int64":       math.MaxInt64,
			"t_list":            []string{"one"},
			"t_struct":          map[string]any{"fieldInt16": 100, "fieldString": "abc", "fieldOptionalString": "optStr"},
			"t_tuple":           []any{-5, "my data", "optString"},
			"t_variant_named":   []any{"fieldOptionalString", "optStr"},
			"t_variant_unnamed": []any{2, "optStr1"},
			"t_dict":            [][]any{{"my_key", 100}},
			"t_tagged":          []any{"fieldString", "100.01"},
		},
		{
			"t_int8":            math.MaxInt8,
			"t_int16":           math.MaxInt16,
			"t_int32":           math.MaxInt32,
			"t_int64":           math.MaxInt64,
			"t_uint8":           math.MaxUint8,
			"t_uint16":          math.MaxUint16,
			"t_uint32":          math.MaxUint32,
			"t_uint64":          uint64(math.MaxUint64),
			"t_float":           float32(42),
			"t_double":          42.0,
			"t_bool":            false,
			"t_string":          "Test byte string 3",
			"t_utf8":            "Test utf8 string 3",
			"t_date":            mustParseTime("2105-12-31 23:59:59").Unix() / secondsPerDay, // Max allowed by YT Date.
			"t_datetime":        mustParseTime("2105-12-31 23:59:59").Unix(),                 // Max allowed by YT Datetime.
			"t_timestamp":       mustParseTime("2105-12-31 23:59:59").UnixMicro(),
			"t_interval":        ytInterval(49673*24*time.Hour - 1000), // Max allowed by YT Duration.
			"t_decimal":         []byte{0x80, 0x00, 0x00, 0x00},        // zero in binary representation.
			"t_yson":            nil,
			"t_opt_int64":       nil,
			"t_list":            []any{"one", "two", "three"},
			"t_struct":          map[string]any{"fieldInt16": 100, "fieldString": "abc", "fieldOptionalString": nil},
			"t_tuple":           []any{-5, "my data", nil},
			"t_variant_named":   []any{"fieldInt16", 12},
			"t_variant_unnamed": []any{1, "str"},
			"t_dict":            [][]any{{"key1", 1}, {"key2", 20}, {"key3", 300}},
			"t_tagged":          []any{"fieldString", "100"},
		},
	}
)

type tableRow struct {
	Boolean           bool             `yson:"boolean"`
	String            string           `yson:"string"`
	Bytes             []byte           `yson:"bytes"`
	Int8              int8             `yson:"int8"`
	Int16             int16            `yson:"int16"`
	Int32             int32            `yson:"int32"`
	Int64             int64            `yson:"int64"`
	Uint8             uint8            `yson:"uint8"`
	Uint16            uint16           `yson:"uint16"`
	Uint32            uint32           `yson:"uint32"`
	Uint64            uint64           `yson:"uint64"`
	Float32           float32          `yson:"float32"`
	Float64           float64          `yson:"float64"`
	Int               int              `yson:"int"`
	Uint              uint             `yson:"uint"`
	List              []string         `yson:"list"`
	Map               map[string]any   `yson:"map"`
	Struct            someStruct       `yson:"struct"`
	YSONRawValue      yson.RawValue    `yson:"yson_raw_value"`
	YSONDuration      yson.Duration    `yson:"yson_duration"`
	Date              schema.Date      `yson:"date"`
	Datetime          schema.Datetime  `yson:"datetime"`
	Timestamp         schema.Timestamp `yson:"timestamp"`
	Interval          schema.Interval  `yson:"interval"`
	IgnoredField      string           `yson:"-"`
	Any               any              `yson:"any"`
	OptionalString    *string          `yson:"optional_string"`
	EmptyInt8         *int8            `yson:"empty_int8"`
	MapStringToIntPtr map[string]*int  `yson:"map_string_to_int_ptr"`
}

type someStruct struct {
	A bool `yson:"a"`
	B string
}

func (tr *tableRow) Init() {
	timestamp := time.Date(2025, time.April, 22, 10, 21, 31, 7, time.UTC)

	tr.Boolean = true
	tr.String = "some-string"
	tr.Bytes = []byte{'a', 'b', 1}
	tr.Int8 = math.MaxInt8
	tr.Int16 = math.MaxInt16
	tr.Int32 = math.MaxInt32
	tr.Int64 = math.MaxInt64
	tr.Uint8 = math.MaxUint8
	tr.Uint16 = math.MaxUint16
	tr.Uint32 = math.MaxUint32
	tr.Uint64 = math.MaxUint64
	tr.Float32 = math.MaxFloat32
	tr.Float64 = math.MaxFloat64
	tr.Int = math.MinInt
	tr.Uint = math.MaxUint
	tr.List = []string{"value1", "value2"}
	tr.Map = map[string]any{"key1": "value1"}
	tr.Struct = someStruct{B: "str_b"}
	tr.YSONRawValue = yson.RawValue(mustNoError(yson.MarshalFormat(map[string]any{"key1": 1, "key2": 2}, yson.FormatBinary)))
	tr.YSONDuration = yson.Duration(5 * time.Minute)
	tr.Date = mustNoError(schema.NewDate(timestamp))
	tr.Datetime = mustNoError(schema.NewDatetime(timestamp))
	tr.Timestamp = mustNoError(schema.NewTimestamp(timestamp))
	tr.Interval = mustNoError(schema.NewInterval(3 * time.Hour))
	tr.IgnoredField = "ignored field"
	tr.Any = "any value"
	tr.OptionalString = ptr.T("optional-string")
	tr.EmptyInt8 = nil
	tr.MapStringToIntPtr = map[string]*int{"one": ptr.T(1), "two": nil}
}

func (tr *tableRow) EqualTo(other tableRow) bool {
	if tr.Boolean != other.Boolean ||
		tr.String != other.String ||
		!assert.ObjectsAreEqual(tr.Bytes, other.Bytes) ||
		tr.Int8 != other.Int8 ||
		tr.Int16 != other.Int16 ||
		tr.Int32 != other.Int32 ||
		tr.Int64 != other.Int64 ||
		tr.Uint8 != other.Uint8 ||
		tr.Uint16 != other.Uint16 ||
		tr.Uint32 != other.Uint32 ||
		tr.Uint64 != other.Uint64 ||
		tr.Int != other.Int ||
		tr.Uint != other.Uint ||
		!assert.ObjectsAreEqual(tr.List, other.List) ||
		!assert.ObjectsAreEqual(tr.Struct, other.Struct) ||
		!assert.ObjectsAreEqual(tr.YSONRawValue, other.YSONRawValue) ||
		!assert.ObjectsAreEqual(tr.YSONDuration, other.YSONDuration) ||
		!assert.ObjectsAreEqual(tr.Date, other.Date) ||
		!assert.ObjectsAreEqual(tr.Datetime, other.Datetime) ||
		!assert.ObjectsAreEqual(tr.Timestamp, other.Timestamp) ||
		!assert.ObjectsAreEqual(tr.Interval, other.Interval) ||
		!assert.ObjectsAreEqual(tr.IgnoredField, other.IgnoredField) ||
		!assert.ObjectsAreEqual(tr.Any, other.Any) ||
		!assert.ObjectsAreEqual(tr.OptionalString, other.OptionalString) ||
		!assert.ObjectsAreEqual(tr.EmptyInt8, other.EmptyInt8) {
		return false
	}

	if math.Abs(float64(tr.Float32-other.Float32)) > 0.0001 {
		return false
	}

	if math.Abs(tr.Float64-other.Float64) > 0.000001 {
		return false
	}

	if len(tr.Map) != len(other.Map) {
		return false
	}
	for k, v := range tr.Map {
		if otherValue, ok := other.Map[k]; !ok || !assert.ObjectsAreEqualValues(v, otherValue) {
			return false
		}
	}

	if len(tr.MapStringToIntPtr) != len(other.MapStringToIntPtr) {
		return false
	}
	for k, v := range tr.MapStringToIntPtr {
		if otherValue, ok := other.MapStringToIntPtr[k]; !ok || !assert.ObjectsAreEqualValues(v, otherValue) {
			return false
		}
	}

	return true
}

func asMap(value any) map[string]any {
	result := make(map[string]any)
	v := reflect.ValueOf(value)
	t := reflect.TypeOf(value)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.Field(i)
		fieldName := field.Tag.Get("yson")
		if fieldName == "" {
			fieldName = field.Name
		} else if fieldName == "-" {
			continue
		}

		if fieldValue.Kind() == reflect.Struct {
			result[fieldName] = asMap(fieldValue.Interface())
		} else {
			result[fieldName] = fieldValue.Interface()
		}
	}

	return result
}

func mustNoError[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

func TestReadTableSkiff(t *testing.T) {
	suite := NewSuite(t)

	suite.RunClientTests(t, []ClientTest{
		{Name: "SkiffReadTableStruct", Test: suite.TestSkiffReadTableStruct, SkipRPC: true},
		{Name: "SkiffReadTableMap", Test: suite.TestSkiffReadTableMap, SkipRPC: true},
		{Name: "SkiffReadTableMapWithComplexTypes", Test: suite.TestSkiffReadTableMapWithComplexTypes, SkipRPC: true},
		{Name: "SkiffReadTableMapWeakSchema", Test: suite.TestSkiffReadTableMapWeakSchema, SkipRPC: true},
		{Name: "SkiffReadTableCompatibleStructs", Test: suite.TestSkiffReadTableCompatibleStructs, SkipRPC: true},
		{Name: "SkiffReadTableIncompatibleStructs", Test: suite.TestSkiffReadTableIncompatibleStructs, SkipRPC: true},
		{Name: "SkiffReadTableIntegerOverflow", Test: suite.TestSkiffReadTableIntegerOverflow, SkipRPC: true},
	})
}

func TestWriteTableSkiff(t *testing.T) {
	suite := NewSuite(t)

	suite.RunClientTests(t, []ClientTest{
		{Name: "SkiffWriteTableStruct", Test: suite.TestSkiffWriteTableStruct, SkipRPC: true},
		{Name: "SkiffWriteTableMap", Test: suite.TestSkiffWriteTableMap, SkipRPC: true},
		{Name: "SkiffHighLevelTableWriter", Test: suite.TestSkiffHighLevelTableWriter, SkipRPC: true},
		{Name: "SkiffWriteTableCompatibleStructs", Test: suite.TestSkiffWriteTableCompatibleStructs, SkipRPC: true},
		{Name: "SkiffWriteTableIncompatibleStructs", Test: suite.TestSkiffWriteTableIncompatibleStructs, SkipRPC: true},
		{Name: "SkiffWriteTableIntegerOverflow", Test: suite.TestSkiffWriteTableIntegerOverflow, SkipRPC: true},
	})
}

func (s *Suite) TestSkiffReadTableStruct(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := s.TmpPath()
	tableRows := make([]tableRow, 3)
	for i := range tableRows {
		tableRows[i].Init()
	}
	mustCreateTableAndWriteRowsYSON(ctx, t, yc, testTable, tableRows)

	readTableRows := mustReadRowsFormat[tableRow](ctx, t, yc, testTable, skiff.MustInferFormat(&tableRow{}))

	var expectedTableRows []tableRow
	for _, row := range tableRows {
		expectedRow := row
		expectedRow.IgnoredField = ""
		expectedTableRows = append(expectedTableRows, expectedRow)
	}
	require.Equal(t, len(tableRows), len(readTableRows))
	for i, expectedTableRow := range expectedTableRows {
		require.True(t, expectedTableRow.EqualTo(readTableRows[i]), "expected:\n %+v\n actual:\n %+v", expectedTableRow, readTableRows[i])
	}
}

func (s *Suite) TestSkiffReadTableMap(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := s.TmpPath()
	tableRows := make([]tableRow, 3)
	for i := range tableRows {
		tableRows[i].Init()
	}
	mustCreateTableAndWriteRowsYSON(ctx, t, yc, testTable, tableRows)

	ysonReadRows := mustReadRowsYSON[map[string]any](ctx, t, yc, testTable)
	skiffReadRows := mustReadRowsFormat[map[string]any](ctx, t, yc, testTable, skiff.MustInferFormat(&tableRow{}))

	require.Equal(t, len(ysonReadRows), len(skiffReadRows))
	for i := range skiffReadRows {
		requireMapsEqual(t, ysonReadRows[i], skiffReadRows[i])
	}
}

func (s *Suite) TestSkiffReadTableMapWithComplexTypes(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := s.TmpPath()
	mustCreateTable(ctx, t, yc, testTable, schemaWithComplexTypes)
	mustWriteRowsYSON(ctx, t, yc, testTable, rowsWithComplexTypes)

	ysonReadRows := mustReadRowsYSON[map[string]any](ctx, t, yc, testTable)
	skiffReadRows := mustReadRowsFormat[map[string]any](ctx, t, yc, testTable, skiff.Format{
		Name:         "skiff",
		TableSchemas: []any{ptr.T(skiff.FromTableSchema(schemaWithComplexTypes))},
	})

	require.Equal(t, len(ysonReadRows), len(skiffReadRows))
	for i := range skiffReadRows {
		requireMapsEqual(t, ysonReadRows[i], skiffReadRows[i])
	}
}

func (s *Suite) TestSkiffReadTableMapWeakSchema(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTableWithWeakSchema := s.TmpPath()
	_, err := yt.CreateTable(ctx, yc, testTableWithWeakSchema)
	require.NoError(t, err)

	tableRows := make([]tableRow, 3)
	for i := range tableRows {
		tableRows[i].Init()
	}
	mustWriteRowsYSON(ctx, t, yc, testTableWithWeakSchema, tableRows)

	ysonReadRows := mustReadRowsYSON[map[string]any](ctx, t, yc, testTableWithWeakSchema)
	skiffReadRows := mustReadRowsFormat[map[string]any](ctx, t, yc, testTableWithWeakSchema, skiff.MustInferFormat(&tableRow{}))

	require.Equal(t, len(ysonReadRows), len(skiffReadRows))
	for i := range skiffReadRows {
		requireMapsEqual(t, ysonReadRows[i], skiffReadRows[i])
	}
}

func (s *Suite) TestSkiffReadTableCompatibleStructs(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	type firstStruct struct {
		Int8   int8   `yson:"int"`
		String string `yson:"string"`
	}
	type secondStruct struct {
		Int32 int32 `yson:"int"`
	}

	firstTableRows := make([]firstStruct, 3)
	for i := range firstTableRows {
		firstTableRows[i] = firstStruct{Int8: 10, String: "str1"}
	}
	firstTestTable := s.TmpPath()
	mustCreateTableAndWriteRowsYSON(ctx, t, yc, firstTestTable, firstTableRows)

	secondStructReadRows := mustReadRowsFormat[secondStruct](ctx, t, yc, firstTestTable, skiff.MustInferFormat(&secondStruct{}))
	secondStructFirstFormatReadRows := mustReadRowsFormat[secondStruct](ctx, t, yc, firstTestTable, skiff.MustInferFormat(&firstStruct{}))

	for i := range secondStructReadRows {
		require.Equal(t, secondStruct{Int32: int32(firstTableRows[i].Int8)}, secondStructReadRows[i])
		require.Equal(t, secondStructReadRows[i], secondStructFirstFormatReadRows[i])
	}

	secondTableRows := make([]secondStruct, 3)
	for i := range firstTableRows {
		secondTableRows[i] = secondStruct{Int32: 20}
	}
	secondTestTable := s.TmpPath()
	mustCreateTableAndWriteRowsYSON(ctx, t, yc, secondTestTable, secondTableRows)

	firstStructSecondFormatReadRows := mustReadRowsFormat[firstStruct](ctx, t, yc, secondTestTable, skiff.MustInferFormat(&secondStruct{}))

	for i := range firstStructSecondFormatReadRows {
		require.Equal(t, firstStruct{Int8: int8(secondTableRows[i].Int32)}, firstStructSecondFormatReadRows[i])
	}
}

func (s *Suite) TestSkiffReadTableIncompatibleStructs(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	type firstStruct struct {
		Int int `yson:"int"`
	}
	type secondStruct struct {
		String string `yson:"int"`
	}

	tableRows := make([]firstStruct, 3)
	for i := range tableRows {
		tableRows[i] = firstStruct{Int: 1000}
	}
	testTable := s.TmpPath()
	mustCreateTableAndWriteRowsYSON(ctx, t, yc, testTable, tableRows)

	_, err := readRowsFormat[secondStruct](ctx, yc, testTable, skiff.MustInferFormat(&firstStruct{}))
	require.Error(t, err)
	require.ErrorContains(t, err, "type string is not compatible with wire type int64")
}

func (s *Suite) TestSkiffReadTableIntegerOverflow(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	type int32Struct struct {
		Int int32 `yson:"int"`
	}
	type int8Struct struct {
		Int int8 `yson:"int"`
	}

	tableRows := make([]int32Struct, 3)
	for i := range tableRows {
		tableRows[i] = int32Struct{Int: math.MaxInt32}
	}
	testTable := s.TmpPath()
	mustCreateTableAndWriteRowsYSON(ctx, t, yc, testTable, tableRows)

	_, err := readRowsFormat[int8Struct](ctx, yc, testTable, skiff.MustInferFormat(&int32Struct{}))
	require.Error(t, err)
	require.ErrorContains(t, err, fmt.Sprintf("value %d overflows type int8", math.MaxInt32))
}

func (s *Suite) TestSkiffWriteTableStruct(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := s.TmpPath()
	tableRows := make([]tableRow, 3)
	for i := range tableRows {
		tableRows[i].Init()
	}

	mustCreateTableAndWriteRowsFormat(ctx, t, yc, testTable, tableRows, skiff.MustInferFormat(&tableRow{}))

	readTableRows := mustReadRowsYSON[tableRow](ctx, t, yc, testTable)

	var expectedTableRows []tableRow
	for _, row := range tableRows {
		expectedRow := row
		expectedRow.IgnoredField = ""
		expectedTableRows = append(expectedTableRows, expectedRow)
	}
	require.Equal(t, len(tableRows), len(readTableRows))
	for i, expectedTableRow := range expectedTableRows {
		require.True(t, expectedTableRow.EqualTo(readTableRows[i]), "expected:\n %+v\n actual:\n %+v", expectedTableRow, readTableRows[i])
	}
}

func (s *Suite) TestSkiffWriteTableMap(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	tableRows := make([]map[string]any, 3)
	for i := range tableRows {
		tr := tableRow{}
		tr.Init()
		tableRows[i] = asMap(tr)
	}

	testTableYSON := s.TmpPath()
	testTableSkiff := s.TmpPath()
	mustCreateTable(ctx, t, yc, testTableYSON, schema.MustInfer(&tableRow{}))
	mustCreateTable(ctx, t, yc, testTableSkiff, schema.MustInfer(&tableRow{}))
	mustWriteRowsFormat(ctx, t, yc, testTableYSON, tableRows, nil)
	mustWriteRowsFormat(ctx, t, yc, testTableSkiff, tableRows, skiff.MustInferFormat(&tableRow{}))

	rowsWrittenInYSON := mustReadRowsYSON[map[string]any](ctx, t, yc, testTableYSON)
	rowsWrittenInSkiff := mustReadRowsYSON[map[string]any](ctx, t, yc, testTableSkiff)

	require.Equal(t, len(rowsWrittenInYSON), len(rowsWrittenInSkiff))
	for i := range rowsWrittenInSkiff {
		requireMapsEqual(t, rowsWrittenInYSON[i], rowsWrittenInSkiff[i])
	}
}

func (s *Suite) TestSkiffHighLevelTableWriter(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := s.TmpPath()
	tableRows := make([]tableRow, 3)
	for i := range tableRows {
		tableRows[i].Init()
	}

	w, err := yt.WriteTable(ctx, yc, testTable, yt.WithWriteTableFormat(skiff.MustInferFormat(&tableRow{})))
	require.NoError(t, err)
	for i := range tableRows {
		require.NoError(t, w.Write(tableRows[i]))
	}
	require.NoError(t, w.Commit())

	readTableRows := mustReadRowsYSON[tableRow](ctx, t, yc, testTable)

	var expectedTableRows []tableRow
	for _, row := range tableRows {
		expectedRow := row
		expectedRow.IgnoredField = ""
		expectedTableRows = append(expectedTableRows, expectedRow)
	}
	require.Equal(t, len(tableRows), len(readTableRows))
	for i, expectedTableRow := range expectedTableRows {
		require.True(t, expectedTableRow.EqualTo(readTableRows[i]), "expected:\n %+v\n actual:\n %+v", expectedTableRow, readTableRows[i])
	}
}

func (s *Suite) TestSkiffWriteTableCompatibleStructs(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	type firstStruct struct {
		Int8   int8    `yson:"int"`
		String *string `yson:"string"`
	}
	type secondStruct struct {
		Int32 int32 `yson:"int"`
	}

	firstTestTable := s.TmpPath()
	w, err := yt.WriteTable(ctx, yc, firstTestTable,
		yt.WithCreateOptions(yt.WithInferredSchema(firstStruct{})),
		yt.WithWriteTableFormat(skiff.MustInferFormat(firstStruct{})),
	)
	require.NoError(t, err)
	require.NoError(t, w.Write(secondStruct{Int32: 42}))
	require.NoError(t, w.Commit())

	w, err = yt.WriteTable(ctx, yc, firstTestTable,
		yt.WithExistingTable(),
		yt.WithAppend(),
		yt.WithWriteTableFormat(skiff.MustInferFormat(secondStruct{})),
	)
	require.NoError(t, err)
	require.NoError(t, w.Write(secondStruct{Int32: -7}))
	require.NoError(t, w.Commit())

	firstTableReadRows := mustReadRowsYSON[firstStruct](ctx, t, yc, firstTestTable)
	require.Equal(t, []firstStruct{firstStruct{Int8: 42}, firstStruct{Int8: -7}}, firstTableReadRows)

	secondTestTable := s.TmpPath()
	w, err = yt.WriteTable(ctx, yc, secondTestTable,
		yt.WithCreateOptions(yt.WithInferredSchema(secondStruct{})),
		yt.WithWriteTableFormat(skiff.MustInferFormat(secondStruct{})),
	)
	require.NoError(t, err)
	require.NoError(t, w.Write(firstStruct{Int8: -15}))
	require.NoError(t, w.Commit())

	secondTableReadRows := mustReadRowsYSON[secondStruct](ctx, t, yc, secondTestTable)
	require.Equal(t, []secondStruct{secondStruct{Int32: -15}}, secondTableReadRows)
}

func (s *Suite) TestSkiffWriteTableIncompatibleStructs(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	type firstStruct struct {
		Int int `yson:"int"`
	}
	type secondStruct struct {
		String string `yson:"int"`
	}

	testTable := s.TmpPath()
	w, err := yt.WriteTable(ctx, yc, testTable,
		yt.WithCreateOptions(yt.WithInferredSchema(firstStruct{})),
		yt.WithWriteTableFormat(skiff.MustInferFormat(firstStruct{})),
	)
	defer w.Rollback()
	require.NoError(t, err)

	require.NoError(t, w.Write(firstStruct{Int: 20}))
	err = w.Write(secondStruct{String: "some-str"})
	require.Error(t, err)
	require.ErrorContains(t, err, "type string is not compatible with wire type int64")
}

func (s *Suite) TestSkiffWriteTableIntegerOverflow(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	type int32Struct struct {
		Int int32 `yson:"int"`
	}
	type int8Struct struct {
		Int int8 `yson:"int"`
	}

	testTable := s.TmpPath()
	w, err := yt.WriteTable(ctx, yc, testTable,
		yt.WithCreateOptions(yt.WithInferredSchema(int8Struct{})),
		yt.WithWriteTableFormat(skiff.MustInferFormat(int8Struct{})),
	)
	defer w.Rollback()
	require.NoError(t, err)

	require.NoError(t, w.Write(int32Struct{Int: 20}))
	err = w.Write(int32Struct{Int: math.MaxInt32})
	require.Error(t, err)
	require.ErrorContains(t, err, fmt.Sprintf("value %d overflows type int8", math.MaxInt32))
}

func mustCreateTableAndWriteRowsYSON[T any](ctx context.Context, t *testing.T, yc yt.Client, testTable ypath.Path, rows []T) {
	t.Helper()
	var row T
	mustCreateTable(ctx, t, yc, testTable, schema.MustInfer(row))
	mustWriteRowsFormat[T](ctx, t, yc, testTable, rows, nil)
}

func mustCreateTable(ctx context.Context, t *testing.T, yc yt.Client, testTable ypath.Path, schema schema.Schema) {
	t.Helper()
	_, err := yt.CreateTable(ctx, yc, testTable, yt.WithSchema(schema))
	require.NoError(t, err)
}

func mustCreateTableAndWriteRowsFormat[T any](ctx context.Context, t *testing.T, yc yt.Client, testTable ypath.Path, rows []T, format any) {
	t.Helper()
	var v T
	mustCreateTable(ctx, t, yc, testTable, schema.MustInfer(&v))
	mustWriteRowsFormat(ctx, t, yc, testTable, rows, format)
}

func mustWriteRowsYSON[T any](ctx context.Context, t *testing.T, yc yt.Client, testTable ypath.Path, rows []T) {
	t.Helper()
	mustWriteRowsFormat(ctx, t, yc, testTable, rows, nil)
}

func mustWriteRowsFormat[T any](ctx context.Context, t *testing.T, yc yt.Client, testTable ypath.Path, rows []T, format any) {
	t.Helper()
	require.NoError(t, writeRowsFormat(ctx, yc, testTable, rows, format))
}

func writeRowsFormat[T any](ctx context.Context, yc yt.Client, testTable ypath.Path, rows []T, format any) error {
	w, err := yc.WriteTable(ctx, testTable, &yt.WriteTableOptions{Format: format})
	if err != nil {
		return err
	}

	for i := range rows {
		if err := w.Write(rows[i]); err != nil {
			return err
		}
	}
	return w.Commit()
}

func mustReadRowsYSON[T any](ctx context.Context, t *testing.T, yc yt.Client, testTable ypath.YPath) []T {
	t.Helper()
	return mustReadRowsFormat[T](ctx, t, yc, testTable, nil)
}

func mustReadRowsFormat[T any](ctx context.Context, t *testing.T, yc yt.Client, testTable ypath.YPath, format any) []T {
	t.Helper()
	rows, err := readRowsFormat[T](ctx, yc, testTable, format)
	require.NoError(t, err)
	return rows
}

func readRowsFormat[T any](ctx context.Context, yc yt.Client, testTable ypath.YPath, format any) ([]T, error) {
	opts := &yt.ReadTableOptions{}
	if format != nil {
		opts.Format = format
	}
	reader, err := yc.ReadTable(ctx, testTable, opts)
	if err != nil {
		return nil, err
	}

	var readRows []T
	for reader.Next() {
		var row T
		if err := reader.Scan(&row); err != nil {
			return nil, err
		}
		readRows = append(readRows, row)
	}
	return readRows, reader.Close()
}

func requireMapsEqual(t *testing.T, expected, actual map[string]any) {
	require.Equal(t, len(expected), len(actual))
	for k := range expected {
		require.Contains(t, actual, k)
		expectedValue := expected[k]
		actualValue := actual[k]
		if _, ok := expectedValue.(float64); ok {
			_, ok := actualValue.(float64)
			require.True(t, ok)
			require.InDelta(t, expectedValue, actualValue, 0.000001, "column: %q", k)
		} else {
			require.Equal(t, expectedValue, actualValue, "column: %q", k)
		}
	}
}

func ytInterval(duration time.Duration) schema.Interval {
	res, err := schema.NewInterval(duration)
	if err != nil {
		panic(err)
	}
	return res
}

func mustParseTime(value string) time.Time {
	t, err := time.Parse(time.DateTime, value)
	if err != nil {
		panic(err)
	}
	return t
}
