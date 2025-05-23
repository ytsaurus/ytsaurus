package integration

import (
	"context"
	"fmt"
	"math"
	"strconv"
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

type ysonStreamUnmarshaler struct {
	str string
}

func (u *ysonStreamUnmarshaler) UnmarshalYSON(r *yson.Reader) error {
	u.str = strconv.FormatInt(r.Int64(), 10)
	return nil
}

func (u *ysonStreamUnmarshaler) MarshalYSON(w *yson.Writer) error {
	i, err := strconv.ParseInt(u.str, 10, 64)
	if err != nil {
		return err
	}
	w.Int64(i)
	return nil
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
		{Name: "SkiffReadTableCompatibleStructs", Test: suite.TestSkiffReadTableCompatibleStructs, SkipRPC: true},
		{Name: "SkiffReadTableIncompatibleStructs", Test: suite.TestSkiffReadTableIncompatibleStructs, SkipRPC: true},
		{Name: "SkiffReadTableIntegerOverflow", Test: suite.TestSkiffReadTableIntegerOverflow, SkipRPC: true},
	})
}

func (s *Suite) TestSkiffReadTableStruct(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := s.TmpPath()
	tableRows := make([]tableRow, 3)
	for i := range tableRows {
		tableRows[i].Init()
	}
	mustWriteRowsYSON(ctx, t, yc, testTable, tableRows)

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
	mustWriteRowsYSON(ctx, t, yc, testTable, tableRows)

	ysonReadRows := mustReadRowsYSON[map[string]any](ctx, t, yc, testTable)
	skiffReadRows := mustReadRowsFormat[map[string]any](ctx, t, yc, testTable, skiff.MustInferFormat(&tableRow{}))

	require.Equal(t, len(ysonReadRows), len(skiffReadRows))
	for i := range skiffReadRows {
		expectedRow := ysonReadRows[i]
		require.Equal(t, len(expectedRow), len(skiffReadRows[i]))
		for k := range expectedRow {
			require.Contains(t, skiffReadRows[i], k)
			expectedValue := expectedRow[k]
			actualValue := skiffReadRows[i][k]
			if _, ok := expectedValue.(float64); ok {
				_, ok := actualValue.(float64)
				require.True(t, ok)
				require.InDelta(t, expectedValue, actualValue, 0.000001)
			} else {
				require.Equal(t, expectedValue, actualValue)
			}
		}
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
	mustWriteRowsYSON(ctx, t, yc, firstTestTable, firstTableRows)

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
	mustWriteRowsYSON(ctx, t, yc, secondTestTable, secondTableRows)

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
	mustWriteRowsYSON(ctx, t, yc, testTable, tableRows)

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
	mustWriteRowsYSON(ctx, t, yc, testTable, tableRows)

	_, err := readRowsFormat[int8Struct](ctx, yc, testTable, skiff.MustInferFormat(&int32Struct{}))
	require.Error(t, err)
	require.ErrorContains(t, err, fmt.Sprintf("value %d overflows type int8", math.MaxInt32))
}

func mustWriteRowsYSON[T any](ctx context.Context, t *testing.T, yc yt.Client, testTable ypath.Path, rows []T) {
	require.NoError(t, writeRowsFormat(ctx, t, yc, testTable, rows))
}

func writeRowsFormat[T any](ctx context.Context, t *testing.T, yc yt.Client, testTable ypath.Path, rows []T) error {
	if _, err := yt.CreateTable(ctx, yc, testTable, yt.WithSchema(schema.MustInfer(rows[0]))); err != nil {
		return err
	}

	w, err := yc.WriteTable(ctx, testTable, &yt.WriteTableOptions{})
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
	return mustReadRowsFormat[T](ctx, t, yc, testTable, nil)
}

func mustReadRowsFormat[T any](ctx context.Context, t *testing.T, yc yt.Client, testTable ypath.YPath, format any) []T {
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
