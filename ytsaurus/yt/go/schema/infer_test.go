package schema

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type innerStruct struct {
	Field int
}

type testBasicTypes struct {
	I   int
	I64 int64
	I32 int32
	I16 int16

	U   uint
	U64 uint64
	U32 uint32
	U16 uint16

	F64 float64
	F32 float32

	S string
	B []byte
	innerStruct

	A0 any
	A1 innerStruct
	A2 map[string]any
	A3 [3]any

	T0 time.Time
}

func TestInfer(t *testing.T) {
	s, err := Infer(&testBasicTypes{})
	require.NoError(t, err)

	require.Equal(t, s, Schema{
		Columns: []Column{
			{Name: "I", Type: TypeInt64, Required: true},
			{Name: "I64", Type: TypeInt64, Required: true},
			{Name: "I32", Type: TypeInt32, Required: true},
			{Name: "I16", Type: TypeInt16, Required: true},
			{Name: "U", Type: TypeUint64, Required: true},
			{Name: "U64", Type: TypeUint64, Required: true},
			{Name: "U32", Type: TypeUint32, Required: true},
			{Name: "U16", Type: TypeUint16, Required: true},
			{Name: "F64", Type: TypeFloat64, Required: true},
			{Name: "F32", Type: TypeFloat32, Required: true},
			{Name: "S", Type: TypeString, Required: true},
			{Name: "B", Type: TypeBytes, Required: true},
			{Name: "Field", Type: TypeInt64, Required: true},
			{Name: "A0", Type: TypeAny},
			{Name: "A1", Type: TypeAny},
			{Name: "A2", Type: TypeAny},
			{Name: "A3", Type: TypeAny},
			{Name: "T0", Type: TypeString, Required: true},
		},
	})
}

type InnerA struct {
	A int
	B *int
}

type InnerD struct {
	D string
}

type InnerB struct {
	C string
	InnerD
}

type testEmbedding struct {
	InnerA
	*InnerB
}

func TestInferEmbedding(t *testing.T) {
	s, err := Infer(&testEmbedding{})
	require.NoError(t, err)

	require.Equal(t, s, Schema{
		Columns: []Column{
			{Name: "A", Type: TypeInt64, Required: true},
			{Name: "B", Type: TypeInt64, Required: false},
			{Name: "C", Type: TypeString, Required: false},
			{Name: "D", Type: TypeString, Required: false},
		},
	})
}

func TestInfer_nonStructEmbedding(t *testing.T) {
	type S string
	type A struct {
		S
	}
	type B struct {
		A
	}

	_, err := Infer(&B{})
	require.Error(t, err)
}

type testPrivateFields struct {
	I int64
	i int64
}

func TestPrivateFields(t *testing.T) {
	s, err := Infer(&testPrivateFields{i: 1})
	require.NoError(t, err)

	require.Equal(t, s, Schema{
		Columns: []Column{
			{Name: "I", Type: TypeInt64, Required: true},
		},
	})
}

func TestInferType(t *testing.T) {
	var a int
	_, err := Infer(a)
	require.Error(t, err)

	var b string
	_, err = Infer(b)
	require.Error(t, err)

	var c any
	_, err = Infer(c)
	require.Error(t, err)

	d := make(map[any]any)
	_, err = Infer(d)
	require.Error(t, err)
}

type textMarshaler struct{}

func (*textMarshaler) MarshalText() (text []byte, err error) {
	panic("implement me")
}

type valueTextMarshaler struct{}

func (valueTextMarshaler) MarshalText() (text []byte, err error) {
	panic("implement me")
}

type binaryMarshaler struct{}

func (*binaryMarshaler) MarshalBinary() (text []byte, err error) {
	panic("implement me")
}

type valueBinaryMarshaler struct{}

func (valueBinaryMarshaler) MarshalBinary() (text []byte, err error) {
	panic("implement me")
}

type testMarshalers struct {
	M0 textMarshaler
	M1 valueTextMarshaler
	M2 binaryMarshaler
	M3 valueBinaryMarshaler

	O0 *textMarshaler
	O1 *valueTextMarshaler
	O2 *binaryMarshaler
	O3 *valueBinaryMarshaler
}

func TestInferMarshalerTypes(t *testing.T) {
	v := &testMarshalers{}

	s, err := Infer(v)
	require.NoError(t, err)

	require.Equal(t, s, Schema{
		Columns: []Column{
			{Name: "M0", Type: TypeString, Required: true},
			{Name: "M1", Type: TypeString, Required: true},
			{Name: "M2", Type: TypeBytes, Required: true},
			{Name: "M3", Type: TypeBytes, Required: true},

			{Name: "O0", Type: TypeString},
			{Name: "O1", Type: TypeString},
			{Name: "O2", Type: TypeBytes},
			{Name: "O3", Type: TypeBytes},
		},
	})
}

type tagStruct struct {
	A int `yson:"a"`
	B int `yson:"-"`
}

func TestInferTags(t *testing.T) {
	s, err := Infer(&tagStruct{})
	require.NoError(t, err)

	require.Equal(t, s, Schema{
		Columns: []Column{
			{Name: "a", Type: TypeInt64, Required: true},
		},
	})
}

type keyStruct struct {
	A int `yson:"a,key"`
	B int `yson:"b"`
}

func TestInferKeyColumns(t *testing.T) {
	s, err := Infer(&keyStruct{})
	require.NoError(t, err)

	require.Equal(t, s, Schema{
		Columns: []Column{
			{Name: "a", Type: TypeInt64, Required: true, SortOrder: SortAscending},
			{Name: "b", Type: TypeInt64, Required: true},
		},
	})
}

type keyDefaultStruct struct {
	A int `yson:",key"`
}

func TestInferKeyWithDefaultName(t *testing.T) {
	s, err := Infer(&keyDefaultStruct{})
	require.NoError(t, err)

	require.Equal(t, s, Schema{
		Columns: []Column{
			{Name: "A", Type: TypeInt64, Required: true, SortOrder: SortAscending},
		},
	})
}

type orderedTableStruct struct {
	TabletIndex int    `yson:"$tablet_index"`
	RowIndex    int    `yson:"$row_index"`
	Value       string `yson:"value"`
}

func TestInferOrderedTable(t *testing.T) {
	s, err := Infer(&orderedTableStruct{})
	require.NoError(t, err)

	require.Equal(t, s, Schema{
		Columns: []Column{
			{Name: "value", Type: TypeString, Required: true},
		},
	})
}

func TestInferMapType(t *testing.T) {
	var a int
	_, err := Infer(a)
	require.Error(t, err)

	var b string
	_, err = Infer(b)
	require.Error(t, err)

	var c any
	_, err = Infer(c)
	require.Error(t, err)

	_, err = InferMap(&testBasicTypes{})
	require.Error(t, err)
}

func TestInferMapNonStringKeys(t *testing.T) {
	a := make(map[int]any)
	_, err := Infer(a)
	require.Error(t, err)
}

func TestInferMap(t *testing.T) {
	var I int
	var I64 int64
	var I32 int32
	var I16 int16

	var U uint
	var U64 uint64
	var U32 uint32
	var U16 uint16

	var S string
	var B []byte

	var A0 any
	var A1 innerStruct
	var A2 map[string]any

	testMap := make(map[string]any)
	testMap["I"] = I
	testMap["I64"] = I64
	testMap["I32"] = I32
	testMap["I16"] = I16

	testMap["U"] = U
	testMap["U64"] = U64
	testMap["U32"] = U32
	testMap["U16"] = U16

	testMap["S"] = S
	testMap["B"] = B

	testMap["A0"] = A0
	testMap["A1"] = A1
	testMap["A2"] = A2

	s, err := InferMap(testMap)
	require.NoError(t, err)

	columnsExpected := []Column{
		{Name: "I", Type: TypeInt64, Required: true},
		{Name: "I64", Type: TypeInt64, Required: true},
		{Name: "I32", Type: TypeInt32, Required: true},
		{Name: "I16", Type: TypeInt16, Required: true},
		{Name: "U", Type: TypeUint64, Required: true},
		{Name: "U64", Type: TypeUint64, Required: true},
		{Name: "U32", Type: TypeUint32, Required: true},
		{Name: "U16", Type: TypeUint16, Required: true},
		{Name: "S", Type: TypeString, Required: true},
		{Name: "B", Type: TypeBytes, Required: true},
		{Name: "A0", Type: TypeAny},
		{Name: "A1", Type: TypeAny},
		{Name: "A2", Type: TypeAny},
	}
	sort.Slice(columnsExpected, func(i, j int) bool {
		return columnsExpected[i].Name < columnsExpected[j].Name
	})

	require.Equal(t, s, Schema{Columns: columnsExpected})
}

type testTimeTypes struct {
	D0 Date
	D1 Datetime
	D2 Timestamp
	D3 Interval
}

func TestInferTime(t *testing.T) {
	s, err := Infer(&testTimeTypes{})
	require.NoError(t, err)

	require.Equal(t, s, Schema{
		Columns: []Column{
			{Name: "D0", Type: TypeDate, Required: true},
			{Name: "D1", Type: TypeDatetime, Required: true},
			{Name: "D2", Type: TypeTimestamp, Required: true},
			{Name: "D3", Type: TypeInterval, Required: true},
		},
	})
}

type testColumnGroup struct {
	C0 int `ytgroup:"a"`
	C1 int `ytgroup:"b"`
	C2 int
}

func TestInferColumnGroup(t *testing.T) {
	s, err := Infer(&testColumnGroup{})
	require.NoError(t, err)

	require.Equal(t, s, Schema{
		Columns: []Column{
			{Name: "C0", Type: TypeInt64, Required: true, Group: "a"},
			{Name: "C1", Type: TypeInt64, Required: true, Group: "b"},
			{Name: "C2", Type: TypeInt64, Required: true},
		},
	})
}
