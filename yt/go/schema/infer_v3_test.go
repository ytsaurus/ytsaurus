package schema

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yterrors"
)

func TestInferV3(t *testing.T) {
	s, err := InferV3(&testBasicTypes{})
	require.NoError(t, err)

	require.Equal(t, Schema{
		Columns: []Column{
			{Name: "I", ComplexType: TypeInt64},
			{Name: "I64", ComplexType: TypeInt64},
			{Name: "I32", ComplexType: TypeInt32},
			{Name: "I16", ComplexType: TypeInt16},
			{Name: "U", ComplexType: TypeUint64},
			{Name: "U64", ComplexType: TypeUint64},
			{Name: "U32", ComplexType: TypeUint32},
			{Name: "U16", ComplexType: TypeUint16},
			{Name: "F64", ComplexType: TypeFloat64},
			{Name: "F32", ComplexType: TypeFloat32},
			{Name: "S", ComplexType: TypeString},
			{Name: "B", ComplexType: TypeBytes},
			{Name: "Field", ComplexType: TypeInt64},
			{Name: "A0", ComplexType: typeYSON},
			{Name: "A1", ComplexType: Struct{
				Members: []StructMember{
					{Name: "Field", Type: TypeInt64},
				}},
			},
			{Name: "A2", ComplexType: Dict{Key: TypeBytes, Value: typeYSON}},
			{Name: "A3", ComplexType: List{Item: typeYSON}},
			{Name: "T0", ComplexType: TypeString},
		},
	}, s)
}

func TestInferV3Embedding(t *testing.T) {
	s, err := InferV3(&testEmbedding{})
	require.NoError(t, err)

	require.Equal(t, Schema{
		Columns: []Column{
			{Name: "A", ComplexType: TypeInt64},
			{Name: "B", ComplexType: Optional{TypeInt64}},
			{Name: "C", ComplexType: Optional{TypeString}},
			{Name: "D", ComplexType: Optional{TypeString}},
		},
	}, s)
}

func TestInferV3_nonStructEmbedding(t *testing.T) {
	type S string
	type A struct {
		S
	}
	type B struct {
		A
	}

	_, err := InferV3(&B{})
	require.Error(t, err)
}

func TestPrivateFieldsV3(t *testing.T) {
	s, err := InferV3(&testPrivateFields{i: 1})
	require.NoError(t, err)

	require.Equal(t, Schema{
		Columns: []Column{
			{Name: "I", ComplexType: TypeInt64},
		},
	}, s)
}

func TestInferV3Type(t *testing.T) {
	var a int
	_, err := InferV3(a)
	require.Error(t, err)

	var b string
	_, err = InferV3(b)
	require.Error(t, err)

	var c any
	_, err = InferV3(c)
	require.Error(t, err)

	d := make(map[any]any)
	_, err = InferV3(d)
	require.Error(t, err)
}

func TestInferV3MarshalerTypes(t *testing.T) {
	v := &testMarshalers{}

	s, err := InferV3(v)
	require.NoError(t, err)

	require.Equal(t, Schema{
		Columns: []Column{
			{Name: "M0", ComplexType: TypeString},
			{Name: "M1", ComplexType: TypeString},
			{Name: "M2", ComplexType: TypeBytes},
			{Name: "M3", ComplexType: TypeBytes},

			{Name: "O0", ComplexType: Optional{TypeString}},
			{Name: "O1", ComplexType: Optional{TypeString}},
			{Name: "O2", ComplexType: Optional{TypeBytes}},
			{Name: "O3", ComplexType: Optional{TypeBytes}},
		},
	}, s)
}

func TestInferV3Tags(t *testing.T) {
	s, err := InferV3(&tagStruct{})
	require.NoError(t, err)

	require.Equal(t, Schema{
		Columns: []Column{
			{Name: "a", ComplexType: TypeInt64},
		},
	}, s)
}

func TestInferV3KeyColumns(t *testing.T) {
	s, err := InferV3(&keyStruct{})
	require.NoError(t, err)

	require.Equal(t, Schema{
		Columns: []Column{
			{Name: "a", ComplexType: TypeInt64, SortOrder: SortAscending},
			{Name: "b", ComplexType: TypeInt64},
		},
	}, s)
}

func TestInferV3KeyWithDefaultName(t *testing.T) {
	s, err := InferV3(&keyDefaultStruct{})
	require.NoError(t, err)

	require.Equal(t, Schema{
		Columns: []Column{
			{Name: "A", ComplexType: TypeInt64, SortOrder: SortAscending},
		},
	}, s)
}

func TestInferV3OrderedTable(t *testing.T) {
	s, err := InferV3(&orderedTableStruct{})
	require.NoError(t, err)

	require.Equal(t, Schema{
		Columns: []Column{
			{Name: "value", ComplexType: TypeString},
		},
	}, s)
}

func TestInferV3MapType(t *testing.T) {
	var a int
	_, err := InferV3Map(a)
	require.Error(t, err)

	var b string
	_, err = InferV3Map(b)
	require.Error(t, err)

	var c any
	_, err = InferV3Map(c)
	require.Error(t, err)

	_, err = InferV3Map(&testBasicTypes{})
	require.Error(t, err)
}

func TestInferV3MapNonStringKeys(t *testing.T) {
	a := make(map[int]any)
	// Notice if map is empty it will be interface(map[int]any) type and InferV3Map will return nil instead of error.
	a[1] = "some"
	_, err := InferV3Map(a)
	require.Error(t, err)
}

func TestInferV3Map(t *testing.T) {
	testMap := generateMapStringToAny()

	s, err := InferV3Map(testMap)
	require.NoError(t, err)

	columnsExpected := []Column{
		{Name: "I", ComplexType: TypeInt64},
		{Name: "I64", ComplexType: TypeInt64},
		{Name: "I32", ComplexType: TypeInt32},
		{Name: "I16", ComplexType: TypeInt16},
		{Name: "U", ComplexType: TypeUint64},
		{Name: "U64", ComplexType: TypeUint64},
		{Name: "U32", ComplexType: TypeUint32},
		{Name: "U16", ComplexType: TypeUint16},
		{Name: "S", ComplexType: TypeString},
		{Name: "B", ComplexType: TypeBytes},
		{Name: "A0", ComplexType: typeYSON},
		{Name: "A1", ComplexType: Struct{
			Members: []StructMember{
				{Name: "Field", Type: TypeInt64},
			}},
		},
		{Name: "A2", ComplexType: Dict{Key: TypeBytes, Value: typeYSON}},
	}
	sort.Slice(columnsExpected, func(i, j int) bool {
		return columnsExpected[i].Name < columnsExpected[j].Name
	})

	require.Equal(t, Schema{Columns: columnsExpected}, s)
}

func TestInferV3Time(t *testing.T) {
	s, err := InferV3(&testTimeTypes{})
	require.NoError(t, err)

	require.Equal(t, Schema{
		Columns: []Column{
			{Name: "D0", ComplexType: TypeDate},
			{Name: "D1", ComplexType: TypeDatetime},
			{Name: "D2", ComplexType: TypeTimestamp},
			{Name: "D3", ComplexType: TypeInterval},
		},
	}, s)
}

func TestInferV3ColumnGroup(t *testing.T) {
	s, err := InferV3(&testColumnGroup{})
	require.NoError(t, err)

	require.Equal(t, Schema{
		Columns: []Column{
			{Name: "C0", ComplexType: TypeInt64, Group: "a"},
			{Name: "C1", ComplexType: TypeInt64, Group: "b"},
			{Name: "C2", ComplexType: TypeInt64},
		},
	}, s)
}

type withYTError struct {
	Error *yterrors.Error `yson:"error"`
}

func TestInferV3Error(t *testing.T) {
	s, err := InferV3(&withYTError{})
	require.NoError(t, err)

	require.Equal(t, Schema{
		Columns: []Column{
			{Name: "error", ComplexType: Optional{Struct{
				Members: []StructMember{
					{Name: "Code", Type: TypeInt64},
					{Name: "Message", Type: TypeString},
					{Name: "Attributes", Type: Dict{Key: TypeBytes, Value: typeYSON}},
					{Name: "InnerErrors", Type: List{Optional{Item: typeYSON}}},
				},
			}}},
		},
	}, s)
}

type someInterface interface {
	Method()
}

type specialStruct struct {
	someInterface `yson:"role"`
}

func TestInferV3Special(t *testing.T) {
	s, err := InferV3(&specialStruct{})
	require.NoError(t, err)

	require.Equal(t, Schema{
		Columns: []Column{
			{Name: "role", ComplexType: typeYSON},
		},
	}, s)
}
