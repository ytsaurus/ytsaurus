package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/yson"
)

func TestSchemaMarshalYSON(t *testing.T) {
	s := MustInfer(&testBasicTypes{})

	ys, err := yson.Marshal(&s)
	require.NoError(t, err)

	var s1 Schema
	err = yson.Unmarshal(ys, &s1)
	require.NoError(t, err)

	require.Equal(t, s, s1)
}

func TestSchemaEquality(t *testing.T) {
	s0 := MustInfer(&testBasicTypes{})

	s1 := s0
	s1.Strict = ptr.Bool(true)

	require.True(t, s0.Equal(s1))
	require.False(t, s0.Equal(s1.WithUniqueKeys()))
}

func TestMergeSameSchemas(t *testing.T) {
	s0 := MustInfer(&testBasicTypes{})
	s1 := MustInfer(&testBasicTypes{})
	require.Equal(t, MergeSchemas(s0, s1), s0)
}

func TestMergeSchemas(t *testing.T) {
	s0 := Schema{
		Strict: ptr.Bool(true),
		Columns: []Column{
			{Type: TypeString, Name: "A", SortOrder: SortAscending},
			{Type: TypeFloat64, Name: "B"},
			{Type: TypeAny, Name: "C"},
		},
	}
	s1 := Schema{
		Strict: ptr.Bool(true),
		Columns: []Column{
			{Type: TypeFloat64, Name: "B", SortOrder: SortAscending},
			{Type: TypeInt64, Name: "A"},
			{Type: TypeBoolean, Name: "D"},
		},
	}
	sm := MergeSchemas(s0, s1)
	require.Equal(t, sm.Columns, []Column{
		{Type: TypeAny, Name: "A"},
		{Type: TypeFloat64, Name: "B"},
		{Type: TypeAny, Name: "C"},
		{Type: TypeBoolean, Name: "D"},
	})
}

func TestSchemaEqual(t *testing.T) {
	s0 := Schema{
		Columns: []Column{},
	}

	s1 := Schema{
		Columns: []Column{},
	}

	require.True(t, s0.Equal(s1))
}

func TestBoolColumn(t *testing.T) {
	c := Column{
		ComplexType: TypeBoolean,
	}

	ys, err := yson.MarshalFormat(c, yson.FormatText)
	require.NoError(t, err)

	require.Equal(t, string(ys), `{name="";"type_v3"=bool;}`)
}
