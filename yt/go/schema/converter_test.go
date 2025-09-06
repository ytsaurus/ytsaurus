package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
)

func TestConvertFromProto_SimpleColumn(t *testing.T) {
	rpcSchema := &rpc_proxy.TTableSchema{
		Strict:     ptr.Bool(true),
		UniqueKeys: ptr.Bool(false),
		Columns: []*rpc_proxy.TColumnSchema{
			{
				Name:       ptr.String("test_column"),
				Type:       ptr.Int32(0x10), // VT_String
				Required:   ptr.Bool(true),
				Group:      ptr.String("group1"),
				Expression: ptr.String("expr1"),
				Aggregate:  ptr.String("sum"),
				Lock:       ptr.String("lock1"),
				SortOrder:  ptr.Int32(0), // SortAscending
			},
		},
	}

	result, err := ConvertFromProto(rpcSchema)
	require.NoError(t, err)

	require.NotNil(t, result.Strict)
	require.True(t, *result.Strict)
	require.False(t, result.UniqueKeys)
	require.Len(t, result.Columns, 1)

	col := result.Columns[0]
	require.Equal(t, "test_column", col.Name)
	require.Equal(t, TypeBytes, col.Type)
	require.True(t, col.Required)
	require.Equal(t, "group1", col.Group)
	require.Equal(t, "expr1", col.Expression)
	require.Equal(t, AggregateFunction("sum"), col.Aggregate)
	require.Equal(t, "lock1", col.Lock)
	require.Equal(t, SortAscending, col.SortOrder)
	require.Nil(t, col.ComplexType)
}

func TestConvertFromProto_WithComplexType(t *testing.T) {
	typeV3 := []byte(`{type_name=struct;members=[{name=id;type=int64};{name=name;type=utf8}]}`)

	rpcSchema := &rpc_proxy.TTableSchema{
		Strict:     ptr.Bool(true),
		UniqueKeys: ptr.Bool(false),
		Columns: []*rpc_proxy.TColumnSchema{
			{
				Name:       ptr.String("person"),
				Type:       ptr.Int32(0x03), // VT_Int64
				TypeV3:     typeV3,
				Required:   ptr.Bool(false),
				Group:      ptr.String(""),
				Expression: ptr.String(""),
				Aggregate:  ptr.String(""),
				Lock:       ptr.String(""),
			},
		},
	}

	result, err := ConvertFromProto(rpcSchema)
	require.NoError(t, err)

	require.Len(t, result.Columns, 1)
	col := result.Columns[0]
	require.Equal(t, "person", col.Name)
	require.False(t, col.Required)
	require.NotNil(t, col.ComplexType)

	structType, ok := col.ComplexType.(Struct)
	require.True(t, ok)
	require.Len(t, structType.Members, 2)
	require.Equal(t, "id", structType.Members[0].Name)
	require.Equal(t, TypeInt64, structType.Members[0].Type)
	require.Equal(t, "name", structType.Members[1].Name)
	require.Equal(t, TypeString, structType.Members[1].Type)
}

func TestConvertFromProto_EmptyTypeV3(t *testing.T) {
	rpcSchema := &rpc_proxy.TTableSchema{
		Strict:     ptr.Bool(true),
		UniqueKeys: ptr.Bool(false),
		Columns: []*rpc_proxy.TColumnSchema{
			{
				Name:       ptr.String("simple_column"),
				Type:       ptr.Int32(0x03), // VT_Int64
				TypeV3:     []byte{},
				Required:   ptr.Bool(true),
				Group:      ptr.String(""),
				Expression: ptr.String(""),
				Aggregate:  ptr.String(""),
				Lock:       ptr.String(""),
			},
		},
	}

	result, err := ConvertFromProto(rpcSchema)
	require.NoError(t, err)

	require.Len(t, result.Columns, 1)
	col := result.Columns[0]
	require.Equal(t, "simple_column", col.Name)
	require.Nil(t, col.ComplexType)
}

func TestConvertFromProto_InvalidYSON(t *testing.T) {
	rpcSchema := &rpc_proxy.TTableSchema{
		Strict:     ptr.Bool(true),
		UniqueKeys: ptr.Bool(false),
		Columns: []*rpc_proxy.TColumnSchema{
			{
				Name:       ptr.String("invalid_column"),
				Type:       ptr.Int32(0x03),
				TypeV3:     []byte(`{invalid_yson`),
				Required:   ptr.Bool(true),
				Group:      ptr.String(""),
				Expression: ptr.String(""),
				Aggregate:  ptr.String(""),
				Lock:       ptr.String(""),
			},
		},
	}

	_, err := ConvertFromProto(rpcSchema)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to unmarshal complex type")
}

func TestConvertType(t *testing.T) {
	tests := []struct {
		name      string
		valueType *int32
		expected  Type
	}{
		{
			name:      "nil value type",
			valueType: nil,
			expected:  TypeAny,
		},
		{
			name:      "null type",
			valueType: ptr.Int32(0x02),
			expected:  TypeNull,
		},
		{
			name:      "int64 type",
			valueType: ptr.Int32(0x03),
			expected:  TypeInt64,
		},
		{
			name:      "string type",
			valueType: ptr.Int32(0x10),
			expected:  TypeBytes,
		},
		{
			name:      "any type",
			valueType: ptr.Int32(0x11),
			expected:  TypeAny,
		},
		{
			name:      "unknown type",
			valueType: ptr.Int32(0xFFFF),
			expected:  TypeAny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertType(tt.valueType)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertFromProto_NullColumn(t *testing.T) {
	rpcSchema := &rpc_proxy.TTableSchema{
		Strict:     ptr.Bool(true),
		UniqueKeys: ptr.Bool(false),
		Columns: []*rpc_proxy.TColumnSchema{
			{
				Name:     ptr.String("null_column"),
				Type:     ptr.Int32(0x02), // Null type
				Required: ptr.Bool(false),
			},
		},
	}

	result, err := ConvertFromProto(rpcSchema)
	require.NoError(t, err)

	require.Len(t, result.Columns, 1)
	col := result.Columns[0]
	require.Equal(t, "null_column", col.Name)
	require.Equal(t, TypeNull, col.Type)
	require.False(t, col.Required)
}

func TestConvertSortOrder(t *testing.T) {
	tests := []struct {
		name      string
		sortOrder *int32
		expected  SortOrder
	}{
		{
			name:      "nil sort order",
			sortOrder: nil,
			expected:  SortNone,
		},
		{
			name:      "ascending sort order",
			sortOrder: ptr.Int32(0),
			expected:  SortAscending,
		},
		{
			name:      "descending sort order",
			sortOrder: ptr.Int32(1),
			expected:  SortDescending,
		},
		{
			name:      "invalid sort order",
			sortOrder: ptr.Int32(999),
			expected:  SortNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertSortOrder(tt.sortOrder)
			require.Equal(t, tt.expected, result)
		})
	}
}
