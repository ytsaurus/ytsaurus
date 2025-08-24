package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
)

func TestConvertFromRPCProxy_SimpleColumn(t *testing.T) {
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

	result, err := ConvertFromRPCProxy(rpcSchema)
	require.NoError(t, err)

	// Check the basic structure
	require.NotNil(t, result.Strict)
	require.True(t, *result.Strict)
	require.False(t, result.UniqueKeys)
	require.Len(t, result.Columns, 1)

	col := result.Columns[0]
	require.Equal(t, "test_column", col.Name)
	require.Equal(t, TypeBytes, col.Type) // Type 0x10 (16) maps to VT_String -> "string" (TypeBytes)
	require.True(t, col.Required)
	require.Equal(t, "group1", col.Group)
	require.Equal(t, "expr1", col.Expression)
	require.Equal(t, AggregateFunction("sum"), col.Aggregate)
	require.Equal(t, "lock1", col.Lock)
	require.Equal(t, SortAscending, col.SortOrder)
	require.Nil(t, col.ComplexType)
}

func TestConvertFromRPCProxy_WithComplexType(t *testing.T) {
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

	result, err := ConvertFromRPCProxy(rpcSchema)
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

func TestConvertFromRPCProxy_EmptyTypeV3(t *testing.T) {
	// Test with empty TypeV3 - should not set ComplexType
	rpcSchema := &rpc_proxy.TTableSchema{
		Strict:     ptr.Bool(true),
		UniqueKeys: ptr.Bool(false),
		Columns: []*rpc_proxy.TColumnSchema{
			{
				Name:       ptr.String("simple_column"),
				Type:       ptr.Int32(0x03), // VT_Int64
				TypeV3:     []byte{},        // Empty
				Required:   ptr.Bool(true),
				Group:      ptr.String(""),
				Expression: ptr.String(""),
				Aggregate:  ptr.String(""),
				Lock:       ptr.String(""),
			},
		},
	}

	result, err := ConvertFromRPCProxy(rpcSchema)
	require.NoError(t, err)

	require.Len(t, result.Columns, 1)
	col := result.Columns[0]
	require.Equal(t, "simple_column", col.Name)
	require.Nil(t, col.ComplexType)
}

func TestConvertFromRPCProxy_InvalidYSON(t *testing.T) {
	rpcSchema := &rpc_proxy.TTableSchema{
		Strict:     ptr.Bool(true),
		UniqueKeys: ptr.Bool(false),
		Columns: []*rpc_proxy.TColumnSchema{
			{
				Name:       ptr.String("invalid_column"),
				Type:       ptr.Int32(0x03),         // VT_Int64
				TypeV3:     []byte(`{invalid_yson`), // Invalid YSON
				Required:   ptr.Bool(true),
				Group:      ptr.String(""),
				Expression: ptr.String(""),
				Aggregate:  ptr.String(""),
				Lock:       ptr.String(""),
			},
		},
	}

	_, err := ConvertFromRPCProxy(rpcSchema)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to unmarshal complex type")
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
