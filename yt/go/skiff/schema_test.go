package skiff

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

func TestWireTypeYSON(t *testing.T) {
	for wire := TypeNothing; wire <= TypeTuple; wire++ {
		ys, err := yson.Marshal(wire)
		require.NoError(t, err)

		var decodedWire WireType
		err = yson.Unmarshal(ys, &decodedWire)
		require.NoError(t, err)

		require.Equal(t, wire, decodedWire)
	}
}

func TestFromTableSchemaWideTemporalTypes(t *testing.T) {
	tableSchema := schema.Schema{Columns: []schema.Column{
		{Name: "date32", Type: schema.TypeDate32, Required: true},
		{Name: "datetime64", Type: schema.TypeDatetime64, Required: true},
		{Name: "timestamp64", Type: schema.TypeTimestamp64, Required: true},
		{Name: "interval64", Type: schema.TypeInterval64, Required: true},
	}}

	require.Equal(t, Schema{
		Type: TypeTuple,
		Children: []Schema{
			{Name: "date32", Type: TypeInt64},
			{Name: "datetime64", Type: TypeInt64},
			{Name: "timestamp64", Type: TypeInt64},
			{Name: "interval64", Type: TypeInt64},
		},
	}, FromTableSchema(tableSchema))
}
