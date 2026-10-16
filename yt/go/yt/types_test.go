package yt

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/guid"
)

func TestIDJSONRoundTrip(t *testing.T) {
	id := guid.FromHalves(0x123456789abcdef0, 0xfedcba9876543210)

	tests := []struct {
		name     string
		value    any
		newValue func() any
	}{
		{"NodeID", NodeID(id), func() any { return new(NodeID) }},
		{"OperationID", OperationID(id), func() any { return new(OperationID) }},
		{"TxID", TxID(id), func() any { return new(TxID) }},
		{"MutationID", MutationID(id), func() any { return new(MutationID) }},
		{"JobID", JobID(id), func() any { return new(JobID) }},
		{"MaintenanceID", MaintenanceID(id), func() any { return new(MaintenanceID) }},
		{"QueryID", QueryID(id), func() any { return new(QueryID) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.value)
			require.NoError(t, err)
			require.Equal(t, `"`+id.String()+`"`, string(data))

			decoded := test.newValue()
			require.NoError(t, json.Unmarshal(data, decoded))
			require.Equal(t, id.String(), decoded.(fmt.Stringer).String())
		})
	}
}
