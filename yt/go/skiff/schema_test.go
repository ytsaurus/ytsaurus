package skiff

import (
	"testing"

	"github.com/stretchr/testify/require"

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
