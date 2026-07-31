package flow

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/schema"
)

func streamSchema(column string) Schema {
	return NewSchema(schema.Schema{Columns: []schema.Column{
		{Name: column, Type: schema.TypeString},
	}})
}

func TestStreamSpecsResolvesStreams(t *testing.T) {
	specs := NewStreamSpecs(map[string]int64{"clicks": 1, "shows": 2}, []Stream{
		NewStream("clicks", streamSchema("url")),
		NewStream("shows", streamSchema("banner")),
	})

	require.Equal(t, 2, specs.Len())

	stream, ok := specs.Stream("clicks")
	require.True(t, ok)
	require.Equal(t, "clicks", stream.ID)
	_, hasColumn := stream.Schema.FindColumn("url")
	require.True(t, hasColumn)

	stream, ok = specs.StreamBySpecID(2)
	require.True(t, ok)
	require.Equal(t, "shows", stream.ID)

	specID, ok := specs.SpecID("shows")
	require.True(t, ok)
	require.Equal(t, int64(2), specID)

	streamID, ok := specs.StreamID(1)
	require.True(t, ok)
	require.Equal(t, "clicks", streamID)
}

func TestStreamSpecsDoNotResolveVisitSpecID(t *testing.T) {
	specs := NewStreamSpecs(map[string]int64{"clicks": 0}, []Stream{NewStream("clicks", streamSchema("url"))})

	_, ok := specs.StreamBySpecID(NoStreamSpecID)
	require.False(t, ok)
	_, ok = specs.StreamID(NoStreamSpecID)
	require.False(t, ok)
}

func TestStreamSpecsLookupsAreIndependent(t *testing.T) {
	specs := NewStreamSpecs(map[string]int64{"mapped-only": 5}, []Stream{
		NewStream("schema-only", streamSchema("url")),
	})

	_, ok := specs.Stream("mapped-only")
	require.False(t, ok)
	_, ok = specs.StreamBySpecID(5)
	require.False(t, ok)
	streamID, ok := specs.StreamID(5)
	require.True(t, ok)
	require.Equal(t, "mapped-only", streamID)

	stream, ok := specs.Stream("schema-only")
	require.True(t, ok)
	require.Equal(t, "schema-only", stream.ID)
	_, ok = specs.SpecID("schema-only")
	require.False(t, ok)
}

func TestStreamSpecsSnapshotMapping(t *testing.T) {
	ids := map[string]int64{"clicks": 1}

	specs := NewStreamSpecs(ids, []Stream{NewStream("clicks", streamSchema("url"))})

	ids["clicks"] = 9

	specID, ok := specs.SpecID("clicks")
	require.True(t, ok)
	require.Equal(t, int64(1), specID)
	_, ok = specs.StreamID(9)
	require.False(t, ok)
}

func TestStreamSpecsKeepFirstDuplicateStream(t *testing.T) {
	specs := NewStreamSpecs(nil, []Stream{
		NewStream("clicks", streamSchema("url")),
		NewStream("clicks", streamSchema("banner")),
	})

	require.Equal(t, 1, specs.Len())

	stream, ok := specs.Stream("clicks")
	require.True(t, ok)
	_, hasColumn := stream.Schema.FindColumn("url")
	require.True(t, hasColumn)
}

func TestZeroStreamSpecsResolveNothing(t *testing.T) {
	var specs StreamSpecs

	require.Equal(t, 0, specs.Len())
	_, ok := specs.Stream("clicks")
	require.False(t, ok)
	_, ok = specs.StreamBySpecID(0)
	require.False(t, ok)
	_, ok = specs.StreamID(0)
	require.False(t, ok)
	_, ok = specs.SpecID("clicks")
	require.False(t, ok)
}
