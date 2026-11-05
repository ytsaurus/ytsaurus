package flow

import (
	"iter"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/wire"
)

func stateKeySchema() Schema {
	return NewSchema(schema.Schema{Columns: []schema.Column{
		{Name: "user_id", Type: schema.TypeUint64},
		{Name: "region", Type: schema.TypeString},
	}})
}

func externalStateSchema() Schema {
	return NewSchema(schema.Schema{Columns: []schema.Column{
		{Name: "count", Type: schema.TypeInt64},
		{Name: "label", Type: schema.TypeString},
	}})
}

func stateKey(t *testing.T, userID uint64, region string) Payload {
	t.Helper()
	key, err := NewPayloadBuilder(stateKeySchema()).Set("user_id", userID).Set("region", region).Finish()
	require.NoError(t, err)
	return key
}

func externalStateRow(t *testing.T, count int64, label string) Payload {
	t.Helper()
	row, err := NewPayloadBuilder(externalStateSchema()).Set("count", count).Set("label", label).Finish()
	require.NoError(t, err)
	return row
}

func collectStates[T any](seq iter.Seq2[Payload, T]) ([]Payload, []T) {
	var keys []Payload
	var values []T
	for key, value := range seq {
		keys = append(keys, key)
		values = append(values, value)
	}
	return keys, values
}

func regions(t *testing.T, keys []Payload) []string {
	t.Helper()
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		region, err := key.String("region")
		require.NoError(t, err)
		out = append(out, region)
	}
	return out
}

func TestStatesHolderLoadDoesNotModify(t *testing.T) {
	h := newInternalStatesHolder("counters")
	require.Equal(t, "counters", h.Name())

	for _, region := range []string{"ru", "tr", "kz"} {
		require.NoError(t, h.Load(stateKey(t, 1, region), InternalState{Data: []byte(region)}))
	}

	require.Equal(t, 3, h.Len())
	require.False(t, h.HasModified())

	modifiedKeys, _ := collectStates(h.Modified())
	require.Empty(t, modifiedKeys)

	allKeys, _ := collectStates(h.All())
	require.Equal(t, []string{"ru", "tr", "kz"}, regions(t, allKeys))
}

func TestStatesHolderReportsOnlyMutatedEntries(t *testing.T) {
	h := newInternalStatesHolder("counters")
	for _, region := range []string{"ru", "tr", "kz"} {
		require.NoError(t, h.Load(stateKey(t, 1, region), InternalState{Data: []byte("loaded")}))
	}

	require.NoError(t, h.Set(stateKey(t, 1, "tr"), InternalState{Data: []byte("written")}))

	require.True(t, h.HasModified())
	modifiedKeys, modifiedValues := collectStates(h.Modified())
	require.Equal(t, []string{"tr"}, regions(t, modifiedKeys))
	require.Equal(t, []InternalState{{Data: []byte("written")}}, modifiedValues)

	require.Equal(t, 3, h.Len())
	loaded, ok := h.Get(stateKey(t, 1, "ru"))
	require.True(t, ok)
	require.Equal(t, []byte("loaded"), loaded.Data)
}

func TestStatesHolderClearRecordsReset(t *testing.T) {
	h := newInternalStatesHolder("counters")
	require.NoError(t, h.Load(stateKey(t, 1, "ru"), InternalState{Data: []byte("loaded")}))
	require.NoError(t, h.Clear(stateKey(t, 1, "ru")))

	cleared, ok := h.Get(stateKey(t, 1, "ru"))
	require.True(t, ok)
	require.True(t, cleared.Reset)
	require.Nil(t, cleared.Data)

	modifiedKeys, modifiedValues := collectStates(h.Modified())
	require.Equal(t, []string{"ru"}, regions(t, modifiedKeys))
	require.Equal(t, []InternalState{{Reset: true}}, modifiedValues)
}

func TestStatesHolderClearsNeverLoadedKey(t *testing.T) {
	h := newInternalStatesHolder("counters")
	require.NoError(t, h.Clear(stateKey(t, 7, "ru")))

	require.True(t, h.HasModified())
	_, modifiedValues := collectStates(h.Modified())
	require.Equal(t, []InternalState{{Reset: true}}, modifiedValues)
}

func TestStatesHolderKeysCompareByContent(t *testing.T) {
	h := newInternalStatesHolder("counters")
	require.NoError(t, h.Set(stateKey(t, 42, "ru"), InternalState{Data: []byte("first")}))

	stored, ok := h.Get(stateKey(t, 42, "ru"))
	require.True(t, ok)
	require.Equal(t, []byte("first"), stored.Data)

	require.NoError(t, h.Set(stateKey(t, 42, "ru"), InternalState{Data: []byte("second")}))
	require.Equal(t, 1, h.Len())

	stored, ok = h.Get(stateKey(t, 42, "ru"))
	require.True(t, ok)
	require.Equal(t, []byte("second"), stored.Data)

	_, modifiedValues := collectStates(h.Modified())
	require.Len(t, modifiedValues, 1)
}

func TestStatesHolderSeparatesDifferingKeys(t *testing.T) {
	h := newInternalStatesHolder("counters")
	require.NoError(t, h.Set(stateKey(t, 1, "ru"), InternalState{Data: []byte("a")}))
	require.NoError(t, h.Set(stateKey(t, 2, "ru"), InternalState{Data: []byte("b")}))
	require.NoError(t, h.Set(stateKey(t, 1, "tr"), InternalState{Data: []byte("c")}))
	require.Equal(t, 3, h.Len())

	nullRegion, err := NewPayloadBuilder(stateKeySchema()).Set("user_id", uint64(1)).Finish()
	require.NoError(t, err)
	require.NoError(t, h.Set(nullRegion, InternalState{Data: []byte("d")}))
	require.Equal(t, 4, h.Len())
}

func TestStatesHolderAbsentKeyDiffersFromEmptyKey(t *testing.T) {
	h := newInternalStatesHolder("counters")
	require.NoError(t, h.Set(Payload{}, InternalState{Data: []byte("absent")}))
	require.NoError(t, h.Set(NewPayload(wire.Row{}, stateKeySchema()), InternalState{Data: []byte("empty")}))
	require.Equal(t, 2, h.Len())

	absent, ok := h.Get(Payload{})
	require.True(t, ok)
	require.Equal(t, []byte("absent"), absent.Data)
}

func TestStatesHolderIterationOrderIsFirstSeen(t *testing.T) {
	h := newInternalStatesHolder("counters")
	require.NoError(t, h.Load(stateKey(t, 1, "tr"), InternalState{Data: []byte("loaded")}))
	require.NoError(t, h.Load(stateKey(t, 1, "ru"), InternalState{Data: []byte("loaded")}))
	require.NoError(t, h.Set(stateKey(t, 1, "kz"), InternalState{Data: []byte("written")}))
	require.NoError(t, h.Set(stateKey(t, 1, "ru"), InternalState{Data: []byte("written")}))

	allKeys, _ := collectStates(h.All())
	require.Equal(t, []string{"tr", "ru", "kz"}, regions(t, allKeys))

	modifiedKeys, _ := collectStates(h.Modified())
	require.Equal(t, []string{"ru", "kz"}, regions(t, modifiedKeys))
}

func TestStatesHolderIterationStopsOnBreak(t *testing.T) {
	h := newInternalStatesHolder("counters")
	for _, region := range []string{"ru", "tr", "kz"} {
		require.NoError(t, h.Set(stateKey(t, 1, region), InternalState{Data: []byte(region)}))
	}

	var visited []string
	for key := range h.Modified() {
		visited = append(visited, regions(t, []Payload{key})...)
		break
	}
	require.Equal(t, []string{"ru"}, visited)
}

func TestStatesHolderRejectsUnusableKey(t *testing.T) {
	h := newInternalStatesHolder("counters")
	badKey := NewPayload(wire.Row{{ID: 0, Type: wire.ValueType(0xEE)}}, stateKeySchema())

	require.Error(t, h.Set(badKey, InternalState{Data: []byte("lost")}))
	require.Error(t, h.Load(badKey, InternalState{Data: []byte("lost")}))
	require.Error(t, h.Clear(badKey))

	require.Zero(t, h.Len())
	require.False(t, h.HasModified())
	_, ok := h.Get(badKey)
	require.False(t, ok)
}

func TestExternalStatesHolderRequiresSchema(t *testing.T) {
	_, err := newExternalStatesHolder("/home/flow/state", Schema{})
	require.ErrorIs(t, err, ErrNoStateSchema)

	h, err := newExternalStatesHolder("/home/flow/state", externalStateSchema())
	require.NoError(t, err)
	require.Equal(t, "/home/flow/state", h.Name())
	require.Equal(t, externalStateSchema().Columns(), h.StateSchema().Columns())
}

func TestInternalStatesHolderHasNoStateSchema(t *testing.T) {
	h := newInternalStatesHolder("counters")
	require.Zero(t, h.StateSchema().Len())
}

func TestExternalStatesHolderHoldsRows(t *testing.T) {
	h, err := newExternalStatesHolder("/home/flow/state", externalStateSchema())
	require.NoError(t, err)

	key := stateKey(t, 1, "ru")
	require.NoError(t, h.Load(key, ExternalState{Value: externalStateRow(t, 10, "loaded")}))
	require.False(t, h.HasModified())

	loaded, ok := h.Get(key)
	require.True(t, ok)
	count, err := loaded.Value.Int64("count")
	require.NoError(t, err)
	require.Equal(t, int64(10), count)

	require.NoError(t, h.Set(key, ExternalState{Value: externalStateRow(t, 11, "written")}))
	require.True(t, h.HasModified())

	modifiedKeys, modifiedValues := collectStates(h.Modified())
	require.Equal(t, []string{"ru"}, regions(t, modifiedKeys))
	require.Len(t, modifiedValues, 1)
	require.False(t, modifiedValues[0].Reset)
	label, err := modifiedValues[0].Value.String("label")
	require.NoError(t, err)
	require.Equal(t, "written", label)
}

func TestExternalStatesHolderClearRecordsReset(t *testing.T) {
	h, err := newExternalStatesHolder("/home/flow/state", externalStateSchema())
	require.NoError(t, err)

	key := stateKey(t, 1, "ru")
	require.NoError(t, h.Load(key, ExternalState{Value: externalStateRow(t, 10, "loaded")}))
	require.NoError(t, h.Clear(key))

	_, modifiedValues := collectStates(h.Modified())
	require.Len(t, modifiedValues, 1)
	require.True(t, modifiedValues[0].Reset)
	require.Nil(t, modifiedValues[0].Value.Row())
}
