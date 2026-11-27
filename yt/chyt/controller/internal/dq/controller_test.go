package dq

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/yson"
)

func TestControllerSnapshotSurvivesPersistence(t *testing.T) {
	controller := &Controller{}
	snapshot, err := controller.GetControllerSnapshot()
	require.NoError(t, err)

	state := strawberry.PersistentState{YTOpControllerSnapshot: snapshot}
	for range 2 {
		encoded, err := yson.Marshal(state)
		require.NoError(t, err)

		var restored strawberry.PersistentState
		require.NoError(t, yson.Unmarshal(encoded, &restored))
		// A missing snapshot triggers another state modification and flush on every pass.
		require.NotNil(t, restored.YTOpControllerSnapshot)
		require.NotEmpty(t, restored.YTOpControllerSnapshot)

		outdated, err := controller.IsControllerSnapshotOutdated(restored.YTOpControllerSnapshot, nil)
		require.NoError(t, err)
		require.False(t, outdated)
		state = restored
	}
}
