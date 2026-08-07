package flow

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/wire"
)

const staticSpecYSON = `{
	computation_class_name = "counter";
	group_by_schema = [
		{name = "hash"; type = "uint64"};
		{name = "user_id"; type = "string"};
	];
	parameters = {
		internal_states = ["windows"; "counters"];
		window_size = 100;
	};
	external_state_managers = {
		"/state" = {parameters = {path = "//placeholder"}};
		"/aux/state" = {};
	};
	external_state_joiners = {
		"/joined" = {computation_id = "profiles"};
	};
}`

const dynamicSpecYSON = `{
	parameters = {
		window_size = 200;
	};
}`

func testStreamSpecs() StreamSpecs {
	return NewStreamSpecs(
		map[string]int64{"clicks": 1},
		[]Stream{NewStream("clicks", streamSchema("url"))},
	)
}

func testJob(t *testing.T) *Job {
	t.Helper()
	job, err := NewJob(
		guid.FromHalves(1, 2),
		"counter",
		testStreamSpecs(),
		[]byte(staticSpecYSON),
		[]byte(dynamicSpecYSON),
	)
	require.NoError(t, err)
	return job
}

func TestNewJobParsesStaticSpec(t *testing.T) {
	job := testJob(t)

	require.Equal(t, guid.FromHalves(1, 2), job.ID())
	require.Equal(t, "counter", job.ComputationID())

	stream, ok := job.StreamSpecs().StreamBySpecID(1)
	require.True(t, ok)
	require.Equal(t, "clicks", stream.ID)

	require.Equal(t, []string{"counters", "windows"}, job.InternalStateNames())
	require.Equal(t, []string{"/aux/state", "/state"}, job.ExternalStateNames())
	require.Equal(t, []string{"/joined"}, job.JoinedExternalStateNames())
}

func TestNewJobParsesGroupBySchema(t *testing.T) {
	job := testJob(t)
	groupBy := job.GroupBySchema()

	require.Equal(t, 2, groupBy.Len())

	id, ok := groupBy.FindColumn("hash")
	require.True(t, ok)
	valueType, ok := groupBy.ColumnType(id)
	require.True(t, ok)
	require.Equal(t, wire.TypeUint64, valueType)

	id, ok = groupBy.FindColumn("user_id")
	require.True(t, ok)
	valueType, ok = groupBy.ColumnType(id)
	require.True(t, ok)
	require.Equal(t, wire.TypeBytes, valueType)
}

func TestNewJobKeepsSpecsSeparate(t *testing.T) {
	job := testJob(t)

	var staticWindow, dynamicWindow int64
	require.NoError(t, job.StaticParameters().Get("window_size", &staticWindow))
	require.NoError(t, job.DynamicParameters().Get("window_size", &dynamicWindow))
	require.Equal(t, int64(100), staticWindow)
	require.Equal(t, int64(200), dynamicWindow)

	require.False(t, job.DynamicParameters().Has("internal_states"))
	require.Equal(t, []string{"internal_states", "window_size"}, job.StaticParameters().Names())
}

func TestJobParametersGet(t *testing.T) {
	parameters := testJob(t).StaticParameters()

	var windowSize int64
	require.NoError(t, parameters.Get("window_size", &windowSize))
	require.Equal(t, int64(100), windowSize)

	var states []string
	require.NoError(t, parameters.Get("internal_states", &states))
	require.Equal(t, []string{"windows", "counters"}, states)

	require.True(t, parameters.Has("window_size"))
	require.False(t, parameters.Has("absent"))

	var missing int64
	require.ErrorIs(t, parameters.Get("absent", &missing), ErrParameterNotFound)

	var wrongType string
	require.Error(t, parameters.Get("window_size", &wrongType))
}

func TestNewJobWithoutStatesOrSchema(t *testing.T) {
	job, err := NewJob(guid.FromHalves(3, 4), "plain", StreamSpecs{}, []byte("{}"), []byte("{}"))
	require.NoError(t, err)

	require.Equal(t, 0, job.GroupBySchema().Len())
	require.Empty(t, job.InternalStateNames())
	require.Empty(t, job.ExternalStateNames())
	require.Empty(t, job.JoinedExternalStateNames())
	require.Empty(t, job.StaticParameters().Names())
	require.Empty(t, job.DynamicParameters().Names())
}

func TestNewJobRejectsMalformedSpec(t *testing.T) {
	_, err := NewJob(guid.FromHalves(1, 1), "counter", StreamSpecs{}, []byte("{not yson"), []byte("{}"))
	require.Error(t, err)

	_, err = NewJob(guid.FromHalves(1, 1), "counter", StreamSpecs{}, []byte("{}"), []byte("{not yson"))
	require.Error(t, err)

	_, err = NewJob(guid.FromHalves(1, 1), "counter", StreamSpecs{},
		[]byte(`{parameters = {internal_states = 42}}`), []byte("{}"))
	require.Error(t, err)
}

func TestJobValidateInternalStateName(t *testing.T) {
	job := testJob(t)

	require.NoError(t, job.ValidateInternalStateName("counters"))
	require.ErrorIs(t, job.ValidateInternalStateName("absent"), ErrUnknownState)

	require.ErrorIs(t, job.ValidateInternalStateName("/counters"), ErrUnknownState)
}

func TestJobValidateExternalStateName(t *testing.T) {
	job := testJob(t)

	require.NoError(t, job.ValidateExternalStateName("/state"))
	require.NoError(t, job.ValidateExternalStateName("/aux/state"))

	require.ErrorIs(t, job.ValidateExternalStateName("/undeclared"), ErrUnknownState)

	for _, name := range []string{"", "state", "/", "/state/", "//state", "/aux//state"} {
		require.ErrorIsf(t, job.ValidateExternalStateName(name), ErrInvalidStateName, "name %q", name)
	}
}

func TestJobStateNamespacesAreDistinct(t *testing.T) {
	job := testJob(t)

	require.NoError(t, job.ValidateExternalStateName("/state"))
	require.ErrorIs(t, job.ValidateJoinedExternalStateName("/state"), ErrUnknownState)

	require.NoError(t, job.ValidateJoinedExternalStateName("/joined"))
	require.ErrorIs(t, job.ValidateExternalStateName("/joined"), ErrUnknownState)
}

func cachedTestJob(t *testing.T, id guid.GUID) *Job {
	t.Helper()
	job, err := NewJob(id, "counter", StreamSpecs{}, []byte("{}"), []byte("{}"))
	require.NoError(t, err)
	return job
}

func TestJobCacheStoresByJobID(t *testing.T) {
	cache := newJobCache()
	id := guid.FromHalves(1, 2)

	_, ok := cache.Get(id)
	require.False(t, ok)

	cache.Put(cachedTestJob(t, id))

	job, ok := cache.Get(id)
	require.True(t, ok)
	require.Equal(t, id, job.ID())

	_, ok = cache.Get(guid.FromHalves(9, 9))
	require.False(t, ok)
}

func TestJobCachePutReplacesJobUnderSameID(t *testing.T) {
	cache := newJobCache()
	id := guid.FromHalves(1, 2)

	cache.Put(cachedTestJob(t, id))
	reconfigured := cachedTestJob(t, id)
	cache.Put(reconfigured)

	job, ok := cache.Get(id)
	require.True(t, ok)
	require.Same(t, reconfigured, job)
	require.Equal(t, 1, cache.Len())
}

func TestJobCacheReportsCPUAsReplayableDeltas(t *testing.T) {
	cache := newJobCache()
	id := guid.FromHalves(1, 2)
	firstRequest := guid.FromHalves(3, 4)
	secondRequest := guid.FromHalves(5, 6)
	cache.Put(cachedTestJob(t, id))

	cache.AddCPUTime(id, 10)
	require.EqualValues(t, 10, cache.ResponseCPUTime(id, firstRequest))

	cache.AddCPUTime(id, 20)
	require.EqualValues(t, 10, cache.ResponseCPUTime(id, firstRequest))
	require.EqualValues(t, 20, cache.ResponseCPUTime(id, secondRequest))
	require.Zero(t, cache.ResponseCPUTime(id, guid.FromHalves(7, 8)))
}

func TestJobCachePreservesCPUWhenJobIsReconfigured(t *testing.T) {
	cache := newJobCache()
	id := guid.FromHalves(1, 2)
	requestID := guid.FromHalves(3, 4)
	cache.Put(cachedTestJob(t, id))
	cache.AddCPUTime(id, 10)

	cache.Put(cachedTestJob(t, id))

	require.EqualValues(t, 10, cache.ResponseCPUTime(id, requestID))
}

func TestJobCacheReportsMemoryAsReplayableGauge(t *testing.T) {
	cache := newJobCache()
	id := guid.FromHalves(1, 2)
	firstRequest := guid.FromHalves(3, 4)
	secondRequest := guid.FromHalves(5, 6)
	cache.Put(cachedTestJob(t, id))

	require.EqualValues(t, 100, cache.ResponseMemoryUsage(id, firstRequest, 100))
	require.EqualValues(t, 100, cache.ResponseMemoryUsage(id, firstRequest, 200))
	require.EqualValues(t, 200, cache.ResponseMemoryUsage(id, secondRequest, 200))
}

func TestJobCacheDropsCPUForDeletedJobs(t *testing.T) {
	cache := newJobCache()
	deleted := guid.FromHalves(3, 4)
	cache.Put(cachedTestJob(t, deleted))
	cache.AddCPUTime(deleted, 20)

	cache.Delete(deleted)
	cache.AddCPUTime(deleted, 30)
	require.Zero(t, cache.ResponseCPUTime(deleted, guid.FromHalves(7, 8)))
}

func TestJobCacheDelete(t *testing.T) {
	cache := newJobCache()
	id := guid.FromHalves(1, 2)

	// Removal is idempotent: unknown ids are ignored.
	cache.Delete(id)

	cache.Put(cachedTestJob(t, id))
	cache.Delete(id)

	_, ok := cache.Get(id)
	require.False(t, ok)
	require.Equal(t, 0, cache.Len())
	cache.Delete(id)
}

func TestJobCacheRegistersARemovedJobAgain(t *testing.T) {
	cache := newJobCache()
	id := guid.FromHalves(1, 2)

	cache.Put(cachedTestJob(t, id))
	cache.Delete(id)

	// A registration processed after a removal recreates the entry; if its
	// job is gone from the worker, the reconcile pass reclaims it.
	cache.Put(cachedTestJob(t, id))
	_, ok := cache.Get(id)
	require.True(t, ok)
}

func TestJobCacheConcurrentAccess(t *testing.T) {
	cache := newJobCache()

	const goroutines = 8
	jobs := make([]*Job, goroutines)
	for i := range jobs {
		jobs[i] = cachedTestJob(t, guid.FromHalves(uint64(i), 1))
	}

	misses := make([]int, goroutines)
	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				cache.Put(jobs[i])
				if job, ok := cache.Get(jobs[i].ID()); !ok || job != jobs[i] {
					misses[i]++
				}
			}
		}()
	}
	wg.Wait()

	require.Equal(t, make([]int, goroutines), misses)
	require.Equal(t, goroutines, cache.Len())
}
