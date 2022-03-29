package integration

import (
	"sort"
	"testing"
	"time"

	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ytprof"
	"a.yandex-team.ru/yt/go/ytprof/internal/storage"
	"a.yandex-team.ru/yt/go/yttest"

	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"
)

var (
	TestProfile = &profile.Profile{
		Period:            234,
		PeriodType:        &profile.ValueType{Type: "cpu", Unit: "seconds"},
		DropFrames:        "none",
		DefaultSampleType: "cpu",
		Comments:          []string{"c1", "c2"},
	}

	TestProfiles = []*profile.Profile{{
		Period:            234,
		PeriodType:        &profile.ValueType{Type: "cpu", Unit: "seconds"},
		DropFrames:        "none",
		DefaultSampleType: "cpu",
		Comments:          []string{"c1", "c2"},
	}, {
		Period:            2324,
		PeriodType:        &profile.ValueType{Type: "cpr", Unit: "ms"},
		DropFrames:        "none",
		DefaultSampleType: "cpu",
		Comments:          []string{"c1123", "c2222"},
	}, {
		Period:            2314,
		PeriodType:        &profile.ValueType{Type: "memory", Unit: "bytes"},
		DropFrames:        "none",
		DefaultSampleType: "memory",
		Comments:          []string{"c11", "c21"},
	}}
)

func checkTestProfiles(t *testing.T, checkProfiles []*profile.Profile) {
	require.Equal(t, len(TestProfiles), len(checkProfiles))
	testString := make([]string, len(TestProfiles))
	checkString := make([]string, len(checkProfiles))
	for index, profile := range TestProfiles {
		testString[index] = profile.String()
	}
	for index, profile := range checkProfiles {
		checkString[index] = profile.String()
	}

	sort.Strings(testString)
	sort.Strings(checkString)

	require.Equal(t, testString, checkString)
}

func TestDataAndMetadataTablesSmall(t *testing.T) {
	env := yttest.New(t)

	tmpPath := env.TmpPath()
	_ = env
	ts := storage.NewTableStorage(env.YT, env.L, tmpPath)
	_ = ts

	require.NoError(t, ytprof.MigrateTables(env.YT, tmpPath))
}

func TestDataAndMetadataTables(t *testing.T) {
	env := yttest.New(t)

	tmpPath := env.TmpPath()
	tsData := storage.NewTableStorage(env.YT, env.L, tmpPath)

	require.NoError(t, ytprof.MigrateTables(env.YT, tmpPath))

	require.NoError(t, tsData.PushData(TestProfiles, env.Ctx))

	tlow, err := schema.NewTimestamp(time.Now().Add(-time.Hour))
	require.NoError(t, err)
	thigh, err := schema.NewTimestamp(time.Now().Add(time.Hour))
	require.NoError(t, err)

	resultIDs, err := tsData.MetadataIdsQuery(tlow, thigh, env.Ctx)
	require.NoError(t, err)
	resultData, err := tsData.FindProfiles(resultIDs, env.Ctx)
	require.NoError(t, err)
	checkTestProfiles(t, resultData)
}

func TestDataExpr(t *testing.T) {
	env := yttest.New(t)

	tmpPath := env.TmpPath()
	tsData := storage.NewTableStorage(env.YT, env.L, tmpPath)

	require.NoError(t, ytprof.MigrateTables(env.YT, tmpPath))

	require.NoError(t, tsData.PushData(TestProfiles, env.Ctx))

	tlow, err := schema.NewTimestamp(time.Now().Add(-time.Hour))
	require.NoError(t, err)
	thigh, err := schema.NewTimestamp(time.Now().Add(time.Hour))
	require.NoError(t, err)

	resultIDs, err := tsData.MetadataIdsQueryExpr(tlow, thigh, env.Ctx, "ProfileType == 'cpu'")
	require.NoError(t, err)
	resultData, err := tsData.FindProfiles(resultIDs, env.Ctx)
	require.NoError(t, err)
	require.Equal(t, len(resultData), 2)
}

func TestMetadataIdsQuery(t *testing.T) {
	env := yttest.New(t)

	tmpPath := env.TmpPath()
	dataPath := tmpPath
	tsData := storage.NewTableStorage(env.YT, env.L, dataPath)

	require.NoError(t, ytprof.MigrateTables(env.YT, tmpPath))

	require.NoError(t, tsData.PushData([]*profile.Profile{TestProfile}, env.Ctx))

	tlow, err := schema.NewTimestamp(time.Now().Add(-time.Hour))
	require.NoError(t, err)
	thigh, err := schema.NewTimestamp(time.Now().Add(time.Hour))
	require.NoError(t, err)

	resultIDs, err := tsData.MetadataIdsQuery(tlow, thigh, env.Ctx)
	require.NoError(t, err)
	require.NotEmpty(t, resultIDs)
}

func TestMetadataQuery(t *testing.T) {
	env := yttest.New(t)

	tmpPath := env.TmpPath()
	dataPath := tmpPath
	tsData := storage.NewTableStorage(env.YT, env.L, dataPath)

	require.NoError(t, ytprof.MigrateTables(env.YT, tmpPath))

	require.NoError(t, tsData.PushData([]*profile.Profile{TestProfile}, env.Ctx))

	tlow, err := schema.NewTimestamp(time.Now().Add(-time.Hour))
	require.NoError(t, err)
	thigh, err := schema.NewTimestamp(time.Now().Add(time.Hour))
	require.NoError(t, err)

	result, err := tsData.MetadataQuery(tlow, thigh, env.Ctx)
	require.NoError(t, err)
	require.NotEmpty(t, result)
}
