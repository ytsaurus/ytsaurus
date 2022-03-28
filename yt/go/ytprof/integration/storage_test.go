package integration

import (
	"sort"
	"testing"
	"time"

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
	dataPath := tmpPath
	tsData := storage.NewTableStorage(env.YT, env.L, dataPath)

	require.NoError(t, ytprof.MigrateTables(env.YT, tmpPath))

	require.NoError(t, tsData.PushData(TestProfiles, env.Ctx))

	resultIDs, err := tsData.MetadataIdsQuery(time.Now().Add(-time.Hour).UnixMilli(), time.Now().Add(time.Hour).UnixMilli(), env.Ctx)
	require.NoError(t, err)
	resultData, err := tsData.FindProfiles(resultIDs, env.Ctx)
	require.NoError(t, err)
	checkTestProfiles(t, resultData)
}

func TestMetadataIdsQuery(t *testing.T) {
	env := yttest.New(t)

	tmpPath := env.TmpPath()
	dataPath := tmpPath
	tsData := storage.NewTableStorage(env.YT, env.L, dataPath)

	require.NoError(t, ytprof.MigrateTables(env.YT, tmpPath))

	require.NoError(t, tsData.PushData([]*profile.Profile{TestProfile}, env.Ctx))

	resultIDs, err := tsData.MetadataIdsQuery(time.Now().Add(-time.Hour).UnixMilli(), time.Now().Add(time.Hour).UnixMilli(), env.Ctx)
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

	result, err := tsData.MetadataQuery(time.Now().Add(-time.Hour).UnixMilli(), time.Now().Add(time.Hour).UnixMilli(), env.Ctx)
	require.NoError(t, err)
	require.NotEmpty(t, result)
}
