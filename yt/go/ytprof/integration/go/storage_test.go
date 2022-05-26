package integration

import (
	"sort"
	"testing"
	"time"

	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ytlog"
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
		Comments:          []string{"c1", "binary_version=c2"},
	}, {
		Period:            2324,
		PeriodType:        &profile.ValueType{Type: "cpr", Unit: "ms"},
		DropFrames:        "none",
		DefaultSampleType: "cpu",
		Comments:          []string{"binary_version=c2", "c2222"},
	}, {
		Period:            2314,
		PeriodType:        &profile.ValueType{Type: "memory", Unit: "bytes"},
		DropFrames:        "none",
		DefaultSampleType: "memory",
		Comments:          []string{"binary_version=c11", "c21"},
	}}

	TestHosts = []string{
		"host1",
		"host2",
		"host3",
	}
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
	l, err := ytlog.New()
	require.NoError(t, err)

	env := yttest.New(t, yttest.WithLogger(l.Structured()))

	tmpPath := env.TmpPath()
	_ = env

	ts := storage.NewTableStorage(env.YT, tmpPath, l)
	_ = ts

	require.NoError(t, ytprof.MigrateTables(env.YT, tmpPath))
}

func TestDataAndMetadataTables(t *testing.T) {
	l, err := ytlog.New()
	require.NoError(t, err)

	env := yttest.New(t, yttest.WithLogger(l.Structured()))

	tmpPath := env.TmpPath()

	tsData := storage.NewTableStorage(env.YT, tmpPath, l)

	require.NoError(t, ytprof.MigrateTables(env.YT, tmpPath))

	require.NoError(t, tsData.PushData(env.Ctx, TestProfiles, TestHosts, "t1", "t2", "t3"))

	tlow, err := schema.NewTimestamp(time.Now().Add(-time.Hour))
	require.NoError(t, err)
	thigh, err := schema.NewTimestamp(time.Now().Add(time.Hour))
	require.NoError(t, err)

	resultIDs, err := tsData.MetadataIdsQuery(env.Ctx, tlow, thigh, 10000)
	require.NoError(t, err)
	resultData, err := tsData.FindProfiles(env.Ctx, resultIDs)
	require.NoError(t, err)
	checkTestProfiles(t, resultData)
}

func TestDataExpr(t *testing.T) {
	l, err := ytlog.New()
	require.NoError(t, err)

	env := yttest.New(t, yttest.WithLogger(l.Structured()))

	tmpPath := env.TmpPath()

	tsData := storage.NewTableStorage(env.YT, tmpPath, l)

	require.NoError(t, ytprof.MigrateTables(env.YT, tmpPath))

	require.NoError(t, tsData.PushData(env.Ctx, TestProfiles, TestHosts, "t1", "t2", "t3"))

	tlow := time.Now().Add(-time.Hour)
	require.NoError(t, err)
	thigh := time.Now().Add(time.Hour)
	require.NoError(t, err)

	metaquery := storage.Metaquery{
		Query:      "Metadata['BinaryVersion'] == 'c2'",
		QueryLimit: 10000,
		Period: storage.TimestampPeriod{
			Start: tlow,
			End:   thigh,
		},
	}

	resultIDs, err := tsData.MetadataIdsQueryExpr(env.Ctx, metaquery)
	require.NoError(t, err)
	resultData, err := tsData.FindProfiles(env.Ctx, resultIDs)
	require.NoError(t, err)
	require.Equal(t, len(resultData), 2)
}

func TestMetadataIdsQuery(t *testing.T) {
	l, err := ytlog.New()
	require.NoError(t, err)

	env := yttest.New(t, yttest.WithLogger(l.Structured()))

	tmpPath := env.TmpPath()
	dataPath := tmpPath

	tsData := storage.NewTableStorage(env.YT, dataPath, l)

	require.NoError(t, ytprof.MigrateTables(env.YT, tmpPath))

	require.NoError(t, tsData.PushData(env.Ctx, []*profile.Profile{TestProfile}, TestHosts, "t1", "t2", "t3"))

	tlow, err := schema.NewTimestamp(time.Now().Add(-time.Hour))
	require.NoError(t, err)
	thigh, err := schema.NewTimestamp(time.Now().Add(time.Hour))
	require.NoError(t, err)

	resultIDs, err := tsData.MetadataIdsQuery(env.Ctx, tlow, thigh, 10000)
	require.NoError(t, err)
	require.NotEmpty(t, resultIDs)
}

func TestMetadataQuery(t *testing.T) {
	l, err := ytlog.New()
	require.NoError(t, err)

	env := yttest.New(t, yttest.WithLogger(l.Structured()))

	dataPath := env.TmpPath()

	tsData := storage.NewTableStorage(env.YT, dataPath, l)

	require.NoError(t, ytprof.MigrateTables(env.YT, dataPath))

	require.NoError(t, tsData.PushData(env.Ctx, []*profile.Profile{TestProfile}, TestHosts, "t1", "t2", "t3"))

	tlow, err := schema.NewTimestamp(time.Now().Add(-time.Hour))
	require.NoError(t, err)
	thigh, err := schema.NewTimestamp(time.Now().Add(time.Hour))
	require.NoError(t, err)

	result, err := tsData.MetadataQuery(env.Ctx, tlow, thigh, 10000)
	require.NoError(t, err)
	require.NotEmpty(t, result)
}
