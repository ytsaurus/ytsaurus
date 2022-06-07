package app_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ytprof"
	"a.yandex-team.ru/yt/go/ytprof/api"
	"a.yandex-team.ru/yt/go/ytprof/internal/app"
)

var (
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

func initApp(t *testing.T) *app.App {
	l := &zap.Logger{L: zaptest.NewLogger(t)}

	c := app.Config{
		HTTPEndpoint: "localhost:0",
		Proxy:        os.Getenv("YT_PROXY"),
		TablePath:    "//home/kristevalex/ytprof/testing",
	}

	a := app.NewApp(l, c)

	return a
}

func TestAppInitialization(t *testing.T) {
	a := initApp(t)

	require.NoError(t, a.Stop())
}

func TestAppList(t *testing.T) {
	a := initApp(t)
	l := a.Logger()

	client := resty.New()

	request := &api.ListRequest{
		Metaquery: &api.Metaquery{
			Query:      "true",
			QueryLimit: 10000,
			TimePeriod: &api.TimePeriod{
				PeriodStartTime: "2022-04-24T00:00:00.000000Z",
				PeriodEndTime:   "2022-04-29T00:00:00.000000Z",
			},
		},
	}

	rsp, err := client.R().
		SetBody(request).
		Post(a.URL() + "/ytprof/api/list")

	require.NoError(t, err)
	l.Debug("responce status",
		log.Int("status code", rsp.StatusCode()),
		log.String("status", rsp.Status()),
		log.String("url", a.URL()+"/ytprof/api/list"),
		log.String("response", string(rsp.Body())))

	require.Equal(t, rsp.StatusCode(), 200, rsp.String())

	require.NoError(t, a.Stop())
}

func TestAppGet(t *testing.T) {
	a := initApp(t)
	l := a.Logger()
	ts := a.TableStorage()

	ctx := context.Background()

	require.NoError(t, ts.PushData(ctx, TestProfiles, TestHosts, "t1", "t2", "t3"))

	tLow, err := schema.NewTimestamp(time.Now().Add(-time.Hour))
	require.NoError(t, err)
	tHigh, err := schema.NewTimestamp(time.Now().Add(time.Hour))
	require.NoError(t, err)

	resultIDs, err := ts.MetadataIdsQuery(ctx, tLow, tHigh, 10000)
	require.NoError(t, err)
	require.Equal(t, len(resultIDs), len(TestProfiles))

	client := resty.New()

	rsp, err := client.R().
		SetQueryParam("ProfileID", ytprof.GUIDFormProfID(resultIDs[0]).String()).
		Get(a.URL() + "/ytprof/api/get")

	require.NoError(t, err)
	l.Debug("responce status",
		log.Int("status code", rsp.StatusCode()),
		log.String("status", rsp.Status()),
		log.String("url", a.URL()+"/ytprof/api/get"),
		log.String("response", string(rsp.Body())))

	require.Equal(t, rsp.StatusCode(), 200, rsp.String())

	require.NoError(t, a.Stop())
}
