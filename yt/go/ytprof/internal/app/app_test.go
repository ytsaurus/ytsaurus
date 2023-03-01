package app_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/ytprof"
	"a.yandex-team.ru/yt/go/ytprof/api"
	"a.yandex-team.ru/yt/go/ytprof/internal/app"
	"a.yandex-team.ru/yt/go/yttest"
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

func initApp(t *testing.T) (*app.App, func()) {
	l, stop := yttest.NewLogger(t)

	c := app.Config{
		HTTPEndpoint: "localhost:0",
		Proxy:        os.Getenv("YT_PROXY"),
		FolderPath:   "//home/kristevalex/ytprof",
	}

	ytConfig := yt.Config{
		Proxy:             c.Proxy,
		ReadTokenFromFile: true,
	}

	yc, err := ythttp.NewClient(&ytConfig)
	require.NoError(t, err)

	_, err = yc.CreateNode(
		context.Background(),
		ypath.Path(c.FolderPath).Child("testing"),
		yt.NodeMap,
		&yt.CreateNodeOptions{Recursive: true, IgnoreExisting: true},
	)
	require.NoError(t, err)

	a := app.NewApp(l, c)

	return a, stop
}

func TestAppInitialization(t *testing.T) {
	a, stop := initApp(t)

	require.NoError(t, a.Stop())
	stop()
}

func TestAppList(t *testing.T) {
	a, stop := initApp(t)
	l := a.Logger()

	client := resty.New()

	request := &api.ListRequest{
		Metaquery: &api.Metaquery{
			System: "testing",
			Query:  "true",
			TimePeriod: &api.TimePeriod{
				PeriodStartTime: "2022-04-24T00:00:00.000000Z",
				PeriodEndTime:   "2022-04-29T00:00:00.000000Z",
			},
			ResultSkip:  0,
			ResultLimit: 10,
			MetadataPattern: &api.Metadata{
				ProfileType: "*",
				UserTags:    map[string]string{},
			},
		},
	}

	rsp, err := client.R().
		SetBody(request).
		Post(a.URL() + "/api/list")

	require.NoError(t, err)
	l.Debug("response status",
		log.Int("status code", rsp.StatusCode()),
		log.String("status", rsp.Status()),
		log.String("url", a.URL()+"/api/list"),
		log.String("response", string(rsp.Body())))

	require.Equal(t, 200, rsp.StatusCode(), rsp.String())

	require.NoError(t, a.Stop())
	stop()
}

func TestAppGet(t *testing.T) {
	a, stop := initApp(t)
	l := a.Logger()
	ts, ok := a.TableStorage("testing")
	require.True(t, ok)

	ctx := context.Background()

	_, err := ts.PushData(ctx, TestProfiles, TestHosts, "t1", "t2", "t3", nil)
	require.NoError(t, err)

	tLow, err := schema.NewTimestamp(time.Now().Add(-time.Hour))
	require.NoError(t, err)
	tHigh, err := schema.NewTimestamp(time.Now().Add(time.Hour))
	require.NoError(t, err)

	resultIDs, err := ts.MetadataIDsQuery(ctx, tLow, tHigh, 10000)
	require.NoError(t, err)
	require.Equal(t, len(resultIDs), len(TestProfiles))

	client := resty.New()

	rsp, err := client.R().
		SetQueryParam("profile_id", ytprof.GUIDFormProfID(resultIDs[0]).String()).
		SetQueryParam("system", "testing").
		Get(a.URL() + "/api/get")

	require.NoError(t, err)
	l.Debug("response status",
		log.Int("status code", rsp.StatusCode()),
		log.String("status", rsp.Status()),
		log.String("url", a.URL()+"/api/get"),
		log.String("response", string(rsp.Body())))

	require.Equal(t, 200, rsp.StatusCode(), rsp.String())

	require.NoError(t, a.Stop())
	stop()
}
