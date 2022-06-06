package app_test

import (
	"os"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/ytprof/api"
	"a.yandex-team.ru/yt/go/ytprof/internal/app"
)

func TestAppInitialization(t *testing.T) {
	l := &zap.Logger{L: zaptest.NewLogger(t)}

	c := app.Config{
		HTTPEndpoint: "localhost:0",
		Proxy:        os.Getenv("YT_PROXY"),
		TablePath:    "//home/kristevalex/ytprof/testing",
	}

	a := app.NewApp(l, c)

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
