package app_test

import (
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/ytprof/internal/app"
)

func TestAppInitialization(t *testing.T) {
	l := &zap.Logger{L: zaptest.NewLogger(t)}

	c := app.Config{
		HTTPEndpoint: "localhost:0",
		Proxy:        "not-perelman",
	}

	a := app.NewApp(l, c)
	client := resty.New()

	rsp, err := client.R().Get(a.URL() + "/ytprof/api/list")
	require.NoError(t, err)
	l.Debug("responce status",
		log.Int("status code", rsp.StatusCode()),
		log.String("status", rsp.Status()),
		log.String("url", a.URL()+"/ytprof/api/list"),
		log.String("response", string(rsp.Body())))

	require.NotEqual(t, rsp.StatusCode(), 404)

	require.NoError(t, a.Stop())
}
