package app

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/metrics/solomon"
	"go.ytsaurus.tech/library/go/httputil/headers"
)

func TestAdminPanelServesMetrics(t *testing.T) {
	get := func(configuredPath, requestedPath string) *httptest.ResponseRecorder {
		panel, err := newAdminPanel(slog.Default(), solomon.NewRegistry(nil), AdminPanelConfig{MetricsPath: configuredPath})
		require.NoError(t, err)

		recorder := httptest.NewRecorder()
		panel.server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, requestedPath, nil))
		return recorder
	}

	for configuredPath, servedPath := range map[string]string{"": "/metrics", "/solomon/all": "/solomon/all"} {
		recorder := get(configuredPath, servedPath)
		require.Equal(t, http.StatusOK, recorder.Code, servedPath)
		require.Equal(t, headers.TypeApplicationXSolomonSpack.String(), recorder.Header().Get(headers.ContentTypeKey), servedPath)
	}

	require.Equal(t, http.StatusNotFound, get("/solomon/all", "/metrics").Code)

	_, err := newAdminPanel(slog.Default(), solomon.NewRegistry(nil), AdminPanelConfig{MetricsPath: "solomon/all"})
	require.ErrorContains(t, err, "must start with")
}

type MyGoodBaseConfig struct {
	Config
	SomeUserCustomField int
}

type MyGoodConfig struct {
	MyGoodBaseConfig
	AnotherUserCustomField string
}

type BadConfig1 struct {
	Foo string
	Bar int
}

func TestResolveConfig(t *testing.T) {
	var config Config
	config.Config.WorkDir = "/work/dir/of/config"
	resolved, err := resolveAppConfig(&config)
	require.NoError(t, err)
	require.Equal(t, "/work/dir/of/config", resolved.WorkDir)

	var goodBaseConfig MyGoodBaseConfig
	goodBaseConfig.WorkDir = "/work/dir/of/config2"
	resolved, err = resolveAppConfig(&goodBaseConfig)
	require.NoError(t, err)
	require.Equal(t, "/work/dir/of/config2", resolved.WorkDir)

	var goodConfig MyGoodConfig
	goodConfig.WorkDir = "/work/dir/of/config3"
	resolved, err = resolveAppConfig(&goodConfig)
	require.NoError(t, err)
	require.Equal(t, "/work/dir/of/config3", resolved.WorkDir)

	var badConfig1 BadConfig1
	_, err = resolveAppConfig(&badConfig1)
	require.Error(t, err, "bad user config type")
}
