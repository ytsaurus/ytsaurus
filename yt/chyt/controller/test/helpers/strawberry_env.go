package helpers

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/chyt/controller/internal/agent"
	"go.ytsaurus.tech/yt/chyt/controller/internal/app"
	"go.ytsaurus.tech/yt/chyt/controller/internal/chyt"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yttest"
)

type Config struct {
	Family string

	ControllerConfig  yson.RawValue
	ControllerFactory strawberry.ControllerFactory

	ClusterInitializerFactory strawberry.ClusterInitializerFactory
}

func PrepareController(t *testing.T, c *Config) (client *RequestClient, teardownCb func(t *testing.T)) {
	if os.Getenv("YT_PROXY") == "" {
		t.Skip("Skipping testing as there is no local yt.")
	}

	setupCluster(t, c)

	stopCh := make(chan struct{})
	apiEndpoint := runController(t, c, stopCh)

	env := yttest.New(t)

	client = &RequestClient{
		Endpoint:   fmt.Sprintf("http://%v", apiEndpoint),
		Proxy:      os.Getenv("YT_PROXY"),
		User:       "root",
		httpClient: &http.Client{},
		t:          t,
		Env: &Env{
			Env:            env,
			StrawberryRoot: "//sys/strawberry",
		},
	}

	return client, func(t *testing.T) {
		close(stopCh)

		Wait(t, func() bool {
			_, err := http.Get(fmt.Sprintf("http://%v/ping", apiEndpoint))
			return err != nil
		})
	}
}

func setupCluster(t *testing.T, c *Config) {
	config := app.ClusterInitializerConfig{
		BaseConfig: app.BaseConfig{
			Proxy:          os.Getenv("YT_PROXY"),
			StrawberryRoot: "//sys/strawberry",
		},
		Families: []string{c.Family},
	}

	familyToInitializerFactory := map[string]strawberry.ClusterInitializerFactory{
		"chyt": chyt.NewClusterInitializer,
	}

	initializer := app.NewClusterInitializer(&config, familyToInitializerFactory)
	err := initializer.InitCluster()
	require.NoError(t, err)
}

func runController(t *testing.T, c *Config, stopCh <-chan struct{}) (apiEndpoint string) {
	passPeriod := yson.Duration(100 * time.Millisecond)
	collectOperationsPeriod := yson.Duration(500 * time.Millisecond)
	revisionCollectPeriod := yson.Duration(100 * time.Millisecond)
	apiEndpoint = ":0"

	config := app.Config{
		LocationProxies: []string{os.Getenv("YT_PROXY")},
		Strawberry: agent.Config{
			Root:                    "//sys/strawberry",
			PassPeriod:              &passPeriod,
			CollectOperationsPeriod: &collectOperationsPeriod,
			RevisionCollectPeriod:   &revisionCollectPeriod,
			Stage:                   "test",
			RobotUsername:           "root",
		},
		Controllers: map[string]yson.RawValue{
			c.Family: c.ControllerConfig,
		},
		HTTPAPIEndpoint: &apiEndpoint,
		DisableAPIAuth:  true,
	}

	app := app.New(&config, &app.Options{}, map[string]strawberry.ControllerFactory{
		c.Family: c.ControllerFactory,
	})
	go func() {
		app.Run(stopCh)
	}()

	app.HTTPAPIServer.WaitReady()
	apiEndpoint = app.HTTPAPIServer.RealAddress()

	rsp, err := http.Get("http://" + apiEndpoint + "/ping")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, rsp.StatusCode)

	return
}
