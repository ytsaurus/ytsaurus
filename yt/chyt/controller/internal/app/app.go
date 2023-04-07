package app

import (
	"context"
	"os"
	"time"

	"go.uber.org/atomic"
	"go.ytsaurus.tech/library/go/core/log"
	logzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/chyt/controller/internal/agent"
	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
	"go.ytsaurus.tech/yt/chyt/controller/internal/httpserver"
	"go.ytsaurus.tech/yt/chyt/controller/internal/monitoring"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/ytlock"
)

// Config is taken from yson config file. It contains both strawberry-specific options
// and controller-specific options; at this point they are opaque and passed as raw YSON.
type Config struct {
	// Token of the user for coordination, operation and state management.
	// If not present, it is taken from STRAWBERRY_TOKEN env var.
	Token string `yson:"token"`

	// CoordinationProxy defines coordination cluster if it is needed; e.g. locke.
	CoordinationProxy *string `yson:"coordination_proxy"`

	// CoordinationPath is the path for a lock at the coordination cluster.
	CoordinationPath ypath.Path `yson:"coordination_path"`

	// LocationProxies defines operating clusters; e.g. hume or localhost:4243.
	LocationProxies []string `yson:"location_proxies"`

	// Strawberry contains strawberry-specific configuration.
	Strawberry *agent.Config `yson:"strawberry"`

	// Controller contains opaque controller config.
	Controller yson.RawValue `yson:"controller"`

	HTTPAPIEndpoint        *string `yson:"http_api_endpoint"`
	HTTPMonitoringEndpoint *string `yson:"http_monitoring_endpoint"`

	// HealthStatusExpirationPeriod defines when agent health status becomes outdated.
	HealthStatusExpirationPeriod *time.Duration `yson:"health_status_expiration_period"`

	BaseACL []yt.ACE `yson:"base_acl"`

	DisableAPIAuth bool `yson:"disable_api_auth"`
}

const (
	DefaultHealthStatusExpirationPeriod = time.Duration(time.Minute)
	DefaultHTTPAPIEndpoint              = ":80"
	DefaultHTTPMonitoringEndpoint       = ":2223"
)

func (c *Config) HealthStatusExpirationPeriodOrDefault() time.Duration {
	if c.HealthStatusExpirationPeriod != nil {
		return *c.HealthStatusExpirationPeriod
	}
	return DefaultHealthStatusExpirationPeriod
}

func (c *Config) HTTPAPIEndpointOrDefault() string {
	if c.HTTPAPIEndpoint != nil {
		return *c.HTTPAPIEndpoint
	}
	return DefaultHTTPAPIEndpoint
}

func (c *Config) HTTPMonitoringEndpointOrDefault() string {
	if c.HTTPMonitoringEndpoint != nil {
		return *c.HTTPMonitoringEndpoint
	}
	return DefaultHTTPMonitoringEndpoint
}

// Location defines an operating cluster.
type Location struct {
	l   *logzap.Logger
	a   *agent.Agent
	c   strawberry.Controller
	ytc yt.Client
}

// Options contains all options not controlled by config (e.g.. taken from CLI flags).
type Options struct {
	LogToStderr bool
}

type App struct {
	l *logzap.Logger
	// ytc is a client for coordination cluster.
	ytc       yt.Client
	coordPath ypath.Path

	locations []*Location

	HTTPAPIServer        *httpserver.HTTPServer
	HTTPMonitoringServer *httpserver.HTTPServer

	isLeader *atomic.Bool
}

func (app App) acquireLock() (lost <-chan struct{}, err error) {
	if app.ytc != nil {
		hostname, err := os.Hostname()
		if err != nil {
			panic(err)
		}
		return ytlock.NewLockOptions(
			app.ytc,
			app.coordPath,
			ytlock.Options{
				CreateIfMissing: true,
				LockMode:        yt.LockExclusive,
				TxAttributes: map[string]interface{}{
					"host": hostname,
					"pid":  os.Getpid(),
				},
			}).Acquire(context.Background())
	} else {
		return make(chan struct{}), nil
	}
}

func New(config *Config, options *Options, cf strawberry.ControllerFactory) (app App) {
	l := newLogger("app", options.LogToStderr)
	app.l = l

	config.Token = getStrawberryToken(config.Token)

	var err error

	if config.CoordinationProxy != nil {
		app.ytc, err = ythttp.NewClient(&yt.Config{
			Token:  config.Token,
			Proxy:  *config.CoordinationProxy,
			Logger: withName(l, "yt"),
		})
		app.coordPath = config.CoordinationPath
		if err != nil {
			l.Fatal("error creating coordination YT client", log.Error(err))
		}
	}

	app.locations = make([]*Location, 0)

	var agentInfos []strawberry.AgentInfo
	healthers := make(map[string]monitoring.Healther)
	for _, proxy := range config.LocationProxies {
		l.Debug("initializing location", log.String("location_proxy", proxy))
		var err error

		loc := &Location{}

		loc.l = newLogger("l."+proxy, options.LogToStderr)

		loc.ytc, err = ythttp.NewClient(&yt.Config{
			Token:  config.Token,
			Proxy:  proxy,
			Logger: withName(loc.l, "yt"),
		})
		if err != nil {
			l.Fatal("error creating YT client", log.Error(err), log.String("proxy", proxy))
		}

		loc.c = cf(withName(loc.l, "c"), loc.ytc, config.Strawberry.Root, proxy, config.Controller)
		loc.a = agent.NewAgent(proxy, loc.ytc, withName(loc.l, "a"), loc.c, config.Strawberry)
		healthers[proxy] = loc.a

		app.locations = append(app.locations, loc)
		agentInfos = append(agentInfos, loc.a.GetAgentInfo())
		l.Debug("location ready")
	}

	var apiConfig = api.HTTPAPIConfig{
		BaseAPIConfig: api.APIConfig{
			ControllerFactory: cf,
			ControllerConfig:  config.Controller,
			BaseACL:           config.BaseACL,
			RobotUsername:     config.Strawberry.RobotUsername,
		},
		ClusterInfos: agentInfos,
		Token:        config.Token,
		Endpoint:     config.HTTPAPIEndpointOrDefault(),
		DisableAuth:  config.DisableAPIAuth,
	}
	app.HTTPAPIServer = api.NewServer(apiConfig, l)

	var monitoringConfig = monitoring.HTTPMonitoringConfig{
		Clusters:                     config.LocationProxies,
		Endpoint:                     config.HTTPMonitoringEndpointOrDefault(),
		HealthStatusExpirationPeriod: config.HealthStatusExpirationPeriodOrDefault(),
	}
	app.HTTPMonitoringServer = monitoring.NewServer(monitoringConfig, l, &app, healthers)

	app.isLeader = atomic.NewBool(false)

	l.Info("app is ready to serve locations", log.Strings("location_proxies", config.LocationProxies))

	return
}

// Run starts the infinite loop consisting of lock acquisition and agent operation.
func (app *App) Run() {
	go app.HTTPAPIServer.Run()
	go app.HTTPMonitoringServer.Run()
	for {
		app.l.Debug("trying to acquire lock")
		lost, err := app.acquireLock()
		if err != nil {
			app.l.Debug("error acquiring lock, backing off", log.Error(err))
			time.Sleep(time.Second * 5)
			continue
		}
		app.l.Info("lock acquired")
		app.isLeader.Store(true)

		for _, loc := range app.locations {
			loc.a.Start()
		}

		<-lost
		app.l.Info("lock lost")
		app.isLeader.Store(false)

		for _, loc := range app.locations {
			loc.a.Stop()
		}
	}
}

func (app *App) IsLeader() bool {
	return app.isLeader.Load()
}
