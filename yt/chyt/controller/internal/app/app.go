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
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/ytlock"
)

// Location defines an operating cluster.
type Location struct {
	l *logzap.Logger
	// as contains mapping from controller family to a corresponding agent.
	as  map[string]*agent.Agent
	ytc yt.Client
}

// Options contains all options not controlled by config (e.g.. taken from CLI flags).
type Options struct {
	LogToStderr bool
}

type App struct {
	l *logzap.Logger
	// ytc is a client for coordination cluster.
	ytc yt.Client

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
				TxAttributes: map[string]any{
					"host": hostname,
					"pid":  os.Getpid(),
				},
			}).Acquire(context.Background())
	} else {
		return make(chan struct{}), nil
	}
}

func New(config *Config, options *Options, cfs map[string]strawberry.ControllerFactory) (app App) {
	l := newLogger("app", options.LogToStderr)
	app.l = l

	config.Token = getStrawberryToken(config.Token)

	var err error

	if config.CoordinationProxy != nil {
		app.ytc, err = ythttp.NewClient(&yt.Config{
			Token:  config.Token,
			Proxy:  *config.CoordinationProxy,
			Logger: l.WithName("yt").(log.Structured),
		})
		app.coordPath = config.CoordinationPath
		if err != nil {
			l.Fatal("error creating coordination YT client", log.Error(err))
		}
	}

	app.locations = make([]*Location, 0)

	var agentInfos []strawberry.AgentInfo
	healthCheckers := make(map[string]monitoring.HealthChecker)
	for _, proxy := range config.LocationProxies {
		l.Debug("initializing location", log.String("location_proxy", proxy))
		var err error

		ytConfig := yt.Config{
			Token: config.Token,
			Proxy: proxy,
		}

		clusterURL, err := ytConfig.GetClusterURL()
		if err != nil {
			l.Fatal("error parsing YT proxy", log.Error(err), log.String("proxy", proxy))
		}

		loc := &Location{}
		loc.l = newLogger("l."+clusterURL.Address, options.LogToStderr)
		ytConfig.Logger = loc.l.WithName("yt").(log.Structured)

		loc.ytc, err = ythttp.NewClient(&ytConfig)
		if err != nil {
			l.Fatal("error creating YT client", log.Error(err), log.String("proxy", proxy))
		}

		aCfg := config.Strawberry.ApplyOverrides(config.LocationStrawberryOverrides[proxy])

		ctlDefaultSpeclet := config.LocationControllerDefaultSpeclet[proxy]

		loc.as = map[string]*agent.Agent{}
		for family, cf := range cfs {
			l := loc.l.WithName(family)
			l.Debug("instantiating controller for location", log.String("location", proxy), log.String("family", family))

			cCfg := cf.Config
			if defaultSpeclet := ctlDefaultSpeclet[family]; len(defaultSpeclet) > 0 {
				newCfg, err := strawberry.AddDefaultSpecletToConfig(cCfg, defaultSpeclet)
				if err != nil {
					l.Fatal("failed to add default speclet for location", log.Error(err))
				}
				cCfg = newCfg
			}

			c := cf.Ctor(l.WithName("c"), loc.ytc, config.Strawberry.RootOrDefault().Child(family), proxy, cCfg)
			a := agent.NewAgent(proxy, config.Token, loc.ytc, l.WithName("a"), c, &aCfg)
			loc.as[family] = a
		}

		// TODO(max42): extend for generic controllers.
		healthCheckers[proxy] = loc.as["chyt"]

		app.locations = append(app.locations, loc)
		for _, a := range loc.as {
			agentInfos = append(agentInfos, a.GetAgentInfo())
		}

		l.Debug("location ready")
	}

	if config.HTTPAPIEndpoint != nil {
		l.Info("initializing HTTP API")
		var apiConfig = api.HTTPAPIConfig{
			BaseAPIConfig: api.APIConfig{
				ControllerFactories:       cfs,
				ControllerMappings:        config.HTTPControllerMappings,
				BaseACL:                   config.BaseACL,
				RobotUsername:             config.Strawberry.RobotUsername,
				AssignAdministerToCreator: config.Strawberry.AssignAdministerToCreatorOrDefault(),
			},
			ClusterInfos:                     agentInfos,
			LocationAliases:                  config.HTTPLocationAliases,
			LocationControllerDefaultSpeclet: config.LocationControllerDefaultSpeclet,
			Token:                            config.Token,
			Endpoint:                         config.HTTPAPIEndpointOrDefault(),
			DisableAuth:                      config.DisableAPIAuth,
		}
		app.HTTPAPIServer = api.NewServer(apiConfig, l)
	}

	if config.HTTPMonitoringEndpoint != nil {
		var monitoringConfig = monitoring.HTTPMonitoringConfig{
			Clusters: config.LocationProxies,
			Endpoint: config.HTTPMonitoringEndpointOrDefault(),
		}
		app.HTTPMonitoringServer = monitoring.NewServer(monitoringConfig, l, &app, healthCheckers)
	}

	app.isLeader = atomic.NewBool(false)

	l.Info("app is ready to serve locations", log.Strings("location_proxies", config.LocationProxies))

	return
}

// Run starts the infinite loop consisting of lock acquisition and agent operation.
func (app *App) Run(stopCh <-chan struct{}) {
	if app.HTTPAPIServer != nil {
		go app.HTTPAPIServer.Run()
	}
	if app.HTTPMonitoringServer != nil {
		go app.HTTPMonitoringServer.Run()
	}

L:
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
			for _, a := range loc.as {
				a.Start()
			}
		}

		select {
		case <-stopCh:
			app.l.Info("app is stopped")
		case <-lost:
			app.l.Info("lock lost")
		}

		app.isLeader.Store(false)

		for _, loc := range app.locations {
			for _, a := range loc.as {
				a.Stop()
			}
		}

		select {
		case <-stopCh:
			break L
		default:
		}
	}

	if app.HTTPAPIServer != nil {
		app.HTTPAPIServer.Stop()
	}
	if app.HTTPMonitoringServer != nil {
		app.HTTPMonitoringServer.Stop()
	}
}

func (app *App) IsLeader() bool {
	return app.isLeader.Load()
}
