package app

import (
	"context"
	"os"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	logzap "a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/chyt/controller/internal/strawberry"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/ytlock"
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
	Strawberry *strawberry.Config `yson:"strawberry"`

	// Controller contains opaque controller config.
	Controller yson.RawValue `yson:"controller"`
}

// Location defines an operating cluster.
type Location struct {
	l   *logzap.Logger
	a   *strawberry.Agent
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

	if config.Token == "" {
		config.Token = os.Getenv("STRAWBERRY_TOKEN")
	}

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
		loc.a = strawberry.NewAgent(proxy, loc.ytc, withName(loc.l, "a"), loc.c, config.Strawberry)

		app.locations = append(app.locations, loc)
		l.Debug("location ready")
	}

	l.Info("app is ready to serve locations", log.Strings("location_proxies", config.LocationProxies))

	return
}

// Run starts the infinite loop consisting of lock acquisition and agent operation.
func (app App) Run() {
	for {
		app.l.Debug("trying to acquire lock")
		lost, err := app.acquireLock()
		if err != nil {
			app.l.Debug("error acquiring lock, backing off", log.Error(err))
			time.Sleep(time.Second * 5)
			continue
		}
		app.l.Info("lock acquired")

		for _, loc := range app.locations {
			loc.a.Start()
		}

		<-lost
		app.l.Info("lock lost")

		for _, loc := range app.locations {
			loc.a.Stop()
		}
	}
}
