package app

import (
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/chyt/controller/internal/strawberry"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
)

// Config is taken from yson config file. It contains both strawberry-specific options
// and controller-specific options; at this point they are opaque and passed as raw YSON.
type Config struct {
	// Token of the user to start operations from and read state.
	Token string `yson:"token"`
	// Proxy for operating cluster; e.g. hume or localhost:4243.
	Proxy string `yson:"proxy"`
	// Root points to root directory with operation states.
	Root ypath.Path `yson:"root"`

	// TODO(max42): unmarshalling of duration works weird; investigate that.

	// Period defines how often agent performs its passes.
	Period uint64 `yson:"period"`

	// Controller contains opaque controller options.
	Controller yson.RawValue `yson:"controller"`
}

// Options contains all options not controlled by config (e.g.. taken from CLI flags).
type Options struct {
	// ForceFlush makes agent flush all operation states on startup. Useful for developing.
	ForceFlush bool
}

type App struct {
	l     log.Logger
	agent *strawberry.Agent
}

func New(l log.Logger, config *Config, options *Options, cfs []strawberry.ControllerFactory) *App {
	App := &App{
		l: l,
	}

	ytc, err := ythttp.NewClient(&yt.Config{
		Token: config.Token,
		Proxy: config.Proxy,
	})

	if err != nil {
		l.Fatal("error creating YT client", log.Error(err))
	}

	if config.Period == 0 {
		l.Fatal("period should be specified")
	}

	controllers := make(map[string]strawberry.Controller, len(cfs))
	for _, cf := range cfs {
		controller := cf(l, ytc, config.Root, config.Proxy)
		controllers[controller.Family()] = controller
	}

	App.agent = strawberry.NewAgent(config.Proxy, ytc, l, controllers, config.Root)
	App.agent.Start(options.ForceFlush, time.Millisecond*time.Duration(config.Period))

	return App
}
