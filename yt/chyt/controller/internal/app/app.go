package app

import (
	"fmt"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/chyt/controller/internal/strawberry"
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

	// Strawberry contains strawberry-specific configuration.
	Strawberry *strawberry.Config `yson:"strawberry"`

	// Controller contains opaque controller options.
	Controllers map[string]yson.RawValue `yson:"controllers"`
}

// Options contains all options not controlled by config (e.g.. taken from CLI flags).
type Options struct {
}

type App struct {
	l     log.Logger
	agent *strawberry.Agent
}

func New(l log.Logger, config *Config, options *Options, factories map[string]strawberry.ControllerFactory) *App {
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

	controllers := make(map[string]strawberry.Controller, len(factories))
	for family, factory := range factories {
		controllerConfig := config.Controllers[family]
		controller := factory(l, ytc, config.Strawberry.Root, config.Proxy, controllerConfig)
		controllers[family] = controller
		if family != controller.Family() {
			panic(fmt.Errorf("strawberry: controller family does not match configuration key: %v != %v", family, controller.Family()))
		}
	}

	App.agent = strawberry.NewAgent(config.Proxy, ytc, l, controllers, config.Strawberry)
	App.agent.Start()

	return App
}
