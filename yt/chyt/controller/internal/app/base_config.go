package app

import "a.yandex-team.ru/yt/go/ypath"

type BaseConfig struct {
	// Token of the user for cluster initialization.
	// If not present, it is taken from STRAWBERRY_TOKEN env var.
	Token string `yson:"token"`

	// LocationProxy to be initialized
	Proxy string `yson:"proxy"`

	// Root points to root directory with operation states.
	StrawberryRoot ypath.Path `yson:"strawberry_root"`
}
