package yttest

import (
	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/yt"
)

type Option interface {
	isOption()
}

type loggerOption struct{ l log.Structured }

func WithLogger(l log.Structured) Option {
	return loggerOption{l: l}
}

func (o loggerOption) isOption() {}

type configOption struct{ c yt.Config }

func WithConfig(c yt.Config) Option {
	return configOption{c: c}
}

func (c configOption) isOption() {}
