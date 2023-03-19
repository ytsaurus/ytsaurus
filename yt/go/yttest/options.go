package yttest

import (
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yt"
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
