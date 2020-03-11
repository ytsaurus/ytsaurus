package yttest

import "a.yandex-team.ru/library/go/core/log"

type Option interface {
	isOption()
}

type loggerOption struct{ l log.Structured }

func WithLogger(l log.Structured) Option {
	return loggerOption{l: l}
}

func (o loggerOption) isOption() {}
