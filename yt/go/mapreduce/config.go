package mapreduce

import (
	"context"

	"a.yandex-team.ru/yt/go/yt"
)

type Option interface {
	MapReduceOption()
}

type contextOption struct {
	ctx context.Context
}

func (*contextOption) MapReduceOption() {}

func WithContext(ctx context.Context) Option {
	return &contextOption{ctx}
}

type configOption struct {
	config *Config
}

func (*configOption) MapReduceOption() {}

func WithConfig(config *Config) Option {
	return &configOption{config}
}

type defaultACLOption struct {
	acl []yt.ACE
}

func (*defaultACLOption) MapReduceOption() {}

func WithDefaultOperationACL(acl []yt.ACE) Option {
	return &defaultACLOption{acl}
}

type Config struct {
	CreateOutputTables bool
}

func DefaultConfig() *Config {
	return &Config{
		CreateOutputTables: true,
	}
}
