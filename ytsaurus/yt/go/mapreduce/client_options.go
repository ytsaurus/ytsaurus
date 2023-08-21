package mapreduce

import (
	"context"

	"go.ytsaurus.tech/yt/go/yt"
)

type Option interface {
	isClientOption()
}

type contextOption struct {
	ctx context.Context
}

func (*contextOption) isClientOption() {}

func WithContext(ctx context.Context) Option {
	return &contextOption{ctx}
}

type configOption struct {
	config *Config
}

func (*configOption) isClientOption() {}

func WithConfig(config *Config) Option {
	return &configOption{config}
}

type defaultACLOption struct {
	acl []yt.ACE
}

func (*defaultACLOption) isClientOption() {}

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
