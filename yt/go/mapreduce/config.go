package mapreduce

import "context"

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

type Config struct {
	CreateOutputTables bool
}

func DefaultConfig() *Config {
	return &Config{
		CreateOutputTables: true,
	}
}
