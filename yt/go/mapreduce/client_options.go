package mapreduce

import (
	"context"

	"github.com/cenkalti/backoff/v4"

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
	// UploadSelfBackoff is used to retry uploading the operation binary to YT.
	//
	// backoff.NewExponentialBackOff will be used by default.
	UploadSelfBackoff backoff.BackOff
	// ShouldRetryTooManyOperationsError determines whether the StartOperation
	// that caused the yterrors.CodeTooManyOperations error should be retried.
	ShouldRetryTooManyOperationsError bool
}

func DefaultConfig() *Config {
	return &Config{
		CreateOutputTables: true,
	}
}
