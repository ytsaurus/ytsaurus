package yt

import (
	"context"

	"a.yandex-team.ru/yt/go/schema"

	"a.yandex-team.ru/yt/go/ypath"
)

type TableWriter interface {
	Write(value interface{}) error
	Commit() error
}

type TableReader interface {
	Scan(value interface{}) error
	Next() bool
	Err() error
	Close() error
}

type CreateTableOption func(options *CreateNodeOptions)

func WithSchema(schema schema.Schema) CreateTableOption {
	return func(options *CreateNodeOptions) {
		if options.Attributes == nil {
			options.Attributes = map[string]interface{}{}
		}

		options.Attributes["schema"] = schema
	}
}

func WithInferredSchema(row interface{}) CreateTableOption {
	return func(options *CreateNodeOptions) {
		if options.Attributes == nil {
			options.Attributes = map[string]interface{}{}
		}

		options.Attributes["schema"] = schema.MustInfer(row)
	}
}

func CreateTable(ctx context.Context, yc CypressClient, path ypath.Path, opts ...CreateTableOption) (id NodeID, err error) {
	var createOptions CreateNodeOptions
	for _, opt := range opts {
		opt(&createOptions)
	}

	return yc.CreateNode(ctx, path, NodeTable, &createOptions)
}
