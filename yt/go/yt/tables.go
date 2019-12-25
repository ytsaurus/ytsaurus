package yt

import (
	"context"

	"a.yandex-team.ru/yt/go/schema"

	"a.yandex-team.ru/yt/go/ypath"
)

// TableWriter is interface for writing stream of rows.
type TableWriter interface {
	// Write writes single row.
	Write(value interface{}) error

	// Commit closes table writer.
	Commit() error

	// Rollback aborts table upload and frees associated resources.
	//
	// It is safe to call Rollback() concurrently with Write or Commit.
	//
	// Rollback blocks until upload transaction is aborted.
	//
	// If you need to cancel table writer without blocking, use context cancelFunc.
	Rollback() error
}

// StartRowIndex returns row index of the first row in table reader.
//
// Index might not be available, depending on the underlying implementation
func StartRowIndex(r TableReader) (rowIndex int64, ok bool) {
	indexer, ok := r.(interface{ StartRowIndex() int64 })
	if ok {
		rowIndex = indexer.StartRowIndex()
	}
	return
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

func WithForce() CreateTableOption {
	return func(options *CreateNodeOptions) {
		options.Force = true
	}
}

func WithRecursive() CreateTableOption {
	return func(options *CreateNodeOptions) {
		options.Recursive = true
	}
}

func CreateTable(ctx context.Context, yc CypressClient, path ypath.Path, opts ...CreateTableOption) (id NodeID, err error) {
	var createOptions CreateNodeOptions
	for _, opt := range opts {
		opt(&createOptions)
	}

	return yc.CreateNode(ctx, path, NodeTable, &createOptions)
}
