package yt

import (
	"context"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
)

// TableWriter is interface for writing stream of rows.
type TableWriter interface {
	// Write writes single row.
	//
	// Error returned from write indicates that the whole write operation has failed.
	Write(value any) error

	// Commit closes table writer.
	Commit() error

	// Rollback aborts table upload and frees associated resources.
	//
	// It is safe to call Rollback() concurrently with Write or Commit.
	//
	// Rollback blocks until upload transaction is aborted.
	//
	// If you need to cancel table writer without blocking, use context cancelFunc.
	//
	// Error returned from Rollback() may be safely ignored.
	Rollback() error
}

// StartRowIndex returns row index of the first row in table reader.
//
// Index might not be available, depending on the underlying implementation
func StartRowIndex(r TableReader) (rowIndex int64, ok bool) {
	rr, ok := r.(interface{ StartRowIndex() (int64, bool) })
	if ok {
		return rr.StartRowIndex()
	}
	return
}

// ApproximateRowCount returns approximation of total number of rows in this reader.
//
// Might not be available, depending on the underlying implementation
func ApproximateRowCount(r TableReader) (count int64, ok bool) {
	rr, ok := r.(interface{ ApproximateRowCount() (int64, bool) })
	if ok {
		return rr.ApproximateRowCount()
	}
	return
}

// TableReader is interface for reading stream of rows.
type TableReader interface {
	// Scan unmarshals current row into value.
	//
	// It is safe to call Scan multiple times for a single row.
	Scan(value any) error

	// Next prepares the next result row for reading with the Scan method.
	//
	// It returns true on success, or false if there is no next result row or an error
	// happened while preparing it. Err should be consulted to distinguish between the two cases.
	Next() bool

	// Err returns error that occurred during read.
	Err() error

	// Close frees any associated resources.
	//
	// User MUST call Close(). Failure to do so will result in resource leak.
	//
	// Error returned from Close() may be safely ignored.
	Close() error
}

type CreateTableOption func(options *CreateNodeOptions)

func WithSchema(schema schema.Schema) CreateTableOption {
	return func(options *CreateNodeOptions) {
		if options.Attributes == nil {
			options.Attributes = map[string]any{}
		}

		options.Attributes["schema"] = schema
	}
}

func WithInferredSchema(row any) CreateTableOption {
	return func(options *CreateNodeOptions) {
		if options.Attributes == nil {
			options.Attributes = map[string]any{}
		}

		options.Attributes["schema"] = schema.MustInfer(row)
	}
}

func WithForce() CreateTableOption {
	return func(options *CreateNodeOptions) {
		options.Force = true
	}
}

func WithIgnoreExisting() CreateTableOption {
	return func(options *CreateNodeOptions) {
		options.IgnoreExisting = true
	}
}

func WithRecursive() CreateTableOption {
	return func(options *CreateNodeOptions) {
		options.Recursive = true
	}
}

func WithAttributes(attrs map[string]any) CreateTableOption {
	return func(options *CreateNodeOptions) {
		if options.Attributes == nil {
			options.Attributes = map[string]any{}
		}

		for k, v := range attrs {
			if _, ok := options.Attributes[k]; !ok {
				options.Attributes[k] = v
			}
		}
	}
}

func CreateTable(ctx context.Context, yc CypressClient, path ypath.Path, opts ...CreateTableOption) (id NodeID, err error) {
	var createOptions CreateNodeOptions
	for _, opt := range opts {
		opt(&createOptions)
	}

	return yc.CreateNode(ctx, path, NodeTable, &createOptions)
}
