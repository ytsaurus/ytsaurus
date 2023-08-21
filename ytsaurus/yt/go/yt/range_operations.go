package yt

import (
	"context"

	"go.ytsaurus.tech/library/go/ptr"
)

const rangeJobsDefaultLimit = 1000

type RangeOperationsCallback func(op OperationStatus)

// RangeOperations iterates over operations with pagination and calls cb on each operation.
func RangeOperations(ctx context.Context, yc Client, opts *ListOperationsOptions, cb RangeOperationsCallback) error {
	if opts == nil {
		opts = &ListOperationsOptions{}
	}

	for incomplete := true; incomplete; {
		rsp, err := yc.ListOperations(ctx, opts)
		if err != nil {
			return err
		}

		for _, o := range rsp.Operations {
			cb(o)
		}

		incomplete = rsp.Incomplete
		if len(rsp.Operations) > 0 {
			lastOp := rsp.Operations[len(rsp.Operations)-1]
			opts.Cursor = &lastOp.StartTime
		}
	}

	return nil
}

// ListAllOperations lists operations with pagination.
//
// Depending on the filters used the result might be quite big.
// Consider using RangeOperations to limit memory consumption.
func ListAllOperations(ctx context.Context, yc Client, opts *ListOperationsOptions) ([]OperationStatus, error) {
	var ops []OperationStatus

	err := RangeOperations(ctx, yc, opts, func(op OperationStatus) {
		ops = append(ops, op)
	})

	if err != nil {
		return nil, err
	}

	return ops, nil
}

type RangeJobsCallback func(job JobStatus)

// RangeJobs iterates over operation jobs with pagination and calls cb on each job.
func RangeJobs(ctx context.Context, yc Client, opID OperationID, opts *ListJobsOptions, cb RangeJobsCallback) error {
	if opts == nil {
		opts = &ListJobsOptions{}
	}

	if opts.Limit == nil {
		opts.Limit = ptr.Int(rangeJobsDefaultLimit)
	}

	if opts.Offset == nil {
		opts.Offset = ptr.Int(0)
	}

	for offset := *opts.Offset; ; offset += *opts.Limit {
		opts.Offset = &offset

		rsp, err := yc.ListJobs(ctx, opID, opts)
		if err != nil {
			return err
		}

		for _, job := range rsp.Jobs {
			cb(job)
		}

		if len(rsp.Jobs) < *opts.Limit {
			return nil
		}
	}
}

// ListAllJobs lists operation jobs with pagination.
//
// Depending on the filters used the result might be quite big.
// Consider using RangeOperations to limit memory consumption.
func ListAllJobs(ctx context.Context, yc Client, opID OperationID, opts *ListJobsOptions) ([]JobStatus, error) {
	var jobs []JobStatus

	err := RangeJobs(ctx, yc, opID, opts, func(op JobStatus) {
		jobs = append(jobs, op)
	})

	if err != nil {
		return nil, err
	}

	return jobs, nil
}
