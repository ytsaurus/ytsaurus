package internal

import (
	"context"

	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

// ExecuteBatchOptions are the options for Client.ExecuteBatch.
type ExecuteBatchOptions struct {
	// Concurrency determines how many requests will be executed in parallel on the cluster.
	//
	// This parameter could be used to avoid RequestLimitExceeded errors.
	Concurrency *int `http:"concurrency,omitnil"`

	*yt.MutatingOptions
}

// BatchSubrequest is a subrequest used in Client.ExecuteBatch.
type BatchSubrequest struct {
	Commmand   string         `yson:"command"`
	Parameters map[string]any `yson:"parameters"`
	Input      any            `yson:"input,omitempty"`
}

// BatchSubrequestResult is the subrequest result returned by Client.ExecuteBatch.
type BatchSubrequestResult struct {
	Output *yson.RawValue  `yson:"output"`
	Error  *yterrors.Error `yson:"error"`
}

type BatchClient interface {
	// ExecuteBatch executes the given set of commands using a single query.
	//
	// http:verb:"execute_batch"
	// http:params:"requests"
	ExecuteBatch(
		ctx context.Context,
		requests []BatchSubrequest,
		options *ExecuteBatchOptions,
	) (results []BatchSubrequestResult, err error)
}
