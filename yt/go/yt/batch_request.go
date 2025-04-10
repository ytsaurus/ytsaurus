package yt

import (
	"context"

	"go.ytsaurus.tech/yt/go/ypath"
)

// BatchRequest allows to send multiple lightweight requests at once significantly reducing time of their execution.
//
// Methods of this interface accept the same arguments as yt.Client methods but return a BatchResponse[T] or VoidBatchResponse,
// which provide result after the execution of ExecuteBatch.
type BatchRequest interface {
	// ExecuteBatch executes all subrequests of batch request.
	//
	// After execution of this method all response objects returned by subrequests will
	// be filled with either result or error.
	//
	// NOTE: it is undefined in which order these requests are executed.
	//
	// NOTE: This method doesn't throw if subrequest emits error.
	// Instead corresponding response will be return error from Error().
	// So it is always important to call Error().
	//
	// Single BatchRequest instance may be executed only once and cannot be modified (filled with additional requests) after execution.
	// Error will be returned on attempt to modify executed batch request or execute it again.
	ExecuteBatch(ctx context.Context, options *BatchRequestOptions) error

	// CreateNode creates cypress node.
	//
	// See CypressClient.CreateNode.
	CreateNode(
		ctx context.Context,
		path ypath.YPath,
		typ NodeType,
		options *CreateNodeOptions,
	) (BatchResponse[NodeID], error)

	// CreateNode creates cypress object.
	//
	// See CypressClient.CreateObject.
	CreateObject(
		ctx context.Context,
		typ NodeType,
		options *CreateObjectOptions,
	) (BatchResponse[NodeID], error)

	// NodeExists сhecks wether cypress node exists.
	//
	// See CypressClient.NodeExists.
	NodeExists(
		ctx context.Context,
		path ypath.YPath,
		options *NodeExistsOptions,
	) (BatchResponse[bool], error)

	// RemoveNode removes cypress node.
	//
	// See CypressClient.RemoveNode.
	RemoveNode(
		ctx context.Context,
		path ypath.YPath,
		options *RemoveNodeOptions,
	) (VoidBatchResponse, error)

	// GetNode gets cypress node.
	//
	// The 'result' will be set to the request's result after VoidBatchResponse.Error() returns nil.
	//
	// See CypressClient.GetNode.
	GetNode(
		ctx context.Context,
		path ypath.YPath,
		result any,
		options *GetNodeOptions,
	) (VoidBatchResponse, error)

	// SetNode sets cypress node.
	//
	// See CypressClient.SetNode.
	SetNode(
		ctx context.Context,
		path ypath.YPath,
		value any,
		options *SetNodeOptions,
	) (VoidBatchResponse, error)

	// MultisetAttributes sets several attributes at the specified path (if the attributes exist already, they are overwritten).
	//
	// See CypressClient.MultisetAttributes.
	MultisetAttributes(
		ctx context.Context,
		path ypath.YPath,
		attributes map[string]any,
		options *MultisetAttributesOptions,
	) (VoidBatchResponse, error)

	// ListNode lists cypress directory.
	//
	// The 'result' will be set to the request's result after VoidBatchResponse.Error() returns nil.
	//
	// See CypressClient.ListNode.
	ListNode(
		ctx context.Context,
		path ypath.YPath,
		result any,
		options *ListNodeOptions,
	) (VoidBatchResponse, error)

	// CopyNode copies cypress node.
	//
	// Cross-cell copying is not supported in batch request.
	//
	// See CypressClient.CopyNode.
	CopyNode(
		ctx context.Context,
		src ypath.YPath,
		dst ypath.YPath,
		options *CopyNodeOptions,
	) (BatchResponse[NodeID], error)

	// MoveNode moves cypress node.
	//
	// Cross-cell copying is not supported in batch request.
	//
	// See CypressClient.MoveNode.
	MoveNode(
		ctx context.Context,
		src ypath.YPath,
		dst ypath.YPath,
		options *MoveNodeOptions,
	) (BatchResponse[NodeID], error)

	// LinkNode creates symbolic link.
	//
	// See CypressClient.LinkNode.
	LinkNode(
		ctx context.Context,
		target ypath.YPath,
		link ypath.YPath,
		options *LinkNodeOptions,
	) (BatchResponse[NodeID], error)
}

// BatchResponse[T] is the response to a batch subrequest, containing a result of type T.
type BatchResponse[T any] interface {
	VoidBatchResponse

	// Result returns the result of the batch subquery if no error occurred.
	// Blocks until the batch request completes.
	//
	// If VoidBatchResponse.Error returns a non-nil error this method will return a default value of type T.
	Result() T
}

// VoidBatchResponse is the response for a batch subrequest without a result.
type VoidBatchResponse interface {
	// Error returns the batch subrequest error or nil if no error occurred.
	// Blocks until the batch request completes.
	Error() error
}

// BatchRequestOptions are the options for BatchRequest.ExecuteBatch.
type BatchRequestOptions struct {
	// Concurrency determines how many requests will be executed in parallel on the cluster.
	//
	// This parameter could be used to avoid RequestLimitExceeded errors.
	//
	// Default value is 50.
	Concurrency *int

	// MutatingOptions will be used for executing subrequests.
	//
	// Subsequent mutation IDs will be generated for the subrequests starting with the specified mutation ID.
	// MutatingOptions of the subrequests will be ignored.
	MutatingOptions *MutatingOptions
}
