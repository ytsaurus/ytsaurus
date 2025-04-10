package httpclient

import (
	"bytes"
	"context"
	"sync"

	"github.com/ydb-platform/fq-connector-go/library/go/ptr"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal"
)

func (c *httpClient) NewBatchRequest() (yt.BatchRequest, error) {
	return &batchRequest{client: c}, nil
}

type batchRequest struct {
	mu          sync.Mutex
	subrequests []internal.BatchSubrequest
	resultChs   []chan<- batchSubrequestResult
	isExecuted  bool

	client *httpClient
}

func (r *batchRequest) ExecuteBatch(ctx context.Context, options *yt.BatchRequestOptions) error {
	if err := r.setExecuted(); err != nil {
		return err
	}

	concurrency := 50
	if options != nil && options.Concurrency != nil {
		concurrency = *options.Concurrency
	}

	var mutatingOptions *yt.MutatingOptions
	if options != nil && options.MutatingOptions != nil {
		mutatingOptions = options.MutatingOptions
	}

	results, err := r.client.ExecuteBatch(
		ctx, r.subrequests, &internal.ExecuteBatchOptions{Concurrency: &concurrency, MutatingOptions: mutatingOptions},
	)
	result := batchSubrequestResult{err: err}
	for i, resultCh := range r.resultChs {
		if err == nil { // no error
			result = batchSubrequestResult{output: results[i].Output}
			if results[i].Error != nil {
				result.err = results[i].Error
			}
		}
		resultCh <- result
		close(resultCh)
	}

	return err
}

func (r *batchRequest) CreateNode(
	ctx context.Context,
	path ypath.YPath,
	typ yt.NodeType,
	options *yt.CreateNodeOptions,
) (yt.BatchResponse[yt.NodeID], error) {
	return registerSubrequestWithResult[yt.NodeID](
		r, requestInfo{params: internal.NewCreateNodeParams(path, typ, options)}, internal.CreateNodeResultDecoder,
	)
}

func (r *batchRequest) CreateObject(
	ctx context.Context,
	typ yt.NodeType,
	options *yt.CreateObjectOptions,
) (yt.BatchResponse[yt.NodeID], error) {
	return registerSubrequestWithResult[yt.NodeID](
		r, requestInfo{params: internal.NewCreateObjectParams(typ, options)}, internal.CreateObjectResultDecoder,
	)
}

func (r *batchRequest) NodeExists(
	ctx context.Context,
	path ypath.YPath,
	options *yt.NodeExistsOptions,
) (yt.BatchResponse[bool], error) {
	return registerSubrequestWithResult[bool](
		r, requestInfo{params: internal.NewNodeExistsParams(path, options)}, internal.NodeExistsResultDecoder,
	)
}

func (r *batchRequest) RemoveNode(
	ctx context.Context,
	path ypath.YPath,
	options *yt.RemoveNodeOptions,
) (yt.VoidBatchResponse, error) {
	return r.registerSubrequest(requestInfo{params: internal.NewRemoveNodeParams(path, options)})
}

func (r *batchRequest) GetNode(
	ctx context.Context,
	path ypath.YPath,
	result any,
	options *yt.GetNodeOptions,
) (yt.VoidBatchResponse, error) {
	return r.registerSubrequestWithOutResult(
		requestInfo{params: internal.NewGetNodeParams(path, options)},
		result,
		internal.GetNodeResultDecoder,
	)
}

func (r *batchRequest) SetNode(
	ctx context.Context,
	path ypath.YPath,
	value any,
	options *yt.SetNodeOptions,
) (yt.VoidBatchResponse, error) {
	input, err := yson.Marshal(value)
	if err != nil {
		return nil, err
	}
	return r.registerSubrequest(requestInfo{params: internal.NewSetNodeParams(path, options), input: input})
}

func (r *batchRequest) MultisetAttributes(
	ctx context.Context,
	path ypath.YPath,
	attributes map[string]any,
	options *yt.MultisetAttributesOptions,
) (yt.VoidBatchResponse, error) {
	input, err := yson.Marshal(attributes)
	if err != nil {
		return nil, err
	}
	return r.registerSubrequest(requestInfo{params: internal.NewMultisetAttributesParams(path, options), input: input})
}

func (r *batchRequest) ListNode(
	ctx context.Context,
	path ypath.YPath,
	result any,
	options *yt.ListNodeOptions,
) (yt.VoidBatchResponse, error) {
	return r.registerSubrequestWithOutResult(
		requestInfo{params: internal.NewListNodeParams(path, options)},
		result,
		internal.ListNodeResultDecoder,
	)
}

func (r *batchRequest) CopyNode(
	ctx context.Context,
	src ypath.YPath,
	dst ypath.YPath,
	options *yt.CopyNodeOptions,
) (yt.BatchResponse[yt.NodeID], error) {
	opts := yt.CopyNodeOptions{}
	if options != nil {
		opts = *options
	}
	if opts.EnableCrossCellCopying != nil && *opts.EnableCrossCellCopying {
		panic("cross-cell copying is not supported in batch request")
	}
	opts.EnableCrossCellCopying = ptr.Bool(false)
	return registerSubrequestWithResult[yt.NodeID](
		r, requestInfo{params: internal.NewCopyNodeParams(src, dst, options)}, internal.CopyMoveNodeResultDecoder,
	)
}

func (r *batchRequest) MoveNode(
	ctx context.Context,
	src ypath.YPath,
	dst ypath.YPath,
	options *yt.MoveNodeOptions,
) (yt.BatchResponse[yt.NodeID], error) {
	opts := yt.MoveNodeOptions{}
	if options != nil {
		opts = *options
	}
	if opts.EnableCrossCellCopying != nil && *opts.EnableCrossCellCopying {
		panic("cross-cell copying is not supported in batch request")
	}
	opts.EnableCrossCellCopying = ptr.Bool(false)
	return registerSubrequestWithResult[yt.NodeID](
		r, requestInfo{params: internal.NewMoveNodeParams(src, dst, options)}, internal.CopyMoveNodeResultDecoder,
	)
}

func (r *batchRequest) LinkNode(
	ctx context.Context,
	target ypath.YPath,
	link ypath.YPath,
	options *yt.LinkNodeOptions,
) (yt.BatchResponse[yt.NodeID], error) {
	return registerSubrequestWithResult[yt.NodeID](
		r, requestInfo{params: internal.NewLinkNodeParams(target, link, options)}, internal.LinkNodeResultDecoder,
	)
}

func (r *batchRequest) registerSubrequest(reqInfo requestInfo) (yt.VoidBatchResponse, error) {
	resultCh, err := r.newSubrequest(reqInfo)
	if err != nil {
		return nil, err
	}
	return newVoidBatchResponse(resultCh), nil
}

func registerSubrequestWithResult[T any](
	r *batchRequest,
	reqInfo requestInfo,
	resultDecoder internal.AnyValueResultDecoder,
) (yt.BatchResponse[T], error) {
	resultCh, err := r.newSubrequest(reqInfo)
	if err != nil {
		return nil, err
	}
	return newBatchResponse(resultCh, func(res []byte) (v T, err error) {
		err = resultDecoder(&v)(&internal.CallResult{YSONValue: res})
		return
	}), nil
}

func (r *batchRequest) registerSubrequestWithOutResult(
	reqInfo requestInfo,
	result any,
	resultDecoder internal.AnyValueResultDecoder,
) (yt.VoidBatchResponse, error) {
	resultCh, err := r.newSubrequest(reqInfo)
	if err != nil {
		return nil, err
	}
	return newBatchResponse(resultCh, func(res []byte) (_ any, err error) {
		err = resultDecoder(result)(&internal.CallResult{YSONValue: res})
		return
	}), nil
}

func (r *batchRequest) newSubrequest(reqInfo requestInfo) (<-chan batchSubrequestResult, error) {
	subrequest, resultCh, err := makeBatchSubrequest(reqInfo)
	if err != nil {
		return nil, err
	}
	if err := r.addSubrequest(subrequest); err != nil {
		return nil, err
	}
	return resultCh, nil
}

func (r *batchRequest) addSubrequest(subrequest *batchSubrequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.isExecuted {
		return xerrors.New("cannot add subrequest since batch request is already executed")
	}

	r.subrequests = append(r.subrequests, subrequest.subrequest)
	r.resultChs = append(r.resultChs, subrequest.resultCh)

	return nil
}

func (r *batchRequest) setExecuted() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isExecuted {
		return xerrors.New("cannot execute batch request since it is already executed")
	}
	r.isExecuted = true
	return nil
}

type requestInfo struct {
	params internal.Params
	input  []byte
}

type batchSubrequest struct {
	subrequest internal.BatchSubrequest
	resultCh   chan<- batchSubrequestResult
}

type batchSubrequestResult struct {
	output *yson.RawValue
	err    error
}

func newBatchSubrequest(
	reqInfo requestInfo,
	resultCh chan<- batchSubrequestResult,
) (*batchSubrequest, error) {
	var paramsYSON bytes.Buffer
	w := yson.NewWriterFormat(&paramsYSON, yson.FormatText)
	w.BeginMap()
	reqInfo.params.MarshalHTTP(w)
	w.EndMap()
	if err := w.Finish(); err != nil {
		return nil, err
	}
	var parameters map[string]any
	if err := yson.Unmarshal(paramsYSON.Bytes(), &parameters); err != nil {
		return nil, err
	}
	var input any
	if reqInfo.input != nil {
		input = yson.RawValue(reqInfo.input)
	}
	return &batchSubrequest{
		resultCh: resultCh,
		subrequest: internal.BatchSubrequest{
			Commmand:   reqInfo.params.HTTPVerb().String(),
			Parameters: parameters,
			Input:      input,
		},
	}, nil
}

func makeBatchSubrequest(reqInfo requestInfo) (*batchSubrequest, <-chan batchSubrequestResult, error) {
	resultCh := make(chan batchSubrequestResult, 1)
	subrequest, err := newBatchSubrequest(reqInfo, resultCh)
	if err != nil {
		return nil, nil, err
	}
	return subrequest, resultCh, nil
}

type voidBatchResponse struct {
	mu         sync.Mutex
	resultFunc func() error
	err        *error
}

func (r *voidBatchResponse) Error() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return *r.err
	}
	r.err = ptr.T(r.resultFunc())
	return *r.err
}

func newVoidBatchResponse(
	resultCh <-chan batchSubrequestResult,
) yt.VoidBatchResponse {
	return &voidBatchResponse{resultFunc: func() error {
		result, ok := <-resultCh
		if !ok {
			panic("internal error: result channel is closed")
		}
		return result.err
	}}
}

func newVoidBatchResponseWithResultDecoding(
	resultCh <-chan batchSubrequestResult,
	resultDecoder func(res []byte) error,
) yt.VoidBatchResponse {
	return &voidBatchResponse{resultFunc: func() error {
		result, ok := <-resultCh
		if !ok {
			panic("internal error: result channel is closed")
		}
		if result.err != nil {
			return result.err
		}
		if result.output == nil {
			return xerrors.New("server did not return output for subrequest")
		}
		return resultDecoder(*result.output)
	}}
}

type batchResponse[T any] struct {
	mu         sync.Mutex
	resultFunc func() batchResult[T]
	result     *batchResult[T]
}

type batchResult[T any] struct {
	result T
	err    error
}

func (r *batchResponse[T]) Result() T {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.result != nil {
		return r.result.result
	}
	r.result = ptr.T(r.resultFunc())
	return r.result.result
}

func (r *batchResponse[T]) Error() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.result != nil {
		return r.result.err
	}
	r.result = ptr.T(r.resultFunc())
	return r.result.err
}

func newBatchResponse[T any](
	resultCh <-chan batchSubrequestResult,
	resultDecoder func(res []byte) (T, error),
) yt.BatchResponse[T] {
	return &batchResponse[T]{resultFunc: func() batchResult[T] {
		result, ok := <-resultCh
		if !ok {
			panic("internal error: result channel is closed")
		}
		if result.err != nil {
			return batchResult[T]{err: result.err}
		}
		if result.output == nil {
			return batchResult[T]{err: xerrors.New("server did not return output for subrequest")}
		}
		v, err := resultDecoder(*result.output)
		return batchResult[T]{result: v, err: err}
	}}
}
