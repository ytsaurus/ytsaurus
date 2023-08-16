package rpcclient

import (
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
	"go.ytsaurus.tech/yt/go/yt"
)

var _ TransactionalRequest = (*CreateNodeRequest)(nil)
var _ MutatingRequest = (*CreateNodeRequest)(nil)

type CreateNodeRequest struct {
	*rpc_proxy.TReqCreateNode
}

func NewCreateNodeRequest(r *rpc_proxy.TReqCreateNode) *CreateNodeRequest {
	return &CreateNodeRequest{TReqCreateNode: r}
}

func (r CreateNodeRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
		log.Int32("typ", r.GetType()),
	}
}

func (r CreateNodeRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *CreateNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *CreateNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

type CreateObjectRequest struct {
	*rpc_proxy.TReqCreateObject
}

func NewCreateObjectRequest(r *rpc_proxy.TReqCreateObject) *CreateObjectRequest {
	return &CreateObjectRequest{TReqCreateObject: r}
}

func (r CreateObjectRequest) Log() []log.Field {
	return []log.Field{
		log.Int32("typ", r.GetType()),
	}
}

func (r CreateObjectRequest) Path() (string, bool) {
	return "", false
}

var _ TransactionalRequest = (*NodeExistsRequest)(nil)
var _ ReadRetryRequest = (*NodeExistsRequest)(nil)

type NodeExistsRequest struct {
	*rpc_proxy.TReqExistsNode
}

func NewNodeExistsRequest(r *rpc_proxy.TReqExistsNode) *NodeExistsRequest {
	return &NodeExistsRequest{TReqExistsNode: r}
}

func (r NodeExistsRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r NodeExistsRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *NodeExistsRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *NodeExistsRequest) ReadRetryOptions() {}

var _ TransactionalRequest = (*RemoveNodeRequest)(nil)
var _ MutatingRequest = (*RemoveNodeRequest)(nil)

type RemoveNodeRequest struct {
	*rpc_proxy.TReqRemoveNode
}

func NewRemoveNodeRequest(r *rpc_proxy.TReqRemoveNode) *RemoveNodeRequest {
	return &RemoveNodeRequest{TReqRemoveNode: r}
}

func (r RemoveNodeRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r RemoveNodeRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *RemoveNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *RemoveNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ TransactionalRequest = (*GetNodeRequest)(nil)
var _ ReadRetryRequest = (*GetNodeRequest)(nil)

type GetNodeRequest struct {
	*rpc_proxy.TReqGetNode
}

func NewGetNodeRequest(r *rpc_proxy.TReqGetNode) *GetNodeRequest {
	return &GetNodeRequest{TReqGetNode: r}
}

func (r GetNodeRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r GetNodeRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *GetNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *GetNodeRequest) ReadRetryOptions() {}

var _ TransactionalRequest = (*SetNodeRequest)(nil)
var _ MutatingRequest = (*SetNodeRequest)(nil)

type SetNodeRequest struct {
	*rpc_proxy.TReqSetNode
}

func NewSetNodeRequest(r *rpc_proxy.TReqSetNode) *SetNodeRequest {
	return &SetNodeRequest{TReqSetNode: r}
}

func (r SetNodeRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r SetNodeRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *SetNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *SetNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ TransactionalRequest = (*MultisetAttributesRequest)(nil)

type MultisetAttributesRequest struct {
	*rpc_proxy.TReqMultisetAttributesNode
}

func NewMultisetAttributesRequest(r *rpc_proxy.TReqMultisetAttributesNode) *MultisetAttributesRequest {
	return &MultisetAttributesRequest{TReqMultisetAttributesNode: r}
}

func (r MultisetAttributesRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r MultisetAttributesRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *MultisetAttributesRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *MultisetAttributesRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ TransactionalRequest = (*ListNodeRequest)(nil)
var _ ReadRetryRequest = (*ListNodeRequest)(nil)

type ListNodeRequest struct {
	*rpc_proxy.TReqListNode
}

func NewListNodeRequest(r *rpc_proxy.TReqListNode) *ListNodeRequest {
	return &ListNodeRequest{TReqListNode: r}
}

func (r ListNodeRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r ListNodeRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *ListNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *ListNodeRequest) ReadRetryOptions() {}

var _ TransactionalRequest = (*CopyNodeRequest)(nil)
var _ MutatingRequest = (*CopyNodeRequest)(nil)

type CopyNodeRequest struct {
	*rpc_proxy.TReqCopyNode
}

func NewCopyNodeRequest(r *rpc_proxy.TReqCopyNode) *CopyNodeRequest {
	return &CopyNodeRequest{TReqCopyNode: r}
}

func (r CopyNodeRequest) Log() []log.Field {
	return []log.Field{
		log.String("src", r.GetSrcPath()),
		log.String("dst", r.GetDstPath()),
	}
}

func (r CopyNodeRequest) Path() (string, bool) {
	return r.GetSrcPath(), true
}

func (r *CopyNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *CopyNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ TransactionalRequest = (*MoveNodeRequest)(nil)
var _ MutatingRequest = (*MoveNodeRequest)(nil)

type MoveNodeRequest struct {
	*rpc_proxy.TReqMoveNode
}

func NewMoveNodeRequest(r *rpc_proxy.TReqMoveNode) *MoveNodeRequest {
	return &MoveNodeRequest{TReqMoveNode: r}
}

func (r MoveNodeRequest) Log() []log.Field {
	return []log.Field{
		log.String("src", r.GetSrcPath()),
		log.String("dst", r.GetDstPath()),
	}
}

func (r MoveNodeRequest) Path() (string, bool) {
	return r.GetSrcPath(), true
}

func (r *MoveNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *MoveNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ TransactionalRequest = (*LinkNodeRequest)(nil)
var _ MutatingRequest = (*LinkNodeRequest)(nil)

type LinkNodeRequest struct {
	*rpc_proxy.TReqLinkNode
}

func NewLinkNodeRequest(r *rpc_proxy.TReqLinkNode) *LinkNodeRequest {
	return &LinkNodeRequest{TReqLinkNode: r}
}

func (r LinkNodeRequest) Log() []log.Field {
	return []log.Field{
		log.String("target", r.GetSrcPath()),
		log.String("link", r.GetDstPath()),
	}
}

func (r LinkNodeRequest) Path() (string, bool) {
	return r.GetDstPath(), true
}

func (r *LinkNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *LinkNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ TransactionalRequest = (*StartTxRequest)(nil)
var _ ReadRetryRequest = (*StartTxRequest)(nil)

type StartTxRequest struct {
	*rpc_proxy.TReqStartTransaction
}

func NewStartTxRequest(r *rpc_proxy.TReqStartTransaction) *StartTxRequest {
	return &StartTxRequest{TReqStartTransaction: r}
}

func (r StartTxRequest) Log() []log.Field {
	return []log.Field{}
}

func (r StartTxRequest) Path() (string, bool) {
	return "", false
}

func (r *StartTxRequest) SetTxOptions(opts *TransactionOptions) {
	if opts == nil {
		return
	}
	r.ParentId = convertTxID(opts.TransactionID)
}

func (r *StartTxRequest) ReadRetryOptions() {}

type StartTabletTxRequest struct {
	*rpc_proxy.TReqStartTransaction
}

func NewStartTabletTxRequest(r *rpc_proxy.TReqStartTransaction) *StartTabletTxRequest {
	return &StartTabletTxRequest{TReqStartTransaction: r}
}

func (r StartTabletTxRequest) Log() []log.Field {
	return []log.Field{}
}

func (r StartTabletTxRequest) Path() (string, bool) {
	return "", false
}

type PingTxRequest struct {
	*rpc_proxy.TReqPingTransaction
}

func NewPingTxRequest(r *rpc_proxy.TReqPingTransaction) *PingTxRequest {
	return &PingTxRequest{TReqPingTransaction: r}
}

func (r PingTxRequest) Log() []log.Field {
	return []log.Field{
		log.Any("id", r.GetTransactionId()),
	}
}

func (r PingTxRequest) Path() (string, bool) {
	return "", false
}

type AbortTxRequest struct {
	*rpc_proxy.TReqAbortTransaction
}

func NewAbortTxRequest(r *rpc_proxy.TReqAbortTransaction) *AbortTxRequest {
	return &AbortTxRequest{TReqAbortTransaction: r}
}

func (r AbortTxRequest) Log() []log.Field {
	return []log.Field{
		log.Any("id", r.GetTransactionId()),
	}
}

func (r AbortTxRequest) Path() (string, bool) {
	return "", false
}

type CommitTxRequest struct {
	*rpc_proxy.TReqCommitTransaction
}

func NewCommitTxRequest(r *rpc_proxy.TReqCommitTransaction) *CommitTxRequest {
	return &CommitTxRequest{TReqCommitTransaction: r}
}

func (r CommitTxRequest) Log() []log.Field {
	return []log.Field{
		log.Any("id", r.GetTransactionId()),
	}
}

func (r CommitTxRequest) Path() (string, bool) {
	return "", false
}

var _ TransactionalRequest = (*WriteFileRequest)(nil)

type WriteFileRequest struct {
	*rpc_proxy.TReqWriteFile
}

func NewWriteFileRequest(r *rpc_proxy.TReqWriteFile) *WriteFileRequest {
	return &WriteFileRequest{TReqWriteFile: r}
}

func (r WriteFileRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r WriteFileRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *WriteFileRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

var _ TransactionalRequest = (*ReadFileRequest)(nil)

type ReadFileRequest struct {
	*rpc_proxy.TReqReadFile
}

func NewReadFileRequest(r *rpc_proxy.TReqReadFile) *ReadFileRequest {
	return &ReadFileRequest{TReqReadFile: r}
}

func (r ReadFileRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r ReadFileRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *ReadFileRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

var _ MutatingRequest = (*PutFileToCacheRequest)(nil)

type PutFileToCacheRequest struct {
	*rpc_proxy.TReqPutFileToCache
}

func NewPutFileToCacheRequest(r *rpc_proxy.TReqPutFileToCache) *PutFileToCacheRequest {
	return &PutFileToCacheRequest{TReqPutFileToCache: r}
}

func (r PutFileToCacheRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
		log.String("md5", r.GetMd5()),
	}
}

func (r PutFileToCacheRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *PutFileToCacheRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ ReadRetryRequest = (*GetFileFromCacheRequest)(nil)

type GetFileFromCacheRequest struct {
	*rpc_proxy.TReqGetFileFromCache
}

func NewGetFileFromCacheRequest(r *rpc_proxy.TReqGetFileFromCache) *GetFileFromCacheRequest {
	return &GetFileFromCacheRequest{TReqGetFileFromCache: r}
}

func (r GetFileFromCacheRequest) Log() []log.Field {
	return []log.Field{
		log.String("md5", r.GetMd5()),
	}
}

func (r GetFileFromCacheRequest) Path() (string, bool) {
	return "", false
}

func (r *GetFileFromCacheRequest) ReadRetryOptions() {}

var _ TransactionalRequest = (*WriteTableRequest)(nil)

type WriteTableRequest struct {
	*rpc_proxy.TReqWriteTable
}

func NewWriteTableRequest(r *rpc_proxy.TReqWriteTable) *WriteTableRequest {
	return &WriteTableRequest{TReqWriteTable: r}
}

func (r WriteTableRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r WriteTableRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *WriteTableRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

var _ TransactionalRequest = (*ReadTableRequest)(nil)

type ReadTableRequest struct {
	*rpc_proxy.TReqReadTable
}

func NewReadTableRequest(r *rpc_proxy.TReqReadTable) *ReadTableRequest {
	return &ReadTableRequest{TReqReadTable: r}
}

func (r ReadTableRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r ReadTableRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *ReadTableRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

var _ TransactionalRequest = (*StartOperationRequest)(nil)
var _ MutatingRequest = (*StartOperationRequest)(nil)

type StartOperationRequest struct {
	*rpc_proxy.TReqStartOperation
}

func NewStartOperationRequest(r *rpc_proxy.TReqStartOperation) *StartOperationRequest {
	return &StartOperationRequest{TReqStartOperation: r}
}

func (r StartOperationRequest) Log() []log.Field {
	return []log.Field{
		log.String("opType", r.GetType().String()),
	}
}

func (r StartOperationRequest) Path() (string, bool) {
	return "", false
}

func (r *StartOperationRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *StartOperationRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

type AbortOperationRequest struct {
	*rpc_proxy.TReqAbortOperation
}

func NewAbortOperationRequest(r *rpc_proxy.TReqAbortOperation) *AbortOperationRequest {
	return &AbortOperationRequest{TReqAbortOperation: r}
}

func (r AbortOperationRequest) Log() []log.Field {
	return []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
	}
}

func (r AbortOperationRequest) Path() (string, bool) {
	return "", false
}

type SuspendOperationRequest struct {
	*rpc_proxy.TReqSuspendOperation
}

func NewSuspendOperationRequest(r *rpc_proxy.TReqSuspendOperation) *SuspendOperationRequest {
	return &SuspendOperationRequest{TReqSuspendOperation: r}
}

func (r SuspendOperationRequest) Log() []log.Field {
	return []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
	}
}

func (r SuspendOperationRequest) Path() (string, bool) {
	return "", false
}

type ResumeOperationRequest struct {
	*rpc_proxy.TReqResumeOperation
}

func NewResumeOperationRequest(r *rpc_proxy.TReqResumeOperation) *ResumeOperationRequest {
	return &ResumeOperationRequest{TReqResumeOperation: r}
}

func (r ResumeOperationRequest) Log() []log.Field {
	return []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
	}
}

func (r ResumeOperationRequest) Path() (string, bool) {
	return "", false
}

type CompleteOperationRequest struct {
	*rpc_proxy.TReqCompleteOperation
}

func NewCompleteOperationRequest(r *rpc_proxy.TReqCompleteOperation) *CompleteOperationRequest {
	return &CompleteOperationRequest{TReqCompleteOperation: r}
}

func (r CompleteOperationRequest) Log() []log.Field {
	return []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
	}
}

func (r CompleteOperationRequest) Path() (string, bool) {
	return "", false
}

type UpdateOperationParametersRequest struct {
	*rpc_proxy.TReqUpdateOperationParameters
}

func NewUpdateOperationParametersRequest(r *rpc_proxy.TReqUpdateOperationParameters) *UpdateOperationParametersRequest {
	return &UpdateOperationParametersRequest{TReqUpdateOperationParameters: r}
}

func (r UpdateOperationParametersRequest) Log() []log.Field {
	return []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
		log.ByteString("params", r.GetParameters()),
	}
}

func (r UpdateOperationParametersRequest) Path() (string, bool) {
	return "", false
}

var _ ReadRetryRequest = (*GetOperationRequest)(nil)

type GetOperationRequest struct {
	*rpc_proxy.TReqGetOperation
}

func NewGetOperationRequest(r *rpc_proxy.TReqGetOperation) *GetOperationRequest {
	return &GetOperationRequest{TReqGetOperation: r}
}

func (r GetOperationRequest) Log() []log.Field {
	return []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
	}
}

func (r GetOperationRequest) Path() (string, bool) {
	return "", false
}

func (r *GetOperationRequest) ReadRetryOptions() {}

var _ ReadRetryRequest = (*ListOperationsRequest)(nil)

type ListOperationsRequest struct {
	*rpc_proxy.TReqListOperations
}

func NewListOperationsRequest(r *rpc_proxy.TReqListOperations) *ListOperationsRequest {
	return &ListOperationsRequest{TReqListOperations: r}
}

func (r ListOperationsRequest) Log() []log.Field {
	return []log.Field{}
}

func (r ListOperationsRequest) Path() (string, bool) {
	return "", false
}

func (r *ListOperationsRequest) ReadRetryOptions() {}

type ListJobsRequest struct {
	*rpc_proxy.TReqListJobs
}

func NewListJobsRequest(r *rpc_proxy.TReqListJobs) *ListJobsRequest {
	return &ListJobsRequest{TReqListJobs: r}
}

func (r ListJobsRequest) Log() []log.Field {
	return []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
	}
}

func (r ListJobsRequest) Path() (string, bool) {
	return "", false
}

type GetJobStderrRequest struct {
	*rpc_proxy.TReqGetJobStderr
}

func NewGetJobStderrRequest(r *rpc_proxy.TReqGetJobStderr) *GetJobStderrRequest {
	return &GetJobStderrRequest{TReqGetJobStderr: r}
}

func (r GetJobStderrRequest) Log() []log.Field {
	return []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
		log.Any("jobID", r.GetJobId()),
	}
}

func (r GetJobStderrRequest) Path() (string, bool) {
	return "", false
}

var _ MutatingRequest = (*AddMemberRequest)(nil)

type AddMemberRequest struct {
	*rpc_proxy.TReqAddMember
}

func NewAddMemberRequest(r *rpc_proxy.TReqAddMember) *AddMemberRequest {
	return &AddMemberRequest{TReqAddMember: r}
}

func (r AddMemberRequest) Log() []log.Field {
	return []log.Field{
		log.String("group", r.GetGroup()),
		log.String("member", r.GetMember()),
	}
}

func (r AddMemberRequest) Path() (string, bool) {
	return "", false
}

func (r *AddMemberRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ MutatingRequest = (*RemoveMemberRequest)(nil)

type RemoveMemberRequest struct {
	*rpc_proxy.TReqRemoveMember
}

func NewRemoveMemberRequest(r *rpc_proxy.TReqRemoveMember) *RemoveMemberRequest {
	return &RemoveMemberRequest{TReqRemoveMember: r}
}

func (r RemoveMemberRequest) Log() []log.Field {
	return []log.Field{
		log.String("group", r.GetGroup()),
		log.String("member", r.GetMember()),
	}
}

func (r RemoveMemberRequest) Path() (string, bool) {
	return "", false
}

func (r *RemoveMemberRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

type AddMaintenanceRequest struct {
	*rpc_proxy.TReqAddMaintenance
}

func NewAddMaintenanceRequest(r *rpc_proxy.TReqAddMaintenance) *AddMaintenanceRequest {
	return &AddMaintenanceRequest{TReqAddMaintenance: r}
}

func (r AddMaintenanceRequest) Log() []log.Field {
	return []log.Field{
		log.Any("component", r.GetComponent()),
		log.String("address", r.GetAddress()),
		log.Any("type", r.GetType()),
		log.String("comment", r.GetComment()),
	}
}

func (r AddMaintenanceRequest) Path() (string, bool) {
	return "", false
}

type RemoveMaintenanceRequest struct {
	*rpc_proxy.TReqRemoveMaintenance
}

func NewRemoveMaintenanceRequest(r *rpc_proxy.TReqRemoveMaintenance) *RemoveMaintenanceRequest {
	return &RemoveMaintenanceRequest{TReqRemoveMaintenance: r}
}

func (r RemoveMaintenanceRequest) Log() []log.Field {
	return []log.Field{
		log.Any("component", r.GetComponent()),
		log.String("address", r.GetAddress()),
		log.Any("ids", r.GetIds()),
		log.Any("type", r.GetType()),
		log.String("user", r.GetUser()),
		log.Bool("mine", r.GetMine()),
	}
}

func (r RemoveMaintenanceRequest) Path() (string, bool) {
	return "", false
}

var _ MutatingRequest = (*TransferAccountResourcesRequest)(nil)

type TransferAccountResourcesRequest struct {
	*rpc_proxy.TReqTransferAccountResources
}

func NewTransferAccountResourcesRequest(r *rpc_proxy.TReqTransferAccountResources) *TransferAccountResourcesRequest {
	return &TransferAccountResourcesRequest{TReqTransferAccountResources: r}
}

func (r TransferAccountResourcesRequest) Log() []log.Field {
	return []log.Field{
		log.String("src_account", r.GetSrcAccount()),
		log.String("dst_account", r.GetDstAccount()),
		log.Any("resource_delta", r.GetResourceDelta()),
	}
}

func (r TransferAccountResourcesRequest) Path() (string, bool) {
	return "", false
}

func (r *TransferAccountResourcesRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ MutatingRequest = (*TransferPoolResourcesRequest)(nil)

type TransferPoolResourcesRequest struct {
	*rpc_proxy.TReqTransferPoolResources
}

func NewTransferPoolResourcesRequest(r *rpc_proxy.TReqTransferPoolResources) *TransferPoolResourcesRequest {
	return &TransferPoolResourcesRequest{TReqTransferPoolResources: r}
}

func (r TransferPoolResourcesRequest) Log() []log.Field {
	return []log.Field{
		log.String("src_pool", r.GetSrcPool()),
		log.String("dst_pool", r.GetDstPool()),
		log.String("pool_tree", r.GetPoolTree()),
		log.Any("resource_delta", r.GetResourceDelta()),
	}
}

func (r TransferPoolResourcesRequest) Path() (string, bool) {
	return "", false
}

func (r *TransferPoolResourcesRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ ReadRetryRequest = (*CheckPermissionRequest)(nil)
var _ TransactionalRequest = (*CheckPermissionRequest)(nil)

type CheckPermissionRequest struct {
	*rpc_proxy.TReqCheckPermission
}

func NewCheckPermissionRequest(r *rpc_proxy.TReqCheckPermission) *CheckPermissionRequest {
	return &CheckPermissionRequest{TReqCheckPermission: r}
}

func (r *CheckPermissionRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
		log.String("user", r.GetUser()),
		log.Int32("permission", r.GetPermission()),
	}
}

func (r *CheckPermissionRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *CheckPermissionRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *CheckPermissionRequest) ReadRetryOptions() {}

type DisableChunkLocationsRequest struct {
	*rpc_proxy.TReqDisableChunkLocations
}

func NewDisableChunkLocationsRequest(r *rpc_proxy.TReqDisableChunkLocations) *DisableChunkLocationsRequest {
	return &DisableChunkLocationsRequest{TReqDisableChunkLocations: r}
}

func (r DisableChunkLocationsRequest) Log() []log.Field {
	return []log.Field{
		log.String("node_address", r.GetNodeAddress()),
		log.Any("location_uuids", r.GetLocationUuids()),
	}
}

func (r DisableChunkLocationsRequest) Path() (string, bool) {
	return "", false
}

type DestroyChunkLocationsRequest struct {
	*rpc_proxy.TReqDestroyChunkLocations
}

func NewDestroyChunkLocationsRequest(r *rpc_proxy.TReqDestroyChunkLocations) *DestroyChunkLocationsRequest {
	return &DestroyChunkLocationsRequest{TReqDestroyChunkLocations: r}
}

func (r DestroyChunkLocationsRequest) Log() []log.Field {
	return []log.Field{
		log.String("node_address", r.GetNodeAddress()),
		log.Any("location_uuids", r.GetLocationUuids()),
	}
}

func (r DestroyChunkLocationsRequest) Path() (string, bool) {
	return "", false
}

type ResurrectChunkLocationsRequest struct {
	*rpc_proxy.TReqResurrectChunkLocations
}

func NewResurrectChunkLocationsRequest(r *rpc_proxy.TReqResurrectChunkLocations) *ResurrectChunkLocationsRequest {
	return &ResurrectChunkLocationsRequest{TReqResurrectChunkLocations: r}
}

func (r ResurrectChunkLocationsRequest) Log() []log.Field {
	return []log.Field{
		log.String("node_address", r.GetNodeAddress()),
		log.Any("location_uuids", r.GetLocationUuids()),
	}
}

func (r ResurrectChunkLocationsRequest) Path() (string, bool) {
	return "", false
}

type RequestRebootRequest struct {
	*rpc_proxy.TReqRequestReboot
}

func NewRequestRebootRequest(r *rpc_proxy.TReqRequestReboot) *RequestRebootRequest {
	return &RequestRebootRequest{TReqRequestReboot: r}
}

func (r RequestRebootRequest) Log() []log.Field {
	return []log.Field{
		log.String("node_address", r.GetNodeAddress()),
	}
}

func (r RequestRebootRequest) Path() (string, bool) {
	return "", false
}

var _ TransactionalRequest = (*LockNodeRequest)(nil)
var _ MutatingRequest = (*LockNodeRequest)(nil)

type LockNodeRequest struct {
	*rpc_proxy.TReqLockNode
}

func NewLockNodeRequest(r *rpc_proxy.TReqLockNode) *LockNodeRequest {
	return &LockNodeRequest{TReqLockNode: r}
}

func (r LockNodeRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
		log.Int32("mode", r.GetMode()),
	}
}

func (r LockNodeRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *LockNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *LockNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ TransactionalRequest = (*UnlockNodeRequest)(nil)
var _ MutatingRequest = (*UnlockNodeRequest)(nil)

type UnlockNodeRequest struct {
	*rpc_proxy.TReqUnlockNode
}

func NewUnlockNodeRequest(r *rpc_proxy.TReqUnlockNode) *UnlockNodeRequest {
	return &UnlockNodeRequest{TReqUnlockNode: r}
}

func (r UnlockNodeRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r UnlockNodeRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *UnlockNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *UnlockNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ TransactionalRequest = (*SelectRowsRequest)(nil)

type SelectRowsRequest struct {
	*rpc_proxy.TReqSelectRows
}

func NewSelectRowsRequest(r *rpc_proxy.TReqSelectRows) *SelectRowsRequest {
	return &SelectRowsRequest{TReqSelectRows: r}
}

func (r SelectRowsRequest) Log() []log.Field {
	return []log.Field{
		log.String("query", r.GetQuery()),
	}
}

func (r SelectRowsRequest) Path() (string, bool) {
	return "", false
}

func (r *SelectRowsRequest) SetTxOptions(opts *TransactionOptions) {
	r.Timestamp = convertTimestamp(&opts.TxStartTimestamp)
}

var _ TransactionalRequest = (*LookupRowsRequest)(nil)

type LookupRowsRequest struct {
	*rpc_proxy.TReqLookupRows
}

func NewLookupRowsRequest(r *rpc_proxy.TReqLookupRows) *LookupRowsRequest {
	return &LookupRowsRequest{TReqLookupRows: r}
}

func (r LookupRowsRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r LookupRowsRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *LookupRowsRequest) SetTxOptions(opts *TransactionOptions) {
	r.Timestamp = convertTimestamp(&opts.TxStartTimestamp)
}

var _ TransactionalRequest = (*LockRowsRequest)(nil)

type LockRowsRequest struct {
	*rpc_proxy.TReqModifyRows
}

func NewLockRowsRequest(r *rpc_proxy.TReqModifyRows) *LockRowsRequest {
	return &LockRowsRequest{TReqModifyRows: r}
}

func (r LockRowsRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
		log.Any("locks", r.GetRowLocks()),
		// log.Any("lockType", r.LockType), // todo
	}
}

func (r LockRowsRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *LockRowsRequest) SetTxOptions(opts *TransactionOptions) {
	if opts == nil {
		return
	}
	r.TransactionId = convertTxID(opts.TransactionID)
}

var _ TransactionalRequest = (*InsertRowsRequest)(nil)

type InsertRowsRequest struct {
	*rpc_proxy.TReqModifyRows
}

func NewInsertRowsRequest(r *rpc_proxy.TReqModifyRows) *InsertRowsRequest {
	return &InsertRowsRequest{TReqModifyRows: r}
}

func (r InsertRowsRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r InsertRowsRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *InsertRowsRequest) SetTxOptions(opts *TransactionOptions) {
	if opts == nil {
		return
	}
	r.TransactionId = convertTxID(opts.TransactionID)
}

var _ TransactionalRequest = (*DeleteRowsRequest)(nil)

type DeleteRowsRequest struct {
	*rpc_proxy.TReqModifyRows
}

func NewDeleteRowsRequest(r *rpc_proxy.TReqModifyRows) *DeleteRowsRequest {
	return &DeleteRowsRequest{TReqModifyRows: r}
}

func (r DeleteRowsRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r DeleteRowsRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *DeleteRowsRequest) SetTxOptions(opts *TransactionOptions) {
	if opts == nil {
		return
	}
	r.TransactionId = convertTxID(opts.TransactionID)
}

var _ MutatingRequest = (*MountTableRequest)(nil)

type MountTableRequest struct {
	*rpc_proxy.TReqMountTable
}

func NewMountTableRequest(r *rpc_proxy.TReqMountTable) *MountTableRequest {
	return &MountTableRequest{TReqMountTable: r}
}

func (r MountTableRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r MountTableRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *MountTableRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ MutatingRequest = (*UnmountTableRequest)(nil)

type UnmountTableRequest struct {
	*rpc_proxy.TReqUnmountTable
}

func NewUnmountTableRequest(r *rpc_proxy.TReqUnmountTable) *UnmountTableRequest {
	return &UnmountTableRequest{TReqUnmountTable: r}
}

func (r UnmountTableRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r UnmountTableRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *UnmountTableRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ MutatingRequest = (*RemountTableRequest)(nil)

type RemountTableRequest struct {
	*rpc_proxy.TReqRemountTable
}

func NewRemountTableRequest(r *rpc_proxy.TReqRemountTable) *RemountTableRequest {
	return &RemountTableRequest{TReqRemountTable: r}
}

func (r RemountTableRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r RemountTableRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *RemountTableRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ MutatingRequest = (*ReshardTableRequest)(nil)

type ReshardTableRequest struct {
	*rpc_proxy.TReqReshardTable
}

func NewReshardTableRequest(r *rpc_proxy.TReqReshardTable) *ReshardTableRequest {
	return &ReshardTableRequest{TReqReshardTable: r}
}

func (r ReshardTableRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r ReshardTableRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *ReshardTableRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ MutatingRequest = (*AlterTableRequest)(nil)

type AlterTableRequest struct {
	*rpc_proxy.TReqAlterTable
}

func NewAlterTableRequest(r *rpc_proxy.TReqAlterTable) *AlterTableRequest {
	return &AlterTableRequest{TReqAlterTable: r}
}

func (r AlterTableRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r AlterTableRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *AlterTableRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ MutatingRequest = (*FreezeTableRequest)(nil)

type FreezeTableRequest struct {
	*rpc_proxy.TReqFreezeTable
}

func NewFreezeTableRequest(r *rpc_proxy.TReqFreezeTable) *FreezeTableRequest {
	return &FreezeTableRequest{TReqFreezeTable: r}
}

func (r FreezeTableRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r FreezeTableRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *FreezeTableRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

var _ MutatingRequest = (*UnfreezeTableRequest)(nil)

type UnfreezeTableRequest struct {
	*rpc_proxy.TReqUnfreezeTable
}

func NewUnfreezeTableRequest(r *rpc_proxy.TReqUnfreezeTable) *UnfreezeTableRequest {
	return &UnfreezeTableRequest{TReqUnfreezeTable: r}
}

func (r UnfreezeTableRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
	}
}

func (r UnfreezeTableRequest) Path() (string, bool) {
	return r.GetPath(), true
}

func (r *UnfreezeTableRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

type AlterTableReplicaRequest struct {
	*rpc_proxy.TReqAlterTableReplica
}

func NewAlterTableReplicaRequest(r *rpc_proxy.TReqAlterTableReplica) *AlterTableReplicaRequest {
	return &AlterTableReplicaRequest{TReqAlterTableReplica: r}
}

func (r AlterTableReplicaRequest) Log() []log.Field {
	return []log.Field{
		log.Any("id", r.GetReplicaId()),
	}
}

func (r AlterTableReplicaRequest) Path() (string, bool) {
	return "", false
}

type GenerateTimestampRequest struct {
	*rpc_proxy.TReqGenerateTimestamps
}

func NewGenerateTimestampRequest(r *rpc_proxy.TReqGenerateTimestamps) *GenerateTimestampRequest {
	return &GenerateTimestampRequest{TReqGenerateTimestamps: r}
}

func (r GenerateTimestampRequest) Log() []log.Field {
	return []log.Field{}
}

func (r GenerateTimestampRequest) Path() (string, bool) {
	return "", false
}

type LocateSkynetShareRequest struct {
	// todo
}

func NewLocateSkynetShareRequest() *LocateSkynetShareRequest {
	return &LocateSkynetShareRequest{}
}

func (r LocateSkynetShareRequest) Log() []log.Field {
	return []log.Field{
		// log.String("path", r.GetPath()), // todo
	}
}

func (r LocateSkynetShareRequest) Path() (string, bool) {
	return "", false // todo
}

type GetInSyncReplicasRequest struct {
	*rpc_proxy.TReqGetInSyncReplicas
}

func NewGetInSyncReplicasRequest(r *rpc_proxy.TReqGetInSyncReplicas) *GetInSyncReplicasRequest {
	return &GetInSyncReplicasRequest{TReqGetInSyncReplicas: r}
}

func (r GetInSyncReplicasRequest) Log() []log.Field {
	return []log.Field{
		log.String("path", r.GetPath()),
		log.UInt64("ts", r.GetTimestamp()),
	}
}

func (r GetInSyncReplicasRequest) Path() (string, bool) {
	return r.GetPath(), true
}
