package rpcclient

import (
	"strings"

	"google.golang.org/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
	"go.ytsaurus.tech/yt/go/yt"
)

func logTransactionalOptions(o *rpc_proxy.TTransactionalOptions) []log.Field {
	if o == nil {
		return nil
	}
	fields := []log.Field{}
	if txID := o.GetTransactionId(); txID != nil {
		fields = append(fields, log.Any("transaction_id", makeGUID(txID)))
	}
	if o.GetPing() {
		fields = append(fields, log.Bool("ping", true))
	}
	if o.GetPingAncestors() {
		fields = append(fields, log.Bool("ping_ancestor_transactions", true))
	}
	if o.GetSuppressTransactionCoordinatorSync() {
		fields = append(fields, log.Bool("suppress_transaction_coordinator_sync", true))
	}
	if o.GetSuppressUpstreamSync() {
		fields = append(fields, log.Bool("suppress_upstream_sync", true))
	}
	return fields
}

func logMutatingOptions(o *rpc_proxy.TMutatingOptions) []log.Field {
	if o == nil {
		return nil
	}
	fields := []log.Field{}
	if mutationID := o.GetMutationId(); mutationID != nil {
		fields = append(fields, log.Any("mutation_id", makeGUID(mutationID)))
	}
	if o.GetRetry() {
		fields = append(fields, log.Bool("retry", true))
	}
	return fields
}

func logPrerequisiteOptions(o *rpc_proxy.TPrerequisiteOptions) []log.Field {
	if o == nil {
		return nil
	}
	fields := []log.Field{}
	if txs := o.GetTransactions(); txs != nil {
		var ids []guid.GUID
		for _, t := range txs {
			if txID := t.GetTransactionId(); txID != nil {
				ids = append(ids, makeGUID(txID))
			}
		}
		if len(ids) > 0 {
			fields = append(fields, log.Any("prerequisite_transaction_ids", ids))
		}
	}
	if revs := o.GetRevisions(); revs != nil {
		fields = append(fields, log.Any("prerequisite_revisions", revs))
	}
	return fields
}

func logMasterReadOptions(o *rpc_proxy.TMasterReadOptions) []log.Field {
	if o == nil {
		return nil
	}
	fields := []log.Field{}
	if o.ReadFrom != nil {
		if rk, err := makeReadKind(o.ReadFrom); err == nil {
			fields = append(fields, log.Any("read_from", rk))
		}
	}
	if o.GetDisablePerUserCache() {
		fields = append(fields, log.Bool("disable_per_user_cache", true))
	}
	if o.ExpireAfterSuccessfulUpdateTime != nil {
		fields = append(fields, log.Int64("expire_after_successful_update_time", o.GetExpireAfterSuccessfulUpdateTime()))
	}
	if o.ExpireAfterFailedUpdateTime != nil {
		fields = append(fields, log.Int64("expire_after_failed_update_time", o.GetExpireAfterFailedUpdateTime()))
	}
	if o.CacheStickyGroupSize != nil {
		fields = append(fields, log.Int32("cache_sticky_group_size", o.GetCacheStickyGroupSize()))
	}
	if o.SuccessStalenessBound != nil {
		fields = append(fields, log.Int64("success_staleness_bound", o.GetSuccessStalenessBound()))
	}
	return fields
}

func logAccessTrackingOptions(o *rpc_proxy.TSuppressableAccessTrackingOptions) []log.Field {
	if o == nil {
		return nil
	}
	fields := []log.Field{}
	if o.GetSuppressAccessTracking() {
		fields = append(fields, log.Bool("suppress_access_tracking", true))
	}
	if o.GetSuppressModificationTracking() {
		fields = append(fields, log.Bool("suppress_modification_tracking", true))
	}
	if o.GetSuppressExpirationTimeoutRenewal() {
		fields = append(fields, log.Bool("suppress_expiration_timeout_renewal", true))
	}
	return fields
}

func logTabletReadOptions(o *rpc_proxy.TTabletReadOptions) []log.Field {
	if o == nil {
		return nil
	}
	fields := []log.Field{}
	if o.ReadFrom != nil {
		if rk, err := makeTabletReadKind(o.ReadFrom); err == nil {
			fields = append(fields, log.Any("tablet_read_from", rk))
		}
	}
	if v := o.CachedSyncReplicasTimeout; v != nil {
		fields = append(fields, log.UInt64("cached_sync_replicas_timeout", o.GetCachedSyncReplicasTimeout()))
	}
	return fields
}

func logRowBatchReadOptions(o *rpc_proxy.TRowBatchReadOptions) []log.Field {
	if o == nil {
		return nil
	}
	fields := []log.Field{}
	if v := o.MaxRowCount; v != nil && *v > 0 {
		fields = append(fields, log.Int64("max_row_count", *v))
	}
	if v := o.MaxDataWeight; v != nil && *v > 0 {
		fields = append(fields, log.Int64("max_data_weight", *v))
	}
	if v := o.DataWeightPerRowHint; v != nil && *v > 0 {
		fields = append(fields, log.Int64("data_weight_per_row_hint", *v))
	}
	return fields
}

func appendEmbeddedOptions(fields []log.Field, opts proto.Message) []log.Field {
	if v, ok := opts.(interface {
		GetTransactionalOptions() *rpc_proxy.TTransactionalOptions
	}); ok {
		fields = append(fields, logTransactionalOptions(v.GetTransactionalOptions())...)
	}
	if v, ok := opts.(interface {
		GetPrerequisiteOptions() *rpc_proxy.TPrerequisiteOptions
	}); ok {
		fields = append(fields, logPrerequisiteOptions(v.GetPrerequisiteOptions())...)
	}
	if v, ok := opts.(interface {
		GetMutatingOptions() *rpc_proxy.TMutatingOptions
	}); ok {
		fields = append(fields, logMutatingOptions(v.GetMutatingOptions())...)
	}
	if v, ok := opts.(interface {
		GetMasterReadOptions() *rpc_proxy.TMasterReadOptions
	}); ok {
		fields = append(fields, logMasterReadOptions(v.GetMasterReadOptions())...)
	}
	if v, ok := opts.(interface {
		GetSuppressableAccessTrackingOptions() *rpc_proxy.TSuppressableAccessTrackingOptions
	}); ok {
		fields = append(fields, logAccessTrackingOptions(v.GetSuppressableAccessTrackingOptions())...)
	}
	if v, ok := opts.(interface {
		GetTabletReadOptions() *rpc_proxy.TTabletReadOptions
	}); ok {
		fields = append(fields, logTabletReadOptions(v.GetTabletReadOptions())...)
	}
	if v, ok := opts.(interface {
		GetRowBatchReadOptions() *rpc_proxy.TRowBatchReadOptions
	}); ok {
		fields = append(fields, logRowBatchReadOptions(v.GetRowBatchReadOptions())...)
	}
	return fields
}

var _ TransactionalRequest = (*CreateNodeRequest)(nil)
var _ MutatingRequest = (*CreateNodeRequest)(nil)

type CreateNodeRequest struct {
	*rpc_proxy.TReqCreateNode
}

func NewCreateNodeRequest(r *rpc_proxy.TReqCreateNode) *CreateNodeRequest {
	return &CreateNodeRequest{TReqCreateNode: r}
}

func (r CreateNodeRequest) Log() []log.Field {
	fields := []log.Field{
		log.String("path", string(r.GetPath())),
	}
	if nt, err := makeNodeType(r.GetType()); err == nil {
		fields = append(fields, log.Any("typ", nt))
	} else {
		fields = append(fields, log.Int32("typ", r.GetType()))
	}
	if r.GetRecursive() {
		fields = append(fields, log.Bool("recursive", true))
	}
	if r.GetIgnoreExisting() {
		fields = append(fields, log.Bool("ignore_existing", true))
	}
	if r.GetForce() {
		fields = append(fields, log.Bool("force", true))
	}
	if r.GetAttributes() != nil {
		if attrs, err := makeAttributes(r.GetAttributes()); err == nil {
			fields = append(fields, log.Any("attributes", attrs))
		}
	}
	fields = appendEmbeddedOptions(fields, r.TReqCreateNode)
	return fields
}

func (r CreateNodeRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *CreateNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *CreateNodeRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *CreateNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *CreateNodeRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

type CreateObjectRequest struct {
	*rpc_proxy.TReqCreateObject
}

func NewCreateObjectRequest(r *rpc_proxy.TReqCreateObject) *CreateObjectRequest {
	return &CreateObjectRequest{TReqCreateObject: r}
}

func (r CreateObjectRequest) Log() []log.Field {
	fields := []log.Field{}
	if nt, err := makeNodeType(r.GetType()); err == nil {
		fields = append(fields, log.Any("typ", nt))
	} else {
		fields = append(fields, log.Int32("typ", r.GetType()))
	}
	if r.Attributes != nil {
		if attrs, err := makeAttributes(r.GetAttributes()); err == nil {
			fields = append(fields, log.Any("attributes", attrs))
		}
	}
	if r.GetIgnoreExisting() {
		fields = append(fields, log.Bool("ignore_existing", true))
	}
	fields = appendEmbeddedOptions(fields, r.TReqCreateObject)
	return fields
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
	fields := []log.Field{
		log.String("path", string(r.GetPath())),
	}
	fields = appendEmbeddedOptions(fields, r.TReqExistsNode)
	return fields
}

func (r NodeExistsRequest) Path() (string, bool) {
	return string(r.GetPath()), true
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
	fields := []log.Field{
		log.String("path", string(r.GetPath())),
	}
	if r.GetRecursive() {
		fields = append(fields, log.Bool("recursive", true))
	}
	if r.GetForce() {
		fields = append(fields, log.Bool("force", true))
	}
	fields = appendEmbeddedOptions(fields, r.TReqRemoveNode)
	return fields
}

func (r RemoveNodeRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *RemoveNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *RemoveNodeRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *RemoveNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *RemoveNodeRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
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
	fields := []log.Field{
		log.String("path", string(r.GetPath())),
	}
	if attrs := r.GetAttributes(); attrs != nil && len(attrs.GetKeys()) > 0 {
		fields = append(fields, log.Any("attributes", attrs.GetKeys()))
	}
	if r.MaxSize != nil {
		fields = append(fields, log.Int64("max_size", r.GetMaxSize()))
	}
	fields = appendEmbeddedOptions(fields, r.TReqGetNode)
	return fields
}

func (r GetNodeRequest) Path() (string, bool) {
	return string(r.GetPath()), true
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
	fields := []log.Field{
		log.String("path", string(r.GetPath())),
	}
	fields = appendEmbeddedOptions(fields, r.TReqSetNode)
	return fields
}

func (r SetNodeRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *SetNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *SetNodeRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *SetNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *SetNodeRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

var _ TransactionalRequest = (*MultisetAttributesRequest)(nil)

type MultisetAttributesRequest struct {
	*rpc_proxy.TReqMultisetAttributesNode
}

func NewMultisetAttributesRequest(r *rpc_proxy.TReqMultisetAttributesNode) *MultisetAttributesRequest {
	return &MultisetAttributesRequest{TReqMultisetAttributesNode: r}
}

func (r MultisetAttributesRequest) Log() []log.Field {
	fields := []log.Field{
		log.String("path", string(r.GetPath())),
	}
	fields = appendEmbeddedOptions(fields, r.TReqMultisetAttributesNode)
	return fields
}

func (r MultisetAttributesRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *MultisetAttributesRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *MultisetAttributesRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *MultisetAttributesRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *MultisetAttributesRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
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
	fields := []log.Field{
		log.String("path", string(r.GetPath())),
	}
	if attrs := r.GetAttributes(); attrs != nil && len(attrs.GetKeys()) > 0 {
		fields = append(fields, log.Any("attributes", attrs.GetKeys()))
	}
	if r.MaxSize != nil {
		fields = append(fields, log.Int64("max_size", r.GetMaxSize()))
	}
	fields = appendEmbeddedOptions(fields, r.TReqListNode)
	return fields
}

func (r ListNodeRequest) Path() (string, bool) {
	return string(r.GetPath()), true
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
	fields := []log.Field{
		log.String("src", string(r.GetSrcPath())),
		log.String("dst", string(r.GetDstPath())),
	}
	if r.GetRecursive() {
		fields = append(fields, log.Bool("recursive", true))
	}
	if r.GetForce() {
		fields = append(fields, log.Bool("force", true))
	}
	if r.GetIgnoreExisting() {
		fields = append(fields, log.Bool("ignore_existing", true))
	}
	if r.GetPreserveAccount() {
		fields = append(fields, log.Bool("preserve_account", true))
	}
	if r.GetPreserveExpirationTime() {
		fields = append(fields, log.Bool("preserve_expiration_time", true))
	}
	if r.GetPreserveExpirationTimeout() {
		fields = append(fields, log.Bool("preserve_expiration_timeout", true))
	}
	if r.GetPreserveCreationTime() {
		fields = append(fields, log.Bool("preserve_creation_time", true))
	}
	if r.GetPessimisticQuotaCheck() {
		fields = append(fields, log.Bool("pessimistic_quota_check", true))
	}
	if r.GetEnableCrossCellCopying() {
		fields = append(fields, log.Bool("enable_cross_cell_copying", true))
	}
	fields = appendEmbeddedOptions(fields, r.TReqCopyNode)
	return fields
}

func (r CopyNodeRequest) Path() (string, bool) {
	return string(r.GetSrcPath()), true
}

func (r *CopyNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *CopyNodeRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *CopyNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *CopyNodeRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
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
	fields := []log.Field{
		log.String("src", string(r.GetSrcPath())),
		log.String("dst", string(r.GetDstPath())),
	}
	if r.GetRecursive() {
		fields = append(fields, log.Bool("recursive", true))
	}
	if r.GetForce() {
		fields = append(fields, log.Bool("force", true))
	}
	if r.GetPreserveAccount() {
		fields = append(fields, log.Bool("preserve_account", true))
	}
	if r.GetPreserveExpirationTime() {
		fields = append(fields, log.Bool("preserve_expiration_time", true))
	}
	if r.GetPreserveExpirationTimeout() {
		fields = append(fields, log.Bool("preserve_expiration_timeout", true))
	}
	if r.GetPessimisticQuotaCheck() {
		fields = append(fields, log.Bool("pessimistic_quota_check", true))
	}
	if r.GetEnableCrossCellCopying() {
		fields = append(fields, log.Bool("enable_cross_cell_copying", true))
	}
	fields = appendEmbeddedOptions(fields, r.TReqMoveNode)
	return fields
}

func (r MoveNodeRequest) Path() (string, bool) {
	return string(r.GetSrcPath()), true
}

func (r *MoveNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *MoveNodeRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *MoveNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *MoveNodeRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
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
	fields := []log.Field{
		log.String("target", string(r.GetSrcPath())),
		log.String("link", string(r.GetDstPath())),
	}
	if r.GetRecursive() {
		fields = append(fields, log.Bool("recursive", true))
	}
	if r.GetIgnoreExisting() {
		fields = append(fields, log.Bool("ignore_existing", true))
	}
	if r.GetForce() {
		fields = append(fields, log.Bool("force", true))
	}
	if r.Attributes != nil {
		if attrs, err := makeAttributes(r.GetAttributes()); err == nil {
			fields = append(fields, log.Any("attributes", attrs))
		}
	}
	fields = appendEmbeddedOptions(fields, r.TReqLinkNode)
	return fields
}

func (r LinkNodeRequest) Path() (string, bool) {
	return string(r.GetDstPath()), true
}

func (r *LinkNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *LinkNodeRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *LinkNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *LinkNodeRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
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
	fields := []log.Field{}
	fields = appendEmbeddedOptions(fields, r.TReqStartTransaction)
	return fields
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
	fields := []log.Field{}
	fields = appendEmbeddedOptions(fields, r.TReqStartTransaction)
	return fields
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
	fields := []log.Field{log.Any("id", r.GetTransactionId())}
	fields = appendEmbeddedOptions(fields, r.TReqPingTransaction)
	return fields
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
	fields := []log.Field{log.Any("id", r.GetTransactionId())}
	fields = appendEmbeddedOptions(fields, r.TReqAbortTransaction)
	return fields
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
	fields := []log.Field{log.Any("id", r.GetTransactionId())}
	fields = appendEmbeddedOptions(fields, r.TReqCommitTransaction)
	return fields
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
	fields := []log.Field{log.String("path", string(r.GetPath()))}
	if r.GetComputeMd5() {
		fields = append(fields, log.Bool("compute_md5", true))
	}
	if cfg := r.GetConfig(); cfg != nil {
		fields = append(fields, log.ByteString("file_writer", cfg))
	}
	fields = appendEmbeddedOptions(fields, r.TReqWriteFile)
	return fields
}

func (r WriteFileRequest) Path() (string, bool) {
	return string(r.GetPath()), true
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
	fields := []log.Field{
		log.String("path", string(r.GetPath())),
	}
	if r.Offset != nil {
		fields = append(fields, log.Int64("offset", r.GetOffset()))
	}
	if r.Length != nil {
		fields = append(fields, log.Int64("length", r.GetLength()))
	}
	if cfg := r.GetConfig(); cfg != nil {
		fields = append(fields, log.ByteString("file_reader", cfg))
	}
	fields = appendEmbeddedOptions(fields, r.TReqReadFile)
	return fields
}

func (r ReadFileRequest) Path() (string, bool) {
	return string(r.GetPath()), true
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
	fields := []log.Field{
		log.String("path", string(r.GetPath())),
		log.String("md5", r.GetMd5()),
	}
	if cp := r.GetCachePath(); cp != nil {
		fields = append(fields, log.String("cache_path", string(cp)))
	}
	if r.GetPreserveExpirationTimeout() {
		fields = append(fields, log.Bool("preserve_expiration_timeout", true))
	}
	fields = appendEmbeddedOptions(fields, r.TReqPutFileToCache)
	return fields
}

func (r PutFileToCacheRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *PutFileToCacheRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *PutFileToCacheRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *PutFileToCacheRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

var _ ReadRetryRequest = (*GetFileFromCacheRequest)(nil)

type GetFileFromCacheRequest struct {
	*rpc_proxy.TReqGetFileFromCache
}

func NewGetFileFromCacheRequest(r *rpc_proxy.TReqGetFileFromCache) *GetFileFromCacheRequest {
	return &GetFileFromCacheRequest{TReqGetFileFromCache: r}
}

func (r GetFileFromCacheRequest) Log() []log.Field {
	fields := []log.Field{log.String("md5", r.GetMd5())}
	fields = appendEmbeddedOptions(fields, r.TReqGetFileFromCache)
	return fields
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
	fields := []log.Field{log.String("path", string(r.GetPath()))}
	fields = appendEmbeddedOptions(fields, r.TReqWriteTable)
	return fields
}

func (r WriteTableRequest) Path() (string, bool) {
	return string(r.GetPath()), true
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
	fields := []log.Field{log.String("path", string(r.GetPath()))}
	fields = appendEmbeddedOptions(fields, r.TReqReadTable)
	return fields
}

func (r ReadTableRequest) Path() (string, bool) {
	return string(r.GetPath()), true
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
	fields := []log.Field{log.String("opType", r.GetType().String())}
	fields = appendEmbeddedOptions(fields, r.TReqStartOperation)
	return fields
}

func (r StartOperationRequest) Path() (string, bool) {
	return "", false
}

func (r *StartOperationRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *StartOperationRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *StartOperationRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *StartOperationRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

type AbortOperationRequest struct {
	*rpc_proxy.TReqAbortOperation
}

func NewAbortOperationRequest(r *rpc_proxy.TReqAbortOperation) *AbortOperationRequest {
	return &AbortOperationRequest{TReqAbortOperation: r}
}

func (r AbortOperationRequest) Log() []log.Field {
	fields := []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqAbortOperation)
	return fields
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
	fields := []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqSuspendOperation)
	return fields
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
	fields := []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqResumeOperation)
	return fields
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
	fields := []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqCompleteOperation)
	return fields
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
	fields := []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
		log.ByteString("params", r.GetParameters()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqUpdateOperationParameters)
	return fields
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
	fields := []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
	}
	if r.Attributes != nil {
		fields = append(fields, log.Any("attributes", makeAttributeFilter(r.GetAttributes())))
	}
	if r.GetIncludeRuntime() {
		fields = append(fields, log.Bool("include_runtime", true))
	}
	fields = appendEmbeddedOptions(fields, r.TReqGetOperation)
	return fields
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
	fields := []log.Field{}
	if v := r.GetFromTime(); v != 0 {
		fields = append(fields, log.UInt64("from_time", v))
	}
	if v := r.GetToTime(); v != 0 {
		fields = append(fields, log.UInt64("to_time", v))
	}
	if v := r.GetCursorTime(); v != 0 {
		fields = append(fields, log.UInt64("cursor_time", v))
	}
	if u := r.GetUserFilter(); u != "" {
		fields = append(fields, log.String("user", u))
	}
	if s := r.GetStateFilter(); s != 0 {
		fields = append(fields, log.Any("state", s))
	}
	if t := r.GetTypeFilter(); t != 0 {
		fields = append(fields, log.Any("type", t))
	}
	if f := r.GetSubstrFilter(); f != "" {
		fields = append(fields, log.String("filter", f))
	}
	if p := r.GetPool(); p != "" {
		fields = append(fields, log.String("pool", p))
	}
	if pt := r.GetPoolTree(); pt != "" {
		fields = append(fields, log.String("pool_tree", pt))
	}
	if r.GetIncludeArchive() {
		fields = append(fields, log.Bool("include_archive", true))
	}
	if l := r.GetLimit(); l != 0 {
		fields = append(fields, log.UInt64("limit", l))
	}
	fields = appendEmbeddedOptions(fields, r.TReqListOperations)
	return fields
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
	fields := []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
	}
	if r.Type != nil {
		if jt, err := makeJobType(r.Type); err == nil {
			fields = append(fields, log.Any("job_type", jt))
		} else {
			fields = append(fields, log.Any("job_type", r.GetType()))
		}
	}
	if r.State != nil {
		if js, err := makeJobState(r.State); err == nil {
			fields = append(fields, log.Any("job_state", js))
		} else {
			fields = append(fields, log.Any("job_state", r.GetState()))
		}
	}
	if r.Address != nil {
		fields = append(fields, log.String("address", r.GetAddress()))
	}
	if r.GetWithStderr() {
		fields = append(fields, log.Bool("with_stderr", true))
	}
	if r.GetWithFailContext() {
		fields = append(fields, log.Bool("with_fail_context", true))
	}
	if r.GetWithMonitoringDescriptor() {
		fields = append(fields, log.Bool("with_monitoring_descriptor", true))
	}
	if r.GetWithInterruptionInfo() {
		fields = append(fields, log.Bool("with_interruption_info", true))
	}
	if r.TaskName != nil {
		fields = append(fields, log.String("task_name", r.GetTaskName()))
	}
	if r.Attributes != nil {
		fields = append(fields, log.Any("attributes", makeAttributeFilter(r.GetAttributes())))
	}
	if r.SortField != nil {
		fields = append(fields, log.Any("sort_field", r.GetSortField()))
	}
	if r.SortOrder != nil {
		fields = append(fields, log.Any("sort_order", r.GetSortOrder()))
	}
	if r.Limit != nil {
		fields = append(fields, log.Int64("limit", r.GetLimit()))
	}
	if r.Offset != nil {
		fields = append(fields, log.Int64("offset", r.GetOffset()))
	}
	if r.DataSource != nil {
		fields = append(fields, log.Any("data_source", r.GetDataSource()))
	}
	fields = appendEmbeddedOptions(fields, r.TReqListJobs)
	return fields
}

func (r ListJobsRequest) Path() (string, bool) {
	return "", false
}

type GetJobRequest struct {
	*rpc_proxy.TReqGetJob
}

func NewGetJobRequest(r *rpc_proxy.TReqGetJob) *GetJobRequest {
	return &GetJobRequest{TReqGetJob: r}
}

func (r GetJobRequest) Log() []log.Field {
	fields := []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
		log.Any("jobID", r.GetJobId()),
	}
	if r.Attributes != nil {
		fields = append(fields, log.Any("attributes", makeAttributeFilter(r.GetAttributes())))
	}
	fields = appendEmbeddedOptions(fields, r.TReqGetJob)
	return fields
}

func (r GetJobRequest) Path() (string, bool) {
	return "", false
}

type GetJobStderrRequest struct {
	*rpc_proxy.TReqGetJobStderr
}

func NewGetJobStderrRequest(r *rpc_proxy.TReqGetJobStderr) *GetJobStderrRequest {
	return &GetJobStderrRequest{TReqGetJobStderr: r}
}

func (r GetJobStderrRequest) Log() []log.Field {
	fields := []log.Field{
		log.Any("opID", r.GetOperationId()),
		log.String("alias", r.GetOperationAlias()),
		log.Any("jobID", r.GetJobId()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqGetJobStderr)
	return fields
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
	fields := []log.Field{
		log.String("group", r.GetGroup()),
		log.String("member", r.GetMember()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqAddMember)
	return fields
}

func (r AddMemberRequest) Path() (string, bool) {
	return "", false
}

func (r *AddMemberRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *AddMemberRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *AddMemberRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

var _ MutatingRequest = (*RemoveMemberRequest)(nil)

type RemoveMemberRequest struct {
	*rpc_proxy.TReqRemoveMember
}

func NewRemoveMemberRequest(r *rpc_proxy.TReqRemoveMember) *RemoveMemberRequest {
	return &RemoveMemberRequest{TReqRemoveMember: r}
}

func (r RemoveMemberRequest) Log() []log.Field {
	fields := []log.Field{
		log.String("group", r.GetGroup()),
		log.String("member", r.GetMember()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqRemoveMember)
	return fields
}

func (r RemoveMemberRequest) Path() (string, bool) {
	return "", false
}

func (r *RemoveMemberRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *RemoveMemberRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *RemoveMemberRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

type AddMaintenanceRequest struct {
	*rpc_proxy.TReqAddMaintenance
}

func NewAddMaintenanceRequest(r *rpc_proxy.TReqAddMaintenance) *AddMaintenanceRequest {
	return &AddMaintenanceRequest{TReqAddMaintenance: r}
}

func (r AddMaintenanceRequest) Log() []log.Field {
	fields := []log.Field{
		log.Any("component", r.GetComponent()),
		log.String("address", r.GetAddress()),
		log.Any("type", r.GetType()),
		log.String("comment", r.GetComment()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqAddMaintenance)
	return fields
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
	fields := []log.Field{
		log.Any("component", r.GetComponent()),
		log.String("address", r.GetAddress()),
		log.Any("ids", r.GetIds()),
		log.Any("type", r.GetType()),
		log.String("user", r.GetUser()),
	}
	if r.GetMine() {
		fields = append(fields, log.Bool("mine", true))
	}
	fields = appendEmbeddedOptions(fields, r.TReqRemoveMaintenance)
	return fields
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
	fields := []log.Field{
		log.String("src_account", r.GetSrcAccount()),
		log.String("dst_account", r.GetDstAccount()),
		log.Any("resource_delta", r.GetResourceDelta()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqTransferAccountResources)
	return fields
}

func (r TransferAccountResourcesRequest) Path() (string, bool) {
	return "", false
}

func (r *TransferAccountResourcesRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *TransferAccountResourcesRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *TransferAccountResourcesRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

var _ MutatingRequest = (*TransferPoolResourcesRequest)(nil)

type TransferPoolResourcesRequest struct {
	*rpc_proxy.TReqTransferPoolResources
}

func NewTransferPoolResourcesRequest(r *rpc_proxy.TReqTransferPoolResources) *TransferPoolResourcesRequest {
	return &TransferPoolResourcesRequest{TReqTransferPoolResources: r}
}

func (r TransferPoolResourcesRequest) Log() []log.Field {
	fields := []log.Field{
		log.String("src_pool", r.GetSrcPool()),
		log.String("dst_pool", r.GetDstPool()),
		log.String("pool_tree", r.GetPoolTree()),
		log.Any("resource_delta", r.GetResourceDelta()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqTransferPoolResources)
	return fields
}

func (r TransferPoolResourcesRequest) Path() (string, bool) {
	return "", false
}

func (r *TransferPoolResourcesRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *TransferPoolResourcesRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *TransferPoolResourcesRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
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
	fields := []log.Field{
		log.String("path", string(r.GetPath())),
		log.String("user", r.GetUser()),
		log.Int32("permission", r.GetPermission()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqCheckPermission)
	return fields
}

func (r *CheckPermissionRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *CheckPermissionRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *CheckPermissionRequest) ReadRetryOptions() {}

type CheckPermissionByACLRequest struct {
	*rpc_proxy.TReqCheckPermissionByAcl
}

func NewCheckPermissionByACLRequest(r *rpc_proxy.TReqCheckPermissionByAcl) *CheckPermissionByACLRequest {
	return &CheckPermissionByACLRequest{TReqCheckPermissionByAcl: r}
}

func (r *CheckPermissionByACLRequest) Log() []log.Field {
	fields := []log.Field{
		log.String("user", r.GetUser()),
		log.Int32("permission", r.GetPermission()),
		log.String("acl", r.GetAcl()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqCheckPermissionByAcl)
	return fields
}

func (r *CheckPermissionByACLRequest) Path() (string, bool) {
	return "", false
}

func (r *CheckPermissionByACLRequest) ReadRetryOptions() {}

type CheckOperationPermissionRequest struct {
	*rpc_proxy.TReqCheckOperationPermission
}

func NewCheckOperationPermissionRequest(r *rpc_proxy.TReqCheckOperationPermission) *CheckOperationPermissionRequest {
	return &CheckOperationPermissionRequest{TReqCheckOperationPermission: r}
}

func (r *CheckOperationPermissionRequest) Log() []log.Field {
	fields := []log.Field{
		log.String("operation_id", r.GetOperationId().String()),
		log.String("user", r.GetUser()),
		log.Int32("permission", r.GetPermission()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqCheckOperationPermission)
	return fields
}

func (r *CheckOperationPermissionRequest) Path() (string, bool) {
	return "", false
}

func (r *CheckOperationPermissionRequest) ReadRetryOptions() {}

type DisableChunkLocationsRequest struct {
	*rpc_proxy.TReqDisableChunkLocations
}

func NewDisableChunkLocationsRequest(r *rpc_proxy.TReqDisableChunkLocations) *DisableChunkLocationsRequest {
	return &DisableChunkLocationsRequest{TReqDisableChunkLocations: r}
}

func (r DisableChunkLocationsRequest) Log() []log.Field {
	fields := []log.Field{
		log.String("node_address", r.GetNodeAddress()),
		log.Any("location_uuids", r.GetLocationUuids()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqDisableChunkLocations)
	return fields
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
	fields := []log.Field{
		log.String("node_address", r.GetNodeAddress()),
		log.Any("location_uuids", r.GetLocationUuids()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqDestroyChunkLocations)
	return fields
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
	fields := []log.Field{
		log.String("node_address", r.GetNodeAddress()),
		log.Any("location_uuids", r.GetLocationUuids()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqResurrectChunkLocations)
	return fields
}

func (r ResurrectChunkLocationsRequest) Path() (string, bool) {
	return "", false
}

type RequestRestartRequest struct {
	*rpc_proxy.TReqRequestRestart
}

func NewRequestRestartRequest(r *rpc_proxy.TReqRequestRestart) *RequestRestartRequest {
	return &RequestRestartRequest{TReqRequestRestart: r}
}

func (r RequestRestartRequest) Log() []log.Field {
	fields := []log.Field{log.String("node_address", r.GetNodeAddress())}
	fields = appendEmbeddedOptions(fields, r.TReqRequestRestart)
	return fields
}

func (r RequestRestartRequest) Path() (string, bool) {
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
	fields := []log.Field{
		log.String("path", string(r.GetPath())),
		log.Int32("mode", r.GetMode()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqLockNode)
	return fields
}

func (r LockNodeRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *LockNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *LockNodeRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *LockNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *LockNodeRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
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
	fields := []log.Field{log.String("path", string(r.GetPath()))}
	fields = appendEmbeddedOptions(fields, r.TReqUnlockNode)
	return fields
}

func (r UnlockNodeRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *UnlockNodeRequest) SetTxOptions(opts *TransactionOptions) {
	r.TransactionalOptions = convertTransactionOptions(opts.TransactionOptions)
}

func (r *UnlockNodeRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *UnlockNodeRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *UnlockNodeRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

var _ TransactionalRequest = (*SelectRowsRequest)(nil)

type SelectRowsRequest struct {
	*rpc_proxy.TReqSelectRows
}

func NewSelectRowsRequest(r *rpc_proxy.TReqSelectRows) *SelectRowsRequest {
	return &SelectRowsRequest{TReqSelectRows: r}
}

func (r SelectRowsRequest) Log() []log.Field {
	fields := []log.Field{log.String("query", r.GetQuery())}
	fields = appendEmbeddedOptions(fields, r.TReqSelectRows)
	return fields
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
	fields := []log.Field{log.String("path", string(r.GetPath()))}
	fields = appendEmbeddedOptions(fields, r.TReqLookupRows)
	return fields
}

func (r LookupRowsRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *LookupRowsRequest) SetTxOptions(opts *TransactionOptions) {
	r.Timestamp = convertTimestamp(&opts.TxStartTimestamp)
}

var _ TransactionalRequest = (*MultiLookupRequest)(nil)

type MultiLookupRequest struct {
	*rpc_proxy.TReqMultiLookup
}

func NewMultiLookupRequest(r *rpc_proxy.TReqMultiLookup) *MultiLookupRequest {
	return &MultiLookupRequest{TReqMultiLookup: r}
}

func (r MultiLookupRequest) Log() []log.Field {
	path, _ := r.Path()
	fields := []log.Field{log.String("path", path)}
	fields = appendEmbeddedOptions(fields, r.TReqMultiLookup)
	return fields
}

func (r MultiLookupRequest) Path() (string, bool) {
	paths := make([]string, 0, len(r.GetSubrequests()))
	for _, subreq := range r.GetSubrequests() {
		paths = append(paths, string(subreq.GetPath()))
	}
	return strings.Join(paths, ", "), true
}

func (r *MultiLookupRequest) SetTxOptions(opts *TransactionOptions) {
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
	fields := []log.Field{
		log.String("path", string(r.GetPath())),
		log.Any("locks", r.GetRowLocks()),
		// log.Any("lockType", r.LockType), // todo
	}
	fields = appendEmbeddedOptions(fields, r.TReqModifyRows)
	return fields
}

func (r LockRowsRequest) Path() (string, bool) {
	return string(r.GetPath()), true
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
	fields := []log.Field{log.String("path", string(r.GetPath()))}
	fields = appendEmbeddedOptions(fields, r.TReqModifyRows)
	return fields
}

func (r InsertRowsRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *InsertRowsRequest) SetTxOptions(opts *TransactionOptions) {
	if opts == nil {
		return
	}
	r.TransactionId = convertTxID(opts.TransactionID)
}

var _ TransactionalRequest = (*PushQueueProducerRequest)(nil)

type PushQueueProducerRequest struct {
	*rpc_proxy.TReqPushQueueProducer
}

func NewPushQueueProducerRequest(r *rpc_proxy.TReqPushQueueProducer) *PushQueueProducerRequest {
	return &PushQueueProducerRequest{TReqPushQueueProducer: r}
}

func (r PushQueueProducerRequest) Log() []log.Field {
	fields := []log.Field{
		log.String("producer_path", string(r.GetProducerPath())),
		log.String("queue_path", string(r.GetQueuePath())),
		log.String("session_id", string(r.GetSessionId())),
		log.Int64("epoch", r.GetEpoch()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqPushQueueProducer)
	return fields
}

func (r PushQueueProducerRequest) Path() (string, bool) {
	return string(r.GetProducerPath()), true
}

func (r PushQueueProducerRequest) SetTxOptions(opts *TransactionOptions) {
	if opts == nil {
		return
	}
	r.TransactionId = convertTxID(opts.TransactionID)
}

type CreateQueueProducerSessionRequest struct {
	*rpc_proxy.TReqCreateQueueProducerSession
}

func NewCreateQueueProducerSessionRequest(r *rpc_proxy.TReqCreateQueueProducerSession) *CreateQueueProducerSessionRequest {
	return &CreateQueueProducerSessionRequest{TReqCreateQueueProducerSession: r}
}

func (r CreateQueueProducerSessionRequest) Log() []log.Field {
	fields := []log.Field{
		log.String("producer_path", string(r.GetProducerPath())),
		log.String("queue_path", string(r.GetQueuePath())),
		log.String("session_id", r.GetSessionId()),
		log.String("mutation_id", r.GetMutatingOptions().GetMutationId().String()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqCreateQueueProducerSession)
	return fields
}

func (r CreateQueueProducerSessionRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r CreateQueueProducerSessionRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *CreateQueueProducerSessionRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

func (r CreateQueueProducerSessionRequest) Path() (string, bool) {
	return string(r.GetProducerPath()), false
}

type RemoveQueueProducerSessionRequest struct {
	*rpc_proxy.TReqRemoveQueueProducerSession
}

func NewRemoveQueueProducerSessionRequest(r *rpc_proxy.TReqRemoveQueueProducerSession) *RemoveQueueProducerSessionRequest {
	return &RemoveQueueProducerSessionRequest{TReqRemoveQueueProducerSession: r}
}

func (r RemoveQueueProducerSessionRequest) Log() []log.Field {
	fields := []log.Field{
		log.String("producer_path", string(r.GetProducerPath())),
		log.String("queue_path", string(r.GetQueuePath())),
		log.String("session_id", r.GetSessionId()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqRemoveQueueProducerSession)
	return fields
}

func (r RemoveQueueProducerSessionRequest) Path() (string, bool) {
	return string(r.GetProducerPath()), false
}

var _ TransactionalRequest = (*DeleteRowsRequest)(nil)

type DeleteRowsRequest struct {
	*rpc_proxy.TReqModifyRows
}

func NewDeleteRowsRequest(r *rpc_proxy.TReqModifyRows) *DeleteRowsRequest {
	return &DeleteRowsRequest{TReqModifyRows: r}
}

func (r DeleteRowsRequest) Log() []log.Field {
	fields := []log.Field{log.String("path", string(r.GetPath()))}
	fields = appendEmbeddedOptions(fields, r.TReqModifyRows)
	return fields
}

func (r DeleteRowsRequest) Path() (string, bool) {
	return string(r.GetPath()), true
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
	fields := []log.Field{
		log.String("path", string(r.GetPath())),
	}
	if r.GetFreeze() {
		fields = append(fields, log.Bool("freeze", true))
	}
	if v := r.GetCellId(); v != nil {
		fields = append(fields, log.Any("cell_id", makeGUID(v)))
	}
	fields = appendEmbeddedOptions(fields, r.TReqMountTable)
	return fields
}

func (r MountTableRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *MountTableRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *MountTableRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *MountTableRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

var _ MutatingRequest = (*UnmountTableRequest)(nil)

type UnmountTableRequest struct {
	*rpc_proxy.TReqUnmountTable
}

func NewUnmountTableRequest(r *rpc_proxy.TReqUnmountTable) *UnmountTableRequest {
	return &UnmountTableRequest{TReqUnmountTable: r}
}

func (r UnmountTableRequest) Log() []log.Field {
	fields := []log.Field{log.String("path", string(r.GetPath()))}
	fields = appendEmbeddedOptions(fields, r.TReqUnmountTable)
	return fields
}

func (r UnmountTableRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *UnmountTableRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *UnmountTableRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *UnmountTableRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

var _ MutatingRequest = (*RemountTableRequest)(nil)

type RemountTableRequest struct {
	*rpc_proxy.TReqRemountTable
}

func NewRemountTableRequest(r *rpc_proxy.TReqRemountTable) *RemountTableRequest {
	return &RemountTableRequest{TReqRemountTable: r}
}

func (r RemountTableRequest) Log() []log.Field {
	fields := []log.Field{log.String("path", string(r.GetPath()))}
	fields = appendEmbeddedOptions(fields, r.TReqRemountTable)
	return fields
}

func (r RemountTableRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *RemountTableRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *RemountTableRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *RemountTableRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

var _ MutatingRequest = (*ReshardTableRequest)(nil)

type ReshardTableRequest struct {
	*rpc_proxy.TReqReshardTable
}

func NewReshardTableRequest(r *rpc_proxy.TReqReshardTable) *ReshardTableRequest {
	return &ReshardTableRequest{TReqReshardTable: r}
}

func (r ReshardTableRequest) Log() []log.Field {
	fields := []log.Field{log.String("path", string(r.GetPath()))}
	fields = appendEmbeddedOptions(fields, r.TReqReshardTable)
	return fields
}

func (r ReshardTableRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *ReshardTableRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *ReshardTableRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *ReshardTableRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

var _ MutatingRequest = (*AlterTableRequest)(nil)

type AlterTableRequest struct {
	*rpc_proxy.TReqAlterTable
}

func NewAlterTableRequest(r *rpc_proxy.TReqAlterTable) *AlterTableRequest {
	return &AlterTableRequest{TReqAlterTable: r}
}

func (r AlterTableRequest) Log() []log.Field {
	fields := []log.Field{log.String("path", string(r.GetPath()))}
	fields = appendEmbeddedOptions(fields, r.TReqAlterTable)
	return fields
}

func (r AlterTableRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *AlterTableRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *AlterTableRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *AlterTableRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

var _ MutatingRequest = (*FreezeTableRequest)(nil)

type FreezeTableRequest struct {
	*rpc_proxy.TReqFreezeTable
}

func NewFreezeTableRequest(r *rpc_proxy.TReqFreezeTable) *FreezeTableRequest {
	return &FreezeTableRequest{TReqFreezeTable: r}
}

func (r FreezeTableRequest) Log() []log.Field {
	fields := []log.Field{log.String("path", string(r.GetPath()))}
	fields = appendEmbeddedOptions(fields, r.TReqFreezeTable)
	return fields
}

func (r FreezeTableRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *FreezeTableRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *FreezeTableRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *FreezeTableRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

var _ MutatingRequest = (*UnfreezeTableRequest)(nil)

type UnfreezeTableRequest struct {
	*rpc_proxy.TReqUnfreezeTable
}

func NewUnfreezeTableRequest(r *rpc_proxy.TReqUnfreezeTable) *UnfreezeTableRequest {
	return &UnfreezeTableRequest{TReqUnfreezeTable: r}
}

func (r UnfreezeTableRequest) Log() []log.Field {
	fields := []log.Field{log.String("path", string(r.GetPath()))}
	fields = appendEmbeddedOptions(fields, r.TReqUnfreezeTable)
	return fields
}

func (r UnfreezeTableRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}

func (r *UnfreezeTableRequest) HasMutatingOptions() bool {
	return r.MutatingOptions != nil
}

func (r *UnfreezeTableRequest) SetMutatingOptions(opts *yt.MutatingOptions) {
	r.MutatingOptions = convertMutatingOptions(opts)
}

func (r *UnfreezeTableRequest) SetRetry(retry bool) {
	*r.MutatingOptions.Retry = retry
}

type AlterTableReplicaRequest struct {
	*rpc_proxy.TReqAlterTableReplica
}

func NewAlterTableReplicaRequest(r *rpc_proxy.TReqAlterTableReplica) *AlterTableReplicaRequest {
	return &AlterTableReplicaRequest{TReqAlterTableReplica: r}
}

func (r AlterTableReplicaRequest) Log() []log.Field {
	fields := []log.Field{log.Any("id", r.GetReplicaId())}
	fields = appendEmbeddedOptions(fields, r.TReqAlterTableReplica)
	return fields
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
	fields := []log.Field{}
	fields = appendEmbeddedOptions(fields, r.TReqGenerateTimestamps)
	return fields
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
	fields := []log.Field{
		// log.String("path", r.GetPath()), // todo
	}
	// no embedded options for placeholder yet
	return fields
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
	fields := []log.Field{
		log.String("path", string(r.GetPath())),
		log.UInt64("ts", r.GetTimestamp()),
	}
	fields = appendEmbeddedOptions(fields, r.TReqGetInSyncReplicas)
	return fields
}

func (r GetInSyncReplicasRequest) Path() (string, bool) {
	return string(r.GetPath()), true
}
