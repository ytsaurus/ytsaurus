package internal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

// Encoder is adapter between typed and untyped layer of API.
type Encoder struct {
	StartCall func() *Call

	Invoke         CallInvoker
	InvokeInTx     CallInvoker
	InvokeRead     ReadInvoker
	InvokeWrite    WriteInvoker
	InvokeReadRow  ReadRowInvoker
	InvokeWriteRow WriteRowInvoker
}

func (e *Encoder) newCall(p Params) *Call {
	call := e.StartCall()
	call.Params = p
	call.APIPath = "/api/v4/"
	call.CallID = guid.New()
	return call
}

func (e *Encoder) newAuthCall(p Params) *Call {
	call := e.StartCall()
	call.Params = p
	call.APIPath = "/auth/"
	call.CallID = guid.New()
	return call
}

func (e *Encoder) do(ctx context.Context, call *Call, decode resultDecoder) error {
	res, err := e.Invoke(ctx, call)
	if err != nil {
		return err
	}
	return decode(res)
}

func (e *Encoder) doInTx(ctx context.Context, call *Call, decode resultDecoder) error {
	res, err := e.InvokeInTx(ctx, call)
	if err != nil {
		return err
	}
	return decode(res)
}

func (e *Encoder) CreateNode(
	ctx context.Context,
	path ypath.YPath,
	typ yt.NodeType,
	options *yt.CreateNodeOptions,
) (id yt.NodeID, err error) {
	call := e.newCall(NewCreateNodeParams(path, typ, options))
	err = e.do(ctx, call, CreateNodeResultDecoder(&id))
	return
}

func (e *Encoder) CreateObject(
	ctx context.Context,
	typ yt.NodeType,
	options *yt.CreateObjectOptions,
) (id yt.NodeID, err error) {
	call := e.newCall(NewCreateObjectParams(typ, options))
	err = e.do(ctx, call, CreateObjectResultDecoder(&id))
	return
}

func (e *Encoder) NodeExists(
	ctx context.Context,
	path ypath.YPath,
	options *yt.NodeExistsOptions,
) (ok bool, err error) {
	call := e.newCall(NewNodeExistsParams(path, options))
	err = e.do(ctx, call, NodeExistsResultDecoder(&ok))
	return
}

func (e *Encoder) RemoveNode(
	ctx context.Context,
	path ypath.YPath,
	options *yt.RemoveNodeOptions,
) (err error) {
	call := e.newCall(NewRemoveNodeParams(path, options))
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) GetNode(
	ctx context.Context,
	path ypath.YPath,
	result any,
	options *yt.GetNodeOptions,
) (err error) {
	call := e.newCall(NewGetNodeParams(path, options))
	err = e.do(ctx, call, GetNodeResultDecoder(result))
	return
}

func (e *Encoder) SetNode(
	ctx context.Context,
	path ypath.YPath,
	value any,
	options *yt.SetNodeOptions,
) (err error) {
	call := e.newCall(NewSetNodeParams(path, options))
	call.YSONValue, err = yson.Marshal(value)
	if err != nil {
		return
	}
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) MultisetAttributes(
	ctx context.Context,
	path ypath.YPath,
	attributes map[string]any,
	options *yt.MultisetAttributesOptions,
) (err error) {
	call := e.newCall(NewMultisetAttributesParams(path, options))
	call.YSONValue, err = yson.Marshal(attributes)
	if err != nil {
		return
	}
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) ListNode(
	ctx context.Context,
	path ypath.YPath,
	result any,
	options *yt.ListNodeOptions,
) (err error) {
	call := e.newCall(NewListNodeParams(path, options))
	err = e.do(ctx, call, ListNodeResultDecoder(result))
	return
}

func (e *Encoder) CopyNode(
	ctx context.Context,
	src ypath.YPath,
	dst ypath.YPath,
	options *yt.CopyNodeOptions,
) (id yt.NodeID, err error) {
	opts := yt.CopyNodeOptions{}
	if options != nil {
		opts = *options
	}
	return e.copyMove(ctx,
		func(enableCrossCellCopying bool) Params {
			opts.EnableCrossCellCopying = ptr.Bool(enableCrossCellCopying)
			return NewCopyNodeParams(src, dst, &opts)
		},
	)
}

func (e *Encoder) MoveNode(
	ctx context.Context,
	src ypath.YPath,
	dst ypath.YPath,
	options *yt.MoveNodeOptions,
) (id yt.NodeID, err error) {
	opts := yt.MoveNodeOptions{}
	if options != nil {
		opts = *options
	}
	return e.copyMove(ctx,
		func(enableCrossCellCopying bool) Params {
			opts.EnableCrossCellCopying = ptr.Bool(enableCrossCellCopying)
			return NewMoveNodeParams(src, dst, &opts)
		},
	)
}

func (e *Encoder) copyMove(
	ctx context.Context,
	newParams func(enableCrossCellCopying bool) Params,
) (id yt.NodeID, err error) {
	resultDecoder := CopyMoveNodeResultDecoder(&id)
	// try copy/move without any extra protection.
	err = e.do(ctx, e.newCall(newParams(false)), resultDecoder)
	if yterrors.ContainsErrorCode(err, yterrors.CodeCrossCellAdditionalPath) {
		// it's copy/move from portal, make it retryable.
		err = e.doInTx(ctx, e.newCall(newParams(true)), resultDecoder)
	}
	return
}

func (e *Encoder) LinkNode(
	ctx context.Context,
	target ypath.YPath,
	link ypath.YPath,
	options *yt.LinkNodeOptions,
) (id yt.NodeID, err error) {
	call := e.newCall(NewLinkNodeParams(target, link, options))
	err = e.do(ctx, call, LinkNodeResultDecoder(&id))
	return
}

func (e *Encoder) LockNode(
	ctx context.Context,
	path ypath.YPath,
	mode yt.LockMode,
	options *yt.LockNodeOptions,
) (lr yt.LockResult, err error) {
	call := e.newCall(NewLockNodeParams(path, mode, options))
	err = e.do(ctx, call, LockNodeResultDecoder(&lr))
	return
}

func (e *Encoder) UnlockNode(
	ctx context.Context,
	path ypath.YPath,
	options *yt.UnlockNodeOptions,
) (err error) {
	call := e.newCall(NewUnlockNodeParams(path, options))
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) AddMember(
	ctx context.Context,
	group string,
	member string,
	options *yt.AddMemberOptions,
) (err error) {
	call := e.newCall(NewAddMemberParams(group, member, options))
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) RemoveMember(
	ctx context.Context,
	group string,
	member string,
	options *yt.RemoveMemberOptions,
) (err error) {
	call := e.newCall(NewRemoveMemberParams(group, member, options))
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) WhoAmI(
	ctx context.Context,
	options *yt.WhoAmIOptions,
) (result *yt.WhoAmIResult, err error) {
	call := e.newAuthCall(NewWhoAmIParams(options))
	err = e.do(ctx, call, WhoAmIResultDecoder(&result))
	return
}

func (e *Encoder) SetUserPassword(
	ctx context.Context,
	user string,
	newPassword string,
	currentPassword string,
	options *yt.SetUserPasswordOptions,
) (err error) {
	newPasswordSHA256 := encodeSHA256(newPassword)
	currentPasswordSHA256 := ""
	if currentPassword != "" {
		currentPasswordSHA256 = encodeSHA256(currentPassword)
	}

	call := e.newCall(NewSetUserPasswordParams(user, newPasswordSHA256, currentPasswordSHA256, options))
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) IssueToken(
	ctx context.Context,
	user string,
	password string,
	options *yt.IssueTokenOptions,
) (token string, err error) {
	passwordSHA256 := ""
	if password != "" {
		passwordSHA256 = encodeSHA256(password)
	}
	call := e.newCall(NewIssueTokenParams(user, passwordSHA256, options))
	err = e.do(ctx, call, IssueTokenResultDecoder(&token))
	return
}

func (e *Encoder) RevokeToken(
	ctx context.Context,
	user string,
	password string,
	token string,
	options *yt.RevokeTokenOptions,
) (err error) {
	passwordSHA256 := ""
	if password != "" {
		passwordSHA256 = encodeSHA256(password)
	}
	tokenSHA256 := encodeSHA256(token)

	call := e.newCall(NewRevokeTokenParams(user, passwordSHA256, tokenSHA256, options))
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) ListUserTokens(
	ctx context.Context,
	user string,
	password string,
	options *yt.ListUserTokensOptions,
) (tokens []string, err error) {
	passwordSHA256 := ""
	if password != "" {
		passwordSHA256 = encodeSHA256(password)
	}
	call := e.newCall(NewListUserTokensParams(user, passwordSHA256, options))
	err = e.do(ctx, call, ListUserTokensResultDecoder(&tokens))
	return
}

func (e *Encoder) BuildMasterSnapshots(
	ctx context.Context,
	options *yt.BuildMasterSnapshotsOptions,
) (response *yt.BuildMasterSnapshotsResponse, err error) {
	call := e.newCall(NewBuildMasterSnapshotsParams(options))
	err = e.do(ctx, call, BuildMasterSnapshotsResultDecoder(&response))
	return
}

func (e *Encoder) BuildSnapshot(
	ctx context.Context,
	options *yt.BuildSnapshotOptions,
) (response *yt.BuildSnapshotResponse, err error) {
	call := e.newCall(NewBuildSnapshotParams(options))
	err = e.do(ctx, call, BuildSnapshotResultDecoder(&response))
	return
}

func (e *Encoder) AddMaintenance(
	ctx context.Context,
	component yt.MaintenanceComponent,
	address string,
	maintenanceType yt.MaintenanceType,
	comment string,
	options *yt.AddMaintenanceOptions,
) (response *yt.AddMaintenanceResponse, err error) {
	call := e.newCall(NewAddMaintenanceParams(component, address, maintenanceType, comment, options))
	err = e.do(ctx, call, AddMaintenanceResultDecoder(&response))
	return
}

func (e *Encoder) RemoveMaintenance(
	ctx context.Context,
	component yt.MaintenanceComponent,
	address string,
	options *yt.RemoveMaintenanceOptions,
) (response *yt.RemoveMaintenanceResponse, err error) {
	params := NewRemoveMaintenanceParams(component, address, options)
	if err = params.options.ValidateFields(); err != nil {
		return
	}
	call := e.newCall(params)
	err = e.do(ctx, call, RemoveMaintenanceResultDecoder(&response))
	return
}

func (e *Encoder) TransferPoolResources(
	ctx context.Context,
	srcPool string,
	dstPool string,
	poolTree string,
	resourceDelta any,
	options *yt.TransferPoolResourcesOptions,
) (err error) {
	call := e.newCall(NewTransferPoolResourcesParams(srcPool, dstPool, poolTree, resourceDelta, options))
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) TransferAccountResources(
	ctx context.Context,
	srcAccount string,
	dstAccount string,
	resourceDelta any,
	options *yt.TransferAccountResourcesOptions,
) (err error) {
	call := e.newCall(NewTransferAccountResourcesParams(srcAccount, dstAccount, resourceDelta, options))
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) CheckPermission(
	ctx context.Context,
	user string,
	permission yt.Permission,
	path ypath.YPath,
	options *yt.CheckPermissionOptions,
) (response *yt.CheckPermissionResponse, err error) {
	call := e.newCall(NewCheckPermissionParams(user, permission, path, options))
	err = e.do(ctx, call, CheckPermissionResultDecoder(&response))
	return
}

func (e *Encoder) CheckPermissionByACL(
	ctx context.Context,
	user string,
	permission yt.Permission,
	ACL []yt.ACE,
	options *yt.CheckPermissionByACLOptions,
) (response *yt.CheckPermissionResponse, err error) {
	call := e.newCall(NewCheckPermissionByACLParams(user, permission, ACL, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		return res.decode(&response)
	})
	return
}

func (e *Encoder) StartTx(
	ctx context.Context,
	options *yt.StartTxOptions,
) (id yt.TxID, err error) {
	call := e.newCall(NewStartTxParams(options))
	err = e.do(ctx, call, StartTxResultDecoder(&id))
	return
}

func (e *Encoder) StartTabletTx(
	ctx context.Context,
	options *yt.StartTabletTxOptions,
) (id yt.TxID, err error) {
	call := e.newCall(NewStartTabletTxParams(options))
	err = e.do(ctx, call, StartTabletTxResultDecoder(&id))
	return
}

func (e *Encoder) PingTx(
	ctx context.Context,
	id yt.TxID,
	options *yt.PingTxOptions,
) (err error) {
	call := e.newCall(NewPingTxParams(id, options))
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) AbortTx(
	ctx context.Context,
	id yt.TxID,
	options *yt.AbortTxOptions,
) (err error) {
	call := e.newCall(NewAbortTxParams(id, options))
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) CommitTx(
	ctx context.Context,
	id yt.TxID,
	options *yt.CommitTxOptions,
) (err error) {
	call := e.newCall(NewCommitTxParams(id, options))
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) StartOperation(
	ctx context.Context,
	opType yt.OperationType,
	spec any,
	options *yt.StartOperationOptions,
) (opID yt.OperationID, err error) {
	call := e.newCall(NewStartOperationParams(opType, spec, options))
	err = e.do(ctx, call, StartOperationResultDecoder(&opID))
	return
}

func (e *Encoder) AbortOperation(
	ctx context.Context,
	opID yt.OperationID,
	options *yt.AbortOperationOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewAbortOperationParams(opID, options)), noopResultDecoder)
}

func (e *Encoder) SuspendOperation(
	ctx context.Context,
	opID yt.OperationID,
	options *yt.SuspendOperationOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewSuspendOperationParams(opID, options)), noopResultDecoder)
}

func (e *Encoder) ResumeOperation(
	ctx context.Context,
	opID yt.OperationID,
	options *yt.ResumeOperationOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewResumeOperationParams(opID, options)), noopResultDecoder)
}

func (e *Encoder) CompleteOperation(
	ctx context.Context,
	opID yt.OperationID,
	options *yt.CompleteOperationOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewCompleteOperationParams(opID, options)), noopResultDecoder)
}

func (e *Encoder) UpdateOperationParameters(
	ctx context.Context,
	opID yt.OperationID,
	params any,
	options *yt.UpdateOperationParametersOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewUpdateOperationParametersParams(opID, params, options)), noopResultDecoder)
}

func (e *Encoder) GetOperation(
	ctx context.Context,
	opID yt.OperationID,
	options *yt.GetOperationOptions,
) (status *yt.OperationStatus, err error) {
	status = &yt.OperationStatus{}
	call := e.newCall(NewGetOperationParams(opID, options))
	err = e.do(ctx, call, GetOperationResultDecoder(&status))
	return
}

func (e *Encoder) GetOperationByAlias(
	ctx context.Context,
	alias string,
	options *yt.GetOperationOptions,
) (status *yt.OperationStatus, err error) {
	status = &yt.OperationStatus{}
	call := e.newCall(NewGetOperationByAliasParams(alias, options))
	err = e.do(ctx, call, GetOperationByAliasResultDecoder(&status))
	return
}

func (e *Encoder) ListOperations(
	ctx context.Context,
	options *yt.ListOperationsOptions,
) (operations *yt.ListOperationsResult, err error) {
	call := e.newCall(NewListOperationsParams(options))
	err = e.do(ctx, call, ListOperationsResultDecoder(&operations))
	return
}

func (e *Encoder) ListJobs(
	ctx context.Context,
	opID yt.OperationID,
	options *yt.ListJobsOptions,
) (r *yt.ListJobsResult, err error) {
	call := e.newCall(NewListJobsParams(opID, options))
	err = e.do(ctx, call, ListJobsResultDecoder(&r))
	return
}

func (e *Encoder) GetJobStderr(
	ctx context.Context,
	opID yt.OperationID,
	jobID yt.JobID,
	options *yt.GetJobStderrOptions,
) (r []byte, err error) {
	call := e.newCall(NewGetJobStderrParams(opID, jobID, options))
	err = e.do(ctx, call, GetJobStderrResultDecoder(&r))
	return
}

func (e *Encoder) WriteFile(
	ctx context.Context,
	path ypath.YPath,
	options *yt.WriteFileOptions,
) (w io.WriteCloser, err error) {
	call := e.newCall(NewWriteFileParams(path, options))
	w, err = e.InvokeWrite(ctx, call)
	return
}

func (e *Encoder) ReadFile(
	ctx context.Context,
	path ypath.YPath,
	options *yt.ReadFileOptions,
) (r io.ReadCloser, err error) {
	call := e.newCall(NewReadFileParams(path, options))
	return e.InvokeRead(ctx, call)
}

func (e *Encoder) PutFileToCache(
	ctx context.Context,
	path ypath.YPath,
	md5 string,
	options *yt.PutFileToCacheOptions,
) (cachedPath ypath.YPath, err error) {
	call := e.newCall(NewPutFileToCacheParams(path, md5, options))
	var resPath ypath.Path
	err = e.do(ctx, call, PutFileToCacheResultDecoder(&resPath))
	cachedPath = resPath
	return
}

func (e *Encoder) GetFileFromCache(
	ctx context.Context,
	md5 string,
	options *yt.GetFileFromCacheOptions,
) (path ypath.YPath, err error) {
	call := e.newCall(NewGetFileFromCacheParams(md5, options))
	var resPath ypath.Path
	err = e.do(ctx, call, GetFileFromCacheResultDecoder(&resPath))
	if resPath != "" {
		path = resPath
	}
	return
}

func (e *Encoder) WriteTableRaw(
	ctx context.Context,
	path ypath.YPath,
	options *yt.WriteTableOptions,
	body *bytes.Buffer,
) (err error) {
	call := e.newCall(NewWriteTableParams(path, options))
	call.YSONValue = body.Bytes()
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) WriteTable(
	ctx context.Context,
	path ypath.YPath,
	options *yt.WriteTableOptions,
) (w yt.TableWriter, err error) {
	call := e.newCall(NewWriteTableParams(path, options))
	w, err = e.InvokeWriteRow(ctx, call)
	return
}

func (e *Encoder) ReadTable(
	ctx context.Context,
	path ypath.YPath,
	options *yt.ReadTableOptions,
) (r yt.TableReader, err error) {
	call := e.newCall(NewReadTableParams(path, options))
	return e.InvokeReadRow(ctx, call)
}

func marshalKeys(keys []any) ([]byte, error) {
	var rows bytes.Buffer

	ys := yson.NewWriterConfig(&rows, yson.WriterConfig{Kind: yson.StreamListFragment, Format: yson.FormatBinary})
	for _, key := range keys {
		ys.Any(key)
	}
	if err := ys.Finish(); err != nil {
		return nil, err
	}

	return rows.Bytes(), nil
}

func (e *Encoder) LookupRows(
	ctx context.Context,
	path ypath.Path,
	keys []any,
	options *yt.LookupRowsOptions,
) (r yt.TableReader, err error) {
	call := e.newCall(NewLookupRowsParams(path, options))

	call.YSONValue, err = marshalKeys(keys)
	if err != nil {
		return nil, err
	}

	return e.InvokeReadRow(ctx, call)
}

func (e *Encoder) SelectRows(
	ctx context.Context,
	query string,
	options *yt.SelectRowsOptions,
) (r yt.TableReader, err error) {
	call := e.newCall(NewSelectRowsParams(query, options))
	return e.InvokeReadRow(ctx, call)
}

func (e *Encoder) writeRows(w yt.TableWriter, rows []any) error {
	for _, row := range rows {
		if err := w.Write(row); err != nil {
			return err
		}
	}

	return w.Commit()
}

func (e *Encoder) InsertRows(
	ctx context.Context,
	path ypath.Path,
	rows []any,
	options *yt.InsertRowsOptions,
) (err error) {
	call := e.newCall(NewInsertRowsParams(path, options))
	w, err := e.InvokeWriteRow(ctx, call)
	if err != nil {
		return err
	}

	return e.writeRows(w, rows)
}

func (e *Encoder) InsertRowBatch(
	ctx context.Context,
	path ypath.Path,
	batch yt.RowBatch,
	options *yt.InsertRowsOptions,
) (err error) {
	call := e.newCall(NewInsertRowsParams(path, options))
	call.RowBatch = batch

	w, err := e.InvokeWriteRow(ctx, call)
	if err != nil {
		return err
	}

	return w.Commit()
}

func (e *Encoder) PushQueueProducer(
	ctx context.Context,
	producerPath ypath.Path,
	queuePath ypath.Path,
	sessionID string,
	epoch int64,
	rows []any,
	options *yt.PushQueueProducerOptions,
) (result *yt.PushQueueProducerResult, err error) {
	call := e.newCall(NewPushQueueProducerParams(producerPath, queuePath, sessionID, epoch, options))
	call.WriteRspChan = make(chan *CallResult, 1)
	w, err := e.InvokeWriteRow(ctx, call)
	if err != nil {
		return nil, err
	}

	err = e.writeRows(w, rows)
	if err != nil {
		return nil, err
	}

	res := <-call.WriteRspChan
	err = PushQueueProducerResultDecoder(&result)(res)

	return
}

func (e *Encoder) PushQueueProducerBatch(
	ctx context.Context,
	producerPath ypath.Path,
	queuePath ypath.Path,
	sessionID string,
	epoch int64,
	batch yt.RowBatch,
	options *yt.PushQueueProducerOptions,
) (result *yt.PushQueueProducerResult, err error) {
	call := e.newCall(NewPushQueueProducerParams(producerPath, queuePath, sessionID, epoch, options))
	call.RowBatch = batch
	call.WriteRspChan = make(chan *CallResult, 1)

	w, err := e.InvokeWriteRow(ctx, call)
	if err != nil {
		return nil, err
	}

	res := <-call.WriteRspChan
	err = PushQueueProducerBatchResultDecoder(&result)(res)
	if err != nil {
		return nil, err
	}
	err = w.Commit()
	return
}

func (e *Encoder) CreateQueueProducerSession(
	ctx context.Context,
	producerPath ypath.Path,
	queuePath ypath.Path,
	sessionID string,
	options *yt.CreateQueueProducerSessionOptions,
) (result *yt.CreateQueueProducerSessionResult, err error) {
	call := e.newCall(NewCreateQueueProducerSessionParams(producerPath, queuePath, sessionID, options))
	err = e.do(ctx, call, CreateQueueProducerSessionResultDecoder(&result))
	return
}

func (e *Encoder) RemoveQueueProducerSession(
	ctx context.Context,
	producerPath ypath.Path,
	queuePath ypath.Path,
	sessionID string,
	options *yt.RemoveQueueProducerSessionOptions,
) (err error) {
	call := e.newCall(NewRemoveQueueProducerSessionParams(producerPath, queuePath, sessionID, options))
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) LockRows(
	ctx context.Context,
	path ypath.Path,
	locks []string,
	lockType yt.LockType,
	keys []any,
	options *yt.LockRowsOptions,
) (err error) {
	if len(locks) == 0 {
		return yterrors.Err("empty locks list",
			yterrors.Attr("path", path.String()),
			yterrors.Attr("method", "lock_rows"))
	}

	call := e.newCall(NewLockRowsParams(path, locks, lockType, options))
	w, err := e.InvokeWriteRow(ctx, call)
	if err != nil {
		return err
	}

	return e.writeRows(w, keys)
}

func (e *Encoder) DeleteRows(
	ctx context.Context,
	path ypath.Path,
	keys []any,
	options *yt.DeleteRowsOptions,
) (err error) {
	call := e.newCall(NewDeleteRowsParams(path, options))
	w, err := e.InvokeWriteRow(ctx, call)
	if err != nil {
		return err
	}

	return e.writeRows(w, keys)
}

func (e *Encoder) DisableChunkLocations(
	ctx context.Context,
	nodeAddress string,
	locationUUIDs []guid.GUID,
	options *yt.DisableChunkLocationsOptions,
) (response *yt.DisableChunkLocationsResponse, err error) {
	call := e.newCall(NewDisableChunkLocationsParams(nodeAddress, locationUUIDs, options))
	err = e.do(ctx, call, DisableChunkLocationsResultDecoder(&response))
	return
}

func (e *Encoder) DestroyChunkLocations(
	ctx context.Context,
	nodeAddress string,
	recoverUnlinkedDisks bool,
	locationUUIDs []guid.GUID,
	options *yt.DestroyChunkLocationsOptions,
) (response *yt.DestroyChunkLocationsResponse, err error) {
	call := e.newCall(NewDestroyChunkLocationsParams(nodeAddress, recoverUnlinkedDisks, locationUUIDs, options))
	err = e.do(ctx, call, DestroyChunkLocationsResultDecoder(&response))
	return
}

func (e *Encoder) ResurrectChunkLocations(
	ctx context.Context,
	nodeAddress string,
	locationUUIDs []guid.GUID,
	options *yt.ResurrectChunkLocationsOptions,
) (response *yt.ResurrectChunkLocationsResponse, err error) {
	call := e.newCall(NewResurrectChunkLocationsParams(nodeAddress, locationUUIDs, options))
	err = e.do(ctx, call, ResurrectChunkLocationsResultDecoder(&response))
	return
}

func (e *Encoder) RequestRestart(
	ctx context.Context,
	nodeAddress string,
	options *yt.RequestRestartOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewRequestRestartParams(nodeAddress, options)), noopResultDecoder)
}

func (e *Encoder) MountTable(
	ctx context.Context,
	path ypath.Path,
	options *yt.MountTableOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewMountTableParams(path, options)), noopResultDecoder)
}

func (e *Encoder) UnmountTable(
	ctx context.Context,
	path ypath.Path,
	options *yt.UnmountTableOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewUnmountTableParams(path, options)), noopResultDecoder)
}

func (e *Encoder) RemountTable(
	ctx context.Context,
	path ypath.Path,
	options *yt.RemountTableOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewRemountTableParams(path, options)), noopResultDecoder)
}

func (e *Encoder) ReshardTable(
	ctx context.Context,
	path ypath.Path,
	options *yt.ReshardTableOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewReshardTableParams(path, options)), noopResultDecoder)
}

func (e *Encoder) AlterTable(
	ctx context.Context,
	path ypath.Path,
	options *yt.AlterTableOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewAlterTableParams(path, options)), noopResultDecoder)
}

func (e *Encoder) FreezeTable(
	ctx context.Context,
	path ypath.Path,
	options *yt.FreezeTableOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewFreezeTableParams(path, options)), noopResultDecoder)
}

func (e *Encoder) UnfreezeTable(
	ctx context.Context,
	path ypath.Path,
	options *yt.UnfreezeTableOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewUnfreezeTableParams(path, options)), noopResultDecoder)
}

func (e *Encoder) AlterTableReplica(
	ctx context.Context,
	id yt.NodeID,
	options *yt.AlterTableReplicaOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewAlterTableReplicaParams(id, options)), noopResultDecoder)
}

func (e *Encoder) CreateTableBackup(
	ctx context.Context,
	manifest yt.BackupManifest,
	options *yt.CreateTableBackupOptions,
) (err error) {
	call := e.newCall(NewCreateTableBackupParams(manifest, options))
	return e.do(ctx, call, noopResultDecoder)
}

func (e *Encoder) RestoreTableBackup(
	ctx context.Context,
	manifest yt.BackupManifest,
	options *yt.RestoreTableBackupOptions,
) (err error) {
	call := e.newCall(NewRestoreTableBackupParams(manifest, options))
	return e.do(ctx, call, noopResultDecoder)
}

func (e *Encoder) LocateSkynetShare(
	ctx context.Context,
	path ypath.YPath,
	options *yt.LocateSkynetShareOptions,
) (l yt.ShareLocation, err error) {
	err = e.do(
		ctx,
		e.newCall(NewLocateSkynetShareParams(path, options)),
		LocateSkynetShareResultDecoder(&l),
	)
	return
}

func (e *Encoder) GenerateTimestamp(
	ctx context.Context,
	options *yt.GenerateTimestampOptions,
) (ts yt.Timestamp, err error) {
	err = e.do(
		ctx,
		e.newCall(NewGenerateTimestampParams(options)),
		GenerateTimestampResultDecoder(&ts))
	return
}

func (e *Encoder) GetInSyncReplicas(
	ctx context.Context,
	path ypath.Path,
	ts yt.Timestamp,
	keys []any,
	options *yt.GetInSyncReplicasOptions,
) (ids []yt.NodeID, err error) {
	call := e.newCall(NewGetInSyncReplicasParams(path, ts, options))

	call.YSONValue, err = marshalKeys(keys)
	if err != nil {
		return nil, err
	}

	err = e.do(ctx, call, GetInSyncReplicasResultDecoder(&ids))

	return
}

func (e *Encoder) StartQuery(
	ctx context.Context,
	engine yt.QueryEngine,
	query string,
	options *yt.StartQueryOptions,
) (id yt.QueryID, err error) {
	call := e.newCall(NewStartQueryParams(engine, query, options))
	err = e.do(ctx, call, StartQueryResultDecoder(&id))
	return
}

func (e *Encoder) AbortQuery(
	ctx context.Context,
	id yt.QueryID,
	options *yt.AbortQueryOptions,
) (err error) {
	call := e.newCall(NewAbortQueryParams(id, options))
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func (e *Encoder) GetQuery(
	ctx context.Context,
	id yt.QueryID,
	options *yt.GetQueryOptions,
) (query *yt.Query, err error) {
	call := e.newCall(NewGetQueryParams(id, options))
	err = e.do(ctx, call, GetQueryResultDecoder(&query))
	return
}

func (e *Encoder) ListQueries(
	ctx context.Context,
	options *yt.ListQueriesOptions,
) (result *yt.ListQueriesResult, err error) {
	call := e.newCall(NewListQueriesParams(options))
	err = e.do(ctx, call, ListQueriesResultDecoder(&result))
	return
}

func (e *Encoder) GetQueryResult(
	ctx context.Context,
	id yt.QueryID,
	resultIndex int64,
	options *yt.GetQueryResultOptions,
) (result *yt.QueryResult, err error) {
	call := e.newCall(NewGetQueryResultParams(id, resultIndex, options))
	err = e.do(ctx, call, GetQueryResultQueryResultDecoder(&result))
	return
}

func (e *Encoder) ReadQueryResult(
	ctx context.Context,
	id yt.QueryID,
	resultIndex int64,
	options *yt.ReadQueryResultOptions,
) (r yt.TableReader, err error) {
	call := e.newCall(NewReadQueryResultParams(id, resultIndex, options))
	return e.InvokeReadRow(ctx, call)
}

func (e *Encoder) AlterQuery(
	ctx context.Context,
	id yt.QueryID,
	options *yt.AlterQueryOptions,
) (err error) {
	call := e.newCall(NewAlterQueryParams(id, options))
	err = e.do(ctx, call, noopResultDecoder)
	return
}

func encodeSHA256(input string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(input)))
}
