package internal

import (
	"bytes"
	"context"
	"io"

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
	InvokeRead     ReadInvoker
	InvokeWrite    WriteInvoker
	InvokeReadRow  ReadRowInvoker
	InvokeWriteRow WriteRowInvoker
}

func (e *Encoder) newCall(p Params) *Call {
	call := e.StartCall()
	call.Params = p
	call.CallID = guid.New()
	return call
}

func (e *Encoder) do(ctx context.Context, call *Call, decode func(res *CallResult) error) error {
	res, err := e.Invoke(ctx, call)
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
	err = e.do(ctx, call, func(res *CallResult) error {
		err = res.decodeSingle("node_id", &id)
		return err
	})
	return
}

func (e *Encoder) CreateObject(
	ctx context.Context,
	typ yt.NodeType,
	options *yt.CreateObjectOptions,
) (id yt.NodeID, err error) {
	call := e.newCall(NewCreateObjectParams(typ, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		err = res.decodeSingle("object_id", &id)
		return err
	})
	return
}

func (e *Encoder) NodeExists(
	ctx context.Context,
	path ypath.YPath,
	options *yt.NodeExistsOptions,
) (ok bool, err error) {
	call := e.newCall(NewNodeExistsParams(path, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		err = res.decodeValue(&ok)
		return err
	})
	return
}

func (e *Encoder) RemoveNode(
	ctx context.Context,
	path ypath.YPath,
	options *yt.RemoveNodeOptions,
) (err error) {
	call := e.newCall(NewRemoveNodeParams(path, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		return nil
	})
	return
}

func (e *Encoder) GetNode(
	ctx context.Context,
	path ypath.YPath,
	result interface{},
	options *yt.GetNodeOptions,
) (err error) {
	call := e.newCall(NewGetNodeParams(path, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		return res.decodeValue(result)
	})
	return
}

func (e *Encoder) SetNode(
	ctx context.Context,
	path ypath.YPath,
	value interface{},
	options *yt.SetNodeOptions,
) (err error) {
	call := e.newCall(NewSetNodeParams(path, options))
	call.YSONValue, err = yson.Marshal(value)
	if err != nil {
		return
	}
	err = e.do(ctx, call, func(res *CallResult) error {
		return nil
	})
	return
}

func (e *Encoder) MultisetAttributes(
	ctx context.Context,
	path ypath.YPath,
	attributes map[string]interface{},
	options *yt.MultisetAttributesOptions,
) (err error) {
	call := e.newCall(NewMultisetAttributesParams(path, options))
	call.YSONValue, err = yson.Marshal(attributes)
	if err != nil {
		return
	}
	err = e.do(ctx, call, func(res *CallResult) error {
		return nil
	})
	return
}

func (e *Encoder) ListNode(
	ctx context.Context,
	path ypath.YPath,
	result interface{},
	options *yt.ListNodeOptions,
) (err error) {
	call := e.newCall(NewListNodeParams(path, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		return res.decodeValue(result)
	})
	return
}

func (e *Encoder) CopyNode(
	ctx context.Context,
	src ypath.YPath,
	dst ypath.YPath,
	options *yt.CopyNodeOptions,
) (id yt.NodeID, err error) {
	call := e.newCall(NewCopyNodeParams(src, dst, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		err = res.decodeSingle("node_id", &id)
		return err
	})
	return
}

func (e *Encoder) MoveNode(
	ctx context.Context,
	src ypath.YPath,
	dst ypath.YPath,
	options *yt.MoveNodeOptions,
) (id yt.NodeID, err error) {
	call := e.newCall(NewMoveNodeParams(src, dst, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		err = res.decodeSingle("node_id", &id)
		return err
	})
	return
}

func (e *Encoder) LinkNode(
	ctx context.Context,
	target ypath.YPath,
	link ypath.YPath,
	options *yt.LinkNodeOptions,
) (id yt.NodeID, err error) {
	call := e.newCall(NewLinkNodeParams(target, link, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		err = res.decodeSingle("node_id", &id)
		return err
	})
	return
}

func (e *Encoder) LockNode(
	ctx context.Context,
	path ypath.YPath,
	mode yt.LockMode,
	options *yt.LockNodeOptions,
) (lr yt.LockResult, err error) {
	call := e.newCall(NewLockNodeParams(path, mode, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		err = res.decode(&lr)
		return err
	})
	return
}

func (e *Encoder) UnlockNode(
	ctx context.Context,
	path ypath.YPath,
	options *yt.UnlockNodeOptions,
) (err error) {
	call := e.newCall(NewUnlockNodeParams(path, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		return nil
	})
	return
}

func (e *Encoder) AddMember(
	ctx context.Context,
	group string,
	member string,
	options *yt.AddMemberOptions,
) (err error) {
	call := e.newCall(NewAddMemberParams(group, member, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		return nil
	})
	return
}

func (e *Encoder) RemoveMember(
	ctx context.Context,
	group string,
	member string,
	options *yt.RemoveMemberOptions,
) (err error) {
	call := e.newCall(NewRemoveMemberParams(group, member, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		return nil
	})
	return
}

func (e *Encoder) TransferPoolResources(
	ctx context.Context,
	srcPool string,
	dstPool string,
	poolTree string,
	resourceDelta interface{},
	options *yt.TransferPoolResourcesOptions,
) (err error) {
	call := e.newCall(NewTransferPoolResourcesParams(srcPool, dstPool, poolTree, resourceDelta, options))
	err = e.do(ctx, call, func(res *CallResult) error { return nil })
	return
}

func (e *Encoder) TransferAccountResources(
	ctx context.Context,
	srcAccount string,
	dstAccount string,
	resourceDelta interface{},
	options *yt.TransferAccountResourcesOptions,
) (err error) {
	call := e.newCall(NewTransferAccountResourcesParams(srcAccount, dstAccount, resourceDelta, options))
	err = e.do(ctx, call, func(res *CallResult) error { return nil })
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
	err = e.do(ctx, call, func(res *CallResult) error {
		err = res.decodeSingle("transaction_id", &id)
		return err
	})
	return
}

func (e *Encoder) StartTabletTx(
	ctx context.Context,
	options *yt.StartTabletTxOptions,
) (id yt.TxID, err error) {
	call := e.newCall(NewStartTabletTxParams(options))
	err = e.do(ctx, call, func(res *CallResult) error {
		err = res.decodeSingle("transaction_id", &id)
		return err
	})
	return
}

func (e *Encoder) PingTx(
	ctx context.Context,
	id yt.TxID,
	options *yt.PingTxOptions,
) (err error) {
	call := e.newCall(NewPingTxParams(id, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		return nil
	})
	return
}

func (e *Encoder) AbortTx(
	ctx context.Context,
	id yt.TxID,
	options *yt.AbortTxOptions,
) (err error) {
	call := e.newCall(NewAbortTxParams(id, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		return nil
	})
	return
}

func (e *Encoder) CommitTx(
	ctx context.Context,
	id yt.TxID,
	options *yt.CommitTxOptions,
) (err error) {
	call := e.newCall(NewCommitTxParams(id, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		return nil
	})
	return
}

func (e *Encoder) StartOperation(
	ctx context.Context,
	opType yt.OperationType,
	spec interface{},
	options *yt.StartOperationOptions,
) (opID yt.OperationID, err error) {
	call := e.newCall(NewStartOperationParams(opType, spec, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		err = res.decodeSingle("operation_id", &opID)
		return err
	})
	return
}

func (e *Encoder) AbortOperation(
	ctx context.Context,
	opID yt.OperationID,
	options *yt.AbortOperationOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewAbortOperationParams(opID, options)), func(res *CallResult) error { return nil })
}

func (e *Encoder) SuspendOperation(
	ctx context.Context,
	opID yt.OperationID,
	options *yt.SuspendOperationOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewSuspendOperationParams(opID, options)), func(res *CallResult) error { return nil })
}

func (e *Encoder) ResumeOperation(
	ctx context.Context,
	opID yt.OperationID,
	options *yt.ResumeOperationOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewResumeOperationParams(opID, options)), func(res *CallResult) error { return nil })
}

func (e *Encoder) CompleteOperation(
	ctx context.Context,
	opID yt.OperationID,
	options *yt.CompleteOperationOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewCompleteOperationParams(opID, options)), func(res *CallResult) error { return nil })
}

func (e *Encoder) UpdateOperationParameters(
	ctx context.Context,
	opID yt.OperationID,
	params interface{},
	options *yt.UpdateOperationParametersOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewUpdateOperationParametersParams(opID, params, options)), func(res *CallResult) error { return nil })
}

func (e *Encoder) GetOperation(
	ctx context.Context,
	opID yt.OperationID,
	options *yt.GetOperationOptions,
) (status *yt.OperationStatus, err error) {
	status = &yt.OperationStatus{}
	call := e.newCall(NewGetOperationParams(opID, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		return res.decode(status)
	})
	return
}

func (e *Encoder) ListOperations(
	ctx context.Context,
	options *yt.ListOperationsOptions,
) (operations *yt.ListOperationsResult, err error) {
	call := e.newCall(NewListOperationsParams(options))
	err = e.do(ctx, call, func(res *CallResult) error {
		return res.decode(&operations)
	})
	return
}

func (e *Encoder) ListJobs(
	ctx context.Context,
	opID yt.OperationID,
	options *yt.ListJobsOptions,
) (r *yt.ListJobsResult, err error) {
	call := e.newCall(NewListJobsParams(opID, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		return res.decode(&r)
	})
	return
}

func (e *Encoder) GetJobStderr(
	ctx context.Context,
	opID yt.OperationID,
	jobID yt.JobID,
	options *yt.GetJobStderrOptions,
) (r []byte, err error) {
	call := e.newCall(NewGetJobStderrParams(opID, jobID, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		r = res.YSONValue
		return nil
	})
	return
}

func (e *Encoder) WriteFile(
	ctx context.Context,
	path ypath.YPath,
	options *yt.WriteFileOptions,
) (w io.WriteCloser, err error) {
	call := e.newCall(NewWriteFileParams(path, options))
	return e.InvokeWrite(ctx, call)
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
	err = e.do(ctx, call, func(res *CallResult) error {
		return res.decodeValue(&cachedPath)
	})
	return
}

func (e *Encoder) GetFileFromCache(
	ctx context.Context,
	md5 string,
	options *yt.GetFileFromCacheOptions,
) (path ypath.YPath, err error) {
	call := e.newCall(NewGetFileFromCacheParams(md5, options))
	err = e.do(ctx, call, func(res *CallResult) error {
		return res.decodeValue(&path)
	})
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
	err = e.do(ctx, call, func(res *CallResult) error {
		return nil
	})
	return
}

func (e *Encoder) WriteTable(
	ctx context.Context,
	path ypath.YPath,
	options *yt.WriteTableOptions,
) (w yt.TableWriter, err error) {
	call := e.newCall(NewWriteTableParams(path, options))
	return e.InvokeWriteRow(ctx, call)
}

func (e *Encoder) ReadTable(
	ctx context.Context,
	path ypath.YPath,
	options *yt.ReadTableOptions,
) (r yt.TableReader, err error) {
	call := e.newCall(NewReadTableParams(path, options))
	return e.InvokeReadRow(ctx, call)
}

func marshalKeys(keys []interface{}) ([]byte, error) {
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
	keys []interface{},
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

func (e *Encoder) writeRows(w yt.TableWriter, rows []interface{}) error {
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
	rows []interface{},
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

func (e *Encoder) LockRows(
	ctx context.Context,
	path ypath.Path,
	locks []string,
	lockType yt.LockType,
	keys []interface{},
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
	keys []interface{},
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
	locationUuids []guid.GUID,
) (response *yt.DisableChunkLocationsResponse, err error) {
	call := e.newCall(NewDisableChunkLocationsParams(nodeAddress, locationUuids))
	err = e.do(ctx, call, func(res *CallResult) error {
		return res.decode(&response)
	})
	return
}

func (e *Encoder) DestroyChunkLocations(
	ctx context.Context,
	nodeAddress string,
	locationUuids []guid.GUID,
) (response *yt.DestroyChunkLocationsResponse, err error) {
	call := e.newCall(NewDestroyChunkLocationsParams(nodeAddress, locationUuids))
	err = e.do(ctx, call, func(res *CallResult) error {
		return res.decode(&response)
	})
	return
}

func (e *Encoder) ResurrectChunkLocations(
	ctx context.Context,
	nodeAddress string,
	locationUuids []guid.GUID,
) (response *yt.ResurrectChunkLocationsResponse, err error) {
	call := e.newCall(NewResurrectChunkLocationsParams(nodeAddress, locationUuids))
	err = e.do(ctx, call, func(res *CallResult) error {
		return res.decode(&response)
	})
	return
}

func (e *Encoder) MountTable(
	ctx context.Context,
	path ypath.Path,
	options *yt.MountTableOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewMountTableParams(path, options)), func(res *CallResult) error { return nil })
}

func (e *Encoder) UnmountTable(
	ctx context.Context,
	path ypath.Path,
	options *yt.UnmountTableOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewUnmountTableParams(path, options)), func(res *CallResult) error { return nil })
}

func (e *Encoder) RemountTable(
	ctx context.Context,
	path ypath.Path,
	options *yt.RemountTableOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewRemountTableParams(path, options)), func(res *CallResult) error { return nil })
}

func (e *Encoder) ReshardTable(
	ctx context.Context,
	path ypath.Path,
	options *yt.ReshardTableOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewReshardTableParams(path, options)), func(res *CallResult) error { return nil })
}

func (e *Encoder) AlterTable(
	ctx context.Context,
	path ypath.Path,
	options *yt.AlterTableOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewAlterTableParams(path, options)), func(res *CallResult) error { return nil })
}

func (e *Encoder) FreezeTable(
	ctx context.Context,
	path ypath.Path,
	options *yt.FreezeTableOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewFreezeTableParams(path, options)), func(res *CallResult) error { return nil })
}

func (e *Encoder) UnfreezeTable(
	ctx context.Context,
	path ypath.Path,
	options *yt.UnfreezeTableOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewUnfreezeTableParams(path, options)), func(res *CallResult) error { return nil })
}

func (e *Encoder) AlterTableReplica(
	ctx context.Context,
	id yt.NodeID,
	options *yt.AlterTableReplicaOptions,
) (err error) {
	return e.do(ctx, e.newCall(NewAlterTableReplicaParams(id, options)), func(res *CallResult) error { return nil })
}

func (e *Encoder) LocateSkynetShare(
	ctx context.Context,
	path ypath.YPath,
	options *yt.LocateSkynetShareOptions,
) (l yt.ShareLocation, err error) {
	err = e.do(
		ctx,
		e.newCall(NewLocateSkynetShareParams(path, options)),
		func(res *CallResult) error {
			return res.decode(&l)
		})
	return
}

func (e *Encoder) GenerateTimestamp(
	ctx context.Context,
	options *yt.GenerateTimestampOptions,
) (ts yt.Timestamp, err error) {
	err = e.do(
		ctx,
		e.newCall(NewGenerateTimestampParams(options)),
		func(res *CallResult) error {
			return res.decodeSingle("timestamp", &ts)
		})
	return
}

func (e *Encoder) GetInSyncReplicas(
	ctx context.Context,
	path ypath.Path,
	ts yt.Timestamp,
	keys []interface{},
	options *yt.GetInSyncReplicasOptions,
) (ids []yt.NodeID, err error) {
	call := e.newCall(NewGetInSyncReplicasParams(path, ts, options))

	call.YSONValue, err = marshalKeys(keys)
	if err != nil {
		return nil, err
	}

	err = e.do(ctx, call, func(res *CallResult) error {
		return res.decode(&ids)
	})

	return
}
