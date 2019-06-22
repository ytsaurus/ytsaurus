#pragma once

#include <mapreduce/yt/interface/fwd.h>
#include <mapreduce/yt/interface/client_method_options.h>

namespace NYT::NDetail::NRawClient {

////////////////////////////////////////////////////////////////////

TNode SerializeParamsForCreate(
    const TTransactionId& transactionId,
    const TYPath& path,
    ENodeType type,
    const TCreateOptions& options);

TNode SerializeParamsForRemove(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TRemoveOptions& options);

TNode SerializeParamsForExists(
    const TTransactionId& transactionId,
    const TYPath& path);

TNode SerializeParamsForGet(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options);

TNode SerializeParamsForSet(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TSetOptions& options);

TNode SerializeParamsForList(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TListOptions& options);

TNode SerializeParamsForCopy(
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options);

TNode SerializeParamsForMove(
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options);

TNode SerializeParamsForLink(
    const TTransactionId& transactionId,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options);

TNode SerializeParamsForLock(
    const TTransactionId& transactionId,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options);

TNode SerializeParamsForUnlock(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TUnlockOptions& options);

TNode SerializeParamsForConcatenate(
    const TTransactionId& transactionId,
    const TVector<TYPath>& sourcePaths,
    const TYPath& destinationPath,
    const TConcatenateOptions& options);

TNode SerializeParamsForPingTx(
    const TTransactionId& transactionId);

TNode SerializeParamsForGetOperation(
    const TOperationId& operationId,
    const TGetOperationOptions& options);

TNode SerializeParamsForAbortOperation(
    const TOperationId& operationId);

TNode SerializeParamsForCompleteOperation(
    const TOperationId& operationId);

TNode SerializeParamsForListOperations(
    const TListOperationsOptions& options);

TNode SerializeParamsForUpdateOperationParameters(
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options);

TNode SerializeParamsForGetJob(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobOptions& options);

TNode SerializeParamsForListJobs(
    const TOperationId& operationId,
    const TListJobsOptions& options);

TNode SerializeParametersForInsertRows(
    const TYPath& path,
    const TInsertRowsOptions& options);

TNode SerializeParametersForDeleteRows(
    const TYPath& path,
    const TDeleteRowsOptions& options);

TNode SerializeParametersForTrimRows(
    const TYPath& path,
    const TTrimRowsOptions& options);

TNode SerializeParamsForParseYPath(
    const TRichYPath& path);

TNode SerializeParamsForEnableTableReplica(
    const TReplicaId& replicaId);

TNode SerializeParamsForDisableTableReplica(
    const TReplicaId& replicaId);

TNode SerializeParamsForAlterTableReplica(
    const TReplicaId& replicaId,
    const TAlterTableReplicaOptions& options);

TNode SerializeParamsForFreezeTable(
    const TYPath& path,
    const TFreezeTableOptions& options);

TNode SerializeParamsForUnfreezeTable(
    const TYPath& path,
    const TUnfreezeTableOptions& options);

TNode SerializeParamsForAlterTable(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TAlterTableOptions& options);

TNode SerializeParamsForGetTableColumnarStatistics(
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& paths);

TNode SerializeParamsForGetFileFromCache(
    const TString& md5Signature,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions&);

TNode SerializeParamsForPutFileToCache(
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions&);

TNode SerializeParamsForCheckPermission(
    const TString& user,
    EPermission permission,
    const TYPath& path,
    const TCheckPermissionOptions& options);

////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail::NRawClient
