#pragma once

#include <mapreduce/yt/interface/fwd.h>
#include <mapreduce/yt/interface/client_method_options.h>

namespace NYT {
namespace NDetail {

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

TNode SerializeParamsForGetOperation(
    const TOperationId& operationId,
    const TGetOperationOptions& options);

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

TNode SerializeParamsForParseYPath(const TRichYPath& path);

TNode SerializeParamsForAlterTableReplica(const TReplicaId& replicaId, const TAlterTableReplicaOptions& options);

TNode SerializeParamsForAlterTable(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TAlterTableOptions& options);

////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
