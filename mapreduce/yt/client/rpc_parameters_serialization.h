#pragma once

#include <mapreduce/yt/interface/fwd.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////

TString ToString(ELockMode mode);
TString ToString(ENodeType type);

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
    const TGetOptions& getOptions);

TNode SerializeParamsForSet(
    const TTransactionId& transactionId,
    const TYPath& path);

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

TNode SerializeParametersForInsertRows(
    const TYPath& path,
    const TInsertRowsOptions& options);

TNode SerializeParametersForDeleteRows(
    const TYPath& path,
    const TDeleteRowsOptions& options);

////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
