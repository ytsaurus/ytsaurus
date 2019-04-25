#pragma once

#include "fwd.h"

#include "client_method_options.h"

#include <library/threading/future/future.h>
#include <util/generic/ptr.h>

namespace NYT {

////////////////////////////////////////////////////////////////////

class IBatchRequestBase
    : public TThrRefBase
{
public:
    virtual ~IBatchRequestBase() = default;

    virtual NThreading::TFuture<TNodeId> Create(
        const TYPath& path,
        ENodeType type,
        const TCreateOptions& options = TCreateOptions()) = 0;

    virtual NThreading::TFuture<void> Remove(
        const TYPath& path,
        const TRemoveOptions& options = TRemoveOptions()) = 0;

    virtual NThreading::TFuture<bool> Exists(const TYPath& path) = 0;

    virtual NThreading::TFuture<TNode> Get(
        const TYPath& path,
        const TGetOptions& options = TGetOptions()) = 0;

    virtual NThreading::TFuture<void> Set(
        const TYPath& path,
        const TNode& node,
        const TSetOptions& options = TSetOptions()) = 0;

    virtual NThreading::TFuture<TNode::TListType> List(
        const TYPath& path,
        const TListOptions& options = TListOptions()) = 0;

    virtual NThreading::TFuture<TNodeId> Copy(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = TCopyOptions()) = 0;

    virtual NThreading::TFuture<TNodeId> Move(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = TMoveOptions()) = 0;

    virtual NThreading::TFuture<TNodeId> Link(
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options = TLinkOptions()) = 0;

    virtual NThreading::TFuture<ILockPtr> Lock(
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options = TLockOptions()) = 0;

    virtual NThreading::TFuture<void> Unlock(
        const TYPath& path,
        const TUnlockOptions& options = TUnlockOptions()) = 0;

    virtual NThreading::TFuture<void> AbortOperation(const TOperationId& operationId) = 0;

    virtual NThreading::TFuture<void> CompleteOperation(const TOperationId& operationId) = 0;

    virtual NThreading::TFuture<void> UpdateOperationParameters(
        const TOperationId& operationId,
        const TUpdateOperationParametersOptions& options) = 0;

    virtual NThreading::TFuture<TRichYPath> CanonizeYPath(const TRichYPath& path) = 0;

    virtual NThreading::TFuture<TVector<TTableColumnarStatistics>> GetTableColumnarStatistics(const TVector<TRichYPath>& paths) = 0;
};

class IBatchRequest
    : public IBatchRequestBase
{
public:
    //
    // Using WithTransaction user can temporary override default transaction.
    // Example of usage:
    //   TBatchRequest batchRequest;
    //   auto noTxResult = batchRequest.Get("//some/path");
    //   auto txResult = batchRequest.WithTransaction(tx).Get("//some/path");
    virtual IBatchRequestBase& WithTransaction(const TTransactionId& transactionId) = 0;
    IBatchRequestBase& WithTransaction(const ITransactionPtr& transaction);

    //
    // Executes all subrequests of batch request.
    // It is undefined in which order these requests are executed.
    //
    // Single TBatchRequest instance may be executed only once
    // and cannot be modified (filled with additional requests) after execution.
    // Exception is thrown on attempt to modify executed batch request
    // or execute it again.
    virtual void ExecuteBatch(const TExecuteBatchOptions& options = TExecuteBatchOptions()) = 0;
};

////////////////////////////////////////////////////////////////////

} // namespace NYT
