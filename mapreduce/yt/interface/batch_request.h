#pragma once

#include "fwd.h"

#include "client_method_options.h"

#include <library/threading/future/future.h>
#include <util/generic/ptr.h>

namespace NYT {

namespace NDetail {
    class TBatchRequestImpl;
} // namespace NDetail

////////////////////////////////////////////////////////////////////

struct IBatchRequest {
    virtual ~IBatchRequest()
    {}

    virtual NThreading::TFuture<TLockId> Create(
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
        const TNode& node) = 0;

    virtual NThreading::TFuture<TNode::TList> List(
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

    virtual NThreading::TFuture<TLockId> Lock(
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options = TLockOptions()) = 0;
};

////////////////////////////////////////////////////////////////////

class TBatchRequest
    : public IBatchRequest
{
public:
    TBatchRequest();
    TBatchRequest(const TTransactionId& defaultTransaction);
    TBatchRequest(const ITransactionPtr& defaultTransaction);

    ~TBatchRequest();

    // Using WithTransaction user can temporary override default transaction.
    // Example of usage:
    //   TBatchRequest batchRequest;
    //   auto noTxResult = batchRequest.Get("//some/path");
    //   auto txResult = batchRequest.WithTransaction(tx).Get("//some/path");
    IBatchRequest& WithTransaction(const TTransactionId& transactionId);
    IBatchRequest& WithTransaction(const ITransactionPtr& transaction);

    virtual NThreading::TFuture<TLockId> Create(
        const TYPath& path,
        ENodeType type,
        const TCreateOptions& options = TCreateOptions()) override;

    virtual NThreading::TFuture<void> Remove(
        const TYPath& path,
        const TRemoveOptions& options = TRemoveOptions()) override;

    virtual NThreading::TFuture<bool> Exists(const TYPath& path) override;

    virtual NThreading::TFuture<TNode> Get(
        const TYPath& path,
        const TGetOptions& options = TGetOptions()) override;

    virtual NThreading::TFuture<void> Set(
        const TYPath& path,
        const TNode& node) override;

    virtual NThreading::TFuture<TNode::TList> List(
        const TYPath& path,
        const TListOptions& options = TListOptions()) override;


    virtual NThreading::TFuture<TNodeId> Copy(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = TCopyOptions()) override;

    virtual NThreading::TFuture<TNodeId> Move(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = TMoveOptions()) override;

    virtual NThreading::TFuture<TNodeId> Link(
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options = TLinkOptions()) override;

    virtual NThreading::TFuture<TLockId> Lock(
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options = TLockOptions()) override;

private:
    TBatchRequest(NDetail::TBatchRequestImpl* impl);

private:
    TTransactionId DefaultTransaction_;
    ::TIntrusivePtr<NDetail::TBatchRequestImpl> Impl_;
    THolder<TBatchRequest> TmpWithTransaction_;

private:
    friend class TClient;
};

////////////////////////////////////////////////////////////////////

} // namespace NYT
