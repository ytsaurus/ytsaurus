#include "batch_request.h"
#include "client.h"

#include <mapreduce/yt/client/batch_request_impl.h>

#include <functional>

namespace NYT {

using NThreading::TPromise;
using NThreading::TFuture;

////////////////////////////////////////////////////////////////////////////////

TBatchRequest::TBatchRequest()
    : Impl_(MakeIntrusive<NDetail::TBatchRequestImpl>())
{ }

TBatchRequest::TBatchRequest(const TTransactionId& defaultTransaction)
    : DefaultTransaction_(defaultTransaction)
    , Impl_(MakeIntrusive<NDetail::TBatchRequestImpl>())
{ }

TBatchRequest::TBatchRequest(const ITransactionPtr& defaultTransaction)
    : DefaultTransaction_(defaultTransaction->GetId())
    , Impl_(MakeIntrusive<NDetail::TBatchRequestImpl>())
{ }

TBatchRequest::TBatchRequest(NDetail::TBatchRequestImpl* impl)
    : Impl_(impl)
{ }

TBatchRequest::~TBatchRequest() = default;

IBatchRequest& TBatchRequest::WithTransaction(const TTransactionId& transactionId)
{
    if (!TmpWithTransaction_) {
        TmpWithTransaction_.Reset(new TBatchRequest(Impl_.Get()));
    }
    TmpWithTransaction_->DefaultTransaction_ = transactionId;
    return *TmpWithTransaction_;
}

IBatchRequest& TBatchRequest::WithTransaction(const ITransactionPtr& transaction)
{
    return WithTransaction(transaction->GetId());
}

TFuture<TNode> TBatchRequest::Get(
    const TYPath& path,
    const TGetOptions& options)
{
    return Impl_->Get(DefaultTransaction_, path, options);
}

TFuture<void> TBatchRequest::Set(const TYPath& path, const TNode& node)
{
    return Impl_->Set(DefaultTransaction_, path, node);
}

TFuture<TNode::TList> TBatchRequest::List(const TYPath& path, const TListOptions& options)
{
    return Impl_->List(DefaultTransaction_, path, options);
}

TFuture<bool> TBatchRequest::Exists(const TYPath& path)
{
    return Impl_->Exists(DefaultTransaction_, path);
}

TFuture<TLockId> TBatchRequest::Lock(
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    return Impl_->Lock(DefaultTransaction_, path, mode, options);
}

TFuture<TLockId> TBatchRequest::Create(
    const TYPath& path,
    ENodeType type,
    const TCreateOptions& options)
{
    return Impl_->Create(DefaultTransaction_, path, type, options);
}

TFuture<void> TBatchRequest::Remove(
    const TYPath& path,
    const TRemoveOptions& options)
{
    return Impl_->Remove(DefaultTransaction_, path, options);
}

TFuture<TNodeId> TBatchRequest::Move(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    return Impl_->Move(DefaultTransaction_, sourcePath, destinationPath, options);
}

TFuture<TNodeId> TBatchRequest::Copy(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    return Impl_->Copy(DefaultTransaction_, sourcePath, destinationPath, options);
}

TFuture<TNodeId> TBatchRequest::Link(
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    return Impl_->Link(DefaultTransaction_, targetPath, linkPath, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
