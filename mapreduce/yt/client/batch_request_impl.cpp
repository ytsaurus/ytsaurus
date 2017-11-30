#include "batch_request_impl.h"

#include "lock.h"

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/finally_guard.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/serialize.h>

#include <mapreduce/yt/interface/node.h>

#include <mapreduce/yt/http/retry_request.h>

#include <mapreduce/yt/raw_client/raw_requests.h>
#include <mapreduce/yt/raw_client/raw_batch_request.h>
#include <mapreduce/yt/raw_client/rpc_parameters_serialization.h>

#include <util/generic/guid.h>
#include <util/string/builder.h>

#include <exception>

namespace NYT {
namespace NDetail {

using NThreading::TFuture;
using NThreading::TPromise;
using NThreading::NewPromise;

////////////////////////////////////////////////////////////////////

TBatchRequest::TBatchRequest(const TTransactionId& defaultTransaction, ::TIntrusivePtr<TClient> client)
    : DefaultTransaction_(defaultTransaction)
    , Impl_(MakeIntrusive<TRawBatchRequest>())
    , Client_(client)
{ }

TBatchRequest::TBatchRequest(TRawBatchRequest* impl, ::TIntrusivePtr<TClient> client)
    : Impl_(impl)
    , Client_(std::move(client))
{ }

TBatchRequest::~TBatchRequest() = default;

IBatchRequestBase& TBatchRequest::WithTransaction(const TTransactionId& transactionId)
{
    if (!TmpWithTransaction_) {
        TmpWithTransaction_.Reset(new TBatchRequest(Impl_.Get(), Client_));
    }
    TmpWithTransaction_->DefaultTransaction_ = transactionId;
    return *TmpWithTransaction_;
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

TFuture<TNode::TListType> TBatchRequest::List(const TYPath& path, const TListOptions& options)
{
    return Impl_->List(DefaultTransaction_, path, options);
}

TFuture<bool> TBatchRequest::Exists(const TYPath& path)
{
    return Impl_->Exists(DefaultTransaction_, path);
}

TFuture<ILockPtr> TBatchRequest::Lock(
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    auto convert = [waitable=options.Waitable_, client=Client_] (TFuture<TNodeId> nodeIdFuture) -> ILockPtr {
        const auto& lockId = nodeIdFuture.GetValue();
        if (waitable) {
            return ::MakeIntrusive<TLock>(lockId, client);
        } else {
            return ::MakeIntrusive<TLock>(lockId);
        }
    };
    return Impl_->Lock(DefaultTransaction_, path, mode, options).Apply(convert);
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

TFuture<TRichYPath> TBatchRequest::CanonizeYPath(const TRichYPath& path)
{
    return Impl_->CanonizeYPath(path);
}

void TBatchRequest::ExecuteBatch(const TExecuteBatchOptions& options)
{
    TAttemptLimitedRetryPolicy retryPolicy(TConfig::Get()->RetryCount);
    NYT::NDetail::ExecuteBatch(Client_->GetAuth(), *Impl_, options, retryPolicy);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
