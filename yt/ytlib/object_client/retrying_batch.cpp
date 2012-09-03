#include "stdafx.h"
#include "retrying_batch.h"

#include <ytlib/misc/thread_affinity.h>

#include <ytlib/ytree/ypath_client.h>

#include <ytlib/rpc/error.h>

namespace NYT {
namespace NObjectClient {

using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TRetryingBatch::TRetryingBatch(IChannelPtr channel)
    : Channel(channel)
    , Finalizing(false)
    , HasFailed(false)
    , FlushedPromise(Null)
    , FinalizedPromise(NewPromise<void>())
{ }

TRetryingBatchPtr TRetryingBatch::SetTimeout(TNullable<TDuration> timeout)
{
    Timeout = timeout;
    return this;
}

TRetryingBatchPtr TRetryingBatch::Add(TYPathRequestPtr request)
{
    TGuard<TSpinLock> guard(SpinLock);
    
    YCHECK(!Finalizing);
    if (HasFailed)
        return this;

    PendingRequests.push_back(request);
    return this;
}

void TRetryingBatch::Flush()
{
    TGuard<TSpinLock> guard(SpinLock);

    if (HasFailed)
        return;

    if (FlushedPromise.IsNull()) {
        DoFlush();
    }
}

TFuture<void> TRetryingBatch::Finalize()
{
    TGuard<TSpinLock> guard(SpinLock);

    YCHECK(!Finalizing);
    Finalizing = true;

    if (HasFailed) {
        return MakeFuture();
    }

    if (FlushedPromise.IsNull()) {
        if (PendingRequests.empty()) {
            return MakeFuture();
        }
        DoFlush();
    }

    return FinalizedPromise;
}

void TRetryingBatch::DoFlush()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock);

    YCHECK(FlushedPromise.IsNull());

    TObjectServiceProxy proxy(Channel);
    auto batchReq = proxy.ExecuteBatch()->SetTimeout(Timeout);

    std::vector<TYPathRequestPtr> requests;
    while (!PendingRequests.empty()) {
        auto req = PendingRequests.back();
        PendingRequests.pop_back();
        requests.push_back(req);
        batchReq->AddRequest(req);
    }

    FlushedPromise = NewPromise<void>();
    batchReq->Invoke().Subscribe(BIND(
        &TRetryingBatch::OnFlushed,
        MakeStrong(this),
        MoveRV(requests)));
}

void TRetryingBatch::OnFlushed(
    const std::vector<NYTree::TYPathRequestPtr>& requests,
    TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    TGuard<TSpinLock> guard(SpinLock);

    if (!batchRsp->IsOK()) {
        if (!IsRetriableError(batchRsp->GetError())) {
            guard.Release();
            OnFailed(batchRsp->GetError());
        }
        return;
    }

    for (int index = static_cast<int>(requests.size()) - 1; index >= 0; --index) {
        auto req = requests[index];
        auto rsp = batchRsp->GetResponse(index);
        if (!rsp->IsOK()) {
            if (!IsRetriableError(batchRsp->GetError())) {
                guard.Release();
                OnFailed(batchRsp->GetError());
            }
            PendingRequests.push_front(req);
        }
    }

    auto flushedPromise = FlushedPromise;
    FlushedPromise.Reset();

    bool finalized = false;

    if (Finalizing) {
        if (PendingRequests.empty()) {
            finalized = true;
        } else {
            DoFlush();
        }
    }

    guard.Release();

    flushedPromise.Set();
    if (finalized) {
        FinalizedPromise.Set();
    }
}

void TRetryingBatch::OnFailed(const TError& error)
{
    {
        TGuard<TSpinLock> guard(SpinLock);       
        HasFailed = true;
    }
    Failed_.Fire(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT
