#pragma once

#include "public.h"
#include "object_service_proxy.h"

#include <ytlib/misc/nullable.h>
#include <ytlib/misc/error.h>

#include <ytlib/actions/signal.h>

#include <ytlib/ytree/public.h>

#include <ytlib/rpc/public.h>

namespace NYT {
namespace NObjectClient {

////////////////////////////////////////////////////////////////////////////////

class TRetryingBatch
    : public TRefCounted
{
public:
    explicit TRetryingBatch(NRpc::IChannelPtr channel);

    TRetryingBatchPtr SetTimeout(TNullable<TDuration> timeout);

    TRetryingBatchPtr Add(NYTree::TYPathRequestPtr request);
    
    void Flush();
    TFuture<void> Finalize();

    DEFINE_SIGNAL(void(const TError&), Failed);

private:
    NRpc::IChannelPtr Channel;

    TNullable<TDuration> Timeout;

    TSpinLock SpinLock;
    TPromise<void> FlushedPromise;
    TPromise<void> FinalizedPromise;
    bool Finalizing;
    bool HasFailed;
    std::deque<NYTree::TYPathRequestPtr> PendingRequests;

    void DoFlush();
    void OnFlushed(
        const std::vector<NYTree::TYPathRequestPtr>& requests,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    void OnFailed(const TError& error);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT
