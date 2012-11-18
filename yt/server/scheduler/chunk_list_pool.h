#pragma once

#include "public.h"

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TChunkListPool
    : public TRefCounted
{
public:
    TChunkListPool(
        TSchedulerConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        IInvokerPtr controlInvoker,
        TOperationPtr operation);

    bool HasEnough(int requestedCount);
    NChunkClient::TChunkListId Extract();

private:
    TSchedulerConfigPtr Config;
    NRpc::IChannelPtr MasterChannel;
    IInvokerPtr ControlInvoker;
    TOperationPtr Operation;

    NLog::TTaggedLogger Logger;
    bool RequestInProgress;
    int LastSuccessCount;
    std::vector<NChunkClient::TChunkListId> Ids;

    void AllocateMore();

    void OnChunkListsCreated(
        int count,
        NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NScheduler
} // namespace NYT
