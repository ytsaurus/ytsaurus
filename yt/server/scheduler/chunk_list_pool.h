#pragma once

#include "public.h"

#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/object_service_proxy.h>

#include <core/logging/log.h>

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
        const TOperationId& operationId,
        const NTransactionClient::TTransactionId& transactionId);

    bool HasEnough(int requestedCount);
    NChunkClient::TChunkListId Extract();

    void Release(const std::vector<NChunkClient::TChunkListId>& ids);

private:
    TSchedulerConfigPtr Config;
    NRpc::IChannelPtr MasterChannel;
    IInvokerPtr ControlInvoker;
    TOperationId OperationId;
    NTransactionClient::TTransactionId TransactionId;

    NLog::TLogger Logger;
    bool RequestInProgress;
    int LastSuccessCount;
    std::vector<NChunkClient::TChunkListId> Ids;

    void AllocateMore();

    void OnChunkListsCreated(
        NObjectClient::TMasterYPathProxy::TRspCreateObjectsPtr rsp);

    void OnChunkListsReleased(
        NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
