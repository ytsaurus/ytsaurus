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
        NRpc::IChannelPtr masterChannel,
        IInvokerPtr controlInvoker,
        TOperationPtr operation,
        const NTransactionClient::TTransactionId& transactionId,
        int initialCount,
        double multiplier);

    int GetSize() const;

    NChunkClient::TChunkListId Extract();

    bool AllocateMore();

private:
    NRpc::IChannelPtr MasterChannel;
    IInvokerPtr ControlInvoker;
    TOperationPtr Operation;
    NTransactionClient::TTransactionId TransactionId;
    int InitialCount;
    double Multiplier;

    NLog::TTaggedLogger Logger;
    bool RequestInProgress;
    int LastSuccessCount;
    std::vector<NChunkClient::TChunkListId> Ids;

    void OnChunkListsCreated(
        int count,
        NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NScheduler
} // namespace NYT
