#pragma once

#include "public.h"

#include <ytlib/chunk_server/public.h>
#include <ytlib/object_server/object_service_proxy.h>
#include <ytlib/logging/tagged_logger.h>

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
        const TTransactionId& transactionId);

    int GetSize() const;

    NChunkServer::TChunkListId Extract();

    bool Allocate(int count);

private:
    NRpc::IChannelPtr MasterChannel;
    IInvokerPtr ControlInvoker;
    TOperationPtr Operation;
    TTransactionId TransactionId;

    NLog::TTaggedLogger Logger;
    bool RequestInProgress;
    std::vector<NChunkServer::TChunkListId> Ids;

    void OnChunkListsCreated(NObjectServer::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NScheduler
} // namespace NYT
