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
        IInvoker::TPtr controlInvoker,
        TOperationPtr operation,
        const TTransactionId& transactionId);

    int GetSize() const;

    NChunkServer::TChunkListId Extract();

    void Allocate(int count);

private:
    NRpc::IChannelPtr MasterChannel;
    IInvoker::TPtr ControlInvoker;
    TOperationPtr Operation;
    TTransactionId TransactionId;

    NLog::TTaggedLogger Logger;
    bool RequestInProgress;
    std::vector<NChunkServer::TChunkListId> Ids;

    void OnChunkListsCreated(NObjectServer::TObjectServiceProxy::TRspExecuteBatch::TPtr batchRsp);
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NScheduler
} // namespace NYT
