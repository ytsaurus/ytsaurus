#pragma once

#include "public.h"

#include <ytlib/rpc/channel.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/cypress/cypress_service_proxy.h>
#include <ytlib/logging/tagged_logger.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TChunkListPool
    : public TRefCounted
{
public:
    TChunkListPool(
        NRpc::IChannel::TPtr masterChannel,
        IInvoker::TPtr controlInvoker,
        TOperationPtr operation,
        const TTransactionId& transactionId);

    int GetSize() const;

    NChunkServer::TChunkListId Extract();

    void Allocate(int count);

private:
    NRpc::IChannel::TPtr MasterChannel;
    IInvoker::TPtr ControlInvoker;
    TOperationPtr Operation;
    TTransactionId TransactionId;

    NLog::TTaggedLogger Logger;
    bool RequestInProgress;
    std::vector<NChunkServer::TChunkListId> Ids;

    void OnChunkListsCreated(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NScheduler
} // namespace NYT
