#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TChunkListPool
    : public TRefCounted
{
public:
    TChunkListPool(
        TControllerAgentConfigPtr config,
        NApi::INativeClientPtr clientPtr,
        IInvokerPtr controlInvoker,
        const TOperationId& operationId,
        const NTransactionClient::TTransactionId& transactionId);

    bool HasEnough(NObjectClient::TCellTag cellTag, int requestedCount);
    NChunkClient::TChunkListId Extract(NObjectClient::TCellTag cellTag);

    void Reinstall(const NChunkClient::TChunkListId& id);

private:
    const TControllerAgentConfigPtr Config_;
    const NApi::INativeClientPtr Client_;
    const IInvokerPtr ControllerInvoker_;
    const TOperationId OperationId_;
    const NTransactionClient::TTransactionId TransactionId_;

    const NLogging::TLogger Logger;

    struct TCellData
    {
        bool RequestInProgress = false;
        int LastSuccessCount = -1;
        std::vector<NChunkClient::TChunkListId> Ids;
    };

    THashMap<NObjectClient::TCellTag, TCellData> CellMap_;

    void AllocateMore(NObjectClient::TCellTag cellTag);

    void OnChunkListsCreated(
        NObjectClient::TCellTag cellTag,
        const NChunkClient::TChunkServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError);
};

DEFINE_REFCOUNTED_TYPE(TChunkListPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
