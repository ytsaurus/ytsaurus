#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TChunkListPool
    : public TRefCounted
{
public:
    TChunkListPool(
        TControllerAgentConfigPtr config,
        NApi::NNative::IClientPtr clientPtr,
        IInvokerPoolPtr controllerInvokerPool,
        TOperationId operationId,
        NTransactionClient::TTransactionId transactionId);

    bool HasEnough(NObjectClient::TCellTag cellTag, int requestedCount);
    NChunkClient::TChunkListId Extract(NObjectClient::TCellTag cellTag);

    void Reinstall(NChunkClient::TChunkListId id);

private:
    const TControllerAgentConfigPtr Config_;
    const NApi::NNative::IClientPtr Client_;
    const IInvokerPoolPtr ControllerInvokerPool_;
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
        const NChunkClient::TChunkServiceProxy::TErrorOrRspCreateChunkListsPtr& rspOrError);
};

DEFINE_REFCOUNTED_TYPE(TChunkListPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
