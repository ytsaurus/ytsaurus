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
        TSchedulerConfigPtr config,
        NApi::INativeClientPtr clientPtr,
        IInvokerPtr controlInvoker,
        const TOperationId& operationId,
        const NTransactionClient::TTransactionId& transactionId);

    bool HasEnough(NObjectClient::TCellTag cellTag, int requestedCount);
    NChunkClient::TChunkListId Extract(NObjectClient::TCellTag cellTag);

    void Release(const std::vector<NChunkClient::TChunkListId>& ids);
    void Reinstall(const NChunkClient::TChunkListId& id);

private:
    const TSchedulerConfigPtr Config_;
    const NApi::INativeClientPtr Client_;
    const IInvokerPtr ControllerInvoker_;
    const TOperationId OperationId_;
    const NTransactionClient::TTransactionId TransactionId_;

    NConcurrency::TPeriodicExecutorPtr ChunkListReleaseExecutor_;

    NLogging::TLogger Logger;

    struct TCellData
    {
        bool RequestInProgress = false;
        int LastSuccessCount = -1;
        std::vector<NChunkClient::TChunkListId> Ids;
    };

    THashMap<NObjectClient::TCellTag, TCellData> CellMap_;

    THashMap<NObjectClient::TCellTag, std::vector<NChunkClient::TChunkListId>> ChunksToRelease_;
    TInstant LastReleaseTime_;

    void AllocateMore(NObjectClient::TCellTag cellTag);

    void OnChunkListsCreated(
        NObjectClient::TCellTag cellTag,
        const NChunkClient::TChunkServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError);

    void OnChunkListsReleased(
        NObjectClient::TCellTag cellTag,
        const NChunkClient::TChunkServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError);
};

DEFINE_REFCOUNTED_TYPE(TChunkListPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
