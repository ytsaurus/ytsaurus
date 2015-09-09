#pragma once

#include "public.h"

#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/object_service_proxy.h>

#include <core/logging/log.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/api/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TChunkListPool
    : public TRefCounted
{
public:
    TChunkListPool(
        TSchedulerConfigPtr config,
        NApi::IClientPtr clientPtr,
        IInvokerPtr controlInvoker,
        const TOperationId& operationId,
        const NTransactionClient::TTransactionId& transactionId);

    bool HasEnough(NObjectClient::TCellTag cellTag, int requestedCount);
    NChunkClient::TChunkListId Extract(NObjectClient::TCellTag cellTag);

    void Release(const std::vector<NChunkClient::TChunkListId>& ids);

private:
    const TSchedulerConfigPtr Config_;
    const NApi::IClientPtr Client_;
    const IInvokerPtr ControlInvoker_;
    const TOperationId OperationId_;
    const NTransactionClient::TTransactionId TransactionId_;

    NLogging::TLogger Logger;

    struct TCellData
    {
        bool RequestInProgress = false;
        int LastSuccessCount = -1;
        std::vector<NChunkClient::TChunkListId> Ids;
    };

    yhash_map<NObjectClient::TCellTag, TCellData> CellMap_;


    void AllocateMore(NObjectClient::TCellTag cellTag);

    void OnChunkListsCreated(
        NObjectClient::TCellTag cellTag,
        const NObjectClient::TMasterYPathProxy::TErrorOrRspCreateObjectsPtr& rspOrError);

    void OnChunkListsReleased(
        NObjectClient::TCellTag cellTag,
        const NObjectClient::TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError);
};

DEFINE_REFCOUNTED_TYPE(TChunkListPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
