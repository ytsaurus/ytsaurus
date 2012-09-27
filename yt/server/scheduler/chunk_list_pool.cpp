#include "stdafx.h"
#include "chunk_list_pool.h"
#include "private.h"

#include <server/chunk_server/chunk_list.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>

namespace NYT {
namespace NScheduler {

using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TChunkListPool::TChunkListPool(
    TSchedulerConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    IInvokerPtr controlInvoker,
    TOperationPtr operation,
    const TTransactionId& transactionId)
    : Config(config)
    , MasterChannel(masterChannel)
    , ControlInvoker(controlInvoker)
    , Operation(operation)
    , TransactionId(transactionId)
    , Logger(OperationLogger)
    , RequestInProgress(false)
    , LastSuccessCount(-1)
{
    YCHECK(config);
    YCHECK(masterChannel);
    YCHECK(controlInvoker);
    YCHECK(operation);

    Logger.AddTag(Sprintf("OperationId: %s", ~operation->GetOperationId().ToString()));

    AllocateMore();
}

bool TChunkListPool::HasEnough(int requestedCount)
{
    int currentSize = static_cast<int>(Ids.size());
    if (currentSize >= requestedCount + Config->ChunkListWatermarkCount) {
        // Enough chunk lists. Above the watermark even after extraction.
        return true;
    } else {
        // Additional chunk lists are definitely needed but still could be a success.
        AllocateMore();
        return currentSize >= requestedCount;
    }
}

TChunkListId TChunkListPool::Extract()
{
    YASSERT(!Ids.empty());
    auto id = Ids.back();
    Ids.pop_back();

    LOG_DEBUG("Extracted chunk list %s from the pool, %d remaining",
        ~id.ToString(),
        static_cast<int>(Ids.size()));

    return id;
}

void TChunkListPool::AllocateMore()
{
    int count =
        LastSuccessCount < 0
        ? Config->ChunkListPreallocationCount
        : static_cast<int>(LastSuccessCount * Config->ChunkListAllocationMultiplier);
    
    count = std::min(count, Config->MaxChunkListAllocationCount);

    if (RequestInProgress) {
        LOG_DEBUG("Cannot allocate more chunk lists, another request is in progress");
        return;
    }

    LOG_INFO("Allocating %d chunk lists for pool", count);

    TObjectServiceProxy objectProxy(MasterChannel);
    auto batchReq = objectProxy.ExecuteBatch();

    for (int index = 0; index < count; ++index) {
        auto req = TTransactionYPathProxy::CreateObject(FromObjectId(TransactionId));
        req->set_type(EObjectType::ChunkList);
        batchReq->AddRequest(req);
    }

    batchReq->Invoke().Subscribe(
        BIND(&TChunkListPool::OnChunkListsCreated, MakeWeak(this), count)
            .Via(ControlInvoker));

    RequestInProgress = true;
}

void TChunkListPool::OnChunkListsCreated(
    int count,
    TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    YASSERT(RequestInProgress);
    RequestInProgress = false;

    if (!batchRsp->IsOK()) {
        LOG_ERROR("Error allocating chunk lists\n%s", ~ToString(batchRsp->GetError()));
        // TODO(babenko): backoff time?
        return;
    }

    LOG_INFO("Chunk lists allocated");

    FOREACH (auto rsp, batchRsp->GetResponses<TTransactionYPathProxy::TRspCreateObject>()) {
        Ids.push_back(TChunkListId::FromProto(rsp->object_id()));
    }

    LastSuccessCount = count;
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NScheduler
} // namespace NYT
