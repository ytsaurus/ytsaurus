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
    NRpc::IChannelPtr masterChannel,
    IInvokerPtr controlInvoker,
    TOperationPtr operation,
    const TTransactionId& transactionId,
    int initialCount,
    double multiplier)
    : MasterChannel(masterChannel)
    , ControlInvoker(controlInvoker)
    , Operation(operation)
    , TransactionId(transactionId)
    , InitialCount(initialCount)
    , Multiplier(multiplier)
    , Logger(OperationLogger)
    , RequestInProgress(false)
    , LastSuccessCount(-1)
{
    Logger.AddTag(Sprintf("OperationId: %s", ~operation->GetOperationId().ToString()));

    YCHECK(AllocateMore());
}

int TChunkListPool::GetSize() const
{
    return static_cast<int>(Ids.size());
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

bool TChunkListPool::AllocateMore()
{
    int count =
        LastSuccessCount < 0
        ? InitialCount
        : static_cast<int>(LastSuccessCount * Multiplier);

    if (RequestInProgress) {
        LOG_DEBUG("Cannot allocate more chunk lists, another request is in progress");
        return false;
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
    return true;
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
