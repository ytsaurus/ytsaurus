#include "stdafx.h"
#include "chunk_list_pool.h"
#include "private.h"

#include <ytlib/transaction_server/transaction_ypath_proxy.h>
#include <ytlib/chunk_server/chunk_list.h>

namespace NYT {
namespace NScheduler {

using namespace NCypress;
using namespace NTransactionServer;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

TChunkListPool::TChunkListPool(
    NRpc::IChannelPtr masterChannel,
    IInvoker::TPtr controlInvoker,
    TOperationPtr operation,
    const TTransactionId& transactionId)
    : MasterChannel(masterChannel)
    , ControlInvoker(controlInvoker)
    , Operation(operation)
    , TransactionId(transactionId)
    , Logger(OperationsLogger)
    , RequestInProgress(false)
{
    Logger.AddTag(Sprintf("OperationId: %s", ~operation->GetOperationId().ToString()));
}

int TChunkListPool::GetSize() const
{
    return static_cast<int>(Ids.size());
}

NYT::NChunkServer::TChunkListId TChunkListPool::Extract()
{
    YASSERT(!Ids.empty());
    auto id = Ids.back();
    Ids.pop_back();

    LOG_DEBUG("Extracted chunk list %s from the pool, %d remaining",
        ~id.ToString(),
        static_cast<int>(Ids.size()));

    return id;
}

void TChunkListPool::Allocate(int count)
{
    if (RequestInProgress) {
        LOG_DEBUG("Cannot allocate more chunk lists, another request is in progress");
        return;
    }

    LOG_INFO("Allocating %d chunk lists for pool", count);

    TCypressServiceProxy cypressProxy(MasterChannel);
    auto batchReq = cypressProxy.ExecuteBatch();

    for (int index = 0; index < count; ++index) {
        auto req = TTransactionYPathProxy::CreateObject(FromObjectId(TransactionId));
        req->set_type(EObjectType::ChunkList);
        batchReq->AddRequest(req);
    }

    batchReq->Invoke().Subscribe(
        BIND(&TChunkListPool::OnChunkListsCreated, MakeWeak(this))
        .Via(ControlInvoker));

    RequestInProgress = true;
}

void TChunkListPool::OnChunkListsCreated(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
{
    YASSERT(RequestInProgress);
    RequestInProgress = false;

    if (!batchRsp->IsOK()) {
        LOG_ERROR("Error allocating chunk lists\n%s", ~batchRsp->GetError().ToString());
        // TODO(babenko): backoff time?
        return;
    }

    LOG_INFO("Chunk lists allocated");

    YASSERT(Ids.empty());

    FOREACH (auto rsp, batchRsp->GetResponses<TTransactionYPathProxy::TRspCreateObject>()) {
        Ids.push_back(TChunkListId::FromProto(rsp->object_id()));
    }
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NScheduler
} // namespace NYT
