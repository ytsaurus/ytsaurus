#include "chunk_list_pool.h"
#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/concurrency/fair_share_invoker_pool.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NControllerAgent {

using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NApi;
using namespace NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TChunkListPool::TChunkListPool(
    TControllerAgentConfigPtr config,
    NNative::IClientPtr client,
    IInvokerPoolPtr controllerInvokerPool,
    TOperationId operationId,
    TTransactionId transactionId,
    EChunkListKind kind)
    : Config_(config)
    , Client_(client)
    , ControllerInvokerPool_(controllerInvokerPool)
    , OperationId_(operationId)
    , TransactionId_(transactionId)
    , Kind_(kind)
    , Logger(ControllerLogger().WithTag("OperationId", operationId))
{
    YT_VERIFY(Config_);
    YT_VERIFY(Client_);
    YT_VERIFY(ControllerInvokerPool_);
}

bool TChunkListPool::HasEnough(TCellTag cellTag, int requestedCount)
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(ControllerInvokerPool_);

    auto& data = CellMap_[cellTag];
    int currentSize = std::ssize(data.Ids);
    if (currentSize >= requestedCount + Config_->ChunkListWatermarkCount) {
        // Enough chunk lists. Above the watermark even after extraction.
        return true;
    } else {
        // Additional chunk lists are definitely needed but still could be a success.
        AllocateMore(cellTag);
        return currentSize >= requestedCount;
    }
}

TChunkListId TChunkListPool::Extract(TCellTag cellTag)
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(ControllerInvokerPool_);

    auto& data = CellMap_[cellTag];

    YT_VERIFY(!data.Ids.empty());
    auto id = data.Ids.back();
    data.Ids.pop_back();

    YT_TLOG_DEBUG("Chunk list extracted from pool")
        .With("ChunkListId", id)
        .With("CellTag", cellTag)
        .With("RemainingCount", data.Ids.size());

    return id;
}

void TChunkListPool::Reinstall(TChunkListId id)
{
    auto cellTag = CellTagFromId(id);
    auto& data = CellMap_[cellTag];
    data.Ids.push_back(id);
    YT_TLOG_DEBUG("Reinstalled chunk list into the pool")
        .With("ChunkListId", id)
        .With("CellTag", cellTag)
        .With("RemainingCount", data.Ids.size());
}

void TChunkListPool::AllocateMore(TCellTag cellTag)
{
    auto& data = CellMap_[cellTag];

    int count = data.LastSuccessCount < 0
        ? Config_->ChunkListPreallocationCount
        : static_cast<int>(data.LastSuccessCount * Config_->ChunkListAllocationMultiplier);

    count = std::min(count, Config_->MaxChunkListAllocationCount);

    if (data.RequestInProgress) {
        YT_TLOG_DEBUG("Cannot allocate more chunk lists for pool, another request is in progress")
            .With("CellTag", cellTag);
        return;
    }

    YT_TLOG_DEBUG("Allocating more chunk lists for pool")
        .With("CellTag", cellTag)
        .With("Count", count);

    auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
    TChunkServiceProxy proxy(channel);

    auto req = proxy.CreateChunkLists();
    GenerateMutationId(req);

    ToProto(req->mutable_transaction_id(), TransactionId_);
    req->set_count(count);
    req->set_kind(ToProto(Kind_));
    // COMPAT(babenko): a pre-"kind" master honors only this flag.
    if (Kind_ == EChunkListKind::Hunk) {
        req->set_is_hunk_chunk_list(true);
    }

    data.RequestInProgress = true;

    req->Invoke().Subscribe(
        BIND(&TChunkListPool::OnChunkListsCreated, MakeWeak(this), cellTag)
            .Via(ControllerInvokerPool_->GetInvoker(EOperationControllerQueue::Default)));
}

void TChunkListPool::OnChunkListsCreated(
    TCellTag cellTag,
    const TChunkServiceProxy::TErrorOrRspCreateChunkListsPtr& rspOrError)
{
    auto& data = CellMap_[cellTag];

    YT_VERIFY(data.RequestInProgress);
    data.RequestInProgress = false;

    if (!rspOrError.IsOK()) {
        YT_TLOG_ERROR("Error allocating chunk lists for pool")
            .With("CellTag", cellTag)
            .With(rspOrError);
        return;
    }

    const auto& rsp = rspOrError.Value();
    auto ids = FromProto<std::vector<TChunkListId>>(rsp->chunk_list_ids());
    data.Ids.insert(data.Ids.end(), ids.begin(), ids.end());
    data.LastSuccessCount = ids.size();

    YT_TLOG_DEBUG("Allocated more chunk lists for pool")
        .With("CellTag", cellTag)
        .With("Count", data.LastSuccessCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
