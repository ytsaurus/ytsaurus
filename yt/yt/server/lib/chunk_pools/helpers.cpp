#include "helpers.h"

#include "chunk_pool.h"

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

namespace NYT::NChunkPools {

using namespace NNodeTrackerClient;
using namespace NControllerAgent;
using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void AccountStripeInList(
    const TChunkStripePtr& stripe,
    const TChunkStripeListPtr& list,
    std::optional<i64> stripeDataWeight,
    std::optional<i64> stripeRowCount,
    NNodeTrackerClient::TNodeId nodeId)
{
    auto statistics = stripe->GetStatistics();
    list->TotalDataWeight += stripeDataWeight.value_or(statistics.DataWeight);
    list->TotalRowCount += stripeRowCount.value_or(statistics.RowCount);
    list->TotalChunkCount += statistics.ChunkCount;

    if (nodeId == InvalidNodeId) {
        return;
    }

    for (const auto& dataSlice : list->Stripes.back()->DataSlices) {
        for (const auto& chunkSlice : dataSlice->ChunkSlices) {
            bool isLocal = false;
            for (auto replica : chunkSlice->GetInputChunk()->GetReplicaList()) {
                if (replica.GetNodeId() == nodeId) {
                    i64 locality = chunkSlice->GetLocality(replica.GetReplicaIndex());
                    if (locality > 0) {
                        list->LocalDataWeight += locality;
                        isLocal = true;
                    }
                }
            }

            if (isLocal) {
                ++list->LocalChunkCount;
            }
        }
    }
}

void AddStripeToList(
    TChunkStripePtr stripe,
    const TChunkStripeListPtr& list,
    std::optional<i64> stripeDataWeight,
    std::optional<i64> stripeRowCount,
    TNodeId nodeId)
{
    list->Stripes.emplace_back(std::move(stripe));
    AccountStripeInList(
        list->Stripes.back(),
        list,
        stripeDataWeight,
        stripeRowCount,
        nodeId);
}

std::vector<TInputChunkPtr> GetStripeListChunks(const TChunkStripeListPtr& stripeList)
{
    std::vector<TInputChunkPtr> chunks;
    for (const auto& stripe : stripeList->Stripes) {
        for (const auto& dataSlice : stripe->DataSlices) {
            chunks.emplace_back(dataSlice->GetSingleUnversionedChunk());
        }
    }
    return chunks;
}

////////////////////////////////////////////////////////////////////////////////

TSuspendableStripe::TSuspendableStripe(TChunkStripePtr stripe)
    : Stripe_(std::move(stripe))
    , Statistics_(Stripe_->GetStatistics())
{ }

const TChunkStripePtr& TSuspendableStripe::GetStripe() const
{
    return Stripe_;
}

const TChunkStripeStatistics& TSuspendableStripe::GetStatistics() const
{
    return Statistics_;
}

bool TSuspendableStripe::Suspend()
{
    return SuspendedStripeCount_++ == 0;
}

bool TSuspendableStripe::IsSuspended() const
{
    return SuspendedStripeCount_ > 0;
}

bool TSuspendableStripe::Resume()
{
    YT_VERIFY(SuspendedStripeCount_ > 0);

    return --SuspendedStripeCount_ == 0;
}

void TSuspendableStripe::Reset(TChunkStripePtr stripe)
{
    YT_VERIFY(stripe);

    Stripe_ = stripe;
}

void TSuspendableStripe::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Stripe_);
    Persist(context, Teleport_);
    Persist(context, SuspendedStripeCount_);
    Persist(context, Statistics_);
}

////////////////////////////////////////////////////////////////////////////////

void ValidateLogger(const NLogging::TLogger& logger)
{
    YT_VERIFY(logger);
    const auto& tag = logger.GetTag();
    YT_VERIFY(tag.find("Name:") != TString::npos);
    // OperationId for YT controllers, QueryId for CHYT.
    YT_VERIFY(tag.find("OperationId:") != TString::npos || tag.find("QueryId:") != TString::npos);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
