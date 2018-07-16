#include "helpers.h"

#include "chunk_pool.h"

namespace NYT {
namespace NChunkPools {

using namespace NNodeTrackerClient;
using namespace NControllerAgent;
using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void AddStripeToList(
    const TChunkStripePtr& stripe,
    i64 stripeDataWeight,
    i64 stripeRowCount,
    const TChunkStripeListPtr& list,
    TNodeId nodeId)
{
    list->Stripes.push_back(stripe);
    list->TotalDataWeight += stripeDataWeight;
    list->TotalRowCount += stripeRowCount;
    list->TotalChunkCount += stripe->GetChunkCount();
    if (nodeId == InvalidNodeId) {
        return;
    }
    for (const auto& dataSlice : stripe->DataSlices) {
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

std::vector<TInputChunkPtr> GetStripeListChunks(const TChunkStripeListPtr& stripeList)
{
    std::vector<TInputChunkPtr> chunks;
    for (const auto& stripe : stripeList->Stripes) {
        for (const auto& dataSlice : stripe->DataSlices) {
            chunks.emplace_back(dataSlice->GetSingleUnversionedChunkOrThrow());
        }
    }
    return chunks;
}

////////////////////////////////////////////////////////////////////////////////

TSuspendableStripe::TSuspendableStripe()
    : ExtractedCookie_(IChunkPoolOutput::NullCookie)
{ }

TSuspendableStripe::TSuspendableStripe(TChunkStripePtr stripe)
    : ExtractedCookie_(IChunkPoolOutput::NullCookie)
    , Stripe_(std::move(stripe))
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
    YCHECK(SuspendedStripeCount_ > 0);

    return --SuspendedStripeCount_ == 0;
}

void TSuspendableStripe::Reset(TChunkStripePtr stripe)
{
    YCHECK(stripe);

    Stripe_ = stripe;
}

void TSuspendableStripe::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ExtractedCookie_);
    Persist(context, Stripe_);
    Persist(context, Teleport_);
    Persist(context, SuspendedStripeCount_);
    Persist(context, Statistics_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT

