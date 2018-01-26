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

std::vector<TChunkId> GetStripeListChunkIds(const TChunkStripeListPtr& stripeList)
{
    std::vector<TChunkId> chunkIds;
    for (const auto& stripe : stripeList->Stripes) {
        for (const auto& dataSlice : stripe->DataSlices) {
            chunkIds.emplace_back(dataSlice->GetSingleUnversionedChunkOrThrow()->ChunkId());
        }
    }
    return chunkIds;
}

////////////////////////////////////////////////////////////////////////////////

TSuspendableStripe::TSuspendableStripe()
    : ExtractedCookie_(IChunkPoolOutput::NullCookie)
{ }

TSuspendableStripe::TSuspendableStripe(TChunkStripePtr stripe)
    : ExtractedCookie_(IChunkPoolOutput::NullCookie)
    , Stripe_(std::move(stripe))
    , OriginalStripe_(Stripe_)
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

void TSuspendableStripe::Suspend()
{
    YCHECK(Stripe_);
    YCHECK(!Suspended_);

    Suspended_ = true;
}

bool TSuspendableStripe::IsSuspended() const
{
    return Suspended_;
}

void TSuspendableStripe::Resume()
{
    YCHECK(Suspended_);

    Suspended_ = false;
}

void TSuspendableStripe::Resume(TChunkStripePtr stripe)
{
    YCHECK(Stripe_);
    YCHECK(Suspended_);

    // NB: do not update statistics on resume to preserve counters.
    Suspended_ = false;
    Stripe_ = stripe;
}

void TSuspendableStripe::ReplaceOriginalStripe()
{
    OriginalStripe_ = Stripe_;
}

void TSuspendableStripe::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ExtractedCookie_);
    Persist(context, Stripe_);
    Persist(context, OriginalStripe_);
    Persist(context, Teleport_);
    Persist(context, Suspended_);
    Persist(context, Statistics_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT

