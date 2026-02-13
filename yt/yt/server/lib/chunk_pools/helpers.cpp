#include "helpers.h"

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

std::vector<TInputChunkPtr> GetStripeListChunks(const TChunkStripeListPtr& stripeList)
{
    std::vector<TInputChunkPtr> chunks;
    for (const auto& stripe : stripeList->Stripes()) {
        for (const auto& dataSlice : stripe->DataSlices()) {
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

void TSuspendableStripe::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Stripe_);
    PHOENIX_REGISTER_FIELD(2, Teleport_);
    PHOENIX_REGISTER_FIELD(3, SuspendedStripeCount_);
    PHOENIX_REGISTER_FIELD(4, Statistics_);
}

PHOENIX_DEFINE_TYPE(TSuspendableStripe);

////////////////////////////////////////////////////////////////////////////////

void ValidateLogger(const TLogger& logger)
{
    YT_VERIFY(logger);
    const auto& tag = logger.GetTag();
    YT_VERIFY(tag.find("Name:") != TString::npos);
    // OperationId for YT controllers, QueryId for CHYT.
    YT_VERIFY(tag.find("OperationId:") != TString::npos || tag.find("QueryId:") != TString::npos);
}

////////////////////////////////////////////////////////////////////////////////

TChunkStripeListPtr MergeStripeLists(const std::vector<TChunkStripeListPtr>& stripeLists)
{
    // Track seen chunks by their IDs to detect duplicates.
    THashSet<TChunkId> seenChunkIds;

    std::vector<TInputChunkPtr> chunks;
    THashSet<int> partitionTags;

    auto result = New<TChunkStripeList>();

    i64 dataWeight = 0;
    i64 rowCount = 0;

    bool hasPartitionTags = stripeLists.empty() ? false : stripeLists.front()->GetFilteringPartitionTags().has_value();

    // Merge stripes from all lists.
    for (const auto& stripeList : stripeLists) {
        THashSet<TChunkId> chunkIdsInStripeList;

        YT_VERIFY(stripeList->GetFilteringPartitionTags().has_value() == hasPartitionTags);
        YT_VERIFY(!stripeList->GetOutputChunkPoolIndex().has_value());

        for (const auto& stripe : stripeList->Stripes()) {
            if (!hasPartitionTags) {
                // This path is currently used only for tests.
                result->AddStripe(stripe);
                continue;
            }

            // Verify that there are no boundary keys, as merging with boundary keys
            // is not possible with current data structures and API.
            YT_VERIFY(!stripe->GetBoundaryKeys());
            YT_VERIFY(!stripe->IsForeign());
            YT_VERIFY(stripe->GetChunkListId() == NullChunkListId);
            YT_VERIFY(!stripe->GetInputChunkPoolIndex().has_value());
            YT_VERIFY(std::ssize(stripe->DataSlices()) == 1);

            const auto& dataSlice = stripe->DataSlices()[0];
            YT_VERIFY(!dataSlice->HasLimits());
            YT_VERIFY(!dataSlice->ReadRangeIndex.has_value());
            YT_VERIFY(!dataSlice->VirtualRowIndex.has_value());

            auto chunkId = dataSlice->GetSingleUnversionedChunk()->GetChunkId();

            InsertOrCrash(chunkIdsInStripeList, chunkId);

            if (seenChunkIds.insert(chunkId).second) {
                result->AddStripe(stripe);
            }
        }

        if (chunkIdsInStripeList.empty()) {
            continue;
        }

        result->SetApproximate(result->IsApproximate() || stripeList->IsApproximate());

        if (!hasPartitionTags) {
            continue;
        }

        auto statistics = stripeList->GetAggregateStatistics();
        dataWeight += statistics.DataWeight;
        rowCount += statistics.RowCount;

        auto& currentPartitionTags = *stripeList->GetFilteringPartitionTags();
        partitionTags.insert(currentPartitionTags.begin(), currentPartitionTags.end());
    }

    if (!partitionTags.empty()) {
        result->SetFilteringPartitionTags(TPartitionTags(partitionTags.begin(), partitionTags.end()), dataWeight, rowCount);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
