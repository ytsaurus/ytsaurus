#include "helpers.h"

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

} // namespace NYT::NChunkPools
