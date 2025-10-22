#pragma once

#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>
#include <yt/yt/ytlib/chunk_pools/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/chunk_stripe_statistics.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

// TODO(apollo1321): Remove this.
//! Recalculate stripe list statistics like TotalChunkCount, TotalDataWeight, etc.
void AccountStripeInList(
    const TChunkStripePtr& stripe,
    const TChunkStripeListPtr& list);

// TODO(apollo1321): Remove this.
//! Add chunk stripe to chunk stripe list and recalculate stripe list statistics like
//! TotalChunkCount, TotalDataWeight, etc.
void AddStripeToList(
    TChunkStripePtr stripe,
    const TChunkStripeListPtr& list);

// TODO(apollo1321): Move to methods of TChunkStripeList.
std::vector<NChunkClient::TInputChunkPtr> GetStripeListChunks(const TChunkStripeListPtr& stripeList);

////////////////////////////////////////////////////////////////////////////////

class TSuspendableStripe
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, Teleport, false);

public:
    //! Used only for persistence.
    TSuspendableStripe() = default;

    explicit TSuspendableStripe(TChunkStripePtr stripe);

    const TChunkStripePtr& GetStripe() const;
    const NTableClient::TChunkStripeStatistics& GetStatistics() const;
    // Increase suspended stripe count by one and return true if 0 -> 1 transition happened.
    bool Suspend();
    // Decrease suspended stripe count by one and return true if 1 -> 0 transition happened.
    bool Resume();
    bool IsSuspended() const;
    void Reset(TChunkStripePtr stripe);

private:
    TChunkStripePtr Stripe_;
    int SuspendedStripeCount_ = 0;
    TPersistentChunkStripeStatistics Statistics_;

    PHOENIX_DECLARE_TYPE(TSuspendableStripe, 0x14cdc54f);
};

////////////////////////////////////////////////////////////////////////////////

//! A helper function for chunk pool logger validation.
//! Yes, we are that serious when it comes to logging.
void ValidateLogger(const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
