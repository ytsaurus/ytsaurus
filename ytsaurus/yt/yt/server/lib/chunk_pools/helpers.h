#pragma once

#include "private.h"
#include "chunk_pool.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <random>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

//! Recalculate stripe list statistics like TotalChunkCount, TotalDataWeight, etc.
//! If third and/or fourth args are present, they are taken instead of
//! corresponding values from chunk stripe statistics.
void AccountStripeInList(
    const TChunkStripePtr& stripe,
    const TChunkStripeListPtr& list,
    std::optional<i64> stripeDataWeight = std::nullopt,
    std::optional<i64> stripeRowCount = std::nullopt,
    NNodeTrackerClient::TNodeId nodeId = NNodeTrackerClient::InvalidNodeId);

//! Add chunk stripe to chunk stripe list and recalculate stripe list statistics like
//! TotalChunkCount, TotalDataWeight, etc.
//! If third and/or fourth args are present, they are taken instead of
//! corresponding values from chunk stripe statistics.
void AddStripeToList(
    TChunkStripePtr stripe,
    const TChunkStripeListPtr& list,
    std::optional<i64> stripeDataWeight = std::nullopt,
    std::optional<i64> stripeRowCount = std::nullopt,
    NNodeTrackerClient::TNodeId nodeId = NNodeTrackerClient::InvalidNodeId);

std::vector<NChunkClient::TInputChunkPtr> GetStripeListChunks(const TChunkStripeListPtr& stripeList);

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): move this class to unordered_pool.cpp and remove unused methods.
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

    void Persist(const TPersistenceContext& context);

private:
    TChunkStripePtr Stripe_;
    int SuspendedStripeCount_ = 0;
    NTableClient::TChunkStripeStatistics Statistics_;
};

////////////////////////////////////////////////////////////////////////////////

//! A helper function for chunk pool logger validation.
//! Yes, we are that serious when it comes to logging.
void ValidateLogger(const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT;::NChunkPools

