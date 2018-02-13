#pragma once

#include "private.h"
#include "chunk_pool.h"

#include <yt/ytlib/chunk_client/input_data_slice.h>

#include <yt/ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

void AddStripeToList(
    const TChunkStripePtr& stripe,
    i64 stripeDataWeight,
    i64 stripeRowCount,
    const TChunkStripeListPtr& list,
    NNodeTrackerClient::TNodeId nodeId = NNodeTrackerClient::InvalidNodeId);

std::vector<NChunkClient::TChunkId> GetStripeListChunkIds(const TChunkStripeListPtr& stripeList);

////////////////////////////////////////////////////////////////////////////////

class TSuspendableStripe
{
public:
    DEFINE_BYVAL_RW_PROPERTY(IChunkPoolOutput::TCookie, ExtractedCookie);
    DEFINE_BYVAL_RW_PROPERTY(bool, Teleport, false);

public:
    TSuspendableStripe();
    explicit TSuspendableStripe(TChunkStripePtr stripe);

    const TChunkStripePtr& GetStripe() const;
    const TChunkStripeStatistics& GetStatistics() const;
    void Suspend();
    bool IsSuspended() const;
    void Resume();
    void Resume(TChunkStripePtr stripe);
    void Reset(TChunkStripePtr stripe);

    void Persist(const TPersistenceContext& context);

private:
    TChunkStripePtr Stripe_;
    bool Suspended_ = false;
    TChunkStripeStatistics Statistics_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
