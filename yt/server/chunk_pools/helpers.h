#pragma once

#include "private.h"

#include <yt/ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

void AddStripeToList(
    const TChunkStripePtr& stripe,
    i64 stripeDataSize,
    i64 stripeRowCount,
    const TChunkStripeListPtr& list,
    NNodeTrackerClient::TNodeId nodeId = NNodeTrackerClient::InvalidNodeId);

////////////////////////////////////////////////////////////////////////////////

} // namesapce NChunkPools
} // namespace NYT