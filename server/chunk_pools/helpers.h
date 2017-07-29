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

////////////////////////////////////////////////////////////////////////////////

TChunkStripeListPtr ApplyChunkMappingToStripe(
    const TChunkStripeListPtr& stripeList,
    const yhash<NChunkClient::TInputChunkPtr, NChunkClient::TInputChunkPtr>& inputChunkMapping);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT