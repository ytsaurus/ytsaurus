#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

// Fetch chunk view sizes to determine if they need to be compacted due to too narrow bounds.
TCompactionHintFetcherPtr CreateChunkViewSizeFetcher(
    TTabletCellId cellId,
    NClusterNode::TClusterNodeDynamicConfigPtr config,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    IInvokerPtr heavyInvoker,
    NApi::NNative::IClientPtr client,
    NChunkClient::IChunkReplicaCachePtr chunkReplicaCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

