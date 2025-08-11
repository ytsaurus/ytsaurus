#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetcherPtr CreateMinHashDigestFetcher(
    TTabletCellId cellId,
    IInvokerPtr invoker,
    INodeMemoryTrackerPtr memoryTracker,
    const NClusterNode::TClusterNodeDynamicConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
