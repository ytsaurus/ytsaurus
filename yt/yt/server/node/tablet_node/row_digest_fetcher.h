#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetcherPtr CreateRowDigestFetcher(
    TTabletCellId cellId,
    IInvokerPtr invoker,
    const NClusterNode::TClusterNodeDynamicConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
