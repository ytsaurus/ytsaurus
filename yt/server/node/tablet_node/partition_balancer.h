#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

void StartPartitionBalancer(
    TTabletNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
