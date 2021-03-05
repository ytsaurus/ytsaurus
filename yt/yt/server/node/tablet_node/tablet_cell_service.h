#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateTabletCellService(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
