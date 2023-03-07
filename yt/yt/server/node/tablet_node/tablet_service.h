#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateTabletService(
    TTabletSlotPtr slot,
    NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
