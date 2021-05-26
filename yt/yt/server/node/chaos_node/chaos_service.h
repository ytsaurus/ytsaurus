#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateChaosService(
    IChaosSlotPtr slot,
    NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
