#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateDataNodeService(
    TDataNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
