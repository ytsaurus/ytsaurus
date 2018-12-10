#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateDataNodeService(
    TDataNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
