#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateDataNodeService(
    TDataNodeConfigPtr config,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
