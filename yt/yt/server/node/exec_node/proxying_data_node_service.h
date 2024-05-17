#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateProxyingDataNodeService(NExecNode::IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
