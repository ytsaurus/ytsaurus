#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateSupervisorService(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent

