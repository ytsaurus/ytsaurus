#pragma once

#include "public.h"

#include <yt/server/node/cell_node/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateSupervisorService(NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent

